# Robustness Hardening Design

Date: 2026-08-17
Status: Draft (pending user review)
Type: Design spec (robustness / safety hardening)
Authors: Sisyphus + user (brainstorming session)

## 1. Background

The v1.0.1 risk audit (`docs/risk-assessment.md`) plus a fresh source review
identified seven confirmed robustness defects in the thread-pool and coroutine
subsystems. Each one turns a plausible user mistake — or a pathological task —
into heap corruption, a permanent hang, or silent state drift:

| # | Defect | Severity | Location (verified) |
|---|--------|----------|---------------------|
| 1 | `loom_pool_destroy` without prior `loom_pool_shutdown` frees every worker-shared structure while workers still run → heap corruption | HIGH | `thread_pool.c:1762` / `pool_destroy_internal` `236-277` |
| 2 | A task that calls `pthread_exit`/`pthread_cancel` kills its worker silently: `thread_alive[idx]` stays true, worker count drifts, no metric event fires | HIGH | `worker_entry` `thread_pool.c:293-471` (`fn(data)` at `459/466`) |
| 3 | A cancelled future-backed task is tombstoned without signalling its future → `loom_future_wait` (infinite) hangs forever | HIGH | tombstone path `thread_pool.c:425-436`; `loom_future_wait` `1379-1393` |
| 4 | `loom_future_destroy` before the task completes → worker writes into freed future → UAF | MED | `loom_future_destroy` `1446-1458` |
| 5 | `loom_coro_destroy` has no state/owner check — destroying a RUNNING/SUSPENDED coroutine frees its stack mid-switch → heap corruption (the only lifecycle function without guards; `resume`/`terminate` both check at `coroutine.c:385-474`) | MED | `coroutine.c:476-488` |
| 6 | `group_destroy` blocks forever when tracked tasks never finish, and can self-deadlock when called from one of the pool's own workers | MED | `task_group.c:201-221` |
| 7 | The 60 s blocking waits use `CLOCK_REALTIME` → wall-clock jumps distort timeout behaviour | LOW | `wait_for_space` `thread_pool.c:1069`; `future_wait_timeout` `1421` |

Two related facts shape the fixes:

- `pool->shutdown` (atomic bool) already exists and is set by
  `loom_pool_shutdown`; `shutdown()` returns only after every worker is joined
  (`thread_pool.c:1473-1517`). It is the natural destroy gate — no new state
  field is needed.
- `LOOMWORKS_METRIC_FAILED` already exists in the event enum (`metrics.h:28`,
  "reserved") but is **never fired**. Defect 2 finally gives it a meaning.

## 2. Goals

- G1. **Lifecycle misuse returns an explicit error instead of UB.** Destroying
  a non-shutdown pool, destroying a pending future, or destroying a running /
  suspended coroutine all return `LOOMWORKS_ERR_INVALID` and leave memory
  untouched.
- G2. **Cancellation is observable.** A cancelled future is completed with an
  error: both `loom_future_wait` and `loom_future_wait_timeout` return the new
  `LOOMWORKS_ERR_CANCELLED` instead of hanging / timing out.
- G3. **Worker abnormal exit is detected and observable.** The join loop
  detects `ESRCH`, corrects `thread_alive` accounting, and fires
  `LOOMWORKS_METRIC_FAILED`.
- G4. **Timeout waits are immune to wall-clock jumps.** Bounded-submit and
  future timeout waits run on `CLOCK_MONOTONIC`.
- G5. **Self-wait deadlock is rejected up front.** Group operations that would
  block inside one of the pool's own workers return `LOOMWORKS_ERR_INVALID`.
- G6. **All existing assertion counts stay green** (12541 / 5603 / 78765 ±ε);
  each fix ships with a dedicated regression test.

## 3. Non-Goals

- NG1. No new queue algorithms, scheduling policies, or performance work (the
  existing `perf.yml` 15% queue-depth gate is unchanged).
- NG2. No portability work (R12: GNU `pthread_tryjoin_np` / macOS ucontext gap
  stays open and documented).
- NG3. **No survival of user-task segfaults.** Catching arbitrary task
  SIGSEGV/SIGBUS is unreliable and masks bugs; a crashing task killing the
  process remains the default behaviour (recorded in `design-decisions.md`).
  Defect 2 covers only *process-surviving* worker death (`pthread_exit`,
  `pthread_cancel`, unwinding past `worker_entry`).
- NG4. No worker auto-recovery, `on_crash` callbacks, or fault-injection
  matrices (that is the "Plan C" superset, deliberately excluded).
- NG5. No new public API beyond one result code
  (`LOOMWORKS_ERR_CANCELLED`) and firing the already-defined
  `LOOMWORKS_METRIC_FAILED`.

## 4. Decisions (from brainstorming session, user-approved)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Strict destroy gate**: `loom_pool_destroy` on a non-shutdown pool returns `LOOMWORKS_ERR_INVALID`; no implicit shutdown | Matches Java Executor / `uv_loop_close`(EBUSY) practice; forces explicit lifecycle management; a forgiving auto-shutdown would mask ordering bugs |
| D2 | New result code `LOOMWORKS_ERR_CANCELLED`; cancelled futures complete with it (both wait variants) | Cancellation is a distinct outcome, not a timeout; `future_wait` on a cancelled task must not hang |
| D3 | `loom_future_destroy` allowed only on a completed future (checked under the future lock); otherwise `ERR_INVALID` | Worker writes and destroy-free both happen under the same lock; an in-lock readiness check makes UAF impossible |
| D4 | `loom_coro_destroy` allowed only in states `NEW`/`DONE`/`ERROR`, by the owning thread; otherwise `ERR_INVALID` | Same guards `resume`/`terminate` already enforce (`coroutine.c:385-474`); forces "terminate first, then destroy" |
| D5 | Worker-crash detection = per-worker `clean_exit` flag set on the normal exit paths, checked at every join site, + accounting fix + `LOOMWORKS_METRIC_FAILED`; **no** signal catching | `pthread_exit`-killed workers are still joinable (join returns 0, never `ESRCH`) — only a *flag* distinguishes clean shutdown exits from abnormal death. Avoids the fragility of in-process signal handling |
| D6 | `group_destroy` keeps its blocking contract (documented, same as pool shutdown) but rejects self-wait from a pool worker with `ERR_INVALID` | Removes the guaranteed-deadlock case without changing the legitimate blocking contract |
| D7 | Timeout waits switch to `CLOCK_MONOTONIC`; `future_wait_timeout`'s deadline argument is documented as monotonic-clock | Wall-clock jumps must not stretch/shrink 60 s backpressure or future deadlines; callers computing deadlines from `CLOCK_REALTIME` must switch (test helpers updated) |
| D8 | Future-backing tasks are marked with a new `is_future` flag on `loom_task_t` so the generic tombstone path can signal the future before freeing the wrapper | No fragile `fn == future_task_wrapper` pointer comparison; explicit and testable |

## 5. Design

### 5.1 Pool lifecycle enforcement (Defect 1)

- `loom_pool_destroy` (`thread_pool.c:1762`) first checks
  `atomic_load(&pool->shutdown)`; if false, return `LOOMWORKS_ERR_INVALID`
  without touching any structure.
- `pool_destroy_internal` (`236-277`) is otherwise unchanged; it is now only
  reachable after a completed `shutdown()` (all workers joined).
- **Call-site sweep**: every existing test/example that destroys without an
  explicit shutdown is updated to call `loom_pool_shutdown` first. New
  regression test asserts the strict gate.
- Docs: `api-reference.md` `loom_pool_destroy` precondition updated to
  "must have called `loom_pool_shutdown`; otherwise `ERR_INVALID`".

### 5.2 Cancellation signals futures (Defects 3 + D2/D8)

- `loom_task_t` gains `bool is_future`; set in both `submit_future` paths
  (`thread_pool.c:1269`, `1346`) where `future_task_ctx_t` is allocated.
- In the tombstone path (`425-436`), before freeing `task->user_data`:
  - if `task->is_future`: take the future lock, set `cancelled = true`,
    `ready = true`, broadcast, unlock — then free the wrapper as today.
  - Lock order is `pool->lock → future->lock`, consistent with the run path
    (which holds only `future->lock`); no inversion exists.
- `loom_future_wait` (`1379`) and `loom_future_wait_timeout` (`1396`): after
  the readiness check, if `cancelled` → `*result = NULL` and return
  `LOOMWORKS_ERR_CANCELLED`; else return the stored result.
- **Test update**: `test_future_cancel_pending`
  (`tests/test_thread_pool.c:2117-2151`) currently expects `ERR_TIMEOUT` from
  `wait_timeout` on a cancelled pending future; expectation becomes
  `ERR_CANCELLED` (semantic correction, in scope).
- New regression tests:
  1. cancelled pending future + `future_wait_timeout` (generous deadline) →
     `ERR_CANCELLED` (also discriminates old-buggy `ERR_TIMEOUT` without a CI
     hang risk),
  2. cancelled pending future + **infinite** `future_wait` in a forked child,
     parent asserts child exits promptly with the `ERR_CANCELLED` path (mirrors
     the asm-spec NULL-link `exit` test pattern; a regression would hang the
     child, not CI).

### 5.3 Future destroy state check (Defect 4 + D3)

- `loom_future_destroy` (`1446-1458`): take the future lock; if `!ready`,
  unlock and return `LOOMWORKS_ERR_INVALID` (nothing freed); if `ready`, free
  under the lock as today. The API doc already says "must be called after
  `loom_future_wait`" — this turns the doc contract into enforcement.
- The worker's `future_task_wrapper` writes the result and sets `ready` under
  the same lock, so there is no window between check and free.
- New regression test: submit a future-backed task that blocks on a gate; call
  `future_destroy` on the pending future → `ERR_INVALID`; release the gate,
  wait, then destroy → `OK`.

### 5.4 Coroutine destroy guards (Defect 5 + D4)

**Implemented (2026-08-17) — DEVIATION from the draft below: only the state
gate landed, the owner check did not.**

- `loom_coro_destroy` (`coroutine.c:476-488`) gained the *state* gate
  `resume`/`terminate` enforce:
  - state must be `NEW` / `DONE` / `ERROR` (`ERR_INVALID` on
    `RUNNING`/`SUSPENDED`).
- **Owner check dropped:** the draft proposed rejecting cross-thread destroy
  (`ERR_INVALID`). The public `coroutine.h` contract explicitly allows
  cross-thread destroy of a quiescent coroutine — stacks come from a
  process-global mmap pool serialized by a mutex, so ownership is not part
  of the destroy contract and enforcing it would regress callers that hand a
  finished coroutine to another thread for cleanup. Owner checks remain on
  `resume`/`terminate` (cross-thread *driving* is the unsafe operation).
- New regression tests: destroy on a freshly-created coroutine → `OK`; destroy
  a RUNNING and a SUSPENDED coroutine → `ERR_INVALID` (memory still intact);
  terminate-then-destroy → `OK`. (The draft's "destroy from a foreign thread
  → `ERR_INVALID`" test was not added — it contradicts the public contract.)

### 5.5 Worker-crash observability (Defect 2 + D5)

Mechanism (corrected during self-review — `ESRCH` alone cannot detect
`pthread_exit`-killed workers, which remain joinable with exit status 0):

- worker state gains a per-worker `_Atomic bool clean_exit`, initialised false
  at worker start.
- The worker sets `clean_exit = true` immediately before *every normal exit
  path*: the shutdown drain exit (`shutdown && queue_len == 0`) and the resize
  shrink exit (`idx >= worker_count`), since a shrink-joined worker that died
  mid-task is equally a crash.
- At **every join site** (shutdown join loop `1473-1517` and the resize join
  loops `1029`/`1044`): after `pthread_join` returns, if `clean_exit` is false
  (or join returned `ESRCH`, i.e. a detached-then-died worker) → the worker
  died mid-task → set `thread_alive[idx] = false` and
  `metrics_fire(pool, LOOMWORKS_METRIC_FAILED)`. A normal exit has
  `clean_exit == true` → no event, accounting as today.
- No signal handlers, no recovery — a dead worker stays dead; the pool reports
  the truth. Process-fatal task segfaults remain default (NG3).
- New regression test: submit a task that calls `pthread_exit(NULL)`; run
  `shutdown()`; assert exactly one `LOOMWORKS_METRIC_FAILED` event was observed
  via the metrics callback, and that a normal shutdown run fires none.

### 5.6 Group self-wait deadlock guard (Defect 6 + D6)

- `worker_entry` sets a `_Thread_local` "current pool" pointer on entry.
- `loom_group_wait` and `loom_group_destroy` (`task_group.c`): if the current
  pool pointer equals `group->pool`, return `LOOMWORKS_ERR_INVALID` before any
  `pthread_cond_wait` — the call would otherwise block forever waiting on work
  that only this worker could run.
- The blocking contract for legitimate (non-worker) callers is documented in
  `api-reference.md` (same semantics as pool shutdown).
- New regression test: a task running in the pool calls `group_wait` on its own
  group → `ERR_INVALID` (no hang); the same group waited from the main thread
  after tasks finish → `OK`.

### 5.7 Monotonic clock for timeout waits (Defect 7 + D7)

- `wait_for_space` (`1069`): deadline computed from
  `clock_gettime(CLOCK_MONOTONIC)` + 60 s; its condvar gains a
  `CLOCK_MONOTONIC` condattr at creation.
- `loom_future_wait_timeout` (`1421`): the future's condvar gains a
  `CLOCK_MONOTONIC` condattr; the deadline parameter is re-documented as
  monotonic-clock (breaking-ish semantic — callers using `CLOCK_REALTIME`
  deadlines must switch; internal test helpers updated).
- Covered by the existing timeout tests (bounded-submit timeout path and
  `test_future_cancel_pending`); no dedicated clock-jump test.

### 5.8 Docs and changelog

1. `api-reference.md`: `destroy` precondition (`SHUTDOWN` required), `wait`/
   `wait_timeout` `ERR_CANCELLED`, `future_destroy` completed-only
   enforcement, `future_wait_timeout` monotonic deadline, `group_destroy`
   blocking contract + self-wait rejection, new `ERR_CANCELLED` entry.
2. `risk-assessment.md`: R4 updated (self-wait guard + documented contract),
   R9 updated (monotonic), R13 updated (destroy guarded); the four post-audit
   findings (pool destroy gate, worker-crash observability, cancelled-future
   hang, future destroy UAF) logged as closed.
3. `design-decisions.md`: strict lifecycle rationale (D1), no-segfault-survival
   decision (NG3), cancellation-as-observable-outcome (D2).
4. `CHANGELOG.md`: v1.1.0 hardening entry.

## 6. Out of scope for this design (implementation-phase items)

- Exact `loom_task_t` field placement and `is_future` init points.
- Condvar `CLOCK_MONOTONIC` attribute plumbing details.
- Concrete regression-test assertion code.
- Call-site sweep diff (which existing tests/examples need `shutdown()` added).

## 7. Acceptance criteria

- AC1. Destroying a non-shutdown pool returns `ERR_INVALID` and leaks nothing
  (regression test + existing destroy tests updated to shutdown first).
- AC2. `ERR_CANCELLED` exists; a cancelled pending future returns it from both
  wait variants; `test_future_cancel_pending` expects `ERR_CANCELLED`; the
  forked-child infinite-wait test exits promptly.
- AC3. `future_destroy` on a pending future returns `ERR_INVALID`; on a
  completed future returns void (destroy succeeds).
- AC4. `coro_destroy` returns `ERR_INVALID` for RUNNING/SUSPENDED and succeeds
  for NEW/DONE/ERROR (state-only gate — cross-thread destroy of a quiescent
  coroutine remains allowed per the public contract; owner checks stay on
  `resume`/`terminate`); coroutine suite stays green.
- AC5. A `pthread_exit`-terminated worker produces exactly one
  `LOOMWORKS_METRIC_FAILED` event at shutdown; a clean shutdown fires none;
  `thread_alive` corrected.
- AC6. `group_wait`/`group_destroy` from a pool worker return `ERR_INVALID`
  without blocking.
- AC7. All baseline suites green with unchanged assertion counts
  (12541 / 5603 / 78765 ±ε); ASan/UBSan/TSan builds green locally and in CI.
- AC8. Docs updated per §5.8 (4 files).
