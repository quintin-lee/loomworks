# Coroutine Deepening: Multi-Yield, Sleep, and Pool Integration — Design Spec

> **Status**: Approved (2026-08-19) — four design sections approved by user via
> brainstorming (A + B1 + C all selected).
>
> **Scope**: stackful coroutine subsystem (`src/coroutine.c`) + thread pool
> integration (`src/thread_pool.c`) + new global timer (`src/timer.c`).
> No changes to pipeline/task_group/metrics public behavior beyond additive
> metrics events for coroutine tasks.

## 1. Motivation

Three production-grade gaps identified during the 2026-08-18 benchmark/sanitizer
round:

1. **Yield is one-shot** (`src/coroutine.c` resume comment contract): a
   SUSPENDED coroutine re-resumed with the same state a second time runs to
   completion because every subsequent internal `loom_coro_yield()` is a no-op
   (`cur->state != RUNNING`). Generator/iterator patterns — a standard stackful
   coroutine capability — are impossible.
2. **`bench_coro_switch` measures the wrong path**: `switch_yielder` loops
   `g_switch_yield_count` yields; because of (1), the second resume drives the
   coroutine to DONE and every later resume returns `ERR_RUNNING` in ~20 ns.
   The exported `coro_switch_ns` (~19.5-22.7 ns) is the fast-fail path, not a
   real context switch round-trip.
3. **No time-based suspension and no coroutine×pool integration**: coroutines
   cannot sleep until a deadline, and a worker cannot host suspended
   coroutines while doing other work (the pool cannot multiplex multiple
   lightweight coroutine tasks per worker).

This spec fixes (1), adds `loom_coro_sleep_until`/`loom_coro_sleep` (C), and
adds coroutine tasks to the pool with per-worker coroutine scheduling (B1).

## 2. Design Overview

Worker threads become the scheduler for the coroutines they own. The
owner-thread affinity contract (resume/terminate only from the owner thread) is
**preserved**: every resume of a pool coroutine task happens on its owner
worker; a pool-owned timer thread only wakes the owner (sem_post) — it never
resumes.

```
                     ┌──────────────────────────────────────────┐
                     │               Thread Pool                 │
                     │                                          │
  submit_coroutine ─►│  ring/deques: loom_task_t{is_coro=1,...} │
                     │                                          │
                     │  worker W loop:                          │
                     │    1. resume own ready-coro FIFO (if any)│
                     │    2. else pop task; is_coro?            │
                     │         create coro(owner=W) + resume    │
                     │         SUSPENDED → push own ready FIFO  │
                     │         DONE/ERROR → metrics + recycle   │
                     │    3. else sem_timedwait(nearest deadline)│
                     │                                          │
                     │  timer_thread: global min-heap           │
                     │    {deadline, task_id, worker_idx}       │
                     │    pop due → push coro into owner ready  │
                     │    FIFO + sem_post(owner work_sem)       │
                     └──────────────────────────────────────────┘
```

## 3. Public API Additions

### 3.1 coroutine.h

```c
/* Suspend the current coroutine until a CLOCK_MONOTONIC absolute deadline.
 * Valid only inside a coroutine (g_current != NULL && state == RUNNING);
 * otherwise LOOMWORKS_CORO_ERR_INVALID.
 * State becomes SLEEPING; the only way to resume is after the deadline.
 * Outside the pool the caller must resume manually after the deadline.
 * Inside a pool coroutine task, the timer system resumes automatically. */
LOOMWORKS_API loom_coro_result_t
loom_coro_sleep_until(int64_t deadline_ns);

/* Convenience: sleep for a relative duration.
 * loom_coro_sleep(d) == loom_coro_sleep_until(monotonic_now_ns() + d). */
LOOMWORKS_API loom_coro_result_t
loom_coro_sleep(int64_t duration_ns);
```

New coroutine state (enum addition, source-compatible):

```c
LOOMWORKS_CORO_SLEEPING,   /* sleeping until deadline; resume only after it */
```

New internal coroutine field (in `struct loom_coroutine`):

```c
int64_t wake_deadline_ns;              /* 0 = not sleeping; CLOCK_MONOTONIC */
loom_coro_sleep_reg_t sleep_reg;       /* optional pool hook, see below    */
typedef void (*loom_coro_sleep_reg_t)(void *ctx, uint64_t task_id,
                                      int64_t deadline_ns);
/* ctx + task_id identify the owning pool/coro task; NULL hook = pure suspend */
```

**Dual-path sleep** (fixed during inline review — a stand-alone coroutine has
no pool timer to register with):

- `loom_coro_sleep_until(d)` sets `state = SLEEPING` and
  `wake_deadline_ns = d`, then:
  - if `sleep_reg` hook is set (pool coroutine task): call the hook to register
    the deadline in the pool timer heap — the pool timer thread wakes the owner
    worker at expiry;
  - if no hook (stand-alone coroutine): pure suspend — the caller must
    `loom_coro_resume` after the deadline (checked by resume, below).
- `loom_coro_resume` on a SLEEPING coroutine:
  - `now < wake_deadline_ns` → keep SLEEPING, return `ERR_RUNNING`
    ("conditionally running" semantics: mid-flight, time-delayed);
  - `now >= wake_deadline_ns` → resume normally (state → RUNNING) and clear
    `wake_deadline_ns`.

No new error code. `sleep_reg` is set exclusively by `loom_pool_submit_coroutine`
(wrapping owner-worker registration); users never touch the field directly.

### 3.2 thread_pool.h

```c
/* Submit a coroutine task. A worker creates the coroutine (owner = that
 * worker) and resumes it. Internal yield()/sleep() suspends the coroutine
 * and the worker moves on to other work; the owner worker resumes it when
 * ready (yield) or after its deadline (sleep, via the pool timer).
 *
 * fn       coroutine entry function (loom_coro_fn, see coroutine.h)
 * arg      opaque argument passed to fn
 * stack_size   coroutine stack size; 0 = LOOMWORKS_CORO_DEFAULT_STACK_SIZE
 * task_id  optional out param, same task id semantics as loom_pool_submit
 *
 * Returns LOOMWORKS_OK or LOOMWORKS_ERR_SHUTDOWN / ERR_ALLOC. */
LOOMWORKS_API loomworks_result_t
loom_pool_submit_coroutine(loom_thread_pool_t *pool,
                           loom_coro_fn fn, void *arg,
                           size_t stack_size,
                           uint64_t *task_id);
```

## 4. Internal Changes

### 4.1 loom_task_t (src/thread_pool_internal.h)

Add fields inside `struct loom_task` (internal struct, ABI-free):

```c
loom_coro_fn   coro_fn;     /* coroutine task entry, valid when is_coro */
size_t         stack_size;  /* 0 = default   */
uint8_t        is_coro;     /* 0 = normal fn task, 1 = coroutine task */
```

Node size grows; the task free-list (`loom_task_t` reuse pool) is unaffected
(functionally). Normal-task submission paths unchanged.

### 4.2 Coroutine scheduler state per worker (thread_pool_internal.h)

```c
/* Per-worker coroutine ready FIFO. Owned exclusively by the worker;
 * the timer thread may push to it ONLY while holding pool->coro_lock,
 * then sem_post(worker work_sem). The owner is the only resumer. */
typedef struct loom_coro_ready {
    loom_coroutine_t *coro;        /* suspended coroutine eligible for resume */
    uint64_t          task_id;     /* for recycle bookkeeping */
    struct loom_coro_ready *next;
} loom_coro_ready_t;
```

Stored in `pool->coro_ready[worker_count]` array (per-worker list head, in
`loom_thread_pool_t`), sized with workers (resize grows it like `deques` but
without alignment requirements). Lock: `pool->coro_lock` guards cross-worker
transitions (timer push); the worker's own list head is read/written by the
owner without the lock except where noted.

### 4.3 Timer (`src/timer.c`, new; internal header `src/timer_internal.h`?

No — keep in `thread_pool_internal.h`; a small explicit struct + helpers in
`src/timer.c`.)

```c
typedef struct {
    int64_t  deadline_ns;   /* CLOCK_MONOTONIC absolute */
    uint64_t task_id;
    uint32_t worker_idx;    /* owner worker */
} loom_timer_entry_t;

/* static min-heap (array) embedded in loom_thread_pool_t:
 * loom_timer_entry_t *timer_heap; size_t timer_len, timer_cap;
 * pthread_mutex_t     timer_lock; */
```

API (internal, `src/timer.c`): `loom_timer_push/pop/peek/remove_by_task`.

### 4.4 worker_entry modifications (src/thread_pool.c)

`worker_entry` (currently thread_pool.c:364) gains, per iteration, before the
normal task-dequeue path:

1. **Resume own ready FIFO**: pop head; if non-NULL resume it.
   - resume OK & state == SUSPENDED → push back to tail (cooperative round-robin;
     the coroutine may yield again).
   - resume OK & state == DONE/ERROR → fire metrics (started once at create,
     completed/failed at DONE/ERROR), recycle task node + destroy coroutine.
   - resume returns ERR_RUNNING (SLEEPING early) → treat as not-ready: keep in
     FIFO (defensive; timer normally removes it) — but see 4.6 invariant.
2. **Normal dequeue** with `is_coro` branch: on taking a coroutine task,
   create+resume outside the lock; SUSPENDED → push ready FIFO; DONE/ERROR →
   metrics + recycle.
3. **sem_wait → sem_timedwait**: when the timer heap is non-empty, time out at
   the nearest deadline so this worker can pick up woken coroutines.

### 4.5 timer thread lifecycle

- Created in `pool_init` when a pool is created (or lazily on first
  `submit_coroutine`/`sleep` registration — decide: **lazily** to keep
  non-coroutine pools zero-overhead); stopped in `shutdown` after workers
  drain, before destroy.
- Loop: `sem_timedwait(min_deadline)` → pop all due entries → for each, push
  coroutine into `coro_ready[worker_idx]` under `coro_lock` and
  `sem_post(&pool->work_sem[worker_idx])` (or a dedicated per-worker coro
  semaphore — prefer reusing `work_sem[worker_idx]`, but verify the drain
  condition at loop top still lets the worker proceed; see risk note).
- Shutdown: timer thread sees `shutdown` flag, exits; leftover SLEEPING
  coroutines are terminated by existing drain logic.

### 4.6 Concurrency invariants

- Coro state writes (RUNNING→SUSPENDED→RUNNING→DONE) happen on the owner
  thread only (existing rule).
- `SLEEPING` is written by the coroutine itself (owner thread); the timer
  system never touches `state` — it only moves the task pointer between
  structures.
- Ready FIFO push by timer thread occurs under `coro_lock` + sem_post; the
  owner worker reads its FIFO head under `coro_lock` when it might race with a
  timer push, else lock-free when it is the only mutator (single-producer
  push only by timer under lock; owner pop takes lock too — simple and correct,
  contention is rare).
- Resize: `coro_ready` array reallocated under the same lock step as deques;
  `worker_idx` in timer entries refers to stable worker index positions.
- `submit_coroutine` during shutdown → `LOOMWORKS_ERR_SHUTDOWN` (matches
  existing submit semantics).
- Execution-time failure: `submit_coroutine` success only guarantees
  enqueueing; coroutine creation/stack allocation happens on the worker.
  A worker-side create failure (ERR_ALLOC/ERR_CONTEXT/ERR_MPROTECT) is
  reported through the same path as a task that errors at runtime: metrics
  `failed` event + task node recycle. There is no future to signal, matching
  `loom_pool_submit` (fire-and-forget) semantics.
- Cancel (`loom_pool_cancel` / `cancel_by_id`) of a coroutine task: if the
  coroutine is SUSPENDED/SLEEPING, mark cancelled (existing flag) and let the
  owner worker terminate it (same "never preempt running" rule as normal
  tasks); SLEEPING entries are removed from the timer heap by
  `timer_remove_by_task` where reachable. Reuses R18 cancelled semantics.

## 5. Fix: Multi-Yield Semantics (Foundation for everything above)

In `loom_coro_resume` (src/coroutine.c):

- Also set `state = RUNNING` for SUSPENDED and SLEEPING resumes
  (currently only the NEW branch sets RUNNING).
- SLEEPING resume before deadline → keep SLEEPING, return ERR_RUNNING.
- SLEEPING resume at/after deadline (timer-triggered) → resume normally with
  state RUNNING.
- `loom_coro_yield()` unchanged (checks RUNNING; now RUNNING is set on every
  (re)entry so loops yield correctly).
- Existing `test_multi_yield_resume` (asserts 2nd resume → DONE) is updated to
  the new contract: 2nd resume → SUSPENDED; 3rd resume → DONE. New
  `test_coro_multi_yield` adds a 3-phase generator-style coroutine.

## 6. bench_coro_switch Fix

`switch_yielder` loops `g_switch_yield_count` yields — with multi-yield
semantics this now genuinely round-trips. Keep the n+1-resume structure but
**assert resume return codes**: any resume != OK is a bench failure (exit 1)
so the scenario can never silently measure the fast-fail path again. Also
warmup must enter the loop (100 resumes).

## 7. Testing Plan

| Test | File | Core assertions |
|---|---|---|
| `test_coro_multi_yield` | tests/test_coroutine.c | 3-phase generator; states SUSPENDED→SUSPENDED→DONE; counter advances per resume |
| `test_coro_sleep_until` | tests/test_coroutine.c | sleep 10 ms; pre-deadline resume → ERR_RUNNING + state unchanged; post-deadline resume → OK → DONE |
| `test_pool_submit_coroutine` | tests/test_thread_pool.c | 1000 coroutine tasks (each yields once); all DONE; metrics started/completed counts match |
| `test_pool_coro_sleep` | tests/test_thread_pool.c | 100 coroutines sleep random 5–15 ms; all DONE after their deadlines; monotonic elapsed ≥ deadline |
| `test_coro_cancel_sleeping` | tests/test_thread_pool.c | cancel SLEEPING coroutine task; not resumed; destroyed; no timer leak (ASan) |
| `test_coro_priority` | tests/test_integration.c | REALTIME coroutine task completes before LOW one |

Assertion-count deltas to be recorded and synced in README/CHANGELOG
(matching the 2026-08-18 convention).

## 8. Docs & Changelog

- `docs/architecture.md`: coroutine state machine (+SLEEPING), worker coroutine
  scheduling loop, timer thread.
- `README.md`: coroutine×pool integration section + assertion numbers.
- `CHANGELOG.md` Unreleased Added: multi-yield semantics, sleep_until/sleep,
  pool coroutine tasks.
- `docs/risk-assessment.md`: revisit R19 (TSan) with the new timer thread;
  register new rows if triage shows races.

## 9. Commit Strategy (direct to master, Conventional Commits)

1. `fix: coroutine multi-yield resume semantics` — coroutine.c, test_coroutine.c,
   bench.c (real measurement + return-code asserts)
2. `feat: coroutine sleep_until with timer heap` — coroutine.c, src/timer.c,
   thread_pool_internal.h, test_coroutine.c
3. `feat: pool coroutine tasks` — thread_pool.c, thread_pool_internal.h,
   test_thread_pool.c, test_integration.c, README.md
4. `docs: coroutine-pool integration + assertion sync` — architecture.md,
   CHANGELOG.md, risk-assessment.md

Each commit keeps the suite green (GNU build/ + fallback build-posix/ +
sanitizer rebuild as appropriate). Standing rules: no subagents, direct master,
clang-format -i before build, rerun-once on libloomworks.a race.

## 10. Risks

| Risk | Mitigation |
|---|---|
| Timer thread lifecycle / shutdown hang | Explicit timer-thread stop in shutdown; SLEEPING drained like other pending work |
| sem_timedwait drain-condition interplay | Verify worker loop top still sees woken coroutine before sleeping again; covered by test_pool_coro_sleep |
| Worker main-loop regression | is_coro branch precedes normal path but normal tasks untouched; full regression (20771+ base assertions) |
| bench_coro_switch number changes after fix | Re-run to set new baseline (export-only key, not gated) |
| TSan on new timer thread | timer state under timer_lock; ASan/UBSan hard gates are the safety net (R19 stays best-effort) |
| Resize + coro_ready array | Same lock-step pattern as deques; resize tests extended with coroutine tasks |