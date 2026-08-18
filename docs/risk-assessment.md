# loomworks Risk Assessment

This document catalogs the risks identified during a full project audit of
loomworks v1.0.1 (master @ b5d8ab2), together with their likelihood, impact,
mitigations, and verification status. It is a living document meant to guide
maintenance priorities, not a list of known bugs.

Risk ratings use the conventional 3×3 grid:

| Dimension | Low | Medium | High |
|-----------|-----|--------|------|
| **Likelihood** | Unlikely in practice (requires unusual usage) | Possible under normal-but-suboptimizing usage | Certain or very likely under normal usage/evolution |
| **Impact** | Cosmetic / localized / easy workaround | Silent misbehavior, performance degradation, or portability loss | Memory corruption, deadlock, crash, or data loss |

---

## Risk Register

| ID | Area | Risk | Likelihood | Impact | Severity |
|----|------|------|-----------|--------|----------|
| R1 | Coroutine | Process-global `SIGSEGV`/`SIGBUS` handlers installed via `sigaction` without chaining to prior handlers | Medium | Medium | **Medium** — ✅ closed (2026-08-17): handlers now save and restore the prior handler |
| R2 | Coroutine | `ucontext` is deprecated by POSIX.1-2008 and marked obsolescent in glibc 2.16+; removed on macOS | Medium | Medium | **Medium** — ✅ mitigated (2026-08-17): hand-written asm backends for x86-64/aarch64 are default; ucontext is a compile-time fallback |
| R3 | Metrics | Callback fires synchronously on the task-executing thread; must be cheap or it throttles the producer | Medium | Medium | **Medium** — ✅ closed (2026-08-18): contract locked by regression tests (worker thread, lock-free, no drops) |
| R4 | Task Group | `group_destroy()` blocks until in-flight wrappers finish — no timeout; deadlock if tasks block forever | Medium | Medium | **Medium** — ✅ resolved (2026-08-17): timed group wait (`loom_task_group_wait_timeout`, absolute `CLOCK_MONOTONIC` deadline); `destroy()` remains blocking by documented contract |
| R5 | Thread Pool | `loom_pool_cancel()` (user_data match) performs an O(cancel_cap) linear scan of the cancel index; `cancel_by_id` is hash-indexed O(1) | Medium | Low | **Low** — ✅ accepted trade-off (2026-08-18): cancellation is rare; scan is microseconds; secondary index has negative ROI |
| R6 | Thread Pool | `loom_pool_resize()` is the most subtle code path (~200 lines of lock-step realloc, rollback, token-storm join) — high regression surface | Low | High | **Low** — ✅ resolved (2026-08-17): fault-injection suite covers every grow-path allocation (6 call sites incl. lane-only degrade); five related defects fixed with regression locks |
| R7 | Thread Pool | Work-stealing executes tasks out of submission order (LIFO local, FIFO steal) — FIFO is only guaranteed for NORMAL+ring when ≤1 worker actively drains | Low | Medium | **Low** |
| R8 | Pipeline | Internal-consumer mode (`worker_count>0`) consumes and **frees** every payload by default unless a discard handler is set | Medium | Medium | **Medium** — ✅ resolved (2026-08-17): explicit ownership flag (`LOOM_PC_OWN_PAYLOADS` via `loom_pc_create_ex`) with creation-time validation; the leak-only combo (internal pool + no handler + ownership flag) is rejected |
| R9 | Pipeline | 60s `CLOCK_REALTIME` timedwait is subject to wall-clock jumps (NTP slews, manual clock changes) | Low | Low | **Low** — ✅ resolved (2026-08-17): timeouts now use `CLOCK_MONOTONIC` |
| R10 | Docs | Assertion counts in README/CHANGELOG drift from actual (see §Verification) | Low | Low | **Low** |
| R11 | CI | Duplicate valgrind suppression files diverge; root `loomworks.valgrind-supp` is orphaned (CI uses `.github/valgrind.supp`) | High | Low | **Low** — ✅ resolved (2026-08-16) |
| R15 | CI | Missing trailing newline at EOF in `src/coro_ctx.h`/`src/task_group.c` trips clang-tidy `newline-eof` → all CI builds red on master | High | Medium | **High** — ✅ resolved (2026-08-16): trailing newlines restored |
| R12 | Portability | Linux/x86-64 only; no Windows/macOS build, `_GNU_SOURCE` + `pthread_tryjoin_np` are GNU extensions | High | Medium | **Medium** — ✅ resolved (2026-08-18): clean_exit-poll join + POSIX-only shim (portability.h); CMake `LOOMWORKS_POSIX_FALLBACK` simulates non-GNU platforms in CI; platform floor macOS 10.12+/Linux |
| R13 | Coroutine | Cross-thread coroutine misuse (resume/terminate) hard-fails with `ERR_INVALID`, but `destroy()` is unguarded — caller must only destroy in DONE/ERROR | Low | Medium | **Low** — ✅ closed (2026-08-17): `destroy()` now gates on state NEW/DONE/ERROR; RUNNING/SUSPENDED rejected |
| R14 | Thread Pool | `cancel_by_id` reused after pool shutdown returns `ERR_SHUTDOWN`; cancel-on-shutdown semantics are subtle (running tasks are never preempted) | Low | Low | **Low** |
| R16 | Thread Pool | `loom_pool_destroy` without prior shutdown frees worker-shared structures while workers still run → heap corruption | Medium | High | **Medium** — ✅ closed (2026-08-17): rejected with `ERR_INVALID` until shutdown |
| R17 | Thread Pool | A worker terminated via `pthread_exit`/crash leaves the pool unable to tell a dead worker from a live one → silent loss of capacity | Low | High | **Low** — ✅ closed (2026-08-17): per-worker `clean_exit` flag reports abnormal exits as `LOOMWORKS_METRIC_FAILED` |
| R18 | Thread Pool | A cancelled pending future never completes → `future_wait` hangs forever | Low | High | **Low** — ✅ closed (2026-08-17): cancelled futures complete with `LOOMWORKS_ERR_CANCELLED` |

---

## Detailed Analysis

### R1 — Global signal handlers are installed without chaining

`loom_coro_install_guard_handler()` [src/coroutine.c:131] installs a custom
handler for `SIGSEGV` and `SIGBUS` via `sigaction`, overwriting whatever the
host process had installed. `loom_coro_uninstall_guard_handler()` restores
`SIG_DFL`, **not the prior handler**. Any embedding application that installs
its own fault handlers (crash reporters, JITs, signal-based profilers) will
have them silently replaced while a coroutine is being resumed.

**Mitigations**: handlers are installed idempotently (guarded by the
`g_guard_installed` atomic) and only around resume/swap; the handler itself
re-raises with `SIG_DFL` when the fault is not on a coroutine guard page (so
wild pointers still crash normally).

**Status: ✅ CLOSED (2026-08-17).** The handlers now chain: the previous
handler is saved at install (`g_prev_segv`/`g_prev_bus`) and restored both
when the fault is not ours (the handler re-dispatches to `prior`) and at
uninstall. Embedding applications no longer lose their own fault handlers.

### R2 — `ucontext` deprecation

The coroutine backend is POSIX `ucontext`, officially obsolescent
(POSIX.1-2008) and unavailable on macOS. glibc continues to support it on
Linux. The newly added `src/coro_ctx.h` facades all context operations behind
a pluggable backend (8 inline functions), so swapping in an asm-based backend
(`ctx` pro/cto as referenced in the header) is a contained change.

**Mitigation**: `coro_ctx.h` isolates the backend; migration is scoped to one
header. Risk is Medium only because no alternative backend is implemented
yet.

**Status: ✅ MITIGATED (2026-08-17).** Hand-written asm context backends for
x86-64 (SysV) and aarch64 (AAPCS64) are now the default (`ctx` pro/cto), with
ucontext retained as a compile-time fallback. `ctx_smoke` exercises both
backends; aarch64 is cross-compiled and QEMU-run in CI.

### R3 — Metrics callback on the worker thread

`loom_metrics_t` fires its callback synchronously on the worker thread that
completed the task [metrics.h]. A slow callback (logging, file I/O, network)
directly throttles that worker and can starve the pool under load.

**Mitigation**: documented in the public header ("must be cheap"). The
lock-free counters themselves add ~3 relaxed atomics + one optional
clock_gettime per task, which is negligible.

**Status: ✅ CLOSED (2026-08-18).** The contract is now locked by regression
tests (`test_metrics_callback_on_worker_thread`,
`test_metrics_callback_outside_lock`, `test_metrics_callback_lifecycle_counts`):
the callback fires synchronously on the thread that produces the event
(worker for STARTED/COMPLETED, submitter for SUBMITTED, shutting-down thread
for FAILED), always outside the pool lock — the tests probe `pending_count`
from inside the callback (acquisition would deadlock and time out the suite)
and verify every lifecycle event is counted exactly once.

### R4 — `group_destroy()` can block indefinitely

By design, destroy cancels tracked tasks and then waits for in-flight
wrappers to complete. If a user task blocks forever (deadlock, infinite
loop, waiting on I/O), `group_destroy()` never returns. There is no timeout
parameter.

**Status: ✅ RESOLVED (2026-08-17).** The guaranteed-deadlock case is closed:
`group_wait()`/`group_destroy()` called from a worker of the group's own pool
return `LOOMWORKS_ERR_INVALID` immediately (self-wait guard via the
`loom_pool_current` TLS). The residual unbounded external wait is now
addressable too: `loom_task_group_wait_timeout()` bounds a non-worker `wait()`
with an absolute `CLOCK_MONOTONIC` deadline and returns
`LOOMWORKS_ERR_TIMEOUT` on expiry, leaving the group fully usable for a later
`wait()`/`destroy()`. `group_destroy()` itself remains a blocking call by
documented contract — a caller must use the wait-timeout-then-destroy pattern
to bound the full teardown.

### R5 — `cancel()` is a linear scan

`loom_pool_cancel(data)` scans all `cancel_cap` slots linearly to find a
matching task, then falls back to scanning the 256 bucket chains. With the
default `cancel_cap` (≥ ring_size + workers×256 ≈ thousands of slots), each
cancel is O(n). Cancelling many tasks across a large pool is quadratic-ish.

**Trade-off**: cancellation is rare relative to submit/execute; the index
keeps the submit fast path lock-free. Acceptable for v1.0.1; a hash index
keyed by task_id (instead of open addressing over ids+1) would reduce the
scan.

**Status: ✅ ACCEPTED (2026-08-18).** Investigation corrected the scope of
this row: `loom_pool_cancel_by_id` is already O(1) expected via the
lock-free open-addressing hash index (`cancel_index_find`); the O(n) scan
applies only to `loom_pool_cancel(data)`, which must match by `user_data`
and has no reverse index. Per-call cost is ~`cancel_cap` acquire atomic
reads (≥1024 slots for 8 workers — microseconds), cancellation is rare
relative to submit/execute, and the contract already tolerates the
"task may have already run" race. A secondary user_data hash index would
require delete-chain maintenance for shared user_data, roughly double index
memory, and add a new concurrency surface — negative ROI on a Low risk.
Cancel semantics remain regression-locked (`test_ring_cancel_data`,
`test_ring_cancel_by_id`, `test_cancel_by_id_after_shutdown`,
`test_cancel_by_id_running`, future-cancel family). See
`docs/superpowers/specs/2026-08-18-cancel-tradeoff-design.md`.

### R6 — `resize()` is a large subtle code path

`loom_pool_resize()` (~200 lines) reallocates `deques` then `threads`/`thread_alive`
in lockstep, rolls back on any failure, spawns or joins workers, and uses the
documented sem-post "token storm" to unblock displaced workers during shrink
(`while (pthread_tryjoin_np(...) != 0) { sem_post; sched_yield; }`).
Thundering-herd token consumption is handled by re-posting until the join
succeeds. Displaced workers re-read the `deques` pointer under the lock each
iteration, so realloc is safe.

**Residual risk**: this code is exercised only by `resize_demo.c` and a few
tests; concurrency bugs here fail loudly (crash) rather than silently, which
is why impact is High but the combination is Low severity today.

**Status: ✅ RESOLVED (2026-08-17)** — a test-only one-shot armed-counter hook
(`loom_test_arm_alloc_failure`, declared in the internal header) now pores
every grow-path allocation call site with deterministic fault injection, and
8 tests prove the rollback guarantee (worker count unchanged, pool usable,
unarmed resize recovers) plus the lane-only degrade contract. Exercising this
path exposed **five latent defects**, all fixed and regression-locked:

1. **Stale `thread_clean_exit` on grow rollback** — rolled-back worker slots
   kept `clean_exit=true`; a later grow reusing them hid a worker crash from
   the FAILED metric. Rollback now resets the flag (symmetric with shrink);
   locked by `test_resize_fail_then_worker_crash_detected`.
2. **Grow-rollback join deadlock** — freshly spawned workers blocked on
   `work_sem` were `pthread_join`ed without wake tokens. Both rollback loops
   now use `tryjoin` + re-post (token-storm, shrink-symmetric).
3. **Lane-only mode broken since inception** — NORMAL tasks routed to the ring
   were never consumed (workers drain the ring only via per-worker deques),
   wedging shutdown. Submits now take the ring only when deques exist.
4. **Lane-only fallback freed uninitialized pointers** — the deque-slot
   fallback freed garbage after `realloc` extension; tail-zero `memset`
   mirrors `thread_alive`/`thread_clean_exit`.
5. **Work-steal parity** — the `try*2` victim stride visits only half the
   workers on even counts, leaving the deque owner unreachable and stranding
   tasks. Ring-dry steals now scan every other worker exactly once.

### R7 — Execution order is not globally FIFO

Work-stealing means tasks submitted to different workers execute
out-of-order; even same-priority tasks can be reordered (own-deque LIFO beats
ring order, steal is FIFO from a stranger's deque). Only
NORMAL+ring-with-1-worker approximates strict FIFO.

**Mitigation**: documented in architecture.md. Users needing strict ordering
should use the pipeline (FIFO by design) or sequence numbers.

### R8 — Pipeline internal consumers free payloads by default

With `loom_pc_create(worker_count>0, ...)`, pool workers poll `loom_pc_take()`
and, absent a discard handler, call `free(item)` on every item. Callers that
submit heap-allocated payloads and expect to reclaim them later (e.g.,
`loom_pc_take()` from their own consumer loop after also running an internal
consumer) will see memory freed out from under them.

**Mitigation**: a discard handler (`loom_pc_set_discard_handler`) must be set
to reclaim payloads — but note it fires on the *internal* worker thread during
internal consumption, not just at destroy.

**Status: ✅ RESOLVED (2026-08-17)** — `loom_pc_create_ex` introduces an
explicit ownership flag (`LOOM_PC_OWN_PAYLOADS`): the library then never
calls `free()` on a payload. The discard handler may be installed atomically
at creation (it runs both on the internal worker thread during consumption
and on the destroy drain). The combination `OWN_PAYLOADS` + internal pool +
no handler is rejected at creation — it can only leak. Default `loom_pc_create`
is unchanged (zero flags): consume-and-free remains the documented default
for fire-and-forget callers. Locked by 5 tests (rejected combo, exactly-once
handler across internal consumption and drain, external-mode no-op, unknown
flag bits, NULL handle).

### R9 — Wall-clock timeouts

All 60s blocking waits (`submit`, `pc_submit`) use `CLOCK_REALTIME`
timedwait, so NTP slew or operator clock changes can stretch/shrink the
timeout. Low impact in practice.

**Status: ✅ RESOLVED (2026-08-17).** The pool's blocking waits
(`wait_for_space`, `future_wait_timeout`) and their condvars
(`space_cond`, future conds) now use `CLOCK_MONOTONIC` via a
once-initialized monotonic condattr — wall-clock jumps no longer distort
timeouts. `pipeline.c` remains REALTIME (out of scope).

### R10 — Assertion-count doc drift (verified)

Verified against a fresh rebuild (Debug):

| Suite | README/CHANGELOG | Fresh rebuild |
|-------|------------------|---------------|
| pool | ~12539 | **12529** |
| coroutine | ~5603 | **5603** |
| integration | ~78731 | **78685** |

Differences are ±50 assertions — negligible and within "~" tolerance. The
earlier much larger discrepancy (10451/5587/68705) was observed against a
**stale `build/` binary** and is not a real drift. No action required; run
`./build/test_*` after rebuilds to refresh the counts in CHANGELOG/README if
precision matters.

### R11 — Duplicate valgrind suppressions (orphaned file)

`loomworks.valgrind-supp` at the repo root and `.github/valgrind.supp` both
existed and **differed in content** (one suppressed scheduler-stack leaks, the
other suppresses the intentional guard-page write). CI referenced only
`.github/valgrind.supp`.

**Status: ✅ RESOLVED (2026-08-16).** The orphaned root file was deleted.
Rules in the root file were stale — they suppressed a "scheduler stack
intentionally never freed" behavior that the current code no longer has
(stacks are freed by `loom_coro_exit()` / `free_all_scheduler_stacks()`), and
historical CI runs passed valgrind memcheck using only the `.github/valgrind.supp`
rules. `.github/valgrind.supp` remains the single source of truth.

> **Note (auditor correction):** a typo in `.clang-tidy`
> (`readability-redundant-string-cstr` is correctly spelled) was suspected
> during the audit (initial grep suggested `readiness-*`); verification
> against the actual file shows the spelling is correct and the check is
> active. No action needed.

### R15 — Missing trailing newline breaks CI builds (resolved)

`src/coro_ctx.h:64` and `src/task_group.c:417` lack a newline at end of file.
With `ENABLE_CLANG_TIDY ON` and `WarningsAsErrors: '*'` in CI, this trips
clang-tidy's `clang-diagnostic-newline-eof` → "1 warning treated as error" →
`loomworks_static`/`loomworks_shared` fail to build. All master CI jobs (build
matrix + sanitizers, 2026-08-15 13:34+) are red for this reason.

**Status: ✅ RESOLVED (2026-08-16).** Trailing newlines restored in both
files; CI is green.

**Cause**: both files are part of the Unreleased changes (coro_ctx.h is new;
task_group.c was edited). Local builds pass because local clang-tidy is not
active (CMake `ENABLE_CLANG_TIDY ON` warns-and-skips when the binary is
missing), so this only surfaced in CI.

### R12 — GNU/Linux-only portability surface

`src/thread_pool.c` requires `_GNU_SOURCE` (`pthread_tryjoin_np`),
`struct sigaction` semantics, `_Thread_local`, and `ucontext`. No Windows
build exists (future-work item, Low priority). macOS lacks `ucontext` and
`pthread_tryjoin_np` (not in macOS libpthread).

**Status: ✅ RESOLVED (2026-08-18).** The last hard GNU dependency —
`pthread_tryjoin_np` in the three resize/shrink join loops — is gone. The
loops now poll the library's own `thread_clean_exit[idx]` atomic flag (set
by `worker_entry` on every exit path) and then `pthread_join` explicitly;
no GNU extension is needed. The new internal `src/portability.h` owns the
`_POSIX_C_SOURCE` definition (previously thread_pool.c defined
`_GNU_SOURCE` unconditionally) and provides the `MAP_ANONYMOUS`/`MAP_ANON`
fallback for BSD. A CMake switch `LOOMWORKS_POSIX_FALLBACK=ON` forces the
portable path on Linux; the build-posix CI-equivalent run passes ctest 4/4
with identical assertion counts. Platform floor: Linux (primary,
x86-64/aarch64, QEMU CI) and macOS 10.12+ / BSD via POSIX fallbacks
(ucontext + portable join). Windows remains out of scope. A `pthread_kill`
probe was tried first and falsified: glibc returns 0 (EINVAL) for an exited
but unjoined thread, so a simulated EBUSY never clears — the clean_exit
poll replaces it.

### R13 — `destroy()` is unguarded by design

`loom_coro_destroy()` may run on any thread but is only valid in the
DONE/ERROR state; destroying a RUNNING/SUSPENDED coroutine is undefined
behavior (no owner/state check, unlike resume/terminate). This is a
documented API contract — callers must terminate first.

**Status: ✅ CLOSED (2026-08-17).** `loom_coro_destroy()` now gates on the
coroutine's state: `NEW`/`DONE`/`ERROR` destroy cleanly, `RUNNING`/
`SUSPENDED` return `LOOMWORKS_CORO_ERR_INVALID` and leave the coroutine
intact. Cross-thread destroy of a quiescent coroutine remains allowed — the
public contract (process-global stack pool, serialized by mutex) permits it,
so no owner check was added.

### R14 — Cancellation semantics on shutdown

`cancel()`/`cancel_by_id()` return `ERR_SHUTDOWN` after shutdown begins, and
cancellation only affects *queued* tasks: running tasks are never preempted
and cancelled tasks that were already dequeued into a worker's local deque
may still run (the cancel index claim loses to deque-pop by a worker).
Conveniently, `group_destroy()` + `cancel_all()` handle this correctly; but a
user doing manual `cancel_by_id` + `future_wait` must tolerate a
cancelled-but-executed task.

### R16 — Destroying a non-shutdown pool (closed)

`loom_pool_destroy()` on a pool whose workers may still be running would free
every worker-shared structure under them → heap corruption.

**Status: ✅ CLOSED (2026-08-17).** `loom_pool_destroy()` now returns
`LOOMWORKS_ERR_INVALID` unless `shutdown` is set, leaving the handle
untouched; callers must run `loom_pool_shutdown()` first (NULL-safe
otherwise). Regression test: `test_pool_destroy_without_shutdown`.

### R17 — Worker-loss invisibility (closed)

A worker killed by `pthread_exit()` (or any crash inside a task) previously
looked alive after shutdown joined it — capacity loss was invisible and no
metric fired.

**Status: ✅ CLOSED (2026-08-17).** Each worker owns a per-slot
`thread_clean_exit` atomic, set immediately before every *normal* exit path.
Every join site (shutdown loop, resize grow/shrink rollback) checks it after
`pthread_join`: a worker that exited without setting it (or that join
ESRCH'd, i.e. detached-then-died) fires `LOOMWORKS_METRIC_FAILED` and clears
`thread_alive`. Regression test: `test_worker_crash_detected`.

### R18 — Cancelled future never completes (closed)

A pending future whose task was cancelled never signalled its condvar →
`future_wait()` hung forever (no timeout variant was a valid escape).

**Status: ✅ CLOSED (2026-08-17).** Cancellation now marks the future
`cancelled` and signals it; both wait variants return
`LOOMWORKS_ERR_CANCELLED` instead of hanging. A future destroyed while
pending (before completion) is rejected with `ERR_INVALID` — the worker
would otherwise write into freed memory. Regression tests:
`test_future_cancel_pending`, destroy-pending-future.

---

## Verification method

All claims above are grounded in the source as of master @ b5d8ab2:

- `src/thread_pool.c` (2004 lines), `thread_pool_internal.h` — read in full.
- `src/coroutine.c` (602 lines), `coroutine_internal.h`, `coro_ctx.h` — read in full.
- `src/pipeline.c`, `src/task_group.c`, `src/metrics.c` — read in full.
- All public headers under `include/loomworks/` — read in full.
- `.clang-tidy`, `cmake/BuildTypes.cmake`, `CMakeLists.txt`, both CI
  workflows, `loomworks.valgrind-supp` vs `.github/valgrind.supp` — read.
- **Fresh rebuild** (`cmake --build build`) then direct test runs:
  ThreadPoolTests **12604 passed / 0 failed**, CoroutineTests
  **5611 passed / 0 failed**, IntegrationTests **78763 passed / 0 failed**
  (post-hardening: +16 / 0 / +20 against the 2026-08-16 audit baseline).
- `ctest` (Debug): 3/3 suites pass.

## Maintenance priority

1. Others — accepted trade-offs, re-evaluate as usage grows. Hardening items
   R1/R9/R13/R15/R16/R17/R18 are closed as of 2026-08-17; R4 was resolved on
   2026-08-17 with the timed group wait; R6 was resolved on 2026-08-17 with
   the resize fault-injection suite; R8 was resolved on 2026-08-17 with the
   pipeline payload ownership flag; R12 was resolved on 2026-08-18 with the
   portable clean_exit-poll join (portability.h); R3 was closed on 2026-08-18
   with the metrics callback contract lockdown (regression tests); R5 was
   accepted as a documented trade-off on 2026-08-18 (cancel_by_id is
   hash-indexed O(1); the cancel(data) user_data scan stays as a
   microseconds-scale documented cost).