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
| R1 | Coroutine | Process-global `SIGSEGV`/`SIGBUS` handlers installed via `sigaction` without chaining to prior handlers | Medium | Medium | **Medium** |
| R2 | Coroutine | `ucontext` is deprecated by POSIX.1-2008 and marked obsolescent in glibc 2.16+; removed on macOS | Medium | Medium | **Medium** |
| R3 | Metrics | Callback fires synchronously on the worker thread; must be cheap or it throttles the pool | Medium | Medium | **Medium** |
| R4 | Task Group | `group_destroy()` blocks until in-flight wrappers finish — no timeout; deadlock if tasks block forever | Medium | Medium | **Medium** |
| R5 | Thread Pool | `loom_pool_cancel()` performs an O(cancel_cap) linear scan of the cancel index on the fast path | Medium | Low | **Low** |
| R6 | Thread Pool | `loom_pool_resize()` is the most subtle code path (~200 lines of lock-step realloc, rollback, token-storm join) — high regression surface | Low | High | **Low** |
| R7 | Thread Pool | Work-stealing executes tasks out of submission order (LIFO local, FIFO steal) — FIFO is only guaranteed for NORMAL+ring when ≤1 worker actively drains | Low | Medium | **Low** |
| R8 | Pipeline | Internal-consumer mode (`worker_count>0`) consumes and **frees** every payload by default unless a discard handler is set | Medium | Medium | **Medium** |
| R9 | Pipeline | 60s `CLOCK_REALTIME` timedwait is subject to wall-clock jumps (NTP slews, manual clock changes) | Low | Low | **Low** |
| R10 | Docs | Assertion counts in README/CHANGELOG drift from actual (see §Verification) | Low | Low | **Low** |
| R11 | CI | Duplicate valgrind suppression files diverge; root `loomworks.valgrind-supp` is orphaned (CI uses `.github/valgrind.supp`) | High | Low | **Low** — ✅ resolved (2026-08-16) |
| R15 | CI | Missing trailing newline at EOF in `src/coro_ctx.h`/`src/task_group.c` trips clang-tidy `newline-eof` → all CI builds red on master | High | Medium | **High** |
| R12 | Portability | Linux/x86-64 only; no Windows/macOS build, `_GNU_SOURCE` + `pthread_tryjoin_np` are GNU extensions | High | Medium | **Medium** |
| R13 | Coroutine | Cross-thread coroutine misuse (resume/terminate) hard-fails with `ERR_INVALID`, but `destroy()` is unguarded — caller must only destroy in DONE/ERROR | Low | Medium | **Low** |
| R14 | Thread Pool | `cancel_by_id` reused after pool shutdown returns `ERR_SHUTDOWN`; cancel-on-shutdown semantics are subtle (running tasks are never preempted) | Low | Low | **Low** |

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

**Recommendation**: chain to the previous handler (save it on install,
restore it on uninstall, or call it when the fault is not ours). Document the
behavior clearly for embedding users.

### R2 — `ucontext` deprecation

The coroutine backend is POSIX `ucontext`, officially obsolescent
(POSIX.1-2008) and unavailable on macOS. glibc continues to support it on
Linux. The newly added `src/coro_ctx.h` facades all context operations behind
a pluggable backend (8 inline functions), so swapping in an asm-based backend
(`ctx` pro/cto as referenced in the header) is a contained change.

**Mitigation**: `coro_ctx.h` isolates the backend; migration is scoped to one
header. Risk is Medium only because no alternative backend is implemented
yet.

### R3 — Metrics callback on the worker thread

`loom_metrics_t` fires its callback synchronously on the worker thread that
completed the task [metrics.h]. A slow callback (logging, file I/O, network)
directly throttles that worker and can starve the pool under load.

**Mitigation**: documented in the public header ("must be cheap"). The
lock-free counters themselves add ~3 relaxed atomics + one optional
clock_gettime per task, which is negligible.

### R4 — `group_destroy()` can block indefinitely

By design, destroy cancels tracked tasks and then waits for in-flight
wrappers to complete. If a user task blocks forever (deadlock, infinite
loop, waiting on I/O), `group_destroy()` never returns. There is no timeout
parameter.

### R5 — `cancel()` is a linear scan

`loom_pool_cancel(data)` scans all `cancel_cap` slots linearly to find a
matching task, then falls back to scanning the 256 bucket chains. With the
default `cancel_cap` (≥ ring_size + workers×256 ≈ thousands of slots), each
cancel is O(n). Cancelling many tasks across a large pool is quadratic-ish.

**Trade-off**: cancellation is rare relative to submit/execute; the index
keeps the submit fast path lock-free. Acceptable for v1.0.1; a hash index
keyed by task_id (instead of open addressing over ids+1) would reduce the
scan.

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
internal consumption, not just at destroy. Semantics are documented; the
"consume-and-free" default is a deliberate choice.

### R9 — Wall-clock timeouts

All 60s blocking waits (`submit`, `pc_submit`) use `CLOCK_REALTIME`
timedwait, so NTP slew or operator clock changes can stretch/shrink the
timeout. Low impact in practice.

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

### R15 — Missing trailing newline breaks CI builds (active)

`src/coro_ctx.h:64` and `src/task_group.c:417` lack a newline at end of file.
With `ENABLE_CLANG_TIDY ON` and `WarningsAsErrors: '*'` in CI, this trips
clang-tidy's `clang-diagnostic-newline-eof` → "1 warning treated as error" →
`loomworks_static`/`loomworks_shared` fail to build. All master CI jobs (build
matrix + sanitizers, 2026-08-15 13:34+) are red for this reason.

**Cause**: both files are part of the Unreleased changes (coro_ctx.h is new;
task_group.c was edited). Local builds pass because local clang-tidy is not
active (CMake `ENABLE_CLANG_TIDY ON` warns-and-skips when the binary is
missing), so this only surfaced in CI.

**Fix**: append a trailing newline to both files (2-line diff). Verification:
push and watch CI go green, or run clang-tidy locally with the exact CI command.

### R12 — GNU/Linux-only portability surface

`src/thread_pool.c` requires `_GNU_SOURCE` (`pthread_tryjoin_np`),
`struct sigaction` semantics, `_Thread_local`, and `ucontext`. No Windows
build exists (future-work item, Low priority). macOS lacks `ucontext` and
`pthread_tryjoin_np` (not in macOS libpthread).

**Mitigation**: `coro_ctx.h` isolates `ucontext`; a `pthread_tryjoin_np`
portability shim (join-with-timeout via timed-wait on exit flag) would
unblock macOS. The code already zero-checks every alloc and has a lane-only
fallback, so non-Linux ports degrade gracefully rather than crash.

### R13 — `destroy()` is unguarded by design

`loom_coro_destroy()` may run on any thread but is only valid in the
DONE/ERROR state; destroying a RUNNING/SUSPENDED coroutine is undefined
behavior (no owner/state check, unlike resume/terminate). This is a
documented API contract — callers must terminate first.

### R14 — Cancellation semantics on shutdown

`cancel()`/`cancel_by_id()` return `ERR_SHUTDOWN` after shutdown begins, and
cancellation only affects *queued* tasks: running tasks are never preempted
and cancelled tasks that were already dequeued into a worker's local deque
may still run (the cancel index claim loses to deque-pop by a worker).
Conveniently, `group_destroy()` + `cancel_all()` handle this correctly; but a
user doing manual `cancel_by_id` + `future_wait` must tolerate a
cancelled-but-executed task.

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
  ThreadPoolTests **12529 passed / 0 failed**, CoroutineTests
  **5603 passed / 0 failed**, IntegrationTests **78685 passed / 0 failed**.
- `ctest` (Debug): 3/3 suites pass.

## Maintenance priority

1. **R15** (missing trailing newline) — CI is red on master; a 2-line fix
   unblocks every other concern. Do this next.
2. **R1** (signal chaining) — the only remaining item with a real behavioral
   hazard for embedders; a contained fix.
3. **R12** (portability) — long pole for macOS; `pthread_tryjoin_np` shim is
   the first step. `coro_ctx.h` already de-risks the ucontext half.
4. **R8** (pipeline free-by-default) — document loudly; consider an explicit
   `LOOM_PC_OWN_PAYLOADS` flag in a future minor release.
5. Others — accepted trade-offs, re-evaluate as usage grows.