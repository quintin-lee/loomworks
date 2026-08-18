# loomworks Design Decisions

This document records key design choices and their rationales, for future maintenance and extension reference.

---

## 1. Why ucontext Instead of makecontext/jmp_buf?

**Decision:** Use POSIX `ucontext` family (`getcontext`/`setcontext`/`swapcontext`/`makecontext`) for coroutine context save and restore.

**Rationale:**
- `ucontext` is a POSIX.1-2001 standard API, natively supported on Linux/glibc, no platform-specific code needed
- `swapcontext` is better suited than `setjmp`/`longjmp`: it saves and restores contexts atomically, naturally fitting coroutine switching
- `makecontext` supports passing 1–5 `unsigned long` arguments, satisfying 64-bit pointer-safe parameter passing

**Alternatives compared:**

| Approach | Pros | Cons |
|----------|------|------|
| `ucontext` (adopted) | POSIX standard, cross-platform | Marked deprecated in glibc 2.16+ (still fully supported on Linux) |
| `setjmp`/`longjmp` | Lighterweight | Cannot save register context, does not support "resume from middle" |
| `makecontext` + `swapcontext` (adopted) | Standard API, full context | Same as above |
| Hand-written assembly | Optimal performance | Platform-bound, high maintenance cost |

**Conclusion:** `ucontext` is the best compromise — stable on Linux/x86_64 with a clean API.

---

## 2. Why mmap + PROT_NONE Instead of malloc Stack?

**Decision:** Allocate coroutine stacks via `mmap(PROT_NONE)` and activate the usable region with `mprotect`.

**Rationale:**
- **Stack overflow detection:** `PROT_NONE` guard pages trigger SIGSEGV/SIGBUS; the signal handler can catch the overflow and return an error code instead of crashing
- **Memory control:** Does not depend on `pthread_attr_setstacksize()`; all stack space is fully controlled by the library
- **Clear address space:** `mmap`-returned addresses are queryable via `stack_info()`, aiding debugging

**Stack layout design:**
```
[PROT_NONE]  ← Bottom guard page (prevents downward stack overflow)
[PROT_NONE]  ← Second guard page
[PROT_RW]    ← Usable stack space
[PROT_NONE]  ← Top guard page (prevents upward stack overflow)
```

**Alternatives compared:**

| Approach | Pros | Cons |
|----------|------|------|
| `mmap` + PROT_NONE (adopted) | Precise control, guard page detection | Requires signal handling, higher complexity |
| `pthread_attr_setstacksize` | Simple | No guard pages, overflow crashes directly |
| `malloc` large buffer | Simple | No overflow detection, wastes memory |
| `sigaltstack` | POSIX standard | Only for signal handlers, not for coroutine stacks |

---

## 3. Why Per-Thread Instead of Global Scheduler?

**Decision:** `g_scheduler` (scheduler context) and `g_guard_jmp` are declared as `_Thread_local`.

**Rationale:**
- `ucontext_t` contains thread stack pointers and register state; using it across threads causes stack corruption (SIGSEGV)
- Testing revealed: if `g_scheduler` is a global variable and coroutines are resumed from thread pool worker threads, `swapcontext` uses the main thread's scheduler context, causing a glibc malloc assertion failure (`chunk_is_mmapped` conflict)
- `_Thread_local` ensures each thread has its own scheduler; coroutines can be used safely within thread pool workers

**Performance impact:** The first coroutine API call per thread allocates a 128KB scheduler stack (`malloc`), which is then reused across coroutines on that thread. It is freed explicitly at thread exit by `loom_coro_exit()` (called by pool workers every loop iteration), and any remaining stacks are freed at process exit by `coro_atexit()`.

**Concurrency requirement:** Because pool workers may legally run coroutines, the scheduler-stack registry (a global linked list) is mutated concurrently by `ensure_scheduler()` (append) and `loom_coro_exit()` (unlink) from many threads. All registry operations are serialized by `g_scheduler_lock` — without it, concurrent list mutation corrupted `next` pointers and caused heap corruption at process exit.

---

## 4. Why Single Lock Instead of Sharded Locks?

**Decision (2026-08-07, superseded for the hot path):** The priority lanes were originally protected by a single `pthread_mutex_t`. The hot path has since moved off this lock entirely: NORMAL-priority tasks go through a lock-free Vyukov ring, and workers drain the ring in batches into lock-free per-worker Chase-Lev deques (see the work-stealing spec). The single pool lock now serializes only the non-NORMAL lanes, shutdown/resize coordination, and spill-back.

**Historical rationale (why a single lock was right at the time):**
- Queue operations were O(1) linked-list insert/delete; lock hold time was short
- Multi-lock/sharded-lock designs add code complexity and bug risk, plus require additional routing logic
- For most application scenarios (tasks with significant granularity), a single lock is not a bottleneck
- Cache-line alignment ensures locks and other hot fields do not share cache lines, reducing false sharing

**When to consider sharded locks:** If task granularity is extremely small (nanosecond-level) and concurrency is very high (thousands of workers), consider splitting the queue into N independent locks. The lock-free ring + per-worker deques already provide this sharding without explicit lock partitioning.

---

## 5. Why Does `destroy` Reject NULL-Pointer Arguments?

**Decision:** `loom_coro_destroy(loom_coroutine_t **coro)` handles NULL `coro` by checking `!*coro`, but does not accept garbage pointers like `0xDEAD`.

**Rationale:**
- Accepting a NULL `coro` pointer itself (the double pointer) is a common C API convention that simplifies caller code
- Accepting arbitrary addresses (e.g., `0xDEAD`) would dereference invalid memory, causing SIGSEGV
- Current implementation: `if (!coro || !*coro) return;` handles both NULL cases

**Design trade-off:**
- Strict mode: Add pointer validity checks (e.g., verify address is within mmap range), but increases overhead and cannot detect all invalid pointers
- Lenient mode: Rely on callers to guarantee valid pointers (current choice)

---

## 6. Shared and Static Library Support

**Decision:** Both `libloomworks.a` (static) and `libloomworks.so` (shared, SOVERSION 1) are built by CMake and usable at runtime.

**Rationale:**
- Modern toolchains (`gcc`/`clang` with default `-fPIC` for `-fPIE`-linked code) handle `_Thread_local` correctly in shared objects, so the historical `R_X86_64_TPOFF32` relocation error no longer applies in practice
- Verified on Linux/x86_64: a test program linked against the shared library successfully creates a pool, submits tasks, and runs coroutines
- Static linking remains the default recommendation where redistribution simplicity matters; the shared library is available for plugin/dynamic-loading use cases

**Historical note:** Earlier versions of this document claimed `.so` was unsupported because coroutines use `_Thread_local` storage and shared-object linking produced `relocation R_X86_64_TPOFF32 against 'g_current' can not be used when making a shared object`. That limitation applied to `local-exec` TLS model builds; the default CMake configuration compiles fine.

---

## 7. Guard Page Layout — Top/Bottom Asymmetry

**Decision:** Coroutine stacks use `[GUARD][usable][GUARD][GUARD]` — one guard above the usable region and two guards below (`LOOMWORKS_CORO_GUARD_PAGES_EACH = 1u`, bottom guard count `guard_nb = 2`).

**Rationale:**
- The top guard (high address, first to be hit by an upward overflow of a normal growing stack) is the primary detection page
- Two guards at the bottom provide a buffer: even if the stack overflows past the first bottom guard, it does not immediately touch adjacent mmap regions before the handler fires
- Exact guard counts are defined in `coroutine.h` (`LOOMWORKS_CORO_GUARD_PAGES_EACH`) and applied in `src/coroutine.c`

**Signal handler logic:** Only access to the first page (`base`) or the last page (`end-ps`) triggers `longjmp`. Access to an intermediate PROT_NONE page re-installs the default handler and crashes, ensuring true out-of-bounds accesses are not silently ignored.

---

## 8. Why `uintptr_t → unsigned long` Cast for makecontext?

**Decision:** `makecontext(&ctx, entry, 1, (unsigned long)(uintptr_t)coro)`.

**Rationale:**
- POSIX `makecontext` requires arguments of type `unsigned long`, with a maximum of 5 arguments
- On x86_64 Linux, `unsigned long` is 64-bit and can hold a pointer without loss
- Using `(uintptr_t)` as an intermediate cast ensures correct pointer-to-integer semantics, rather than direct truncation
- This approach works correctly in both ILP32 (32-bit) and LP64 (64-bit) data models

**64-bit safety verification:**
```c
// Correct: uintptr_t → unsigned long (guaranteed lossless conversion)
makecontext(&ctx, entry, 1, (unsigned long)(uintptr_t)coro);

// Incorrect: direct truncation of 64-bit pointer to 32-bit
makecontext(&ctx, entry, 1, (unsigned long)coro);  // May truncate on ILP32
```

---

## 9. Why Not Use C11 `_Generic` for Unified API?

**Decision:** Thread pool and coroutine APIs remain in separate namespaces (`loom_pool_*` vs `loom_coro_*`).

**Rationale:**
- `_Generic` selectors are complex to implement in C11 and provide poor IDE autocomplete
- Separate namespaces are clearer and easier to understand and maintain
- The two subsystems have very different functionality; a unified API adds little value

---

## 11. Future Work

The following enhancements are planned for future releases:

| Item | Description | Priority |
|------|-------------|----------|
| **Task priority queue** | Bucketized per-priority FIFO queue (256 buckets + occupancy bitmap), O(1) enqueue/dequeue — **DONE (2026-08-08)** | — |
| **Lock-free NORMAL fast path** | Vyukov bounded ring with spill to the priority lanes — **DONE (2026-08-09)** | — |
| **Task cancellation** | Open-addressing cancel index: `cancel` / `cancel_by_id` / `cancel_all` — **DONE (2026-08-09)** | — |
| **Semaphore wakeup** | POSIX counting semaphore replaces condvar-based worker wakeup — **DONE (2026-08-08)** | — |
| **Profiling hooks** | Metrics API: submitted/started/completed/cancelled/failed counters, latency, snapshot — **DONE (2026-08-08)** | — |
| **Pool runtime health** | `loom_pool_active_count` / `idle_count` / `utilization` — **DONE (2026-08-08)** | — |
| **Resizable worker pool** | `loom_pool_resize()` implemented and tested — **DONE** | — |
| **Coroutine stack pooling** | Reuse mmap'd coroutine stacks across create/destroy (cap 64, exact-size match) — **DONE (2026-08-10)** | — |
| **Scheduler-stack lifecycle** | Explicit per-thread free (`loom_coro_exit`) + locked registry — **DONE (2026-08-10)** | — |
| **Epoch-based reclamation** | Superseded by the bucketized O(1) queue + lock-free node pool — **CLOSED (2026-08-08)** | — |
| **Ring acceptance scaling gate** | "worker_scaling-8 ≥ worker_scaling-1" — met under the parallel-workload benchmark (see ring-acceptance spec §8); residual single-producer inversion addressed by the work-stealing scheduler — **DONE** | — |
| **Work-stealing scheduler** | Per-worker Chase-Lev deques + bulk ring→deque batches + FIFO cross-worker steal — **DONE (2026-08-12)** | — |
| **Windows support** | Port to Windows using SwitchToThread + VirtualAlloc | Low |
| **Valgrind integration** | Register coroutine stacks with Valgrind to eliminate false leaks | Medium |

---

## 10. Cache-Line Alignment Strategy

**Decision:** All locks, queue pointers use `__attribute__((aligned(64)))` or padding fields for separation.

**Rationale:**
- Modern CPU cache line size is 64 bytes (x86_64)
- Multi-threaded concurrent access to different fields in the same cache line causes false sharing, with 10–100× performance degradation
- Queue head/tail pointers are separated from locks onto different cache lines, ensuring worker dequeue operations do not contend with main-thread lock operations

**Layout verification (current):**
```c
struct loom_thread_pool {
    // scalars: worker_count, stack_size, queue_capacity, …
    __attribute__((aligned(64)))
    pthread_mutex_t lock;       // cache line 0 — lane buckets, submit funnel
    pthread_cond_t  drain_cond; // cache line 1 — shutdown completion
    sem_t           work_sem;   // counting semaphore — worker wakeup
    pthread_cond_t  space_cond; // bounded-queue capacity wait
    _Atomic bool shutdown; _Atomic bool draining; _Atomic bool joined;
    loom_task_t *buckets_head[256]; loom_task_t *buckets_tail[256];
    _Atomic uint64_t nonempty_bits[4]; _Atomic uint32_t queue_len;
    // ring (head/tail/cells/mask/count), cancel slots, node pool (ABA stack)
    _Atomic uint32_t active_workers; _Atomic uint64_t next_task_id;
    pthread_t *threads; uint32_t max_worker_count;
};
```

Note: per-worker context structures (`loom_worker_ctx_t`) were an early
design experiment and were removed; workers initially shared one queue layer
(lanes + ring) and one `work_sem`. The work-stealing scheduler (2026-08-12)
re-introduced per-worker state in lock-free form: each worker owns a Chase-Lev
deque (256 slots, `_Atomic top` + relaxed `bottom`), so the per-worker state
carries no mutex and false-sharing is contained by the deque's cache-line
padding. The queue layer is now three-tier: priority lanes (locked), Vyukov
ring (lock-free), per-worker deques (lock-free).

---

## 12. Why Hand-Written Assembly Context Backend (x86-64 + aarch64)?

**Decision:** replace POSIX `ucontext` as the default context backend with
hand-written assembly on x86-64 (SysV) and aarch64 (AAPCS64), keeping
ucontext as a compile-time fallback (see decision 1, which remains the
fallback path).

**Rationale:**
- macOS removed `ucontext` — Apple Silicon has no `ucontext.h` — so a
  portable default cannot rely on it
- The glibc `getcontext`/`swapcontext` pair is comparatively slow for a hot
  resume/yield path; the asm backend keeps the switch to a handful of moves
- A hand-written assembly context backend needs only the callee-saved GPRs
  plus the FP control words (mxcsr + x87cw on x86-64; fpcr + fpsr on
  aarch64) — far less than the fxsave-class state ucontext preserves
- The `ctx_smoke` stress test proves semantic parity: 100 000 swap
  round-trips with register corruption checks, FP control round-trip across
  yields, and the fn-return trampoline (include-test on both backends)

**Trade-off:** FP *control* state round-trips a yield, but FP *register*
state (vector registers) does not; ucontext does preserve it. No coroutine
in this library performs FP arithmetic across a yield today, and the value
registers are not observable through the public API.

**Both arches in lockstep:** the backend is only as trustworthy as the
weakest port, and aarch64 is exercised in CI via cross-compilation and QEMU
execution, not just syntax checks.

**Verified parity:** the same test suites are run through ucontext in CI
and must print byte-identical assertion counts to the asm backend. The
NULL-link return path exits with `EXIT_SUCCESS` on both backends, matching
glibc's observed behaviour for a `makecontext` function that returns.

---

## 13. Strict Destroy Lifecycle Gates (2026-08-17)

**Decision:** every destroy entry point rejects misuse with an error code
instead of silently proceeding:

- `loom_pool_destroy` — rejected with `LOOMWORKS_ERR_INVALID` unless
  `loom_pool_shutdown()` ran first (handle left untouched).
- `loom_future_destroy` — rejected with `LOOMWORKS_ERR_INVALID` while the
  future is not yet complete; a worker may still write the result into the
  future's storage, so freeing it early would trample freed memory.
- `loom_coro_destroy` — rejected with `LOOMWORKS_ERR_INVALID` unless the
  coroutine is quiescent (state `NEW`, `DONE` or `ERROR`). Destroying a
  running or suspended coroutine frees its stack mid-switch; the frame is
  still executing on it.

**Rationale:** all three signatures were widened from `void` to
`loom_result_t` / `loom_coro_result_t` so misuse is observable and testable.
The gates force the documented lifecycle (`submit → wait/shutdown →
destroy`) instead of relying on the caller getting it right, and existence
tests (`test_pool_destroy_without_shutdown`, destroy-pending-future,
destroy-running-coroutine) lock the behaviour in.

**Trade-off:** callers that previously ignored the return value now have a
diagnosable error path — a source-compatible change (returning a value does
not break callers that discard it).

---

## 14. Cancelled Futures Complete with `LOOMWORKS_ERR_CANCELLED` (2026-08-17)

**Decision:** when a pending future's task is cancelled before it runs, the
future completes with `LOOMWORKS_ERR_CANCELLED` (result value stays
`NULL`); both `loom_future_wait` and `loom_future_wait_timeout` return it.
Destroying the future is then legal — it has completed.

**Rationale:** cancellation is cooperative: the task is merely removed from
the queue, the future is never signalled by a worker, so a waiter would
otherwise block forever. Completing the future with the cancellation code
makes the cancellation observable and guarantees waiters never strand. It
also keeps the strict destroy gate (decision 13) workable — every future
eventually reaches a terminal state.

**Trade-off:** a waiter cannot distinguish "cancelled" from "task ran and
returned NULL" via the result alone; it must check the error code. That is
the point — the two cases are genuinely different and callers should treat
them differently.

---

## 15. Worker-Crash Detection via `clean_exit` Flag + `FAILED` Metric (2026-08-17)

**Decision:** each worker slot carries a `thread_clean_exit[]` flag, set by
the worker itself on its normal exit paths and checked by the joining side.
If a worker dies without the flag (crash, or `pthread_exit` inside a task),
`loom_pool_shutdown` fires `LOOMWORKS_METRIC_FAILED`; `loom_metrics_failed`
counts it.

**Rationale:** `pthread_join` alone cannot distinguish a clean exit from a
crash — it returns success either way, and ESRCH only appears if the join
races the exit. A dedicated flag set at the worker's own intentional exit
paths gives the joining side ground truth: flag absent → the worker is
gone without the pool's consent. This closes the pre-existing gap where a
worker that died mid-task was silently indistinguishable from an idle one.

**Trade-off:** the flag lives in pool-owned storage indexed by slot, so
resize grow/shrink must realloc and reset it (mirroring `thread_alive`).

---

## 16. Monotonic Timeout Clocks (2026-08-17)

**Decision:** all timeout waits use `CLOCK_MONOTONIC`: the condition
variables for queue space and futures are created with a monotonic
`pthread_condattr_t` via `pthread_once`, and `clock_gettime` reads
`CLOCK_MONOTONIC` for both `wait_for_space` and
`loom_future_wait_timeout`. Deadline arguments in the public headers are
documented as monotonic.

**Rationale:** `CLOCK_REALTIME` steps when an admin or NTP changes the
clock; a deadline computed from a realtime clock can then expire instantly
(a step backward) or hang far past its deadline (a step forward). Monotonic
time advances at a steady rate, so a `deadline`/timeout is a reliable
upper bound on the wait.

**Trade-off:** deadlines are no longer wall-clock values — callers that
want "wait until 14:00" semantics must translate explicitly. Timeout-based
code (the common case) is unaffected and becomes more predictable. The
pipeline module's condvar was left on the default clock; its waits are
private and out of scope for this hardening pass.

---

## 17. Group Self-Wait Rejection via `loom_pool_current` TLS (2026-08-17)

**Decision:** a `_Thread_local` holds the pool of the worker enclosing the
currently executing code (`g_current_pool`, set by `worker_entry`,
readable via the internal `loom_pool_current()`). `loom_task_group_wait`
and `loom_task_group_destroy` compare it to the group's backing pool and
return `LOOMWORKS_ERR_INVALID` when they match.

**Rationale:** a worker that blocks on its own pool's group wait loops on
`g->pending > 0` while every task on that group needs a worker slot to
run — the pool starves and the wait never completes. The TLS costs one
store per worker start and is race-free (thread-local). Both group entry
points were widened to return `loom_result_t` so the rejection is
observable.

**Trade-off:** the guard cannot cover an *indirect* stall (a worker waiting
on a different thread's pool) — that remains a documented caller
contract: wait/destroy must come from a thread that does not participate in
the group's tasks.

## 18. Test-Only Allocation Fault-Injection Hook (2026-08-17)

Fault-injected resize failures must be deterministic and must route through
the same error handling as a real OOM. A one-shot armed counter
(`loom_test_arm_alloc_failure`) is checked at each guarded allocation
call-site inside `loom_pool_resize`; the (n+1)-th check after arming fires
once and auto-disarms, so a single misfire cannot cascade (the lane-only
degrade on a deque-slot calloc failure must not fail the subsequent
realloc). Unarmed, the check is a single negative atomic branch — zero
behavior change. The hook is declared only in the internal header; no
public API is added.

The suite it enables found five real defects (stale `clean_exit` on
rollback, a rollback join deadlock, lane-only mode stranding ring tasks,
freed uninitialized fallback memory, and a work-steal parity gap that could
strand tasks on even worker counts) — each fixed and locked with a
regression test, so the hook pays for itself as a resealable test seam.

## 19. Pipeline Payload Ownership Flag (2026-08-17)

Internal-consumer pipelines freed every payload by default, which is correct
for fire-and-forget callers but a foot-gun for callers that reclaim
payloads. Ownership is now explicit at creation: `loom_pc_create_ex()` takes
a flags bitmask, and `LOOM_PC_OWN_PAYLOADS` promises the library never calls
`free()` on a payload. The discard handler — the caller's cleanup hook — can
be installed atomically at creation (it still fires on the internal worker
thread during consumption and on the destroy drain). The combination
ownership flag + internal pool + no handler is rejected at creation because
it can only leak; with no internal pool the flag is a no-op.

The old entry point is preserved as a zero-flag wrapper, so payload-owning
callers migrate by opting into `create_ex` — the free-by-default behavior
never silently changes for existing users.

## 20. Portable join via clean_exit poll (2026-08-18)

`pthread_tryjoin_np` is a GNU extension absent from macOS/BSD, so the three
resize/shrink join loops were the library's last hard GNU dependency
(`_GNU_SOURCE` was defined unconditionally in `thread_pool.c`). The shipped
fix does not shim tryjoin at all: each loop polls the library's own
`thread_clean_exit[idx]` atomic flag — set by `worker_entry` on every exit
path before breaking out — and then calls `pthread_join`, which returns
immediately once the flag is set. This is platform-neutral, cheaper than a
probe, and removes the feature-test-macro dependency entirely.

A `pthread_kill(thread, 0)` probe was tried first and falsified on glibc: a
thread that has exited but not yet been joined returns 0 (errno EINVAL),
never ESRCH, so a simulated EBUSY never clears and the join loop spins
forever on the fallback path. The new internal `src/portability.h` only
owns `_POSIX_C_SOURCE` (before any system header) and the
`MAP_ANONYMOUS`/`MAP_ANON` fallback. The CMake switch
`LOOMWORKS_POSIX_FALLBACK=ON` forces the portable path on Linux so CI can
prove both code paths behave identically (build-posix ctest 4/4, same
assertion counts).
