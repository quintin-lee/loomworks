# loomworks — Architecture & Design Review

**Date:** 2026-08-17
**Scope:** Architecture/design review only — no code changes, no new spec, no implementation
**Reviewed at:** `b4e4796`

---

## 1. Executive Summary

loomworks is a mature, well-disciplined C11 concurrency library (~14k LOC, v1.0.1). The design reads like an academic reference implementation: Vyukov ring buffers, Chase-Lev work-stealing deques, ABA-tagged Treiber stacks, priority lanes with O(1) bitmap dispatch, and a pluggable context-switching backend with hand-written assembly on two ISAs. Code quality gates are exemplary: `-Wall -Wextra -Werror -pedantic`, full sanitizer matrix, valgrind, QEMU cross-execution, byte-exact backend-parity, and — most recently — deterministic fault-injection suites.

**Overall verdict: healthy, well-architected, production-minded.** The remaining issues are *documentation drift* and *one pre-existing test flake* — not design defects.

---

## 2. Subsystem Review

### 2.1 Thread Pool (core, `thread_pool.c` ~2000 LOC) — ★★★★★
- **Priority dispatch**: 256 buckets (REALTIME=0 … LOW=10) + packed bitsets + ctz → O(1) lane selection. A flood of NORMAL tasks cannot starve REALTIME/HIGH because workers bypass the lane when `p <= 4` (Step 0 in drain order) — a subtle, well-thought starvation guard.
- **Queue tiering**: Vyukov bounded ring (seq-protocol lock-free) as NORMAL fast path, spill-to-lane on full; per-worker Chase-Lev deques (LIFO local / FIFO steal, bulk-claim 8). Three-tier drain order: deque → ring → random steal → lowest lane.
- **Lifecycle**: graceful shutdown posts work_sem exactly worker_count times; workers *never re-sleep* post-shutdown (sched_yield loop) — a documented deadlock-avoidance invariant. Resize is careful (token reposting + tryjoin loop) and now covered by a deterministic fault-injection suite (R6 closed).
- **Cancellation**: open-addressing cancel index (0=EMPTY/1=TOMBSTONE). Clean, but see R5.
- **Memory**: ABA-tagged Treiber node pool avoids the classic pointer-tagging ABA hazard. 64-byte cache-line alignment everywhere (`_Alignas(64)`).

### 2.2 Coroutines (~600 LOC + asm backends) — ★★★★☆
- Guard pages (mprotect PROT_NONE) at both ends of every stack; SIGSEGV/SIGBUS → longjmp into an error state instead of crashing the process. Signal handlers now **chain** prior handlers correctly (R1 closed).
- 128 KiB `_Thread_local` scheduler stack, stack pool (≤64 exact-size matches, zero syscall on hit).
- **Assembly context backend**: x86-64 SysV + aarch64 AAPCS64 in lockstep, saves callee-saved GPRs + FP *control* words (not full fxsave-class state), ucontext fallback, `_Static_assert` offset locks against the `.S` files, `.note.GNU-stack` non-exec. Semantics verified byte-identical to glibc — including the surprising NULL-link → `exit(EXIT_SUCCESS)` discovery (R2 mitigated).
- Trade-off: FP *register* state does not round-trip a yield — only control words do. Documented; today no coroutine does FP math across a yield.

### 2.3 Pipeline / 2.4 Task Group / 2.5 Metrics — ★★★★☆
Thin, correct layers over the pool. Pipeline's internal-consumer/discard-handler contract and metrics' CAS-guarded latency aggregation are sound. Task group gained timed wait (`wait_timeout`, R4 closed). Recent hardening added worker-crash detection (FAILED metric), `ERR_CANCELLED` on cancelled futures, destroy guards, and CLOCK_MONOTONIC timeouts.

---

## 3. Concurrency & Memory Model

- Mutex+condvar for structure, atomics for hot paths (ring seq, deque, active_workers, metrics).
- Single-scheduler-per-thread `_Thread_local` + owner-thread enforcement (cross-thread resume/terminate → `ERR_INVALID`) keeps coroutine lifecycle single-threaded by construction — the strongest safety property in the library.
- Timeout waits moved to CLOCK_MONOTONIC — no more NTP-slew exposure (R9 closed).
- Remaining accepted risks: metrics callback executes on a worker thread (R3, documented "must be cheap"); `cancel()` is O(n) linear scan (R5).

---

## 4. QA & Verification — ★★★★★

- **CI**: gcc/clang × Debug/Release, ASan/TSan/UBSan, valgrind (`--error-exitcode` + suppressions), aarch64 cross-compile **executed under QEMU**, and a ucontext-parity job proving backend-identical assertion counts.
- **Empirical counts**: coroutine 5603 and ctx_smoke 200014 are deterministic; thread_pool ~12.5k and integration ~78.7k drift ± with timing (documented criterion: 0 failed).
- **Fault injection**: resize grow-path fault-injection suite closes R6 with deterministic regression locks.
- Heads-up: `test_thread_pool` has a **pre-existing ~20% shutdown-drain / resize-spill flake** (`tests/test_thread_pool.c:2871`, `:2925`) — fires identically on both backends, unrelated to the asm work, but will periodically red CI until fixed.

---

## 5. Findings (ordered by materiality)

| # | Finding | Severity | Type |
|---|---|---|---|
| F1 | `test_thread_pool` shutdown-drain/resize-spill flake (~20% of runs, line 2871/2925) | **Medium** — periodic red CI | Test bug |
| F2 | `architecture.md` §3.5 (swap diagram, L322-334), §7.3 (L371-372, L526), §10 (L607) still use ucontext/swapcontext terminology | Low | Doc drift |
| F3 | README stale: project-structure omits `src/ctx_*.S` + `tests/ctx_smoke.c`; POSIX note (L258) still claims ucontext(3) required (now fallback-only); assertion counts slightly behind current | Low | Doc drift |
| F4 | `compile_commands.json` stale | Low | Tooling |
| F5 | Accepted design risks, documented: R3 metrics callback on worker thread (must be cheap); R5 `cancel()` O(n) linear scan | Info | Accepted risk |

Closed since initial review: R1 (handler chaining), R2 (asm backend), R4 (timed group wait), R6 (resize fault-injection), R9 (monotonic clocks).

No critical design defects, no memory-safety or concurrency bugs found.

---

## 6. Recommendations (optional follow-up)

1. **Fix the shutdown-drain flake** (F1) — the only thing that can randomly fail CI; deserves a dedicated debugging session.
2. **One documentation de-drift pass** (F2-F3): architecture §3.5/§7.3/§10 → backend-neutral terminology, README counts + structure list + POSIX note.
3. **Refresh compile_commands** (F4) — trivial.
4. Leave F5 as documented, accepted design decisions.