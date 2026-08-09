# Ring Fast Path Acceptance — Design

**Date:** 2026-08-09
**Status:** Draft (Approach A: worktree A/B baseline comparison)
**Library:** loomworks (C11 thread pool + coroutine library)
**Base:** `dbb9c65` — `feat(thread-pool): ⚡ add lock-free Vyukov ring fast path for NORMAL`

## 1. Motivation

The lock-free Vyukov ring fast path (plan `2026-08-08-thread-pool-lockfree-queue.md`,
Tasks 1–5) is implemented and committed, but its **acceptance is incomplete**:

1. **The hard performance gates were never measured.** The plan's §7/design's §6
   acceptance criteria — `worker_scaling-8 ≥ worker_scaling-1` (the entire point
   of the ring: invert the contention curve) and `submit_latency` decrease —
   have no recorded before/after numbers. Without the benchmark, the ring's
   core claim is unverified.
2. **Plan Task 4/5/6 regression tests were never registered.** Only 2 of the
   plan's 10 ring tests exist in `tests/test_thread_pool.c`
   (`test_ring_basic`, `test_ring_multithread_stress`). The guards for bounded
   exact-capacity, unbounded spill, ring cancel-by-id/data, tombstone-skip,
   priority preemption, and shutdown-drain are missing — the very behaviors the
   implementation claims to support are unprotected against regression.
3. **No acceptance ledger exists.** `.superpowers/sdd/progress.md` has no entry
   for the lock-free queue plan's completion, so the before/after deltas and the
   valgrind environment constraint are undocumented.

Goal: complete the acceptance — add the missing regression guards, run the
A/B benchmark against a clean pre-ring baseline, record the results and the
known constraints, and leave the ring's claims either proven or refuted.

## 2. Approach (A — worktree A/B baseline)

Compare the committed ring (`dbb9c65`) against a **clean pre-ring baseline**
built from `024254d` (the commit immediately before the ring fast path — the
semaphore wakeup is already in place, the ring is not). This isolates the
ring's contribution from the earlier semaphore change.

```
git worktree add /tmp/lw-baseline 024254d
cmake -S /tmp/lw-baseline -B /tmp/lw-baseline/build_plan (same flags as repo)
# baseline bench:  /tmp/lw-baseline/build_plan/examples/bench
# current bench:   build_plan/examples/bench
```

**Why not the recorded spec numbers:** the design §6 table (505K @ 8 workers)
and the plan Task 7 table (227,587 @ 8 workers) disagree with each other and
both predate the current code. A fresh measured baseline removes the ambiguity.

**Load mitigation:** the machine hosts concurrent Codex sessions running ctest
on another project. Each side of the A/B is run with `taskset -c 0-3` pinned to
the same CPUs, 3 runs each, median reported — jitter is then attributable and
comparable.

## 3. Regression guards (plan Tasks 4/5/6 tests)

Add the 8 missing ring tests from the plan (adapting the plan's line anchors to
the current file — `main()` is now @2139, not @2095):

| Test | Guards | Plan source |
|------|--------|-------------|
| `test_ring_bounded_full` | bounded pool (cap 5 → ring 8): 6th submit → `ERR_INVALID`, pending == 5, exactly 5 run | Task 4 |
| `test_ring_unbounded_spill` | unbounded: 4097th submit spills to NORMAL lane, pending == 4097, all run exactly once | Task 4 |
| `test_ring_cancel_by_id` | ring cancel-by-id: middle of 3 cancelled, runs exactly 2, double-cancel → `ERR_INVALID` | Task 5 |
| `test_ring_cancel_not_found` | unknown id → `ERR_INVALID`, no side effects | Task 5 |
| `test_ring_tombstone_skip` | 64 ring tasks cancelled then released: **zero** execute, pending → 0 | Task 5 |
| `test_ring_cancel_data` | `loom_pool_cancel(data)` matches ring tasks by user_data, count == 2 | Task 5 |
| `test_ring_priority_preempt` | 1 worker: RT/HIGH lanes drain before queued NORMAL ring tasks (strict 3-step order) | Task 6 |
| `test_ring_shutdown_drains` | shutdown blocks until all 500 ring tasks complete | Task 6 |

House rules (match existing file): `gate_task`/`ring_*` spin-gate helpers with
`sched_yield`, `ASSERT` macro, producer threads never touch `ASSERT` (collect
failure outside), `RUN_TEST` registration in `main()`, summary counter updates.

**Verification of worker ordering** (already confirmed in code, locked by
`test_ring_priority_preempt`): drain order is `lane_has_priority(pool, 4)` →
`ring_try_dequeue` → only when `!ring_has_work` take p≥5 lanes
(`[worker_entry](src/thread_pool.c:274)`). Shutdown's "empty" definition must
include `ring_count == 0` — the new `test_ring_shutdown_drains` enforces it.

## 4. Benchmark acceptance (plan Task 7)

**Command** (both sides, identical): `taskset -c 0-3 ./<tree>/examples/bench
--iterations 100 --tasks 10000` × 3, median.

**Hard gates** (from the design §6 / plan Task 7):

| Gate | Baseline (measured at `024254d`) | Acceptance |
|------|----------------------------------|------------|
| worker_scaling 8 ≥ worker_scaling 1 | TBD | ring must invert the curve |
| submit_latency avg | TBD | must decrease |

**Soft gates (±10%):** bounded_queue (0/128/1024/8192), future_overhead,
coro_create_destroy (untouched by this change).

**Known constraint:** valgrind tree is environment-blocked (missing glibc
debuginfo, no sudo in container — `memcmp` redirection fails at startup).
Recorded in the ledger as a documented limitation, not a pass.

## 5. Ledger

Append to `.superpowers/sdd/progress.md`: date, commit under test, both hard
gate numbers (before/after), soft-gate deltas, test counts (before/after),
valgrind limitation, and a verdict on the ring's core claim.

## 6. Error handling / risks

- **Bench jitter:** concurrent Codex load → taskset pinning + median of 3;
  if a hard gate is marginal (<5% margin), re-run both sides back-to-back.
- **Test hangs:** the pin-and-fill tests hang on a broken ring — ctest timeout
  catches them; keep the existing per-test expectations.
- **Baseline mismatch:** if `024254d` fails to build with current CMake flags
  (it is the immediate parent of the ring commit; `dbb9c65^` is the same tree,
  so there is no earlier usable pre-ring baseline), fall back to the recorded
  spec §6 numbers (505K @ 8 workers) as a soft reference and note in the ledger
  that the A/B could not be executed.

## 7. Testing (of the acceptance itself)

The acceptance is verification work; its own "tests" are the gates above.
The 8 new tests guard *already-committed* behavior (Tasks 4–6 code landed in
`dbb9c65` even though their tests never did), so they must pass on first run —
the RED requirement applied at plan time when the features were unbuilt.
Sequence: GREEN on plan/asan/ubsan → bench → ledger.

Scope check: single focused deliverable (acceptance of one feature), no
decomposition needed.
