# Shutdown-Drain Flake Fix Design

Date: 2026-08-17
Status: Draft (pending user review)
Type: Test-fix spec (test-side race elimination)
Authors: Sisyphus + user (brainstorming session)

## 1. Background

The v1.0.1 design review (`docs/superpowers/specs/2026-08-15-concurrency-semantics-hardening-design.md` and the architecture review at HEAD `c15ce20`) recorded finding **F1**: `test_thread_pool` flakes ~20% of the time. Symptom clusters:

- `FAIL at tests/test_thread_pool.c:3504` — `test_shutdown_drains_deque`'s observer spun 40M times without ever seeing queue-resident tasks
- `FAIL at :3558` — `test_resize_down_spills_deque`'s spill observer, same pattern
- `FAIL at :781` / `:942` — task-group "wait from own worker" rejection asserts

F1 was suspected to be a product defect in the shutdown-drain / resize-spill / work-stealing paths. This session's deep-dive disproves that: **the product code is correct; F1 is two independent test-side races.** DIAG instrumentation proved at every trip the pool was fully drained with zero lost tasks (`counter=300`, `ring_count=0`, `deque_total=0`, `deques[0].len=0`).

## 2. Root-Cause Analysis

### Class A — task-group "wait from own worker" races (≈50% of failures)

`loom_task_group_wait_timeout` correctly rejects a wait issued from one of the group-pool's own workers (`src/task_group.c:437-439`, via `loom_pool_current()`), returning `LOOMWORKS_ERR_INVALID`. Two tests race against that:

| Test | Bug |
|------|-----|
| `test_task_group_wait_from_worker` (`tests/test_thread_pool.c:786-800`) | `g_group_wait_rc = -1` reset **after** `loom_task_group_submit`; a fast worker writes `ERR_INVALID` before main reaches the reset, main then clobbers the observed value with `-1`, and the assert compares `-1` vs `ERR_INVALID` |
| `test_task_group_wait_timeout_from_worker` (`tests/test_thread_pool.c:947-960`) | identical reset-after-submit race |

The sibling `test_task_group_destroy_from_worker` already has the correct pattern — reset *before* submit.

### Class B — drain-observation races (≈50% of failures)

`test_shutdown_drains_deque` and `test_resize_down_spills_deque` intend "call shutdown/resize while tasks are deque-resident." Mechanically: a `gate_task` parks the worker, 300 tasks sit in the ring, main sets `g_gate_release=1`, then spins on `deques[0].len > 0` (shutdown) / `deque_total > 0` (resize) with a 40M-iteration safety valve.

The deque is only non-empty for a **sub-microsecond window** while the worker is mid-drain between bulk pulls. If main is descheduled right after release (or the worker bursts through all 300 tasks before main observes), main spins 40M times on an already-empty deque → false FAIL at the safety-valve assert (`:3511` / `:3574`). DIAG instrumentation (`counter=300` at every trip) proves all tasks ran — a pure observer race.

Deterministic freeze is impossible by design: a worker blocked in a gate task holds its deque *empty* (the gate was popped), and any waiter-style task runs *last* (LIFO pop order) after the deque self-drains. So the fix cannot park the worker with a non-empty deque — it must make the drain **observable**.

## 3. Goals

- G1. Eliminate Class A: `g_group_wait_rc` reset happens before submit in both racy tests (mirroring `destroy_from_worker`).
- G2. Eliminate Class B: the deque-residency observation window is widened from sub-µs to ~10 ms so the observer reliably catches drain residency.
- G3. Preserve each test's intent (shutdown/resize must run deque-resident and spill tasks exactly once; group wait from worker must be rejected).
- G4. No product-code or public-API changes — the pool implementation is correct.

## 4. Design

### 4.1 Class A — reset before submit

Move `g_group_wait_rc = -1;` ahead of `loom_task_group_submit(...)` in both tests. Two one-line moves, no semantics change. After a worker write or main's reset, the value is always the submitted task's outcome; the assert then compares `ERR_INVALID` — deterministic either way.

### 4.2 Class B — slow observation task

Add a resident helper next to `simple_task` in `tests/test_thread_pool.c`:

```c
static void slow_count_task(void *arg)
{
    volatile long sink = 0;
    for (long i = 0; i < 65536L; i++) sink += i;   /* ~20-40 us */
    (void)sink;
    volatile int *counter = (volatile int *)arg;
    __sync_fetch_and_add(counter, 1);
}
```

Use it in the two drain/spill tests instead of `simple_task`. 300 tasks × ~30 µs ≈ **10 ms continuously non-empty deque**, vs. the observer's ~60 ms 40M-spin window. Deque residency is now reliably observable; the spill/drain invariants (`counter == 300`, zero lost tasks) are still asserted afterward. Suite cost: +20–40 ms.

*Why not stronger alternatives?* `sched_yield()` in the spin loops is strictly weaker (still racy if the worker finishes before main's first observation). A second gate to freeze a non-empty deque is impossible per §2's LIFO argument.

### 4.3 Class C — known environmental flake (observed, not fixed)

`test_steal_trigger` (`tests/test_thread_pool.c:3590`) uses a 10 s time-deadline for its ring-drain observation and its own comment (`:3624-3628`) acknowledges workers may be preempted mid-drain on a loaded box. Under extreme host load (load average 31.8 on 8 cores, from parallel ASAN+LTO link of an unrelated project, two qemu processes, and an index worker) it missed the deadline once in 36 runs. It is untouchable by this fix (my diff doesn't modify it; the added busy-wait totals ~30–60 ms, far below the 10 s deadline). Left as-is; known-rare, load-dependent.

## 5. Implementation (completed)

Single file changed: `tests/test_thread_pool.c` (`git diff`: +20/−4).

1. Added `slow_count_task` helper after `simple_task` (L42).
2. `test_shutdown_drains_deque`: submit loop → `slow_count_task` (L3505).
3. `test_resize_down_spills_deque`: submit loop → `slow_count_task` (L3559).
4. `test_task_group_wait_from_worker`: reset moved before submit (L789).
5. `test_task_group_wait_timeout_from_worker`: reset moved before submit (L950).

## 6. Verification

Debug build `/tmp/opencode/flake_build/test_thread_pool`, rebuilt clean. **36 full runs** (each ~25 s, ~12720–12734 assertions):

- 35/36 `rc=0`, `Results: 127xx passed, 0 failed`
- **0 Class A failures, 0 Class B failures** — the pre-fix baseline was ~20% (~6/30) across these exact tests
- The single failure (`fix_35`) was the Class C environmental steal-deadline miss under load 31.8 (see §4.3)

Evidence: `/tmp/opencode/fix_loop.log` (runs 1–16, all clean), `/tmp/opencode/fix_loop2.log` (runs 17–36, clean except `fix_35`); run logs `/tmp/opencode/fix_*.log`.

## 7. Open Questions / Follow-ups

- Q1. Class C: should `test_steal_trigger`'s 10 s deadline also get a slow-task-style stabilization, or a longer deadline, in a separate follow-up?
- Q2. Transition this spec to a writing-plans implementation plan, or is the (already implemented and verified) change acceptable as-is?

## 8. Status

Draft — pending user review. Ready to transition to `writing-plans` on approval.