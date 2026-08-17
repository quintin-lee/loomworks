# Resize Fault-Injection Design (R6 closure)

**Date:** 2026-08-17
**Status:** Approved (user + inline self-review)
**Scope:** `loom_pool_resize` allocation-failure paths only. No scope creep.

---

## §1 Background

`loom_pool_resize` is the most subtle function in the thread pool (~200 lines): it
grows/shrinks the worker-array family (`threads`, `thread_alive`,
`thread_clean_exit`), the work-stealing deque array and its per-deque slot
arrays, without ever touching the queue accounting. Its failure paths are
partially covered by happy-path and rollback tests, but **no test injects a
mid-operation allocation failure**. Risk-assessment row R6 (Low/High/Low,
"resize ~200 lines subtle") therefore stays open:

> "Test coverage for resize remains partial (grow/shrink/rollback happy paths;
> no systematic fault-injection of mid-realloc alloc failures)."

During design, a latent bug was identified in exactly this surface:

**Stale `thread_clean_exit` after a failed grow.** The grow-rollback join loops
(`thread_pool.c` ~L2001-2006 and ~L2016-2021, the `worker_arg_t` malloc-failure
and `pthread_create`-failure paths) reset `thread_alive[j]` after joining a
worker that was created earlier in the failed call — but NOT
`thread_clean_exit[j]`. A worker created in a failed grow exits through the
resize-shrink spill path, which sets `thread_clean_exit[idx] = true`. After the
rollback the slot therefore holds a stale `true`. A later, successful grow
reuses the same slot for a fresh worker; if that worker *crashes*, the
shutdown join loop reads the stale `true` and silently misses the crash
(`LOOMWORKS_METRIC_FAILED` never fires). The shrink path (~L2053-2054) already
resets both flags; the grow-rollback paths are inconsistent with it.

---

## §2 Goals / Non-Goals

**Goals**

1. Systematic fault-injection coverage of every allocation call-site in
   `loom_pool_resize`'s grow path.
2. Prove the rollback guarantee: after any injected failure, `worker_count`
   is unchanged, the pool stays fully usable (previously submitted tasks all
   run), a later unarmed resize succeeds, and shutdown/destroy are clean.
3. Fix and lock down the stale `thread_clean_exit` bug (regression test).

**Non-Goals**

- No fault injection for `pthread_create` failure (`LOOMWORKS_ERR_THREAD`):
  its rollback is structurally identical to the `worker_arg_t` malloc-failure
  path under test (same loop shape, same asserts), so coverage transfers. A
  second hook would add surface for near-zero new proof.
- No injection in the shrink path (it performs no allocations).
- No public API change. The hook lives in `src/thread_pool_internal.h` and is
  consumed only by the test binary.
- No performance impact when unarmed (a single negative-branch atomic load).

---

## §3 Fault-Injection Hook

### Semantics

A module-level one-shot counter armed by a test-only function:

```c
/* thread_pool.c, module scope */
static _Atomic long g_test_alloc_fail_at = -1;   /* -1 = fault injection disabled */

/* Returns true exactly once: on the (n+1)-th check after being armed with n.
 * Fires once, then self-resets to -1 — a single misfire cannot cascade into
 * subsequent call sites (critical for the lane-only degrade test, where a
 * calloc failure must NOT also fail the following realloc). */
static bool test_alloc_fail_next(void)
{
    long v = atomic_load_explicit(&g_test_alloc_fail_at, memory_order_relaxed);
    if (v < 0) {
        return false;                       /* unarmed: zero-overhead path */
    }
    if (v == 0) {
        atomic_store_explicit(&g_test_alloc_fail_at, -1, memory_order_relaxed);
        return true;                        /* fire this check, then disarm */
    }
    atomic_store_explicit(&g_test_alloc_fail_at, v - 1, memory_order_relaxed);
    return false;
}
```

### Public (internal-header) API

```c
/* thread_pool_internal.h */
void loom_test_arm_alloc_failure(long n);   /* n checks pass, n+1-th fails once; n<0 disarms */
```

Implementation: `atomic_store_explicit(&g_test_alloc_fail_at, n, memory_order_relaxed);`

Tests arm immediately before the resize call on a quiescent pool (no other
thread allocates through the six guarded sites), so the count sequence is
deterministic. Each test starts with a defensive `loom_test_arm_alloc_failure(-1)`.

---

## §4 Injection Points

`loom_pool_resize` grow path, pool(2) → grow(8): positional order of guarded
allocation checks:

| # | Site | Current line | Guard shape | Injected result |
|---|------|--------------|-------------|-----------------|
| 1 | deques array realloc | ~L1926 (gated by `deques != NULL`) | `if (test_alloc_fail_next()) { unlock; return LOOMWORKS_ERR_ALLOC; }` | nothing touched yet → `ERR_ALLOC`, state pristine |
| 2 | per-deque `slots` calloc (loop `old_max..count`) | ~L1941 | `pool->deques[i].slots = test_alloc_fail_next() ? NULL : (loom_task_t **)calloc(...)` | existing `slots_failed` path → **lane-only fallback; resize returns `OK`** (locked contract) |
| 3 | `threads` realloc | ~L1961 | same guard as #1 | `ERR_ALLOC` |
| 4 | `thread_alive` realloc + tail memset | ~L1967 | same guard as #1 | `ERR_ALLOC` |
| 5 | `thread_clean_exit` realloc + tail memset | ~L1976 | same guard as #1 | `ERR_ALLOC` |
| 6 | `worker_arg_t` malloc (spawn loop `old_count..count`) | ~L1995 | `worker_arg_t *wa = test_alloc_fail_next() ? NULL : (worker_arg_t *)malloc(sizeof(*wa));` | existing `if (!wa)` rollback → `ERR_ALLOC`, created workers joined |

All guards route through the *existing* NULL/failure handling — no new error
semantics are invented; injected allocation failure is indistinguishable from
a real OOM.

---

## §5 Fix: stale `thread_clean_exit` after grow rollback

In both grow-rollback join loops add one store, symmetric with the shrink path
(~L2053-2054):

```c
atomic_store_explicit(&pool->thread_alive[j], false, memory_order_release);
atomic_store_explicit(&pool->thread_clean_exit[j], false, memory_order_release);  /* NEW */
```

`atomic_load_explicit(...thread_alive[j], acquire)` must remain the loop
condition (as now). No reset is needed at spawn time: after the rollback reset,
a reused slot starts any later worker with `clean_exit == false`, which is the
correct "has not cleanly exited" baseline for crash detection.

Why this is correct: `clean_exit` is set to `true` only by a worker's own
clean exit paths; the joining side is the ONLY writer that resets it. Reset
after join is safe because the thread handle no longer refers to a live
worker, and `thread_alive[j] == false` prevents the slot from being reused
until a fresh worker is spawned — which then inherits the baseline.

---

## §6 Test Suite (tests/test_thread_pool.c)

New section `/* ---------- Test: resize fault injection ---------- */`.
Arm values are computed from the positional order above (pool(2) → grow(8),
`old_max = old_count = 2`, so `count - old_max = 6`):

### 6.1 `test_resize_alloc_fail_deques_realloc`  — arm 0
- resize(8) → `LOOMWORKS_ERR_ALLOC`; only the deques realloc check runs
- `loom_pool_worker_count(pool) == 2`
- pool usable: submit a task, it runs
- unarmed resize(8) → `OK`; `worker_count == 8`; shutdown; destroy

### 6.2 `test_resize_alloc_fail_deque_slots` — arm 1 (site 2, first calloc)
- resize(8) → `LOOMWORKS_OK` (lane-only degrade is NOT an error)
- `pool->deques == NULL` (test binary already includes
  `../src/thread_pool_internal.h`, so the struct is visible)
- `worker_count == 8`; submitted tasks complete (lane-only works)
- shutdown; destroy — clean

### 6.3 `test_resize_alloc_fail_threads_realloc` — arm 7 (= 1 + 6)
- resize(8) → `ERR_ALLOC`; `worker_count == 2`
- later unarmed resize(8) → `OK`; shutdown; destroy

### 6.4 `test_resize_alloc_fail_alive_realloc` — arm 8 (= 1 + 6 + 1)
- same asserts as 6.3

### 6.5 `test_resize_alloc_fail_clean_exit_realloc` — arm 9 (= 1 + 6 + 2)
- same asserts as 6.3

### 6.6 `test_resize_alloc_fail_worker_arg` — arm 10 (site 6, first new worker)
- resize(8) → `ERR_ALLOC`; no worker was created → `worker_count == 2`;
  `thread_alive[2] == false`
- later unarmed resize(8) → `OK` with 6 new workers; tasks run; shutdown; destroy

### 6.7 `test_resize_alloc_fail_worker_arg_mid` — arm 11 (site 6, second new worker)
- creates worker 2 then fails → rollback joins it → `ERR_ALLOC`;
  `worker_count == 2`; `thread_alive[2] == false`
- later unarmed resize(8) → `OK`; shutdown; destroy

### 6.8 `test_resize_fail_then_worker_crash_detected` — arm 15 (site 6, worker #5)
- **pre-condition:** resize(8) → `ERR_ALLOC`; workers 2..6 were created and
  joined by rollback → slots 2..6 hold stale `clean_exit == true` (pre-fix)
- unarmed resize(8) → `OK` (reuses slots 2..7; slot 7 is fresh)
- submit **6 gated crash tasks** (park on a gate, then `pthread_exit(NULL)`)
  — with 2 original + 6 new workers, exactly 6 tasks one-per-worker
- release gate → all 6 crash
- `loom_pool_shutdown(pool)`; metrics callback must have counted `failed == 6`
- pre-fix: slots 2..6 read stale `true` → only slot 7 fires → `failed == 1` → test RED
- post-fix: all six fire → `failed == 6` → GREEN

> Robustness note: whichever workers claim the 6 tasks, the assertion
> `failed == 6` holds only if EVERY slot that ran a crash has `clean_exit ==
> false`. Post-fix: originals are `false` from pool init, reused slots are
> reset by the rollback fix, slot 7 is fresh — so 6 always. Pre-fix the reused
> slots are stale `true` — so the count is always < 6. The test cannot pass by
> scheduling luck.

### Registration
All 8 tests registered in `main()` after the existing resize tests. Each starts
with defensive `loom_test_arm_alloc_failure(-1)`.

---

## §7 Documentation

- `docs/risk-assessment.md`: R6 register row + detailed section → ✅ RESOLVED
  (2026-08-17): fault-injection suite for every grow-path allocation +
  stale-`thread_clean_exit` fix; maintenance-priority item 3 removed, list
  renumbered.
- `CHANGELOG.md` [Unreleased] ### Fixed: two bullets — (a) grow rollback now
  resets `thread_clean_exit` (crash detection no longer misses workers after a
  failed grow); (b) new resize fault-injection test suite.
- `docs/design-decisions.md`: new decision (18): one-shot armed allocation
  fault-injection hook; lane-only degrade on deque-slot calloc failure remains
  the documented non-error fallback.
- `docs/api-reference.md`: untouched (no public API change).

---

## §8 Acceptance Criteria

- AC1: All 8 new tests pass; pre-fix 6.8 fails (bug reproduced), post-fix green.
- AC2: Rollback guarantee proven for every position: injected failure leaves
  `worker_count` at its prior value, the pool executes previously submitted
  tasks, and an unarmed resize reaches the target size.
- AC3: Lane-only degrade (site 2) returns `OK` with `pool->deques == NULL`, and
  the pool functions lane-only.
- AC4: Unarmed hook is a zero-behavior-change path — full existing suite still
  green (test_thread_pool 12636/0, coroutine 5611/0, integration 78757/0 ±40,
  ctx_smoke 200014).
- AC5: Stale-`thread_clean_exit` fix is regression-locked by 6.8.
- AC6: Docs updated per §7; spec committed; plan written (uncommitted, plans
  dir gitignored); commits follow Conventional Commits.

---

## §9 Verification Sequence

1. TDD: write 8 tests → RED (6.8 must fail on the stale-flag bug; the rest
   fail to link against the not-yet-existing `loom_test_arm_alloc_failure`).
2. Implement hook + guards (6 sites) + rollback fix.
3. `clang-format -i` all edited files, `cmake --build build --parallel 4`
   (rerun once if transient `libloomworks.a` parallel link race).
4. `ctest --test-dir build --output-on-failure` → 4/4.
5. Commit: `fix(pool): reset thread_clean_exit on resize rollback` (hook,
   guards, fix, tests) — or split per plan.
6. Docs commit; final report to user (baselines, commits, bug-fix note).