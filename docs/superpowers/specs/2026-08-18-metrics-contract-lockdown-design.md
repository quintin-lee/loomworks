# R3 Metrics Callback Contract Lockdown Design

**Date:** 2026-08-18
**Status:** Approved (user, m0779)
**Scope:** Test + documentation only. Zero API changes, zero behavior changes.

## 1. Background

Risk register R3 (Medium/Medium/Medium, maintenance priority #1):
> Callback fires synchronously on the worker thread; must be cheap or it throttles the pool.

The library contract (documented in `metrics.h` / `metrics.c` headers) is:
- `loom_metrics_fire` is called on the **worker thread** (or submitting/joining thread) for STARTED/COMPLETED/SUBMITTED/CANCELLED/FAILED events
- The callback runs **outside the pool lock** — every one of the 12 `metrics_fire` call sites in `thread_pool.c` fires after `pthread_mutex_unlock(&pool->lock)` (verified during exploration)
- The callback must be **cheap and non-blocking**; a slow callback throttles that worker by design

Residual R3 risk is **contract drift**, not a current defect: nothing in the test suite proves the callback runs on a worker thread, runs lock-free, or that events are never dropped. A future refactor could move a `metrics_fire` call inside the lock, or a callback could start blocking, and no test would notice.

## 2. Goal

Lock the contract with regression tests that fail (or hang, exposing via ctest timeout) if the contract is violated. No mechanism change — the synchronous, worker-thread, lock-free contract is a documented design choice, not a bug.

## 3. Design

### 3.1 New test fixtures (`tests/test_thread_pool.c`)

File-scope additions near the existing metrics test section:

```c
/* ---------- Test: metrics callback contract ---------- */
static pthread_t         g_metric_cb_thread;   /* last thread that ran the callback */
static pthread_t         g_main_thread;
static volatile int      g_cb_pending_ok;      /* callback called pending_count() successfully */
static _Atomic int       g_cb_calls;           /* total callback invocations */
static metrics_event_ctx_t g_cb_ctx;           /* event counts from the contract callback */
```

New callback:

```c
static void metrics_contract_cb(loom_metric_event_t event, const loom_thread_pool_t *pool,
                                void *user_data)
{
    (void)event;
    (void)user_data;
    g_metric_cb_thread = pthread_self();
    atomic_fetch_add_explicit(&g_cb_calls, 1, memory_order_relaxed);
    /* The contract: this callback runs OUTSIDE the pool lock. pending_count()
     * takes the pool lock — if the callback were invoked under the lock this
     * would deadlock (and the test would hang → ctest timeout). */
    g_cb_pending_ok = (loom_pool_pending_count(pool) >= 0) ? 1 : g_cb_pending_ok;
}
```

Note: `loom_pool_pending_count` signature is `uint32_t loom_pool_pending_count(const loom_thread_pool_t *pool)` — the callback's `pool` parameter is `const loom_thread_pool_t *`, so no cast is needed. `>= 0` is always true for a successful call; failure would be a crash or hang, not a return code.

### 3.2 New tests

**T1 `test_metrics_callback_on_worker_thread`**
1. `g_main_thread = pthread_self();` `g_metric_cb_thread = 0;` (sentinel)
2. Create pool (2 workers, capacity 0), `metrics_event_ctx_t ctx = {0}`, `loom_metrics_create(pool, metrics_contract_cb, &ctx, &metrics)`
3. Reset gates (`g_gate_started = 0; g_gate_release = 0; g_gate_parked = 0;`)
4. Submit `gate_task` (parks), spin `while (!g_gate_started) sched_yield();`
5. Submit one `increment_task`, then `g_gate_release = 1;`
6. `loom_pool_shutdown(pool)` (joins workers, drains)
7. Assert: `!pthread_equal(g_metric_cb_thread, g_main_thread)` — the callback ran on a worker thread, not the main thread
8. `loom_metrics_destroy(&metrics); loom_pool_destroy(&pool);`

**T2 `test_metrics_callback_outside_lock`**
1. Create pool (2 workers), metrics with `metrics_contract_cb`
2. `g_cb_pending_ok = 0; g_cb_calls = 0;`
3. Submit 4 `increment_task` tasks; `loom_pool_shutdown(pool)` (drains)
4. Assert: `g_cb_pending_ok == 1` (pending_count() succeeded from inside the callback → callback ran lock-free)
5. Assert: `atomic_load(&g_cb_calls) >= 4` (STARTED+COMPLETED+SUBMITTED for 4 tasks, ≥ 4 invocations — proves not a single lucky call)
6. Destroy

**T3 `test_metrics_callback_lifecycle_counts`**
1. Create pool (2 workers), metrics with the existing counting callback (`metrics_event_callback`, which fills `metrics_event_ctx_t`)
2. Reset gates; submit 1 `gate_task` + 2 `increment_task`; wait `g_gate_parked == 1`; release
3. `loom_pool_shutdown(pool)`
4. Assert: `ctx.submitted == 3`, `ctx.completed == 3`, `ctx.cancelled == 0`, `ctx.failed == 0` — the synchronous lock-free callback never drops events
5. Destroy

### 3.3 Docs

- `docs/risk-assessment.md`: R3 register row → `✅ closed (2026-08-18): contract locked by regression tests — callback runs on worker thread, outside the pool lock, events never dropped; synchronous worker-thread callback is a documented design choice`. Detailed section: add Status ✅ CLOSED paragraph describing the three tests.
- `docs/api-reference.md` §5 Metrics: state the contract explicitly: callback is invoked synchronously on the worker thread (or the submitting/joining thread) outside the pool lock; it must be cheap and non-blocking; the contract is regression-locked by tests.
- `docs/design-decisions.md`: append decision 21 (synchronous lock-free callback contract; why test-locking instead of async queue — async would change timing semantics, break the documented contract, add queue memory and new threading surface).
- `CHANGELOG.md`: [Unreleased] bullet under a suitable section: metrics callback contract locked by regression tests.

## 4. Verification

- GNU path: `cmake --build build --parallel 4` + `ctest --test-dir build --output-on-failure` — expect 4/4, test_thread_pool count increases by the new asserts, **0 failed**
- Fallback path: `cmake --build build-posix --parallel 4` + `ctest --test-dir build-posix --output-on-failure` — expect 4/4 same new count
- Baselines (must stay green / only increase): GNU test_thread_pool 20745/0, coroutine 5611/0, integration 78763/0 (±40), ctx_smoke 200014

## 5. Commits

- Code + tests: `test(metrics): lock callback contract (worker thread, lock-free, no drops)` — `tests/test_thread_pool.c`
- Docs: `docs: record metrics callback contract lockdown (R3 closed)` — `docs/risk-assessment.md`, `docs/api-reference.md`, `docs/design-decisions.md`, `CHANGELOG.md`

## 6. Acceptance Criteria

- AC1: T1 asserts callback thread != main thread
- AC2: T2 asserts pending_count() succeeds from inside the callback (lock-free proof) and callback invoked ≥ 4 times
- AC3: T3 asserts submitted/completed/cancelled/failed counts exact (no dropped events)
- AC4: Both build paths ctest 4/4, 0 failures
- AC5: No public API changes; no behavior changes
- AC6: R3 register row + detail + docs updated
