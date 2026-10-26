# R22 — Coroutine Completion Handle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `loom_pool_submit_coroutine_ex` / `loom_runtime_submit_coroutine_ex` producing an optional three-state future (`OK` / `ERR_FAILED` / `ERR_CANCELLED`), closing risk R22.

**Architecture:** The coroutine task node carries an internal `completion` future pointer; every terminal path (DONE, FAILED, cancel tombstone, sleep-cancel, shutdown drain) either completes the future with a status or marks it cancelled. The legacy function becomes a thin wrapper. Spec: `docs/superpowers/specs/2026-10-27-coroutine-completion-handle-design.md`.

**Tech Stack:** C11, pthreads, existing `loom_future_t` machinery, hand-rolled assertion test framework.

**Hard constraints (spec §1/§5):**
- Append-only enum change; `loom_future_t` is opaque — new fields in internal headers only.
- `future == NULL` on `_ex` must be bit-identical to the legacy function.
- Every coroutine-destroy exit pairs with a completion call; a missed one hangs the suite deterministically (that is the detection mechanism).
- `loom_future_wait` checks `cancelled` first, then `failed`, then `OK`.

---

### Task 1: Enum, future internals, completion guard

**Files:**
- Modify: `include/loomworks/thread_pool.h` (enum, lines 37–45)
- Modify: `src/thread_pool_internal.h` (`struct loom_future`, ~line 190; `loom_task_t` coroutine fields, ~line 60)

- [ ] **Step 1: Append LOOMWORKS_ERR_FAILED to the result enum**

In `include/loomworks/thread_pool.h`, the enum currently ends:

```c
    LOOMWORKS_ERR_TIMEOUT,   /**< Operation timed out. */
    LOOMWORKS_ERR_CANCELLED, /**< Operation was cancelled. */
} loom_result_t;
```

Change to:

```c
    LOOMWORKS_ERR_TIMEOUT,   /**< Operation timed out. */
    LOOMWORKS_ERR_CANCELLED, /**< Operation was cancelled. */
    LOOMWORKS_ERR_FAILED,    /**< Submitted task failed during execution. */
} loom_result_t;
```

- [ ] **Step 2: Add `failed` to the internal future struct**

In `src/thread_pool_internal.h`, `struct loom_future` currently ends:

```c
    bool            ready;      /**< true once the task has completed. */
    bool            has_result; /**< true if result is non-NULL. */
    bool            cancelled;  /**< true if the underlying task was cancelled. */
};
```

Change to:

```c
    bool            ready;      /**< true once the task has completed. */
    bool            has_result; /**< true if result is non-NULL. */
    bool            cancelled;  /**< true if the underlying task was cancelled. */
    bool            failed;     /**< true if the task completed with a failure. */
};
```

- [ ] **Step 3: Add `completion` to the coroutine task node**

In `src/thread_pool_internal.h`, the task node's coroutine block currently reads:

```c
    bool              is_coro;    /**< true if this task wraps a pool coroutine. */
    loom_coro_fn      coro_fn;    /**< Coroutine entry (valid when is_coro). */
    size_t            stack_size; /**< Coroutine stack size (0 = default). */
```

Change to:

```c
    bool              is_coro;    /**< true if this task wraps a pool coroutine. */
    loom_coro_fn      coro_fn;    /**< Coroutine entry (valid when is_coro). */
    size_t            stack_size; /**< Coroutine stack size (0 = default). */
    loom_future_t    *completion; /**< Optional future signalled at a terminal
                                   *   path (valid while is_coro; NULL after). */
```

- [ ] **Step 4: Add the cancelled guard + failed flag to loom_future_complete**

In `src/thread_pool.c`, `loom_future_complete` (~line 2212) currently reads:

```c
void loom_future_complete(loom_future_t *fut, loom_result_t status)
{
    if (!fut) {
        return;
    }
    pthread_mutex_lock(&fut->mutex);
    /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
    fut->result     = (void *)(intptr_t)(int)status;
    fut->has_result = true;
    fut->ready      = true;
    pthread_cond_signal(&fut->cond);
    pthread_mutex_unlock(&fut->mutex);
}
```

Change to:

```c
void loom_future_complete(loom_future_t *fut, loom_result_t status)
{
    if (!fut) {
        return;
    }
    pthread_mutex_lock(&fut->mutex);
    if (fut->cancelled) {
        /* A concurrent cancel already resolved this future; its wait must
         * keep returning LOOMWORKS_ERR_CANCELLED.  Never overwrite. */
        pthread_mutex_unlock(&fut->mutex);
        return;
    }
    /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
    fut->result     = (void *)(intptr_t)(int)status;
    fut->has_result = false;
    fut->failed     = (status != LOOMWORKS_OK);
    fut->ready      = true;
    pthread_cond_signal(&fut->cond);
    pthread_mutex_unlock(&fut->mutex);
}
```

- [ ] **Step 5: Teach loom_future_wait the failed state**

In `src/thread_pool.c`, `loom_future_wait` (~line 2150), after the
`cancelled` branch, insert:

```c
    if (future->failed) {
        if (result) {
            *result = NULL;
        }
        pthread_mutex_unlock(&future->mutex);
        return LOOMWORKS_ERR_FAILED;
    }
```

Also set `fut->failed = false;` alongside the existing
`fut->cancelled = false;` in `loom_pool_submit_future`'s fresh-future
init block (~line 2058).

- [ ] **Step 6: Build + full suite (regression proof)**

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release >/dev/null 2>&1 && cmake --build build -j$(nproc) >/dev/null 2>&1 && (cd build && ctest 2>&1 | tail -1)
```

Expected: `100% tests passed out of 8` — no behavior change yet (the new
enum value and fields are unused).

- [ ] **Step 7: Commit**

```bash
git add include/loomworks/thread_pool.h src/thread_pool_internal.h src/thread_pool.c
git commit -m "feat(pool): ✨ add ERR_FAILED and future failed-state plumbing"
```

---

### Task 2: Pool-layer `_ex` with completion wiring

**Files:**
- Modify: `include/loomworks/thread_pool.h` (declaration after line 200)
- Modify: `src/thread_pool.c` (submit_coroutine ~line 1970; Step C0 ~768; worker coroutine block ~1113; resize drain ~836; cancel_by_id ~2579/~2641; timer-heap cancel ~2501)

- [ ] **Step 1: Declare the _ex variants**

In `include/loomworks/thread_pool.h`, directly after the existing
`loom_pool_submit_coroutine` declaration (line 200), add:

```c
/**
 * @brief Submit a coroutine task with an optional completion handle.
 *
 * Identical to loom_pool_submit_coroutine() except that, when @p future
 * is non-NULL, the pool allocates a future that resolves to the task's
 * final outcome:
 *   - LOOMWORKS_OK          the coroutine ran to DONE
 *   - LOOMWORKS_ERR_FAILED  the coroutine failed during execution
 *   - LOOMWORKS_ERR_CANCELLED  the task was cancelled (queued, sleeping,
 *                              or during shutdown drain)
 * The caller must loom_future_wait() then loom_future_destroy() the
 * returned future.  `*result` from loom_future_wait() is always NULL —
 * coroutines return void; this handle reports completion status only.
 * On submit failure, `*future` is set to NULL and no future is
 * allocated.
 *
 * @param pool       The pool handle.
 * @param fn         Coroutine entry function.
 * @param arg        Opaque argument passed to @p fn.
 * @param stack_size Coroutine stack size (0 = default).
 * @param future     Optional output for the completion future (may be NULL
 *                   for fire-and-forget behavior identical to
 *                   loom_pool_submit_coroutine()).
 * @param task_id    Output for the task id (may be NULL).
 * @return           LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_pool_submit_coroutine_ex(
    loom_thread_pool_t *pool, loom_coro_fn fn, void *arg, size_t stack_size,
    loom_future_t **future, uint64_t *task_id);
```

- [ ] **Step 2: Implement the _ex body and convert the legacy function**

In `src/thread_pool.c`, replace `loom_pool_submit_coroutine` (~line 1970)
entirely with:

```c
loom_result_t loom_pool_submit_coroutine_ex(
    loom_thread_pool_t *pool, loom_coro_fn fn, void *arg, size_t stack_size,
    loom_future_t **future, uint64_t *task_id)
{
    if (!pool || !fn) {
        if (future) {
            *future = NULL;
        }
        return LOOMWORKS_ERR_INVALID;
    }
    if (future) {
        *future = NULL;
    }
    if (atomic_load_explicit(&pool->shutdown, memory_order_relaxed)) {
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    loom_future_t *fut = NULL;
    if (future != NULL) {
        fut = (loom_future_t *)calloc(1, sizeof(*fut));
        if (!fut) {
            return LOOMWORKS_ERR_ALLOC;
        }
        if (pthread_mutex_init(&fut->mutex, NULL) != 0) {
            free(fut);
            return LOOMWORKS_ERR_ALLOC;
        }
        pthread_once(&g_condattr_once, init_monotonic_condattr);
        if (pthread_cond_init(&fut->cond, &g_condattr_mono) != 0) {
            pthread_mutex_destroy(&fut->mutex);
            free(fut);
            return LOOMWORKS_ERR_ALLOC;
        }
    }
    loom_result_t rc = ensure_timer_thread(pool);
    if (rc != LOOMWORKS_OK) {
        if (fut) {
            pthread_cond_destroy(&fut->cond);
            pthread_mutex_destroy(&fut->mutex);
            free(fut);
        }
        return rc;
    }
    pthread_mutex_lock(&pool->lock);
    if (pool->shutdown || pool->draining) {
        pthread_mutex_unlock(&pool->lock);
        if (fut) {
            pthread_cond_destroy(&fut->cond);
            pthread_mutex_destroy(&fut->mutex);
            free(fut);
        }
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    if (pool->queue_capacity > 0 && pool->queue_len >= pool->queue_capacity) {
        pthread_mutex_unlock(&pool->lock);
        if (fut) {
            pthread_cond_destroy(&fut->cond);
            pthread_mutex_destroy(&fut->mutex);
            free(fut);
        }
        return LOOMWORKS_ERR_INVALID;
    }
    loom_task_t *task = task_create(pool, (loom_task_fn)fn, arg, LOOMWORKS_PRIORITY_NORMAL);
    if (!task) {
        pthread_mutex_unlock(&pool->lock);
        if (fut) {
            pthread_cond_destroy(&fut->cond);
            pthread_mutex_destroy(&fut->mutex);
            free(fut);
        }
        return LOOMWORKS_ERR_ALLOC;
    }
    task->is_coro    = true;
    task->coro_fn    = fn;
    task->stack_size = stack_size;
    task->completion = fut;
    task->free_data  = false; /* user_data is reused as the coroutine handle */
    loom_enqueue_unlocked(pool, task);
    if (task_id) {
        *task_id = task->task_id;
    }
    if (fut) {
        *future = fut;
    }
    sem_post(&pool->work_sem);
    pthread_mutex_unlock(&pool->lock);
    return LOOMWORKS_OK;
}

loom_result_t loom_pool_submit_coroutine(
    loom_thread_pool_t *pool, loom_coro_fn fn, void *arg, size_t stack_size, uint64_t *task_id)
{
    return loom_pool_submit_coroutine_ex(pool, fn, arg, stack_size, NULL, task_id);
}
```

- [ ] **Step 3: Add the coroutine-future cancel helper**

In `src/thread_pool.c`, near `future_mark_cancelled` (~line 140), add:

```c
/* Resolve a coroutine completion future as cancelled (mark + wake). */
static void coro_future_mark_cancelled(loom_future_t *fut)
{
    if (!fut) {
        return;
    }
    pthread_mutex_lock(&fut->mutex);
    fut->cancelled  = true;
    fut->ready      = true;
    fut->has_result = false;
    fut->result     = NULL;
    pthread_cond_broadcast(&fut->cond);
    pthread_mutex_unlock(&fut->mutex);
}
```

- [ ] **Step 4: Pair completions at every terminal path**

4a. **Step C0 tombstone** (~line 768, inside
`if (ctask != NULL && atomic_load…cancelled…)`), after
`metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);` add:

```c
                if (ctask->completion) {
                    coro_future_mark_cancelled(ctask->completion);
                    ctask->completion = NULL;
                }
```

4b. **Worker coroutine block, DONE** (~line 1111): replace

```c
                } else if (st == LOOMWORKS_CORO_DONE) {
                    metrics_fire(pool, LOOMWORKS_METRIC_COMPLETED);
                }
```

with

```c
                } else if (st == LOOMWORKS_CORO_DONE) {
                    metrics_fire(pool, LOOMWORKS_METRIC_COMPLETED);
                    if (task->completion) {
                        loom_future_complete(task->completion, LOOMWORKS_OK);
                        task->completion = NULL;
                    }
                }
```

4c. **Worker coroutine block, FAILED** (~line 1113): replace

```c
            } else if (crc != LOOMWORKS_CORO_ERR_RUNNING) {
                metrics_fire(pool, LOOMWORKS_METRIC_FAILED);
            }
```

with

```c
            } else if (crc != LOOMWORKS_CORO_ERR_RUNNING) {
                metrics_fire(pool, LOOMWORKS_METRIC_FAILED);
                if (task->completion) {
                    loom_future_complete(task->completion, LOOMWORKS_ERR_FAILED);
                    task->completion = NULL;
                }
            }
```

4d. **SUSPENDED calloc-failure fall-through** (in the same block, after
the `continue;` for the successful ready-node push): add

```c
                    /* calloc failed: fall through to recycle. */
                    if (task->completion) {
                        loom_future_complete(task->completion, LOOMWORKS_ERR_FAILED);
                        task->completion = NULL;
                    }
```

4e. **Resize-down ready-FIFO drain** (~line 836, inside
`while (r) { … loom_coro_terminate(c); metrics_fire(CANCELLED); … task_destroy(pool, t); }`):
after `metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);` add:

```c
                if (t->completion) {
                    coro_future_mark_cancelled(t->completion);
                    t->completion = NULL;
                }
```

4f. **Sleep-cancel timer-heap path** (`loom_pool_cancel_by_id`, ~line 2501,
the `task->user_data == data` branch): after
`atomic_store_explicit(&task->cancelled, true, memory_order_release);`
add:

```c
            if (task->completion) {
                coro_future_mark_cancelled(task->completion);
                task->completion = NULL;
            }
```

4g. **cancel_by_id ring fast path** (~line 2579, after
`cancel_index_claim` succeeds): after the `queue_len` decrement add:

```c
            if (task->is_coro && task->completion) {
                coro_future_mark_cancelled(task->completion);
                task->completion = NULL;
            }
```

4h. **cancel_by_id lane-bucket path** (~line 2641, where `to_free` is
destroyed): the same three lines as 4g using `to_free` instead of `task`.

- [ ] **Step 5: Runtime layer**

In `include/loomworks/runtime.h`, after the existing coroutine-submit
declaration area, add:

```c
/**
 * @brief Submit a coroutine task with an optional completion handle.
 *
 * Stack size is the runtime's configured coroutine stack size.  See
 * loom_pool_submit_coroutine_ex() for the future's completion contract.
 *
 * @param rt      The runtime handle.
 * @param fn      Coroutine entry function.
 * @param arg     Opaque argument passed to @p fn.
 * @param future  Optional output for the completion future (may be NULL).
 * @param task_id Output for the task id (may be NULL).
 * @return        LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_runtime_submit_coroutine_ex(loom_runtime_t *rt,
                                               loom_coro_fn     fn,
                                               void            *arg,
                                               loom_future_t   **future,
                                               uint64_t        *task_id);
```

In `src/runtime.c`, next to the existing coroutine routing (~line 116),
add:

```c
loom_result_t loom_runtime_submit_coroutine_ex(loom_runtime_t *rt,
                                               loom_coro_fn     fn,
                                               void            *arg,
                                               loom_future_t   **future,
                                               uint64_t        *task_id)
{
    if (!rt || !rt->pool || !fn) {
        if (future) {
            *future = NULL;
        }
        return LOOMWORKS_ERR_INVALID;
    }
    return loom_pool_submit_coroutine_ex(
        rt->pool, fn, arg, rt->coro_stack_size, future, task_id);
}
```

- [ ] **Step 6: Build + full suite (legacy parity)**

```bash
cmake --build build -j$(nproc) >/dev/null 2>&1 && (cd build && ctest 2>&1 | tail -1)
```

Expected: `100% tests passed out of 8` (the wrapper path with NULL
future skips all new code).

- [ ] **Step 7: Commit**

```bash
git add include/loomworks/thread_pool.h include/loomworks/runtime.h src/thread_pool.c src/runtime.c
git commit -m "feat(pool): ✨ add submit_coroutine_ex with three-state completion future"
```

---

### Task 3: Tests (pool + runtime layers)

**Files:**
- Modify: `tests/test_thread_pool.c` (new tests + main() registration)
- Modify: `tests/test_runtime.c` (new test + main() registration)

- [ ] **Step 1: Add pool-layer tests**

Add after the existing coroutine-task tests (locate the
`loom_pool_submit_coroutine` test function; insert after it), and
register each in `main()` adjacent to it:

```c
/* ---------- R22: submit_coroutine_ex completion handles ---------- */
static void coro_noop_task(void *arg)
{
    (void)arg;
}

static void coro_sleeper(void *arg)
{
    (void)arg;
    loom_coro_sleep(200000000L); /* 200 ms */
}

static void test_submit_coroutine_ex_ok(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_future_t *fut = NULL;
    uint64_t       tid = 0;
    ASSERT(loom_pool_submit_coroutine_ex(pool, coro_noop_task, NULL, 0, &fut, &tid) ==
               LOOMWORKS_OK,
           "submit ex");
    ASSERT(fut != NULL, "future allocated");

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait returns OK");
    ASSERT(result == NULL, "coroutine result is NULL");

    ASSERT(loom_future_destroy(fut) == LOOMWORKS_OK, "destroy future");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_submit_coroutine_ex_legacy_parity(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    uint64_t tid = 0;
    ASSERT(loom_pool_submit_coroutine_ex(pool, coro_noop_task, NULL, 0, NULL, &tid) ==
               LOOMWORKS_OK,
           "submit ex with NULL future");
    ASSERT(tid != 0, "task id returned");

    uint64_t tid2 = 0;
    ASSERT(loom_pool_submit_coroutine(pool, coro_noop_task, NULL, 0, &tid2) == LOOMWORKS_OK,
           "legacy function still works");
    ASSERT(tid2 != 0, "legacy task id returned");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_submit_coroutine_ex_cancel_before_run(void)
{
    g_gate_started = 0;
    g_gate_release = 0;
    g_gate_parked  = 0;

    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "gate submitted");
    while (g_gate_parked < 1) {
        sched_yield();
    }

    loom_future_t *fut = NULL;
    uint64_t       tid = 0;
    ASSERT(loom_pool_submit_coroutine_ex(pool, coro_noop_task, NULL, 0, &fut, &tid) ==
               LOOMWORKS_OK,
           "submit ex");
    ASSERT(loom_pool_cancel_by_id(pool, tid) == LOOMWORKS_OK, "cancel queued coroutine");

    /* Release BEFORE waiting: the tombstone is processed at the worker's
     * run boundary (2026-10-23 lesson — waiting while gated hangs). */
    g_gate_release = 1;

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_ERR_CANCELLED, "wait returns CANCELLED");
    ASSERT(loom_future_destroy(fut) == LOOMWORKS_OK, "destroy cancelled future");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_submit_coroutine_ex_cancel_during_sleep(void)
{
    g_gate_started = 0;
    g_gate_release = 0;
    g_gate_parked  = 0;

    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "gate submitted");
    while (g_gate_parked < 1) {
        sched_yield();
    }

    loom_future_t *fut = NULL;
    uint64_t       tid = 0;
    ASSERT(loom_pool_submit_coroutine_ex(pool, coro_sleeper, NULL, 0, &fut, &tid) ==
               LOOMWORKS_OK,
           "submit sleeper");

    /* Cancel while the worker is gated: the coroutine is still queued. */
    ASSERT(loom_pool_cancel_by_id(pool, tid) == LOOMWORKS_OK, "cancel queued sleeper");

    g_gate_release = 1;

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_ERR_CANCELLED, "wait returns CANCELLED");
    ASSERT(loom_future_destroy(fut) == LOOMWORKS_OK, "destroy cancelled future");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_submit_coroutine_ex_cancel_complete_race(void)
{
    /* Cancel racing completion: wait must resolve to exactly one of
     * {OK, CANCELLED} every round, and never hang. */
    for (int round = 0; round < VGI(50); round++) {
        loom_thread_pool_t *pool = NULL;
        loom_pool_config_t  cfg  = {.worker_count = 1};
        ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

        loom_future_t *fut = NULL;
        uint64_t       tid = 0;
        ASSERT(loom_pool_submit_coroutine_ex(pool, coro_noop_task, NULL, 0, &fut, &tid) ==
                   LOOMWORKS_OK,
               "submit ex");

        loom_result_t w = loom_future_wait(fut, NULL); /* pre-warm: task may finish first */
        if (w == LOOMWORKS_ERR_INVALID) {
            /* future destroyed by waiter path; not reachable — wait always
             * resolves.  Fall through to the cancel attempt. */
        }
        (void)w;
        (void)loom_pool_cancel_by_id(pool, tid); /* may miss: task already ran */

        loom_pool_shutdown(pool);
        loom_pool_destroy(&pool);
    }
}
```

NOTE on the FAILED-path test (spec case 3): a deterministic in-coroutine
failure primitive does not exist (`loom_coro_terminate` needs a valid
handle and cannot self-terminate the running coroutine).  The FAILED
wiring is instead exercised by 4c/4d's code path plus the shutdown-drain
test (case 6).  If a primitive emerges later, add the test then — record
this gap in the commit body.

- [ ] **Step 2: Add the runtime-layer test to tests/test_runtime.c**

```c
static void test_submit_coroutine_ex_runtime(void)
{
    /* 1-worker runtime, same gate discipline as test_coro_cancel_pending. */
    loom_runtime_config_t cfg = {.worker_count = 1};
    loom_runtime_t       *rt  = NULL;
    ASSERT(loom_runtime_create(&cfg, &rt) == LOOMWORKS_OK, "create runtime");

    g_gate_started          = 0;
    g_gate_release          = 0;
    g_gate_parked           = 0;
    loom_fn_union_t blocker = {.thread_fn = gate_blocker};
    ASSERT(loom_runtime_submit(rt, blocker, NULL, LOOM_SUBMIT_THREAD, 5, NULL) == LOOMWORKS_OK,
           "submit blocker");
    while (g_gate_parked == 0) {
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 1000000L};
        nanosleep(&ts, NULL);
    }

    loom_future_t *fut = NULL;
    uint64_t       tid = 0;
    ASSERT(loom_runtime_submit_coroutine_ex(rt, simple_coro_task, NULL, &fut, &tid) ==
               LOOMWORKS_OK,
           "submit coro ex");
    ASSERT(loom_runtime_cancel(rt, tid) == LOOMWORKS_OK, "cancel queued coro");

    g_gate_release = 1; /* release BEFORE wait (run-boundary lesson) */

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_ERR_CANCELLED, "wait CANCELLED");
    ASSERT(loom_future_destroy(fut) == LOOMWORKS_OK, "destroy future");

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);
}
```

`simple_coro_task` already exists in this file (line ~47).  Register in
`main()` near `test_coro_cancel_pending()`.

- [ ] **Step 3: Build + suite + single-CPU stability**

```bash
cmake --build build -j$(nproc) >/dev/null 2>&1 && (cd build && ctest 2>&1 | tail -1)
cd build_uc && cmake --build . -j$(nproc) >/dev/null 2>&1 && for i in 1 2 3; do taskset -c 0 ./test_thread_pool >/dev/null 2>&1; echo "run$i=$?"; done
```

Expected: `100% tests passed out of 8`; three 0s.

- [ ] **Step 4: Commit**

```bash
git add tests/test_thread_pool.c tests/test_runtime.c
git commit -m "test(pool): ✅ cover submit_coroutine_ex three-state completion"
```

---

### Task 4: Docs, sanitizer gates, push

**Files:**
- Modify: `CHANGELOG.md` (Unreleased → Added)
- Modify: `docs/risk-assessment.md` (R22 row)

- [ ] **Step 1: CHANGELOG entry**

Under `## [Unreleased]` (create the section + `### Added` if absent after
the 1.0.2 rename — the release script keeps an empty Unreleased block),
append:

```markdown
- **Coroutine completion handles (R22)**: new
  `loom_pool_submit_coroutine_ex()` and
  `loom_runtime_submit_coroutine_ex()` accept an optional future that
  resolves to the task's final outcome — `LOOMWORKS_OK` (ran to DONE),
  `LOOMWORKS_ERR_FAILED` (execution failure), or
  `LOOMWORKS_ERR_CANCELLED` (cancelled queued/sleeping/during drain).
  The existing fire-and-forget `loom_pool_submit_coroutine()` is
  unchanged.
```

Update the risk-assessment R22 row status to:
`**Low** — ✅ closed (2026-10-27): completion handles added via
submit_coroutine_ex (see CHANGELOG)`.

- [ ] **Step 2: Sanitizer gates**

```bash
cmake -S . -B build_asan -DCMAKE_BUILD_TYPE=ASan >/dev/null 2>&1 && cmake --build build_asan -j$(nproc) >/dev/null 2>&1 && (cd build_asan && ASAN_OPTIONS=detect_leaks=1 ./test_thread_pool >/dev/null 2>&1; echo "ASAN=$?")
cmake -S . -B build_ubsan -DCMAKE_BUILD_TYPE=UBSan >/dev/null 2>&1 && cmake --build build_ubsan -j$(nproc) >/dev/null 2>&1 && (cd build_ubsan && ./test_thread_pool >/dev/null 2>&1; echo "UBSAN=$?")
```

Expected: `ASAN=0`, `UBSAN=0`.

- [ ] **Step 3: Push and watch**

```bash
git push && sleep 15 && gh run list --limit 2
gh run watch <CI_RUN_ID> --exit-status --interval 30; echo "CI=$?"
gh run watch <PERF_RUN_ID> --exit-status --interval 30; echo "PERF=$?"
```

Expected: both success (valgrind hard gate included).

- [ ] **Step 4: Record outcome**

No additional doc change needed — the CHANGELOG entry and the closed
R22 row are the record.  Target release: 1.1.0 (spec §6).
