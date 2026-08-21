#define _POSIX_C_SOURCE 200809L
#include "loomworks/runtime.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_passes   = 0;
static int g_failures = 0;

/* Metrics counter used by the test callback (global because C11 has no
 * per-test closure support). Reset in test_metrics_callback. */
static _Atomic uint64_t g_metric_submitted = 0;

/* Gate mechanism for blocking tests: the main thread sets started, then
 * waits until the worker has parked; main then submits queued tasks and
 * calls cancel_all while the worker is still blocked on the gate. */
static _Atomic int g_gate_started = 0;
static _Atomic int g_gate_release = 0;
static _Atomic int g_gate_parked  = 0;

#define ASSERT(expr, msg)                                                                          \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "FAIL: %s at %s:%d\n", msg, __FILE__, __LINE__);                       \
            g_failures++;                                                                          \
        } else {                                                                                   \
            g_passes++;                                                                            \
        }                                                                                          \
    } while (0)

/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */

static void simple_thread_task(void *arg)
{
    int *counter = (int *)arg;
    if (counter) {
        __sync_fetch_and_add(counter, 1);
    }
}

static void simple_coro_task(void *arg)
{
    int *counter = (int *)arg;
    (void)counter;
    loom_coro_yield();
    __sync_fetch_and_add(counter, 1);
}

static void *result_task(void *arg)
{
    (void)arg;
    long *val = (long *)malloc(sizeof(long));
    if (val) {
        *val = 42L;
    }
    return val;
}

/* ------------------------------------------------------------------ */

static void test_create_destroy(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create with defaults");
    ASSERT(rt != NULL, "runtime not null");
    loom_runtime_destroy(&rt);
    ASSERT(rt == NULL, "destroy sets to null");

    /* double destroy is a no-op */
    loom_runtime_destroy(&rt);

    /* NULL out-param is a no-op */
    loom_runtime_destroy(NULL);
}

static void test_submit_thread(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    int             counter = 0;
    uint64_t        tid     = 0;
    loom_fn_union_t fn      = {.thread_fn = simple_thread_task};
    ASSERT(loom_runtime_submit(rt, fn, &counter, LOOM_SUBMIT_THREAD, 5, &tid) == LOOMWORKS_OK,
           "submit thread task");

    /* Wait for task to finish by shutting down. */
    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    ASSERT(counter == 1, "thread task executed");
}

static void test_submit_coro(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    int             counter = 0;
    uint64_t        tid     = 0;
    loom_fn_union_t fn      = {.coro_fn = simple_coro_task};
    ASSERT(loom_runtime_submit(rt, fn, &counter, LOOM_SUBMIT_CORO, 0, &tid) == LOOMWORKS_OK,
           "submit coroutine task");

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    ASSERT(counter == 1, "coroutine task executed");
}

static void test_mixed_submit(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    _Atomic int thread_count = 0;
    _Atomic int coro_count   = 0;

    for (int i = 0; i < 10; i++) {
        loom_fn_union_t thread_fn = {.thread_fn = simple_thread_task};
        loom_fn_union_t coro_fn   = {.coro_fn = simple_coro_task};
        ASSERT(loom_runtime_submit(rt, thread_fn, &thread_count, LOOM_SUBMIT_THREAD, 5, NULL) ==
                   LOOMWORKS_OK,
               "submit thread task");
        ASSERT(loom_runtime_submit(rt, coro_fn, &coro_count, LOOM_SUBMIT_CORO, 0, NULL) ==
                   LOOMWORKS_OK,
               "submit coroutine task");
    }

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    ASSERT((int)atomic_load_explicit(&thread_count, memory_order_relaxed) == 10,
           "all thread tasks executed");
    ASSERT((int)atomic_load_explicit(&coro_count, memory_order_relaxed) == 10,
           "all coroutine tasks executed");
}

/* ------------------------------------------------------------------ */

static void gate_blocker(void *arg)
{
    (void)arg;
    atomic_fetch_add_explicit(&g_gate_parked, 1, memory_order_relaxed);
    g_gate_started = 1;
    while (!g_gate_release) {
        /* spin: hold the worker so later submissions stay queued */
    }
}

static void test_cancel_pending(void)
{
    /* Single-worker runtime guarantees the task stays queued until
     * we cancel it — no race with the worker picking it up first. */
    loom_runtime_config_t cfg = {.worker_count = 1};
    loom_runtime_t       *rt  = NULL;
    ASSERT(loom_runtime_create(&cfg, &rt) == LOOMWORKS_OK, "create runtime (1 worker)");

    /* Block the worker so our task stays queued. */
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

    int             counter = 0;
    uint64_t        tid     = 0;
    loom_fn_union_t fn      = {.thread_fn = simple_thread_task};
    ASSERT(loom_runtime_submit(rt, fn, &counter, LOOM_SUBMIT_THREAD, 5, &tid) == LOOMWORKS_OK,
           "submit thread task");

    /* Cancel before the worker picks it up. */
    ASSERT(loom_runtime_cancel(rt, tid) == LOOMWORKS_OK, "cancel pending task");

    g_gate_release = 1;
    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    ASSERT(counter == 0, "cancelled task did not execute");
}

static void test_cancel_all(void)
{
    /* Use a single-worker runtime so ALL submitted tasks must queue
     * behind the blocker and are guaranteed pending when cancel_all runs. */
    loom_runtime_config_t cfg = {.worker_count = 1};
    loom_runtime_t       *rt  = NULL;
    ASSERT(loom_runtime_create(&cfg, &rt) == LOOMWORKS_OK, "create runtime (1 worker)");

    /* Block the only worker with a gate task so queued tasks stay pending. */
    g_gate_started          = 0;
    g_gate_release          = 0;
    g_gate_parked           = 0;
    loom_fn_union_t blocker = {.thread_fn = gate_blocker};
    ASSERT(loom_runtime_submit(rt, blocker, NULL, LOOM_SUBMIT_THREAD, 5, NULL) == LOOMWORKS_OK,
           "submit blocker");

    /* Wait until the worker has parked on the gate. */
    while (g_gate_parked == 0) {
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 1000000L};
        nanosleep(&ts, NULL);
    }

    /* Submit 5 tasks that will queue behind the blocked worker. */
    int             counter = 0;
    loom_fn_union_t fn      = {.thread_fn = simple_thread_task};
    for (int i = 0; i < 5; i++) {
        loom_runtime_submit(rt, fn, &counter, LOOM_SUBMIT_THREAD, 5, NULL);
    }

    /* Cancel all pending tasks while the worker is still blocked. */
    uint32_t cancelled = 0;
    loom_runtime_cancel_all(rt, &cancelled);
    ASSERT(cancelled >= 1, "at least one task was cancelled");

    /* Release the gate so the worker can finish and shutdown completes. */
    g_gate_release = 1;

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);
}

static void test_submit_future(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    loom_future_t *fut = NULL;
    uint64_t       tid = 0;
    ASSERT(loom_runtime_submit_future(rt, result_task, NULL, &fut, &tid) == LOOMWORKS_OK,
           "submit future task");
    ASSERT(fut != NULL, "future not null");
    ASSERT(tid > 0, "task_id returned");

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait future");
    if (result == NULL) {
        fprintf(stderr, "FAIL: result is NULL at %s:%d\n", __FILE__, __LINE__);
        g_failures++;
        loom_future_destroy(fut);
        loom_runtime_shutdown(rt);
        loom_runtime_destroy(&rt);
        return;
    }
    ASSERT(*(long *)result == 42L, "result value is 42");
    free(result);

    loom_future_destroy(fut);

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);
}

static void test_submit_future_cancel(void)
{
    /* Use a 2-worker runtime: one worker is blocked by the gate,
     * leaving the future task queued behind it so we can cancel before
     * it runs. */
    loom_runtime_config_t cfg = {.worker_count = 2};
    loom_runtime_t       *rt  = NULL;
    ASSERT(loom_runtime_create(&cfg, &rt) == LOOMWORKS_OK, "create runtime");

    /* Park one worker so the future task stays queued. */
    g_gate_started          = 0;
    g_gate_release          = 0;
    g_gate_parked           = 0;
    loom_fn_union_t blocker = {.thread_fn = gate_blocker};
    ASSERT(loom_runtime_submit(rt, blocker, NULL, LOOM_SUBMIT_THREAD, 5, NULL) == LOOMWORKS_OK,
           "submit gate blocker");
    while (g_gate_parked == 0) {
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 1000000L};
        nanosleep(&ts, NULL);
    }

    loom_future_t *fut = NULL;
    uint64_t       tid = 0;
    ASSERT(loom_runtime_submit_future(rt, result_task, NULL, &fut, &tid) == LOOMWORKS_OK,
           "submit future task");
    ASSERT(fut != NULL, "future not null");

    /* Cancel while the future is still queued (worker is blocked by gate). */
    ASSERT(loom_runtime_cancel(rt, tid) == LOOMWORKS_OK, "cancel future task");

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_ERR_CANCELLED, "wait returns cancelled");
    ASSERT(result == NULL, "result is NULL for cancelled future");

    /* Destroying a completed (cancelled) future is now legal. */
    ASSERT(loom_future_destroy(fut) == LOOMWORKS_OK, "destroy cancelled future");

    /* Release the gate so the remaining worker can finish. */
    g_gate_release = 1;

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);
}

static void test_coro_cancel_pending(void)
{
    /* Submit a coroutine task and cancel it before it starts. */
    loom_runtime_config_t cfg = {.worker_count = 1};
    loom_runtime_t       *rt  = NULL;
    ASSERT(loom_runtime_create(&cfg, &rt) == LOOMWORKS_OK, "create runtime (1 worker)");

    /* Block the worker so the coroutine stays queued. */
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

    int             counter = 0;
    uint64_t        tid     = 0;
    loom_fn_union_t fn      = {.coro_fn = simple_coro_task};
    ASSERT(loom_runtime_submit(rt, fn, &counter, LOOM_SUBMIT_CORO, 0, &tid) == LOOMWORKS_OK,
           "submit coroutine task");

    /* Cancel before the worker picks it up. */
    ASSERT(loom_runtime_cancel(rt, tid) == LOOMWORKS_OK, "cancel pending coroutine task");

    g_gate_release = 1;
    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    ASSERT(counter == 0, "cancelled coroutine task did not execute");
}

static void test_resize(void)
{
    loom_runtime_t       *rt  = NULL;
    loom_runtime_config_t cfg = {.worker_count = 2};
    ASSERT(loom_runtime_create(&cfg, &rt) == LOOMWORKS_OK, "create with 2 workers");

    uint32_t wc_before = loom_runtime_worker_count(rt);
    ASSERT(wc_before == 2u, "worker count is 2");

    /* Resize up. */
    ASSERT(loom_runtime_resize(rt, 4) == LOOMWORKS_OK, "resize up to 4");
    uint32_t wc_after = loom_runtime_worker_count(rt);
    ASSERT(wc_after == 4u, "worker count is 4 after resize");

    /* Resize down. */
    ASSERT(loom_runtime_resize(rt, 1) == LOOMWORKS_OK, "resize down to 1");
    ASSERT(loom_runtime_worker_count(rt) == 1u, "worker count is 1 after resize down");

    /* Resize to 0 is rejected. */
    ASSERT(loom_runtime_resize(rt, 0) == LOOMWORKS_ERR_INVALID, "resize to 0 rejected");

    /* NULL runtime is rejected. */
    ASSERT(loom_runtime_resize(NULL, 4) == LOOMWORKS_ERR_INVALID, "NULL resize rejected");

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);
}

static void
metric_submitted_cb(loom_metric_event_t event, const loom_thread_pool_t *pool, void *data)
{
    (void)pool;
    (void)data;
    if (event == LOOMWORKS_METRIC_SUBMITTED) {
        atomic_fetch_add_explicit(&g_metric_submitted, 1, memory_order_relaxed);
    }
}

static void test_metrics_callback(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    /* Reset the global counter so previous tests don't pollute this one. */
    atomic_store_explicit(&g_metric_submitted, 0, memory_order_relaxed);

    /* Wire a metrics collector onto the backing pool so events fire.
     * (loom_runtime_set_metrics_callback only stores metric_cb which is not
     * used by metrics_fire — we attach a collector directly instead.) */
    loom_metrics_t *metrics = NULL;
    ASSERT(loom_metrics_create(loom_runtime_pool(rt), metric_submitted_cb, NULL, &metrics) ==
               LOOMWORKS_OK,
           "create metrics collector");

    loom_fn_union_t fn = {.thread_fn = simple_thread_task};
    ASSERT(loom_runtime_submit(rt, fn, NULL, LOOM_SUBMIT_THREAD, 5, NULL) == LOOMWORKS_OK,
           "submit thread task");

    /* Give worker time to process. */
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 10000000L}; /* 10 ms */
    nanosleep(&ts, NULL);

    ASSERT(g_metric_submitted >= 1, "metrics callback fired");

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);
    loom_metrics_destroy(&metrics);
}

static void test_queries(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    uint32_t wc = loom_runtime_worker_count(rt);
    ASSERT(wc > 0, "worker count > 0");

    uint32_t pending = loom_runtime_pending_count(rt);
    ASSERT(pending == 0, "pending count is 0");

    loom_runtime_destroy(&rt);

    /* NULL-safe queries return 0 */
    ASSERT(loom_runtime_worker_count(NULL) == 0, "NULL worker count is 0");
    ASSERT(loom_runtime_pending_count(NULL) == 0, "NULL pending count is 0");
    ASSERT(loom_runtime_active_count(NULL) == 0, "NULL active count is 0");
    ASSERT(loom_runtime_idle_count(NULL) == 0, "NULL idle count is 0");
    ASSERT(loom_runtime_utilization(NULL) == 0.0, "NULL utilization is 0.0");
}

static void test_invalid_runtime(void)
{
    loom_fn_union_t fn = {.thread_fn = simple_thread_task};
    ASSERT(loom_runtime_submit(NULL, fn, NULL, LOOM_SUBMIT_THREAD, 0, NULL) ==
               LOOMWORKS_ERR_INVALID,
           "submit on NULL returns ERR_INVALID");
    ASSERT(loom_runtime_cancel(NULL, 0) == LOOMWORKS_ERR_INVALID,
           "cancel on NULL returns ERR_INVALID");
    ASSERT(loom_runtime_resize(NULL, 4) == LOOMWORKS_ERR_INVALID,
           "resize on NULL returns ERR_INVALID");
    ASSERT(loom_runtime_submit_future(NULL, result_task, NULL, NULL, NULL) == LOOMWORKS_ERR_INVALID,
           "submit_future on NULL returns ERR_INVALID");
}

static void test_stress_mixed(void)
{
    /* Submit 100 thread + 100 coro tasks and verify all complete. */
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    _Atomic int thread_done = 0;
    _Atomic int coro_done   = 0;

    for (int i = 0; i < 100; i++) {
        loom_fn_union_t tfn = {.thread_fn = simple_thread_task};
        loom_fn_union_t cfn = {.coro_fn = simple_coro_task};
        ASSERT(loom_runtime_submit(rt, tfn, &thread_done, LOOM_SUBMIT_THREAD, 5, NULL) ==
                   LOOMWORKS_OK,
               "submit thread task");
        ASSERT(loom_runtime_submit(rt, cfn, &coro_done, LOOM_SUBMIT_CORO, 0, NULL) == LOOMWORKS_OK,
               "submit coroutine task");
    }

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    ASSERT((int)atomic_load_explicit(&thread_done, memory_order_relaxed) == 100,
           "all 100 thread tasks executed");
    ASSERT((int)atomic_load_explicit(&coro_done, memory_order_relaxed) == 100,
           "all 100 coroutine tasks executed");
}

/* ------------------------------------------------------------------ */

int main(void)
{
    test_create_destroy();
    test_submit_thread();
    test_submit_coro();
    test_mixed_submit();
    test_cancel_pending();
    test_cancel_all();
    test_submit_future();
    test_submit_future_cancel();
    test_coro_cancel_pending();
    test_resize();
    test_metrics_callback();
    test_queries();
    test_invalid_runtime();
    test_stress_mixed();

    fprintf(stdout, "PASS %d  FAIL %d\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
