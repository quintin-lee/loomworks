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

static void simple_thread_task(void *arg)
{
    int *counter = (int *)arg;
    __sync_fetch_and_add(counter, 1);
}

static void simple_coro_task(void *arg)
{
    int *counter = (int *)arg;
    loom_coro_yield();
    __sync_fetch_and_add(counter, 1);
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

static void test_cancel_pending(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    int             counter = 0;
    uint64_t        tid     = 0;
    loom_fn_union_t fn      = {.thread_fn = simple_thread_task};
    ASSERT(loom_runtime_submit(rt, fn, &counter, LOOM_SUBMIT_THREAD, 5, &tid) == LOOMWORKS_OK,
           "submit thread task");

    /* Cancel before the worker picks it up. */
    ASSERT(loom_runtime_cancel(rt, tid) == LOOMWORKS_OK, "cancel pending task");

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    ASSERT(counter == 0, "cancelled task did not execute");
}

static void test_cancel_all(void)
{
    loom_runtime_t *rt = NULL;
    ASSERT(loom_runtime_create(NULL, &rt) == LOOMWORKS_OK, "create runtime");

    int             counter = 0;
    loom_fn_union_t fn      = {.thread_fn = simple_thread_task};
    for (int i = 0; i < 5; i++) {
        loom_runtime_submit(rt, fn, &counter, LOOM_SUBMIT_THREAD, 5, NULL);
    }

    /* cancel_all is non-blocking; workers may pick up tasks quickly.
     * We just verify the API works without crashing. */
    uint32_t cancelled = 0;
    loom_runtime_cancel_all(rt, &cancelled);
    ASSERT(cancelled <= 5, "cancel count within bounds");

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    ASSERT(counter <= 5, "total executions within bounds");
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
    test_queries();
    test_invalid_runtime();

    fprintf(stdout, "PASS %d  FAIL %d\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
