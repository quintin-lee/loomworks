#define _POSIX_C_SOURCE 200809L
#include "ctpool/ctpool.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>

static int g_passes   = 0;
static int g_failures = 0;

#define ASSERT(expr, msg) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s at %s:%d\n", msg, __FILE__, __LINE__); \
        g_failures++; \
    } else { \
        g_passes++; \
    } \
} while (0)

/* ---------- Helpers ---------- */
static void pool_task(void *arg)
{
    int *sum = (int *)arg;
    __sync_fetch_and_add(sum, 1);
}

static void *pool_result_task(void *arg)
{
    (void)arg;
    long *val = (long *)malloc(sizeof(long));
    if (val) *val = 12345L;
    return (void *)val;
}

static void coro_task(void *arg)
{
    int *counter = (int *)arg;
    __sync_fetch_and_add(counter, 1);
}

static void yield_coro_task(void *arg)
{
    int *counter = (int *)arg;
    __sync_fetch_and_add(counter, 1);
    ctpool_coro_yield();
    __sync_fetch_and_add(counter, 1);
}

/* ---------- Test: stress pool ---------- */
static void test_stress_pool(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ctpool_pool_config_t cfg = { .worker_count = 16, .queue_capacity = 0 };
    ASSERT(ctpool_pool_create(&cfg, &pool) == CTPPOOL_OK, "create stress pool");

    const int N = 50000;
    int counter = 0;
    for (int i = 0; i < N; i++) {
        ASSERT(ctpool_pool_submit(pool, pool_task, &counter) == CTPPOOL_OK, "submit task");
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);

    ASSERT(counter == N, "stress pool: all tasks completed");
}

/* ---------- Test: stress coroutines ---------- */
static void test_stress_coroutines(void)
{
    const int N = 100;
    int counter = 0;

    for (int i = 0; i < N; i++) {
        ctpool_coroutine_t *coro = NULL;
        ASSERT(ctpool_coro_create(coro_task, &counter, 0, &coro) == CTPPOOL_CORO_OK, "create coro");
        ASSERT(ctpool_coro_resume(coro) == CTPPOOL_CORO_OK, "resume coro");
        ctpool_coro_destroy(&coro);
    }

    ASSERT(counter == N, "stress coroutines: all completed");
}

/* ---------- Test: pool + coroutine interop ---------- */
typedef struct {
    int                  *counter;
    ctpool_thread_pool_t *pool;
} interop_arg_t;

static void interop_task(void *arg)
{
    interop_arg_t *ia = (interop_arg_t *)arg;
    ctpool_coroutine_t *coro = NULL;
    ctpool_coro_create(coro_task, ia->counter, 0, &coro);
    ctpool_coro_resume(coro);
    ctpool_coro_destroy(&coro);
    free(ia);
}

static void test_pool_coro_interop(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    int counter = 0;
    const int N = 100;

    for (int i = 0; i < N; i++) {
        interop_arg_t *ia = (interop_arg_t *)malloc(sizeof(*ia));
        ia->counter = &counter;
        ia->pool    = pool;
        ASSERT(ctpool_pool_submit(pool, interop_task, ia) == CTPPOOL_OK, "submit interop task");
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);

    ASSERT(counter == N, "pool+coroutine interop completed");
}

/* ---------- Test: guard page allocation ---------- */
static void test_guard_page(void)
{
    ctpool_coroutine_t *coro = NULL;
    int counter = 0;

    ASSERT(ctpool_coro_create(coro_task, &counter, 4096, &coro) == CTPPOOL_CORO_OK, "create with guard pages");
    if (coro) {
        ASSERT(ctpool_coro_resume(coro) == CTPPOOL_CORO_OK, "resume with guard pages");
        ctpool_coro_destroy(&coro);
    }
}

/* ---------- Test: config validation ---------- */
static void test_config_validation(void)
{
    ctpool_pool_config_t cfg;
    cfg.worker_count   = 0;
    cfg.stack_size     = 0;
    cfg.queue_capacity = 0;

    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(&cfg, &pool) == CTPPOOL_OK, "defaults config");
    ASSERT(ctpool_pool_worker_count(pool) > 0, "auto worker count");
    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
}

/* ---------- Test: future in pool ---------- */
static void test_future_in_pool(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    const int N = 100;
    long total = 0;

    for (int i = 0; i < N; i++) {
        ctpool_future_t *fut = NULL;
        ASSERT(ctpool_pool_submit_future(pool, pool_result_task, NULL, &fut) == CTPPOOL_OK, "submit future");
        if (fut) {
            void *res = NULL;
            ASSERT(ctpool_future_wait(fut, &res) == CTPPOOL_OK, "wait future");
            if (res) {
                total += *(long *)res;
                free(res);
            }
            ctpool_future_destroy(fut);
        }
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);

    ASSERT(total == (long)N * 12345L, "future results correct");
}

/* ---------- Test: stress yield coroutines ---------- */
static void test_stress_yield_coroutines(void)
{
    const int N = 200;
    int counter = 0;

    for (int i = 0; i < N; i++) {
        ctpool_coroutine_t *coro = NULL;
        ASSERT(ctpool_coro_create(yield_coro_task, &counter, 0, &coro) == CTPPOOL_CORO_OK, "create yield coro");
        ASSERT(ctpool_coro_resume(coro) == CTPPOOL_CORO_OK, "1st resume");
        ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_SUSPENDED, "SUSPENDED");
        ASSERT(ctpool_coro_resume(coro) == CTPPOOL_CORO_OK, "2nd resume");
        ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_DONE, "DONE");
        ctpool_coro_destroy(&coro);
    }

    ASSERT(counter == N * 2, "all yield coroutines completed");
}

/* ---------- Test: pool with many futures ---------- */
static void test_pool_many_futures(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    const int N = 500;
    long total = 0;

    ctpool_future_t *futures[N];
    for (int i = 0; i < N; i++) {
        ASSERT(ctpool_pool_submit_future(pool, pool_result_task, NULL, &futures[i]) == CTPPOOL_OK, "submit future");
    }

    for (int i = 0; i < N; i++) {
        void *res = NULL;
        ASSERT(ctpool_future_wait(futures[i], &res) == CTPPOOL_OK, "wait future");
        if (res) {
            total += *(long *)res;
            free(res);
        }
        ctpool_future_destroy(futures[i]);
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);

    ASSERT(total == (long)N * 12345L, "many futures results correct");
}

/* ---------- Test: concurrent pool + coroutines ---------- */
typedef struct {
    ctpool_thread_pool_t *pool;
    int                   iters;
} coro_in_pool_arg_t;

static void *coro_in_pool_task(void *arg)
{
    coro_in_pool_arg_t *ca = (coro_in_pool_arg_t *)arg;
    int local_count = 0;

    for (int i = 0; i < ca->iters; i++) {
        ctpool_coroutine_t *coro = NULL;
        ctpool_coro_create(coro_task, &local_count, 0, &coro);
        ctpool_coro_resume(coro);
        ctpool_coro_destroy(&coro);
    }

    free(ca);
    return NULL;
}

static void test_concurrent_pool_coroutines(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    const int N = 50;
    pthread_t tids[4];

    for (int t = 0; t < 4; t++) {
        coro_in_pool_arg_t *ca = (coro_in_pool_arg_t *)malloc(sizeof(*ca));
        ca->pool  = pool;
        ca->iters = N;
        pthread_create(&tids[t], NULL, coro_in_pool_task, ca);
    }
    for (int t = 0; t < 4; t++) {
        pthread_join(tids[t], NULL);
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(true, "concurrent pool+coroutines completed");
}

/* ---------- Test: bounded queue with concurrent workers ---------- */
static void test_bounded_queue_concurrent(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ctpool_pool_config_t cfg = { .worker_count = 4, .queue_capacity = 20 };
    ASSERT(ctpool_pool_create(&cfg, &pool) == CTPPOOL_OK, "create bounded pool");

    int counter = 0;
    const int N = 100;

    for (int i = 0; i < N; i++) {
        ctpool_result_t rc = ctpool_pool_submit(pool, pool_task, &counter);
        if (rc != CTPPOOL_OK) {
            /* Queue full — that's expected with capacity 20 and 4 workers */
            ASSERT(rc == CTPPOOL_ERR_INVALID, "full queue returns ERR_INVALID");
        }
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(counter <= N, "bounded queue: tasks completed");
}

/* ---------- Test: yield in pool workers ---------- */
static void test_yield_in_pool(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    int counter = 0;
    const int N = 50;

    for (int i = 0; i < N; i++) {
        interop_arg_t *ia = (interop_arg_t *)malloc(sizeof(*ia));
        ia->counter = &counter;
        ia->pool    = pool;
        ctpool_pool_submit(pool, interop_task, ia);
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(counter == N, "yield coroutines in pool completed");
}

/* ---------- Test: sequential coroutines with large stack ---------- */
static void test_large_stack_coroutines(void)
{
    const int N = 50;
    int counter = 0;

    for (int i = 0; i < N; i++) {
        ctpool_coroutine_t *coro = NULL;
        ASSERT(ctpool_coro_create(coro_task, &counter, 262144, &coro) == CTPPOOL_CORO_OK, "create 256KB stack coro");
        ASSERT(ctpool_coro_resume(coro) == CTPPOOL_CORO_OK, "resume large stack coro");
        ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_DONE, "DONE");
        ctpool_coro_destroy(&coro);
    }

    ASSERT(counter == N, "large stack coroutines completed");
}

/* ================================================================
 *  Main
 * ================================================================ */

int main(void)
{
    printf("=== Integration Tests ===\n");

    test_stress_pool();
    test_stress_coroutines();
    test_pool_coro_interop();
    test_guard_page();
    test_config_validation();
    test_future_in_pool();
    test_stress_yield_coroutines();
    test_pool_many_futures();
    test_concurrent_pool_coroutines();
    test_bounded_queue_concurrent();
    test_yield_in_pool();
    test_large_stack_coroutines();

    printf("\nResults: %d passed, %d failed\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
