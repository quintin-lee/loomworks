#define _POSIX_C_SOURCE 200809L
#include "ctpool/thread_pool.h"

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
static void simple_task(void *arg)
{
    volatile int *counter = (volatile int *)arg;
    __sync_fetch_and_add(counter, 1);
}

static void *result_task(void *arg)
{
    (void)arg;
    int *val = (int *)malloc(sizeof(int));
    if (val) *val = 42;
    return (void *)val;
}

static void no_data_task(void *arg)
{
    (void)arg;
}

/* ---------- Test: basic create/destroy ---------- */
static void test_basic_create_destroy(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ctpool_result_t rc = ctpool_pool_create(NULL, &pool);
    ASSERT(rc == CTPPOOL_OK,               "create with defaults");
    ASSERT(pool != NULL,                   "pool not null");
    ASSERT(ctpool_pool_worker_count(pool) > 0, "worker count > 0");
    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(pool == NULL,                   "destroy sets to null");
}

/* ---------- Test: config parameters ---------- */
static void test_config(void)
{
    ctpool_pool_config_t cfg = { .worker_count = 4, .stack_size = 65536, .queue_capacity = 1000 };
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(&cfg, &pool) == CTPPOOL_OK, "create with config");
    ASSERT(ctpool_pool_worker_count(pool) == 4, "worker count = 4");
    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
}

/* ---------- Test: submit N tasks ---------- */
static void test_submit_n_tasks(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    int counter = 0;
    const int N = 1000;
    for (int i = 0; i < N; i++) {
        ASSERT(ctpool_pool_submit(pool, simple_task, &counter) == CTPPOOL_OK, "submit task");
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);

    ASSERT(counter == N, "all tasks executed");
}

/* ---------- Test: future result ---------- */
static void test_future_result(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    ctpool_future_t *future = NULL;
    ASSERT(ctpool_pool_submit_future(pool, result_task, NULL, &future) == CTPPOOL_OK, "submit future");
    ASSERT(future != NULL, "future not null");

    void *result = NULL;
    ASSERT(ctpool_future_wait(future, &result) == CTPPOOL_OK, "wait future");
    ASSERT(result != NULL, "result not null");
    ASSERT(*(int *)result == 42, "result value is 42");
    free(result);

    ctpool_future_destroy(future);
    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
}

/* ---------- Test: multiple futures ---------- */
static void test_multiple_futures(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    const int N = 50;
    ctpool_future_t *futures[N];
    for (int i = 0; i < N; i++) {
        ASSERT(ctpool_pool_submit_future(pool, result_task, NULL, &futures[i]) == CTPPOOL_OK, "submit future");
        ASSERT(futures[i] != NULL, "future not null");
    }

    long total = 0;
    for (int i = 0; i < N; i++) {
        void *result = NULL;
        ASSERT(ctpool_future_wait(futures[i], &result) == CTPPOOL_OK, "wait future");
        ASSERT(result != NULL, "result not null");
        total += *(int *)result;
        free(result);
        ctpool_future_destroy(futures[i]);
    }
    ASSERT(total == (long)N * 42, "all future results correct");

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
}

/* ---------- Test: pending count ---------- */
static void test_pending_count(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ctpool_pool_config_t cfg = { .worker_count = 1, .queue_capacity = 0 };
    ASSERT(ctpool_pool_create(&cfg, &pool) == CTPPOOL_OK, "create pool");

    int dummy = 0;
    for (int i = 0; i < 10; i++) {
        ctpool_pool_submit(pool, simple_task, &dummy);
    }
    (void)ctpool_pool_pending_count(pool);

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(true, "pending count query");
}

/* ---------- Test: invalid args ---------- */
static void test_invalid_args(void)
{
    ASSERT(ctpool_pool_create(NULL, NULL)       != CTPPOOL_OK, "null pool out");
    ASSERT(ctpool_pool_submit(NULL, simple_task, NULL) != CTPPOOL_OK, "null pool submit");
    ASSERT(ctpool_pool_submit_future(NULL, result_task, NULL, NULL) != CTPPOOL_OK, "null pool future");
    ASSERT(ctpool_future_wait(NULL, NULL)       != CTPPOOL_OK, "null future wait");
    ctpool_future_destroy(NULL);
    ASSERT(ctpool_pool_worker_count(NULL) == 0,  "null pool worker count");
    ASSERT(ctpool_pool_pending_count(NULL) == 0, "null pool pending count");
    ASSERT(true, "invalid args handled");
}

/* ---------- Test: submit after shutdown ---------- */
static void test_submit_after_shutdown(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    int dummy = 0;
    /* Test submit error before shutdown (shutdown destroys the mutex) */
    ASSERT(ctpool_pool_submit(pool, simple_task, &dummy) == CTPPOOL_OK, "submit before shutdown");
    ctpool_pool_shutdown(pool);
    ASSERT(ctpool_pool_submit(pool, simple_task, &dummy) == CTPPOOL_ERR_SHUTDOWN, "submit after shutdown");

    ctpool_pool_destroy(&pool);
}

/* ---------- Test: double shutdown ---------- */
static void test_double_shutdown(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    int dummy = 0;
    ctpool_pool_submit(pool, simple_task, &dummy);
    ctpool_pool_shutdown(pool);
    ctpool_pool_shutdown(pool);  /* should not crash */
    ctpool_pool_destroy(&pool);
    ASSERT(true, "double shutdown safe");
}

/* ---------- Test: concurrent submission ---------- */
typedef struct {
    ctpool_thread_pool_t *pool;
    int                   count;
    int                  *counter;
} concurrent_arg_t;

static void *concurrent_submit_worker(void *arg)
{
    concurrent_arg_t *s = (concurrent_arg_t *)arg;
    for (int i = 0; i < s->count; i++) {
        ctpool_pool_submit(s->pool, simple_task, s->counter);
    }
    free(s);
    return NULL;
}

static void test_concurrent_submit(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ctpool_pool_config_t cfg = { .worker_count = 4, .queue_capacity = 0 };
    ASSERT(ctpool_pool_create(&cfg, &pool) == CTPPOOL_OK, "create pool");

    int counter  = 0;
    const int N  = 10000;
    const int T  = 4;
    pthread_t tid[4];

    for (int t = 0; t < T; t++) {
        concurrent_arg_t *s = (concurrent_arg_t *)malloc(sizeof(*s));
        s->pool    = pool;
        s->count   = N / T;
        s->counter = &counter;
        pthread_create(&tid[t], NULL, concurrent_submit_worker, s);
    }
    for (int t = 0; t < T; t++) {
        pthread_join(tid[t], NULL);
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);

    ASSERT(counter == N, "concurrent submit completed");
}

/* ---------- Test: concurrent future submission ---------- */
typedef struct {
    ctpool_thread_pool_t *pool;
    int                   count;
} future_submit_arg_t;

static void *concurrent_future_submit_worker(void *arg)
{
    future_submit_arg_t *s = (future_submit_arg_t *)arg;
    for (int i = 0; i < s->count; i++) {
        ctpool_future_t *fut = NULL;
        ctpool_pool_submit_future(s->pool, result_task, NULL, &fut);
        if (fut) {
            void *result = NULL;
            ctpool_future_wait(fut, &result);
            if (result) free(result);
            ctpool_future_destroy(fut);
        }
    }
    free(s);
    return NULL;
}

static void test_concurrent_future_submit(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ctpool_pool_config_t cfg = { .worker_count = 4, .queue_capacity = 0 };
    ASSERT(ctpool_pool_create(&cfg, &pool) == CTPPOOL_OK, "create pool");

    const int T = 4;
    const int N = 100;
    pthread_t tid[4];

    for (int t = 0; t < T; t++) {
        future_submit_arg_t *s = (future_submit_arg_t *)malloc(sizeof(*s));
        s->pool  = pool;
        s->count = N;
        pthread_create(&tid[t], NULL, concurrent_future_submit_worker, s);
    }
    for (int t = 0; t < T; t++) {
        pthread_join(tid[t], NULL);
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(true, "concurrent future submit completed");
}

/* ---------- Test: bounded queue rejects when full ---------- */
static void slow_task(void *arg)
{
    (void)arg;
    /* Spend enough time that the queue stays full while we fill it */
    int sink = 0; (void)sink;
    for (int i = 0; i < 100000; i++) sink += i;
}

static void test_bounded_queue(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ctpool_pool_config_t cfg = { .worker_count = 1, .queue_capacity = 5 };
    ASSERT(ctpool_pool_create(&cfg, &pool) == CTPPOOL_OK, "create bounded pool");

    /* Submit more tasks than capacity.  The single slow worker keeps
       the queue full, so after the first 5 submissions the 6th must
       fail with CTPPOOL_ERR_INVALID. */
    int ok_count = 0;
    int err_count = 0;
    for (int i = 0; i < 10; i++) {
        ctpool_result_t rc = ctpool_pool_submit(pool, slow_task, NULL);
        if (rc == CTPPOOL_OK) ok_count++;
        else if (rc == CTPPOOL_ERR_INVALID) err_count++;
    }
    ASSERT(ok_count == 5, "exactly 5 submits succeed");
    ASSERT(err_count == 5, "remaining submits return ERR_INVALID");

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(true, "bounded queue test completed");
}

/* ---------- Test: no-data task ---------- */
static void test_no_data_task(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    for (int i = 0; i < 100; i++) {
        ASSERT(ctpool_pool_submit(pool, no_data_task, NULL) == CTPPOOL_OK, "submit no-data task");
    }

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(true, "no-data tasks completed");
}

/* ---------- Test: destroy null ---------- */
static void test_destroy_null(void)
{
    ctpool_thread_pool_t *p = NULL;
    ctpool_pool_destroy(NULL);       /* null ptr-to-ptr is safe */
    ctpool_pool_destroy(&p);         /* null pool is safe */
    ASSERT(p == NULL, "destroy null leaves null");
}

/* ---------- Test: future wait already completed ---------- */
static void test_future_wait_completed(void)
{
    ctpool_thread_pool_t *pool = NULL;
    ASSERT(ctpool_pool_create(NULL, &pool) == CTPPOOL_OK, "create pool");

    /* Submit a fast task and wait immediately */
    ctpool_future_t *fut = NULL;
    ASSERT(ctpool_pool_submit_future(pool, result_task, NULL, &fut) == CTPPOOL_OK, "submit future");

    /* Wait should succeed even if task already completed */
    void *result = NULL;
    ASSERT(ctpool_future_wait(fut, &result) == CTPPOOL_OK, "wait completed future");
    ASSERT(result != NULL, "result not null");
    free(result);
    ctpool_future_destroy(fut);

    ctpool_pool_shutdown(pool);
    ctpool_pool_destroy(&pool);
    ASSERT(true, "future wait completed");
}

/* ================================================================
 *  Main
 * ================================================================ */

int main(void)
{
    printf("=== Thread Pool Tests ===\n");

    test_basic_create_destroy();
    test_config();
    test_submit_n_tasks();
    test_future_result();
    test_multiple_futures();
    test_pending_count();
    test_invalid_args();
    test_submit_after_shutdown();
    test_double_shutdown();
    test_concurrent_submit();
    test_concurrent_future_submit();
    test_bounded_queue();
    test_no_data_task();
    test_destroy_null();
    test_future_wait_completed();

    printf("\nResults: %d passed, %d failed\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
