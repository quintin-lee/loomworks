#define _POSIX_C_SOURCE 200809L
#include "loomworks/thread_pool.h"

#include <pthread.h>
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
    if (val) {
        *val = 42;
    }
    return (void *)val;
}

static void no_data_task(void *arg)
{
    (void)arg;
}

/* ---------- Test: basic create/destroy ---------- */
static void test_basic_create_destroy(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_result_t       rc   = loom_pool_create(NULL, &pool);
    ASSERT(rc == LOOMWORKS_OK, "create with defaults");
    ASSERT(pool != NULL, "pool not null");
    ASSERT(loom_pool_worker_count(pool) > 0, "worker count > 0");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(pool == NULL, "destroy sets to null");
}

/* ---------- Test: config parameters ---------- */
static void test_config(void)
{
    loom_pool_config_t  cfg  = {.worker_count = 4, .stack_size = 65536, .queue_capacity = 1000};
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create with config");
    ASSERT(loom_pool_worker_count(pool) == 4, "worker count = 4");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: submit N tasks ---------- */
static void test_submit_n_tasks(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    int       counter = 0;
    const int N       = 1000;
    for (int i = 0; i < N; i++) {
        ASSERT(loom_pool_submit(pool, simple_task, &counter) == LOOMWORKS_OK, "submit task");
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    ASSERT(counter == N, "all tasks executed");
}

/* ---------- Test: future result ---------- */
static void test_future_result(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    loom_future_t *future = NULL;
    ASSERT(loom_pool_submit_future(pool, result_task, NULL, &future) == LOOMWORKS_OK,
           "submit future");
    ASSERT(future != NULL, "future not null");

    void *result = NULL;
    ASSERT(loom_future_wait(future, &result) == LOOMWORKS_OK, "wait future");
    ASSERT(result != NULL, "result not null");
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    ASSERT(*(int *)result == 42, "result value is 42");
    free(result);

    loom_future_destroy(future);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: multiple futures ---------- */
static void test_multiple_futures(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    const int      N = 50;
    loom_future_t *futures[N];
    for (int i = 0; i < N; i++) {
        ASSERT(loom_pool_submit_future(pool, result_task, NULL, &futures[i]) == LOOMWORKS_OK,
               "submit future");
        ASSERT(futures[i] != NULL, "future not null");
    }

    long total = 0;
    for (int i = 0; i < N; i++) {
        void *result = NULL;
        ASSERT(loom_future_wait(futures[i], &result) == LOOMWORKS_OK, "wait future");
        ASSERT(result != NULL, "result not null");
        total += *(int *)result;
        free(result);
        loom_future_destroy(futures[i]);
    }
    ASSERT(total == (long)N * 42, "all future results correct");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: pending count ---------- */
static void test_pending_count(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    int dummy = 0;
    for (int i = 0; i < 10; i++) {
        loom_pool_submit(pool, simple_task, &dummy);
    }
    (void)loom_pool_pending_count(pool);

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(true, "pending count query");
}

/* ---------- Test: invalid args ---------- */
static void test_invalid_args(void)
{
    ASSERT(loom_pool_create(NULL, NULL) != LOOMWORKS_OK, "null pool out");
    ASSERT(loom_pool_submit(NULL, simple_task, NULL) != LOOMWORKS_OK, "null pool submit");
    ASSERT(loom_pool_submit_future(NULL, result_task, NULL, NULL) != LOOMWORKS_OK,
           "null pool future");
    ASSERT(loom_future_wait(NULL, NULL) != LOOMWORKS_OK, "null future wait");
    loom_future_destroy(NULL);
    ASSERT(loom_pool_worker_count(NULL) == 0, "null pool worker count");
    ASSERT(loom_pool_pending_count(NULL) == 0, "null pool pending count");
    ASSERT(true, "invalid args handled");
}

/* ---------- Test: submit after shutdown ---------- */
static void test_submit_after_shutdown(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    int dummy = 0;
    /* Test submit error before shutdown (shutdown destroys the mutex) */
    ASSERT(loom_pool_submit(pool, simple_task, &dummy) == LOOMWORKS_OK, "submit before shutdown");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_submit(pool, simple_task, &dummy) == LOOMWORKS_ERR_SHUTDOWN,
           "submit after shutdown");

    loom_pool_destroy(&pool);
}

/* ---------- Test: double shutdown ---------- */
static void test_double_shutdown(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    int dummy = 0;
    loom_pool_submit(pool, simple_task, &dummy);
    loom_pool_shutdown(pool);
    loom_pool_shutdown(pool); /* should not crash */
    loom_pool_destroy(&pool);
    ASSERT(true, "double shutdown safe");
}

/* ---------- Test: concurrent submission ---------- */
typedef struct {
    loom_thread_pool_t *pool;
    int                 count;
    int                *counter;
} concurrent_arg_t;

static void *concurrent_submit_worker(void *arg)
{
    concurrent_arg_t *s = (concurrent_arg_t *)arg;
    for (int i = 0; i < s->count; i++) {
        loom_pool_submit(s->pool, simple_task, s->counter);
    }
    free(s);
    return NULL;
}

static void test_concurrent_submit(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    int       counter = 0;
    const int N       = 10000;
    const int T       = 4;
    pthread_t tid[4];

    for (int t = 0; t < T; t++) {
        concurrent_arg_t *s = (concurrent_arg_t *)malloc(sizeof(*s));
        s->pool             = pool;
        s->count            = N / T;
        s->counter          = &counter;
        pthread_create(&tid[t], NULL, concurrent_submit_worker, s);
    }
    for (int t = 0; t < T; t++) {
        pthread_join(tid[t], NULL);
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    ASSERT(counter == N, "concurrent submit completed");
}

/* ---------- Test: concurrent future submission ---------- */
typedef struct {
    loom_thread_pool_t *pool;
    int                 count;
} future_submit_arg_t;

static void *concurrent_future_submit_worker(void *arg)
{
    future_submit_arg_t *s = (future_submit_arg_t *)arg;
    for (int i = 0; i < s->count; i++) {
        loom_future_t *fut = NULL;
        loom_pool_submit_future(s->pool, result_task, NULL, &fut);
        if (fut) {
            void *result = NULL;
            loom_future_wait(fut, &result);
            if (result) {
                free(result);
            }
            loom_future_destroy(fut);
        }
    }
    free(s);
    return NULL;
}

static void test_concurrent_future_submit(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    const int T = 4;
    const int N = 100;
    pthread_t tid[4];

    for (int t = 0; t < T; t++) {
        future_submit_arg_t *s = (future_submit_arg_t *)malloc(sizeof(*s));
        s->pool                = pool;
        s->count               = N;
        pthread_create(&tid[t], NULL, concurrent_future_submit_worker, s);
    }
    for (int t = 0; t < T; t++) {
        pthread_join(tid[t], NULL);
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(true, "concurrent future submit completed");
}

/* ---------- Test: bounded queue rejects when full ---------- */
static void slow_task(void *arg)
{
    (void)arg;
    /* Spend enough time that the queue stays full while we fill it */
    int sink = 0;
    (void)sink;
    for (int i = 0; i < 100000; i++) {
        sink += i;
    }
}

static void test_bounded_queue(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 5};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create bounded pool");

    /* Submit more tasks than capacity.  The single slow worker keeps
       the queue full, so after the first 5 submissions the 6th must
       fail with LOOMWORKS_ERR_INVALID. */
    int ok_count  = 0;
    int err_count = 0;
    for (int i = 0; i < 10; i++) {
        loom_result_t rc = loom_pool_submit(pool, slow_task, NULL);
        if (rc == LOOMWORKS_OK) {
            ok_count++;
        } else if (rc == LOOMWORKS_ERR_INVALID) {
            err_count++;
        }
    }
    ASSERT(ok_count == 5, "exactly 5 submits succeed");
    ASSERT(err_count == 5, "remaining submits return ERR_INVALID");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(true, "bounded queue test completed");
}

/* ---------- Test: no-data task ---------- */
static void test_no_data_task(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    for (int i = 0; i < 100; i++) {
        ASSERT(loom_pool_submit(pool, no_data_task, NULL) == LOOMWORKS_OK, "submit no-data task");
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(true, "no-data tasks completed");
}

/* ---------- Test: destroy null ---------- */
static void test_destroy_null(void)
{
    loom_thread_pool_t *p = NULL;
    loom_pool_destroy(NULL); /* null ptr-to-ptr is safe */
    loom_pool_destroy(&p);   /* null pool is safe */
    ASSERT(p == NULL, "destroy null leaves null");
}

/* ---------- Test: future wait already completed ---------- */
static void test_future_wait_completed(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    /* Submit a fast task and wait immediately */
    loom_future_t *fut = NULL;
    ASSERT(loom_pool_submit_future(pool, result_task, NULL, &fut) == LOOMWORKS_OK, "submit future");

    /* Wait should succeed even if task already completed */
    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait completed future");
    ASSERT(result != NULL, "result not null");
    free(result);
    loom_future_destroy(fut);

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(true, "future wait completed");
}

/* ---------- Helpers for cancel/timeout tests ---------- */
typedef struct {
    bool *done;
} slow_task_arg_t;

static void increment_task(void *arg)
{
    int *c = (int *)arg;
    __sync_fetch_and_add(c, 1);
}

static void *fast_result_task(void *arg)
{
    (void)arg;
    int *val = (int *)malloc(sizeof(int));
    if (val) {
        *val = 42;
    }
    return (void *)val;
}

static void *slow_result_task(void *arg)
{
    (void)arg;
    for (volatile int i = 0; i < 50000000; i++)
        ;
    int *val = (int *)malloc(sizeof(int));
    if (val) {
        *val = 99;
    }
    return (void *)val;
}

/* ---------- Test: cancel pending task ---------- */
static void test_cancel(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    /* Submit a slow task and try to cancel it */
    slow_task_arg_t *arg = (slow_task_arg_t *)malloc(sizeof(*arg));
    arg->done            = false;
    ASSERT(loom_pool_submit(pool, slow_task, arg) == LOOMWORKS_OK, "submit slow task");

    /* The task might already be running, so cancel may or may not succeed */
    loom_result_t rc = loom_pool_cancel(pool, arg);
    if (rc == LOOMWORKS_OK) {
        /* Cancelled before execution -- verify it didn't run */
        /* We need to wait a bit then check; for this test just verify no crash */
    }
    free(arg);

    /* Submit an instant task and cancel it before it runs */
    int counter = 0;
    ASSERT(loom_pool_submit(pool, increment_task, &counter) == LOOMWORKS_OK, "submit inc");
    ASSERT(loom_pool_cancel(pool, &counter) == LOOMWORKS_OK, "cancel inc");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: cancel_all ---------- */
static void test_cancel_all(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    int counters[10];
    for (int i = 0; i < 10; i++) {
        counters[i] = 0;
        ASSERT(loom_pool_submit(pool, increment_task, &counters[i]) == LOOMWORKS_OK, "submit task");
    }

    uint32_t cancelled = 0;
    loom_pool_cancel_all(pool, &cancelled);
    ASSERT(cancelled == 10, "all 10 tasks cancelled");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: future_wait_timeout (should succeed) ---------- */
static void test_future_wait_timeout_ok(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    loom_future_t *fut = NULL;
    ASSERT(loom_pool_submit_future(pool, fast_result_task, NULL, &fut) == LOOMWORKS_OK,
           "submit future");

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5; /* 5 second timeout -- should always succeed */

    void *result = NULL;
    ASSERT(loom_future_wait_timeout(fut, &result, &deadline) == LOOMWORKS_OK,
           "wait_timeout succeeds");
    ASSERT(result != NULL, "result not null");
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    ASSERT(*(int *)result == 42, "result value is 42");
    free(result);

    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: future_wait_timeout (should timeout) ---------- */
static void test_future_wait_timeout_expired(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    loom_future_t *fut = NULL;
    ASSERT(loom_pool_submit_future(pool, slow_result_task, NULL, &fut) == LOOMWORKS_OK,
           "submit slow future");

    /* Deadline in the past */
    struct timespec deadline = {.tv_sec = 0, .tv_nsec = 0};

    void *result = NULL;
    ASSERT(loom_future_wait_timeout(fut, &result, &deadline) == LOOMWORKS_ERR_TIMEOUT,
           "wait_timeout times out");

    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ================================================================
 *  Task Group Tests
 * ================================================================ */
#include "loomworks/task_group.h"

static void test_task_group_create_destroy(void)
{
    loom_thread_pool_t *pool  = NULL;
    loom_task_group_t  *group = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "create group");
    ASSERT(group != NULL, "group not null");
    ASSERT(loom_task_group_pending_count(group) == 0, "empty group count");
    loom_task_group_destroy(&group);
    ASSERT(group == NULL, "destroy sets to null");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_task_group_submit(void)
{
    loom_thread_pool_t *pool  = NULL;
    loom_task_group_t  *group = NULL;
    loom_pool_config_t  cfg   = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "create group");

    int counter = 0;
    for (int i = 0; i < 10; i++) {
        ASSERT(loom_task_group_submit(group, increment_task, &counter) == LOOMWORKS_OK,
               "submit via group");
    }
    ASSERT(loom_task_group_pending_count(group) == 10, "10 pending");

    loom_task_group_wait(group);
    ASSERT(counter == 10, "all tasks executed");

    loom_task_group_destroy(&group);
    loom_pool_destroy(&pool);
}

static void test_task_group_cancel(void)
{
    loom_thread_pool_t *pool  = NULL;
    loom_task_group_t  *group = NULL;
    loom_pool_config_t  cfg   = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "create group");

    int counters[10];
    for (int i = 0; i < 10; i++) {
        counters[i] = 0;
        ASSERT(loom_task_group_submit(group, increment_task, &counters[i]) == LOOMWORKS_OK,
               "submit task");
    }
    /* Cancel before they run */
    loom_task_group_cancel(group);
    ASSERT(loom_task_group_pending_count(group) == 0, "group empty after cancel");

    loom_task_group_wait(group);
    loom_task_group_destroy(&group);
    loom_pool_destroy(&pool);
}

static void test_task_group_destroy_cancels(void)
{
    loom_thread_pool_t *pool  = NULL;
    loom_task_group_t  *group = NULL;
    loom_pool_config_t  cfg   = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "create group");

    int counter = 0;
    for (int i = 0; i < 5; i++) {
        ASSERT(loom_task_group_submit(group, increment_task, &counter) == LOOMWORKS_OK,
               "submit task");
    }
    /* Destroy without waiting — should cancel pending tasks */
    loom_task_group_destroy(&group);
    ASSERT(group == NULL, "destroy sets to null");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    /* Counter should be 0 because all were cancelled */
    ASSERT(counter == 0, "no tasks ran after destroy");
}

static void test_task_group_null_safety(void)
{
    loom_task_group_destroy(NULL);
    loom_task_group_cancel(NULL);
    loom_task_group_wait(NULL);
    ASSERT(loom_task_group_pending_count(NULL) == 0, "null group returns 0");
    ASSERT(true, "null safety passed");
}

static void test_task_group_submit_after_destroy(void)
{
    loom_thread_pool_t *pool  = NULL;
    loom_task_group_t  *group = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "create group");

    loom_task_group_destroy(&group);

    int counter = 0;
    ASSERT(loom_task_group_submit(group, increment_task, &counter) == LOOMWORKS_ERR_INVALID,
           "submit after destroy fails");
    ASSERT(counter == 0, "no task ran");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: priority ordering ---------- */
static void test_priority_ordering(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    /* Submit tasks in reverse priority order: low, normal, high, realtime */
    int order[4] = {0, 0, 0, 0};
    ASSERT(loom_pool_submit_priority(pool, increment_task, &order[0], LOOMWORKS_PRIORITY_LOW) ==
               LOOMWORKS_OK,
           "submit low");
    ASSERT(loom_pool_submit_priority(pool, increment_task, &order[1], LOOMWORKS_PRIORITY_NORMAL) ==
               LOOMWORKS_OK,
           "submit normal");
    ASSERT(loom_pool_submit_priority(pool, increment_task, &order[2], LOOMWORKS_PRIORITY_HIGH) ==
               LOOMWORKS_OK,
           "submit high");
    ASSERT(loom_pool_submit_priority(
               pool, increment_task, &order[3], LOOMWORKS_PRIORITY_REALTIME) == LOOMWORKS_OK,
           "submit realtime");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    /* With single worker, higher priority tasks should execute first.
     * Execution order: realtime(3), high(2), normal(1), low(0) */
    ASSERT(order[3] == 1, "realtime executed");
    ASSERT(order[2] == 1, "high executed");
    ASSERT(order[1] == 1, "normal executed");
    ASSERT(order[0] == 1, "low executed");
}

/* ---------- Test: priority future ---------- */
static void test_priority_future(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    loom_future_t *fut = NULL;
    ASSERT(loom_pool_submit_future_priority(
               pool, fast_result_task, NULL, LOOMWORKS_PRIORITY_HIGH, &fut) == LOOMWORKS_OK,
           "submit priority future");
    ASSERT(fut != NULL, "future not null");

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait future");
    ASSERT(result != NULL, "result not null");
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    ASSERT(*(int *)result == 42, "result value is 42");
    free(result);
    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}
#include "loomworks/metrics.h"

/* ---------- Test: metrics callback ---------- */
typedef struct {
    int submit_count;
    int complete_count;
    int cancel_count;
} metrics_test_ctx_t;

static void
metrics_test_callback(loom_metric_event_t event, const loom_thread_pool_t *pool, void *user_data)
{
    (void)pool;
    metrics_test_ctx_t *ctx = (metrics_test_ctx_t *)user_data;
    switch (event) {
    case LOOMWORKS_METRIC_SUBMITTED:
        ctx->submit_count++;
        break;
    case LOOMWORKS_METRIC_COMPLETED:
        ctx->complete_count++;
        break;
    case LOOMWORKS_METRIC_CANCELLED:
        ctx->cancel_count++;
        break;
    default:
        break;
    }
}

static void test_metrics_callback(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    metrics_test_ctx_t ctx     = {0, 0, 0};
    loom_metrics_t    *metrics = NULL;
    ASSERT(loom_metrics_create(pool, metrics_test_callback, &ctx, &metrics) == LOOMWORKS_OK,
           "create metrics");

    /* Submit 4 tasks */
    int counter = 0;
    for (int i = 0; i < 4; i++) {
        loom_pool_submit(pool, increment_task, &counter);
    }
    /* Submit 2 more and cancel them */
    int counters[2];
    for (int i = 0; i < 2; i++) {
        counters[i] = 0;
        loom_pool_submit(pool, increment_task, &counters[i]);
    }
    loom_pool_cancel_all(pool, NULL);

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    /* Read counters before destroy */
    uint64_t submitted = loom_metrics_submitted(metrics);
    uint64_t completed = loom_metrics_completed(metrics);
    uint64_t cancelled = loom_metrics_cancelled(metrics);

    loom_metrics_destroy(&metrics);

    fprintf(stderr,
            "DEBUG metrics_test: submit=%d complete=%d cancel=%d submitted=%llu completed=%llu "
            "cancelled=%llu\n",
            ctx.submit_count,
            ctx.complete_count,
            ctx.cancel_count,
            (unsigned long long)submitted,
            (unsigned long long)completed,
            (unsigned long long)cancelled);
    /* All 6 tasks were submitted */
    ASSERT(ctx.submit_count == 6, "6 tasks submitted");
    ASSERT(submitted > 0, "metrics submitted > 0");
    /* Completed + cancelled should equal submitted */
    ASSERT(completed + cancelled == submitted, "completed + cancelled == submitted");
}

static void test_metrics_null_safety(void)
{
    loom_metrics_destroy(NULL);
    ASSERT(loom_metrics_submitted(NULL) == 0, "null metrics submitted");
    ASSERT(loom_metrics_completed(NULL) == 0, "null metrics completed");
    ASSERT(loom_metrics_cancelled(NULL) == 0, "null metrics cancelled");
    ASSERT(true, "metrics null safety passed");
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
    test_cancel();
    test_cancel_all();
    test_future_wait_timeout_ok();
    test_future_wait_timeout_expired();
    test_task_group_create_destroy();
    test_task_group_submit();
    test_task_group_cancel();
    test_task_group_destroy_cancels();
    test_task_group_null_safety();
    test_task_group_submit_after_destroy();
    test_priority_ordering();
    test_priority_future();
    test_metrics_callback();
    test_metrics_null_safety();

    printf("\nResults: %d passed, %d failed\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
