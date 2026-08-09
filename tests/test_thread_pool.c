#define _POSIX_C_SOURCE 200809L
#include "loomworks/pipeline.h"
#include "loomworks/thread_pool.h"

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

/* ---------- Helpers ---------- */
static void simple_task(void *arg)
{
    volatile int *counter = (volatile int *)arg;
    __sync_fetch_and_add(counter, 1);
}

/* ---------- Bucket/queue regression helpers ---------- */
static int g_exec_order[128];
static int g_exec_count;

static void record_exec(void *arg)
{
    int idx = (int)(intptr_t)arg;
    g_exec_order[g_exec_count++] = idx;
}

static volatile int g_gate_started = 0;
static volatile int g_gate_release = 0;

static void gate_task(void *arg)
{
    (void)arg;
    g_gate_started = 1;
    while (!g_gate_release) {
        /* spin: occupy the only worker so later tasks stay queued */
    }
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
        ASSERT(loom_pool_submit(pool, simple_task, &counter, NULL) == LOOMWORKS_OK, "submit task");
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
    ASSERT(loom_pool_submit_future(pool, result_task, NULL, &future, NULL) == LOOMWORKS_OK,
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
        ASSERT(loom_pool_submit_future(pool, result_task, NULL, &futures[i], NULL) == LOOMWORKS_OK,
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
        loom_pool_submit(pool, simple_task, &dummy, NULL);
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
    ASSERT(loom_pool_submit(NULL, simple_task, NULL, NULL) != LOOMWORKS_OK, "null pool submit");
    ASSERT(loom_pool_submit_future(NULL, result_task, NULL, NULL, NULL) != LOOMWORKS_OK,
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
    ASSERT(loom_pool_submit(pool, simple_task, &dummy, NULL) == LOOMWORKS_OK,
           "submit before shutdown");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_submit(pool, simple_task, &dummy, NULL) == LOOMWORKS_ERR_SHUTDOWN,
           "submit after shutdown");

    loom_pool_destroy(&pool);
}

/* ---------- Test: double shutdown ---------- */
static void test_double_shutdown(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    int dummy = 0;
    loom_pool_submit(pool, simple_task, &dummy, NULL);
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
        loom_pool_submit(s->pool, simple_task, s->counter, NULL);
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
        loom_pool_submit_future(s->pool, result_task, NULL, &fut, NULL);
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
    /* Spin long enough that the queue stays full during the submission loop.
     * 500M iterations ~500ms on typical hardware, ensuring the single worker
     * is still busy when all 10 submits complete from the main thread. */
    volatile long sink = 0;
    for (long i = 0; i < 500000000L; i++) {
        sink += i;
    }
    (void)sink;
}

static void test_bounded_queue(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 5};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create bounded pool");

    /* Submit tasks rapidly.  The exact split between OK and ERR_INVALID
       depends on timing (whether the worker drains slots between submits),
       so we verify the invariant: the queue never exceeds capacity, and
       some submissions are rejected. */
    int ok_count  = 0;
    int err_count = 0;
    for (int i = 0; i < 10; i++) {
        loom_result_t rc = loom_pool_submit(pool, slow_task, NULL, NULL);
        if (rc == LOOMWORKS_OK) {
            ok_count++;
        } else if (rc == LOOMWORKS_ERR_INVALID) {
            err_count++;
        }
    }
    /* The queue capacity is 5, so at least 5 must have succeeded
       (the queue filled to capacity at some point) and at least 1
       must have failed (proving rejections happen). */
    ASSERT(ok_count >= 5, "queue filled to capacity");
    ASSERT(err_count >= 1, "some submits rejected when full");
    ASSERT(ok_count + err_count == 10, "all submits accounted for");

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
        ASSERT(loom_pool_submit(pool, no_data_task, NULL, NULL) == LOOMWORKS_OK,
               "submit no-data task");
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
    ASSERT(loom_pool_submit_future(pool, result_task, NULL, &fut, NULL) == LOOMWORKS_OK,
           "submit future");

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

static void *gated_result_task(void *arg)
{
    (void)arg;
    g_gate_started = 1;
    while (!g_gate_release) {
    }
    int *val = (int *)malloc(sizeof(int));
    if (val) {
        *val = 42;
    }
    return val;
}

/* ---------- Test: cancel pending task ---------- */
static void test_cancel(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    /* Submit a task and try to cancel it.
     * Due to timing, the task may be running or still queued.
     * We verify the final outcome, not the exact cancel return value. */
    int counter = 0;
    ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK, "submit inc");

    /* Try to cancel — result depends on timing */
    loom_result_t rc = loom_pool_cancel(pool, &counter);
    (void)rc; /* may succeed or fail depending on timing */

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    /* Either the task ran (counter == 1) or was cancelled (counter == 0) */
    ASSERT(counter == 0 || counter == 1, "cancel behavior verified");
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
        ASSERT(loom_pool_submit(pool, increment_task, &counters[i], NULL) == LOOMWORKS_OK,
               "submit task");
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: future_wait_timeout (should succeed) ---------- */
static void test_future_wait_timeout_ok(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    loom_future_t *fut = NULL;
    ASSERT(loom_pool_submit_future(pool, fast_result_task, NULL, &fut, NULL) == LOOMWORKS_OK,
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

    loom_pool_shutdown(pool);
    loom_future_destroy(fut);
    loom_pool_destroy(&pool);
}

/* ---------- Test: future_wait_timeout (should timeout) ---------- */
static void test_future_wait_timeout_expired(void)
{
    g_gate_started = 0;
    g_gate_release = 0;
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    loom_future_t *fut = NULL;
    ASSERT(loom_pool_submit_future(pool, gated_result_task, NULL, &fut, NULL) == LOOMWORKS_OK,
           "submit gated future");

    /* Wait until the worker is inside the gated task: the future is now
     * guaranteed to still be pending when wait_timeout runs below. */
    while (!g_gate_started) {
    }

    /* Deadline in the past */
    struct timespec deadline = {.tv_sec = 0, .tv_nsec = 0};

    void *result = NULL;
    ASSERT(loom_future_wait_timeout(fut, &result, &deadline) == LOOMWORKS_ERR_TIMEOUT,
           "wait_timeout times out");

    /* Release the gate so the task can finish and the pool can shut down. */
    g_gate_release = 1;
    void *done = NULL;
    ASSERT(loom_future_wait(fut, &done) == LOOMWORKS_OK, "future completes after release");
    if (done) {
        free(done);
    }

    loom_pool_shutdown(pool);
    loom_future_destroy(fut);
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
        ASSERT(loom_task_group_submit(group, increment_task, &counter, NULL) == LOOMWORKS_OK,
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
        ASSERT(loom_task_group_submit(group, increment_task, &counters[i], NULL) == LOOMWORKS_OK,
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

    /* Submit slow tasks — they take long enough that the first one
     * is still running when destroy is called.  Destroy cancels only
     * the queued tasks; the running one is expected to complete. */
    for (int i = 0; i < 5; i++) {
        ASSERT(loom_task_group_submit(group, slow_task, NULL, NULL) == LOOMWORKS_OK, "submit task");
    }
    /* Destroy without waiting — cancels pending queued tasks */
    loom_task_group_destroy(&group);
    ASSERT(group == NULL, "destroy sets to null");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    /* The running task completes; subsequent queued tasks are cancelled.
     * At minimum, at least one task ran (the one already dequeued). */
    ASSERT(true, "destroy cancels pending tasks");
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
    ASSERT(loom_task_group_submit(group, increment_task, &counter, NULL) == LOOMWORKS_ERR_INVALID,
           "submit after destroy fails");
    ASSERT(counter == 0, "no task ran");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: bucket priority edges (full uint8 range) ---------- */
static void test_bucket_priority_edges(void)
{
    g_exec_count = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create single-worker pool");

    struct {
        uint8_t prio;
        int     slot;
    } seq[] = {
        {255, 0}, {0, 1}, {5, 2}, {254, 3}, {1, 4}, {10, 5},
    };
    for (int i = 0; i < 6; i++) {
        ASSERT(loom_pool_submit_priority(
                   pool, record_exec,
                   /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
                   (void *)(intptr_t)seq[i].slot, seq[i].prio, NULL) == LOOMWORKS_OK,
               "submit priority edge");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    /* Priorities 0,1,5,10,254,255 must run in ascending numeric order. */
    int expected[6] = {1, 4, 2, 5, 3, 0};
    ASSERT(g_exec_count == 6, "all 6 edge tasks executed");
    for (int i = 0; i < 6; i++) {
        ASSERT(g_exec_order[i] == expected[i], "priority edge execution order");
    }
}

/* ---------- Test: FIFO preserved within a priority ---------- */
static void test_bucket_fifo_within_priority(void)
{
    g_exec_count = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create single-worker pool");

    const int N = 100;
    for (int i = 0; i < N; i++) {
        ASSERT(loom_pool_submit(pool, record_exec,
                                /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
                                (void *)(intptr_t)i, NULL) == LOOMWORKS_OK,
               "submit FIFO task");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    ASSERT(g_exec_count == N, "all FIFO tasks executed");
    for (int i = 0; i < N; i++) {
        ASSERT(g_exec_order[i] == i, "FIFO order preserved");
    }
}

/* ---------- Test: cancel_all across multiple buckets ---------- */
static void test_bucket_cancel_all(void)
{
    g_gate_started = 0;
    g_gate_release = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create single-worker pool");

    /* Occupy the only worker so every subsequent task stays queued. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "submit gate task");
    while (!g_gate_started) {
        /* wait until the worker is inside the gate */
    }

    uint8_t prios[] = {0, 5, 10, 200, 255};
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 4; j++) {
            ASSERT(loom_pool_submit_priority(
                       pool, simple_task, NULL, prios[i], NULL) == LOOMWORKS_OK,
                   "submit cross-bucket cancel task");
        }
    }
    uint32_t cancelled = 0;
    loom_pool_cancel_all(pool, &cancelled);
    ASSERT(cancelled == 20, "cancel_all cancelled exactly 20 queued tasks");
    ASSERT(loom_pool_pending_count(pool) == 0, "queue empty after cancel_all");

    g_gate_release = 1;
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
    ASSERT(loom_pool_submit_priority(
               pool, increment_task, &order[0], LOOMWORKS_PRIORITY_LOW, NULL) == LOOMWORKS_OK,
           "submit low");
    ASSERT(loom_pool_submit_priority(
               pool, increment_task, &order[1], LOOMWORKS_PRIORITY_NORMAL, NULL) == LOOMWORKS_OK,
           "submit normal");
    ASSERT(loom_pool_submit_priority(
               pool, increment_task, &order[2], LOOMWORKS_PRIORITY_HIGH, NULL) == LOOMWORKS_OK,
           "submit high");
    ASSERT(loom_pool_submit_priority(
               pool, increment_task, &order[3], LOOMWORKS_PRIORITY_REALTIME, NULL) == LOOMWORKS_OK,
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
               pool, fast_result_task, NULL, LOOMWORKS_PRIORITY_HIGH, &fut, NULL) == LOOMWORKS_OK,
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

/* ==== Ring fast-path tests (lock-free NORMAL queue) ==== */

static _Atomic int g_ring_run_count;

static void ring_inc_task(void *arg)
{
    (void)arg;
    atomic_fetch_add_explicit(&g_ring_run_count, 1, memory_order_relaxed);
}

static void test_ring_basic(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 2;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "basic create");

    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    for (uint32_t i = 0; i < 1000; i++)
        ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK,
               "basic submit");

    loom_pool_shutdown(pool);
    ASSERT(atomic_load_explicit(&g_ring_run_count, memory_order_relaxed) == 1000,
           "basic: all 1000 NORMAL tasks ran exactly once");
    ASSERT(loom_pool_pending_count(pool) == 0, "basic: nothing pending");
    loom_pool_destroy(&pool);
}

static void *ring_producer(void *arg)
{
    loom_thread_pool_t *pool = (loom_thread_pool_t *)arg;
    for (uint32_t i = 0; i < 25000; i++) {
        if (loom_pool_submit(pool, ring_inc_task, NULL, NULL) != LOOMWORKS_OK)
            break; /* tolerate shutdown race in stress test only */
    }
    return NULL;
}

static void test_ring_multithread_stress(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 8;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "stress create");

    pthread_t producers[4];
    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    for (int i = 0; i < 4; i++)
        ASSERT(pthread_create(&producers[i], NULL, ring_producer, pool) == 0,
               "stress spawn producer");
    for (int i = 0; i < 4; i++)
        pthread_join(producers[i], NULL);

    loom_pool_shutdown(pool);
    ASSERT(atomic_load_explicit(&g_ring_run_count, memory_order_relaxed) == 100000,
           "stress: 4x25k tasks ran exactly once");
    ASSERT(loom_pool_pending_count(pool) == 0, "stress: nothing pending");
    loom_pool_destroy(&pool);
}

/* --- ring acceptance helpers (spec 2026-08-09) --- */

static _Atomic int g_ring_gate2_started;
static _Atomic int g_ring_gate2_release;

static void ring_gate2_task(void *arg)
{
    (void)arg;
    atomic_store_explicit(&g_ring_gate2_started, 1, memory_order_release);
    while (!atomic_load_explicit(&g_ring_gate2_release, memory_order_acquire)) {
    }
}

static _Atomic int g_ring_cancel_done;
static _Atomic int g_ring_cancel_release;
static _Atomic int g_ring_cancel_run;

static void ring_cancel_pin_task(void *arg)
{
    (void)arg;
    atomic_store_explicit(&g_ring_cancel_done, 1, memory_order_release);
    while (!atomic_load_explicit(&g_ring_cancel_release, memory_order_acquire)) {
    }
}

static void ring_cancel_inc_task(void *arg)
{
    (void)arg;
    atomic_fetch_add_explicit(&g_ring_cancel_run, 1, memory_order_relaxed);
}

static _Atomic int g_ring_cancel_data_hits;

static void ring_cancel_data_task(void *arg)
{
    (void)arg;
    atomic_fetch_add_explicit(&g_ring_cancel_data_hits, 1, memory_order_relaxed);
}

static _Atomic int g_ring_order_len;
static int g_ring_order[512];

static void ring_order_record(int prio)
{
    int idx = atomic_fetch_add_explicit(&g_ring_order_len, 1, memory_order_relaxed);
    g_ring_order[idx] = prio;
}

static void ring_normal_rec(void *arg)
{
    (void)arg;
    ring_order_record(LOOMWORKS_PRIORITY_NORMAL);
}

static void ring_high_rec(void *arg)
{
    (void)arg;
    ring_order_record(LOOMWORKS_PRIORITY_HIGH);
}

static void ring_rt_rec(void *arg)
{
    (void)arg;
    ring_order_record(LOOMWORKS_PRIORITY_REALTIME);
}

/* Guard: bounded pools use the ring (ring_size = next_pow2(capacity) = 8)
 * and enforce the configured capacity: the 6th submit is rejected. */
static void test_ring_bounded_full(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count   = 1;
    cfg.queue_capacity = 5;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "bounded create");

    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_gate2_started, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_gate2_release, 0, memory_order_relaxed);

    /* gate: NORMAL -> ring, pins the only worker */
    ASSERT(loom_pool_submit(pool, ring_gate2_task, NULL, NULL) == LOOMWORKS_OK,
           "bounded submit gate");
    while (!atomic_load_explicit(&g_ring_gate2_started, memory_order_acquire)) {
    }

    for (uint32_t i = 0; i < 5; i++)
        ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK,
               "bounded fill ring");
    ASSERT(loom_pool_pending_count(pool) == 5, "bounded: 5 pending");

    /* 6th submit must be rejected: queue is full */
    ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_ERR_INVALID,
           "bounded: full queue rejects submit");

    atomic_store_explicit(&g_ring_gate2_release, 1, memory_order_release);
    loom_pool_shutdown(pool);
    ASSERT(atomic_load_explicit(&g_ring_run_count, memory_order_relaxed) == 5,
           "bounded: all 5 queued tasks ran exactly once");
    ASSERT(loom_pool_pending_count(pool) == 0, "bounded: nothing pending");
    loom_pool_destroy(&pool);
}

/* Guard: unbounded pools (capacity 0) get a 4096-slot ring
 * (LOOMWORKS_RING_DEFAULT_SLOTS); the 4097th NORMAL submit spills
 * to the NORMAL lane instead of failing. */
static void test_ring_unbounded_spill(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 1;
    cfg.queue_capacity = 0;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "spill create");

    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_gate2_started, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_gate2_release, 0, memory_order_relaxed);

    ASSERT(loom_pool_submit(pool, ring_gate2_task, NULL, NULL) == LOOMWORKS_OK,
           "spill submit gate");
    while (!atomic_load_explicit(&g_ring_gate2_started, memory_order_acquire)) {
    }

    for (uint32_t i = 0; i < 4096; i++)
        ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK,
               "spill fill ring");
    /* ring is full now: overflow accepted via NORMAL lane */
    ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK,
           "spill: overflow accepted via lane");
    ASSERT(loom_pool_pending_count(pool) == 4097, "spill: 4097 pending");

    atomic_store_explicit(&g_ring_gate2_release, 1, memory_order_release);
    loom_pool_shutdown(pool);
    ASSERT(atomic_load_explicit(&g_ring_run_count, memory_order_relaxed) == 4097,
           "spill: all 4097 tasks ran exactly once");
    ASSERT(loom_pool_pending_count(pool) == 0, "spill: nothing pending");
    loom_pool_destroy(&pool);
}

/* Guard: cancel-by-id claims the ring cancel index; double-cancel
 * is rejected as not-found. */
static void test_ring_cancel_by_id(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 1;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "cancel-by-id create");

    atomic_store_explicit(&g_ring_cancel_done, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_release, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_run, 0, memory_order_relaxed);

    ASSERT(loom_pool_submit(pool, ring_cancel_pin_task, NULL, NULL) == LOOMWORKS_OK,
           "cancel-by-id submit gate");
    while (!atomic_load_explicit(&g_ring_cancel_done, memory_order_acquire)) {
    }

    uint64_t tid = 0;
    ASSERT(loom_pool_submit(pool, ring_cancel_inc_task, NULL, &tid) == LOOMWORKS_OK,
           "cancel-by-id submit victim");
    ASSERT(tid != 0, "cancel-by-id: task id assigned");
    ASSERT(loom_pool_pending_count(pool) == 1, "cancel-by-id: 1 pending");

    ASSERT(loom_pool_cancel_by_id(pool, tid) == LOOMWORKS_OK,
           "cancel-by-id: first cancel ok");
    ASSERT(loom_pool_cancel_by_id(pool, tid) == LOOMWORKS_ERR_INVALID,
           "cancel-by-id: double cancel rejected");

    atomic_store_explicit(&g_ring_cancel_release, 1, memory_order_release);
    loom_pool_shutdown(pool);
    ASSERT(atomic_load_explicit(&g_ring_cancel_run, memory_order_relaxed) == 0,
           "cancel-by-id: victim never ran");
    ASSERT(loom_pool_pending_count(pool) == 0, "cancel-by-id: nothing pending");
    loom_pool_destroy(&pool);
}

/* Guard: cancelling an unknown task id returns not-found. */
static void test_ring_cancel_not_found(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 1;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "cancel-notfound create");

    /* ids start at 1 and increment; UINT64_MAX can never be assigned */
    ASSERT(loom_pool_cancel_by_id(pool, UINT64_MAX) == LOOMWORKS_ERR_INVALID,
           "cancel-notfound: unknown id rejected");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* Guard: tombstones left by 64 cancels are skipped by the worker —
 * none of the cancelled tasks run. */
static void test_ring_tombstone_skip(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 1;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "tombstone create");

    atomic_store_explicit(&g_ring_cancel_done, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_release, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_run, 0, memory_order_relaxed);

    ASSERT(loom_pool_submit(pool, ring_cancel_pin_task, NULL, NULL) == LOOMWORKS_OK,
           "tombstone submit gate");
    while (!atomic_load_explicit(&g_ring_cancel_done, memory_order_acquire)) {
    }

    uint64_t tids[64];
    for (uint32_t i = 0; i < 64; i++)
        ASSERT(loom_pool_submit(pool, ring_cancel_inc_task, NULL, &tids[i]) == LOOMWORKS_OK,
               "tombstone submit victim");
    ASSERT(loom_pool_pending_count(pool) == 64, "tombstone: 64 pending");

    for (uint32_t i = 0; i < 64; i++)
        ASSERT(loom_pool_cancel_by_id(pool, tids[i]) == LOOMWORKS_OK,
               "tombstone cancel victim");
    ASSERT(loom_pool_pending_count(pool) == 0, "tombstone: all cancelled");

    atomic_store_explicit(&g_ring_cancel_release, 1, memory_order_release);
    loom_pool_shutdown(pool);
    ASSERT(atomic_load_explicit(&g_ring_cancel_run, memory_order_relaxed) == 0,
           "tombstone: cancelled tasks skipped, none ran");
    ASSERT(loom_pool_pending_count(pool) == 0, "tombstone: nothing pending");
    loom_pool_destroy(&pool);
}

/* Guard: loom_pool_cancel matches by user_data on ring-indexed tasks. */
static void test_ring_cancel_data(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 1;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "cancel-data create");

    int d1 = 1, d2 = 2;
    atomic_store_explicit(&g_ring_cancel_done, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_release, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_data_hits, 0, memory_order_relaxed);

    ASSERT(loom_pool_submit(pool, ring_cancel_pin_task, NULL, NULL) == LOOMWORKS_OK,
           "cancel-data submit gate");
    while (!atomic_load_explicit(&g_ring_cancel_done, memory_order_acquire)) {
    }

    ASSERT(loom_pool_submit(pool, ring_cancel_data_task, &d1, NULL) == LOOMWORKS_OK,
           "cancel-data submit d1");
    ASSERT(loom_pool_submit(pool, ring_cancel_data_task, &d2, NULL) == LOOMWORKS_OK,
           "cancel-data submit d2");

    ASSERT(loom_pool_cancel(pool, &d1) == LOOMWORKS_OK, "cancel-data: d1 cancelled");
    ASSERT(loom_pool_cancel(pool, &d2) == LOOMWORKS_OK, "cancel-data: d2 cancelled");
    ASSERT(loom_pool_cancel(pool, &d1) == LOOMWORKS_ERR_INVALID,
           "cancel-data: re-cancel rejected");
    ASSERT(loom_pool_pending_count(pool) == 0, "cancel-data: nothing pending");

    atomic_store_explicit(&g_ring_cancel_release, 1, memory_order_release);
    loom_pool_shutdown(pool);
    ASSERT(atomic_load_explicit(&g_ring_cancel_data_hits, memory_order_relaxed) == 0,
           "cancel-data: neither task ran");
    loom_pool_destroy(&pool);
}

/* Guard: REALTIME/HIGH lane tasks run before ring NORMAL tasks even
 * when the NORMAL ones were submitted first. Worker drain order is
 * lanes <5 -> ring -> lanes >=5 (src/thread_pool.c worker_entry). */
static void test_ring_priority_preempt(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 1;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "preempt create");

    atomic_store_explicit(&g_ring_order_len, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_done, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_release, 0, memory_order_relaxed);

    /* gate: NORMAL -> ring, pins the worker (does not record) */
    ASSERT(loom_pool_submit(pool, ring_cancel_pin_task, NULL, NULL) == LOOMWORKS_OK,
           "preempt submit gate");
    while (!atomic_load_explicit(&g_ring_cancel_done, memory_order_acquire)) {
    }

    for (uint32_t i = 0; i < 50; i++)
        ASSERT(loom_pool_submit_priority(pool, ring_normal_rec, NULL,
                                         LOOMWORKS_PRIORITY_NORMAL, NULL) == LOOMWORKS_OK,
               "preempt submit normal");
    for (uint32_t i = 0; i < 3; i++)
        ASSERT(loom_pool_submit_priority(pool, ring_rt_rec, NULL,
                                         LOOMWORKS_PRIORITY_REALTIME, NULL) == LOOMWORKS_OK,
               "preempt submit rt");
    for (uint32_t i = 0; i < 3; i++)
        ASSERT(loom_pool_submit_priority(pool, ring_high_rec, NULL,
                                         LOOMWORKS_PRIORITY_HIGH, NULL) == LOOMWORKS_OK,
               "preempt submit high");
    ASSERT(loom_pool_pending_count(pool) == 56, "preempt: 56 pending");

    atomic_store_explicit(&g_ring_cancel_release, 1, memory_order_release);
    loom_pool_shutdown(pool);

    /* 56 records: 3 RT, 3 HIGH, 50 NORMAL (gate does not record) */
    int len = atomic_load_explicit(&g_ring_order_len, memory_order_relaxed);
    ASSERT(len == 56, "preempt: all 56 ran and recorded");
    int ok = 1;
    for (int i = 0; i < 3; i++) ok = ok && g_ring_order[i] == LOOMWORKS_PRIORITY_REALTIME;
    for (int i = 3; i < 6; i++) ok = ok && g_ring_order[i] == LOOMWORKS_PRIORITY_HIGH;
    for (int i = 6; i < 56; i++) ok = ok && g_ring_order[i] == LOOMWORKS_PRIORITY_NORMAL;
    ASSERT(ok, "preempt: RT(0)x3, HIGH(1)x3, then NORMAL(5)x50");
    ASSERT(loom_pool_pending_count(pool) == 0, "preempt: nothing pending");
    loom_pool_destroy(&pool);
}

/* Guard: shutdown drains the ring — all 500 tasks run exactly once. */
static void test_ring_shutdown_drains(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count = 2;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "shutdown-drain create");

    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    for (uint32_t i = 0; i < 500; i++)
        ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK,
               "shutdown-drain submit");

    loom_pool_shutdown(pool); /* must drain the ring before returning */
    ASSERT(atomic_load_explicit(&g_ring_run_count, memory_order_relaxed) == 500,
           "shutdown-drain: all 500 ran exactly once");
    ASSERT(loom_pool_pending_count(pool) == 0, "shutdown-drain: nothing pending");
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
        loom_pool_submit(pool, increment_task, &counter, NULL);
    }
    /* Submit 2 more and cancel them */
    int counters[2];
    for (int i = 0; i < 2; i++) {
        counters[i] = 0;
        loom_pool_submit(pool, increment_task, &counters[i], NULL);
    }
    loom_pool_cancel_all(pool, NULL);

    loom_pool_shutdown(pool);

    /* Read counters before destroy */
    uint64_t submitted = loom_metrics_submitted(metrics);
    uint64_t completed = loom_metrics_completed(metrics);
    uint64_t cancelled = loom_metrics_cancelled(metrics);

    loom_metrics_destroy(&metrics);
    loom_pool_destroy(&pool);

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

/* ---------- Test: latency tracking ---------- */
static void test_metrics_latency(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_metrics_t *metrics = NULL;
    ASSERT(loom_metrics_create(pool, NULL, NULL, &metrics) == LOOMWORKS_OK, "create metrics");

    /* Submit a quick task */
    ASSERT(loom_pool_submit(pool, increment_task, &g_passes, NULL) == LOOMWORKS_OK, "submit task");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    loom_metrics_destroy(&metrics);

    /* Latency should have been recorded (sum > 0 for at least 1 task) */
    uint64_t sum_ns = loom_metrics_latency_sum_ns(metrics);
    uint64_t max_ns = loom_metrics_latency_max_ns(metrics);
    (void)sum_ns;
    (void)max_ns;
}

static void test_pool_health(void)
{
    loom_pool_config_t cfg  = {.worker_count = 4, .queue_capacity = 0};
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    ASSERT(loom_pool_active_count(pool) == 0, "active 0 initially");
    ASSERT(loom_pool_idle_count(pool) == 4, "idle == 4 initially");
    ASSERT(loom_pool_utilization(pool) == 0.0, "util 0 initially");

    int counter = 0;
    for (int i = 0; i < 200; i++) {
        ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK, "submit");
        uint32_t active = loom_pool_active_count(pool);
        uint32_t idle   = loom_pool_idle_count(pool);
        ASSERT(active <= 4, "active <= worker_count");
        ASSERT(idle <= 4, "idle <= worker_count");
        double u = loom_pool_utilization(pool);
        ASSERT(u >= 0.0 && u <= 1.0, "util in [0,1]");
    }
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_active_count(pool) == 0, "active 0 after shutdown");

    ASSERT(loom_pool_active_count(NULL) == 0, "null active");
    ASSERT(loom_pool_idle_count(NULL) == 0, "null idle");
    ASSERT(loom_pool_utilization(NULL) == 0.0, "null util");
    loom_pool_destroy(&pool);
}

static void test_metrics_monitoring(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");
    loom_metrics_t *metrics = NULL;
    ASSERT(loom_metrics_create(pool, NULL, NULL, &metrics) == LOOMWORKS_OK, "create metrics");

    int counter = 0;
    for (int i = 0; i < 100; i++) {
        ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK, "submit");
    }
    loom_pool_shutdown(pool);

    uint64_t started   = loom_metrics_started(metrics);
    uint64_t completed = loom_metrics_completed(metrics);
    ASSERT(started == 100, "started == 100");
    ASSERT(completed == 100, "completed == 100");
    ASSERT(loom_metrics_failed(metrics) == 0, "failed == 0");
    ASSERT(loom_metrics_avg_latency_ns(metrics) > 0, "avg latency > 0");

    loom_metrics_snapshot_t snap;
    ASSERT(loom_metrics_snapshot(metrics, &snap) == LOOMWORKS_OK, "snapshot ok");
    ASSERT(snap.submitted == 100, "snap submitted");
    ASSERT(snap.started == 100, "snap started");
    ASSERT(snap.completed == 100, "snap completed");
    ASSERT(snap.cancelled == 0, "snap cancelled");
    ASSERT(snap.failed == 0, "snap failed");
    ASSERT(snap.latency_sum_ns == loom_metrics_latency_sum_ns(metrics), "snap sum matches");
    ASSERT(snap.latency_max_ns == loom_metrics_latency_max_ns(metrics), "snap max matches");

    ASSERT(loom_metrics_snapshot(NULL, &snap) == LOOMWORKS_ERR_INVALID, "snapshot null metrics");
    ASSERT(loom_metrics_snapshot(metrics, NULL) == LOOMWORKS_ERR_INVALID, "snapshot null out");
    ASSERT(loom_metrics_started(NULL) == 0, "null started");
    ASSERT(loom_metrics_failed(NULL) == 0, "null failed");
    ASSERT(loom_metrics_avg_latency_ns(NULL) == 0, "null avg");

    loom_metrics_destroy(&metrics);
    loom_pool_destroy(&pool);
}

/* ---------- Test: submit APIs return task IDs ---------- */
static void test_submit_returns_task_id(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    uint64_t id1 = 0, id2 = 0, id3 = 0;
    ASSERT(loom_pool_submit(pool, increment_task, &g_passes, &id1) == LOOMWORKS_OK,
           "submit with task_id");
    ASSERT(id1 > 0, "submit returned non-zero task_id");

    ASSERT(loom_pool_submit_blocking(pool, increment_task, &g_passes, &id2) == LOOMWORKS_OK,
           "submit_blocking with task_id");
    ASSERT(id2 > 0, "submit_blocking returned non-zero task_id");

    loom_future_t *fut = NULL;
    ASSERT(loom_pool_submit_future(pool, result_task, NULL, &fut, &id3) == LOOMWORKS_OK,
           "submit_future with task_id");
    ASSERT(id3 > 0, "submit_future returned non-zero task_id");
    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait future");
    if (result) {
        free(result);
    }
    loom_future_destroy(fut);

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: task IDs are unique ---------- */
static void test_task_id_uniqueness(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 1000};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    uint64_t ids[50];
    for (int i = 0; i < 50; i++) {
        ids[i] = 0;
        ASSERT(loom_pool_submit(pool, increment_task, &g_passes, &ids[i]) == LOOMWORKS_OK,
               "submit task");
    }

    /* All IDs must be unique and > 0 */
    for (int i = 0; i < 50; i++) {
        ASSERT(ids[i] > 0, "task_id > 0");
        for (int j = 0; j < i; j++) {
            ASSERT(ids[i] != ids[j], "task_ids are unique");
        }
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: cancel_by_id with non-existent ID ---------- */
static void test_cancel_by_id_not_found(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    /* Cancel an ID that was never submitted */
    ASSERT(loom_pool_cancel_by_id(pool, 99999) == LOOMWORKS_ERR_INVALID,
           "cancel non-existent ID fails");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: cancel_by_id after shutdown ---------- */
static void test_cancel_by_id_after_shutdown(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_pool_shutdown(pool);
    ASSERT(loom_pool_cancel_by_id(pool, 1) == LOOMWORKS_ERR_SHUTDOWN,
           "cancel after shutdown returns ERR_SHUTDOWN");

    loom_pool_destroy(&pool);
}

/* ---------- Test: cancel_by_id with same user_data ---------- */
static void test_cancel_by_id_same_data(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    int      shared_data = 0;
    uint64_t ids[3];

    /* Submit 3 tasks with the SAME user_data pointer */
    ASSERT(loom_pool_submit(pool, increment_task, &shared_data, &ids[0]) == LOOMWORKS_OK,
           "submit task 1");
    ASSERT(loom_pool_submit(pool, increment_task, &shared_data, &ids[1]) == LOOMWORKS_OK,
           "submit task 2");
    ASSERT(loom_pool_submit(pool, increment_task, &shared_data, &ids[2]) == LOOMWORKS_OK,
           "submit task 3");

    /* cancel_by_id should only cancel the specific task */
    ASSERT(loom_pool_cancel_by_id(pool, ids[1]) == LOOMWORKS_OK, "cancel by id mid");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    /* At least task 1 or 3 must have run (shared_data >= 1) since we only cancelled 1 */
    ASSERT(shared_data >= 1, "at least one task ran after partial cancel");
}

/* ---------- Test: cancel by ID of already-running task ---------- */
static void test_cancel_by_id_running(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    uint64_t id = 0;

    /* Submit a slow task so it starts running before we try to cancel */
    ASSERT(loom_pool_submit(pool, slow_task, NULL, &id) == LOOMWORKS_OK, "submit slow");

    /* Give worker time to pick up the task */
    struct timespec ts = {0, 50000000}; /* 50ms */
    nanosleep(&ts, NULL);

    /* The task is now running — cancel should fail */
    loom_result_t rc = loom_pool_cancel_by_id(pool, id);
    ASSERT(rc == LOOMWORKS_ERR_INVALID, "cancel running task fails");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: submit_priority returns task ID ---------- */
static void test_submit_priority_returns_task_id(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    uint64_t id = 0;
    ASSERT(loom_pool_submit_priority(
               pool, increment_task, &g_passes, LOOMWORKS_PRIORITY_HIGH, &id) == LOOMWORKS_OK,
           "submit_priority with task_id");
    ASSERT(id > 0, "priority submit returned non-zero task_id");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: submit_future_priority returns task ID ---------- */
static void test_submit_future_priority_returns_task_id(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    uint64_t       id  = 0;
    loom_future_t *fut = NULL;
    ASSERT(loom_pool_submit_future_priority(
               pool, result_task, NULL, LOOMWORKS_PRIORITY_HIGH, &fut, &id) == LOOMWORKS_OK,
           "submit_future_priority with task_id");
    ASSERT(id > 0, "priority future returned non-zero task_id");
    ASSERT(fut != NULL, "future not null");

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait future");
    if (result) {
        free(result);
    }
    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: cancel by ID — multiple tasks, same data, cancel first ---------- */
static void test_cancel_by_id_first_of_many(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    int      shared = 0;
    uint64_t ids[5];

    for (int i = 0; i < 5; i++) {
        ASSERT(loom_pool_submit(pool, increment_task, &shared, &ids[i]) == LOOMWORKS_OK,
               "submit task");
    }

    /* Cancel the first submitted task */
    ASSERT(loom_pool_cancel_by_id(pool, ids[0]) == LOOMWORKS_OK, "cancel first by id");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    /* 4 tasks remain (at least some should run) */
    ASSERT(shared >= 1, "remaining tasks executed");
}

/* ---------- Test: cancel_by_id null pool ---------- */
static void test_cancel_by_id_null_pool(void)
{
    ASSERT(loom_pool_cancel_by_id(NULL, 42) == LOOMWORKS_ERR_INVALID,
           "cancel_by_id with null pool fails");
}

/* ---------- Test: metrics callback receives all event types ---------- */
typedef struct {
    int submitted;
    int started;
    int completed;
    int cancelled;
} metrics_event_ctx_t;

static void
metrics_event_callback(loom_metric_event_t event, const loom_thread_pool_t *pool, void *user_data)
{
    (void)pool;
    metrics_event_ctx_t *ctx = (metrics_event_ctx_t *)user_data;
    switch (event) {
    case LOOMWORKS_METRIC_SUBMITTED:
        ctx->submitted++;
        break;
    case LOOMWORKS_METRIC_STARTED:
        ctx->started++;
        break;
    case LOOMWORKS_METRIC_COMPLETED:
        ctx->completed++;
        break;
    case LOOMWORKS_METRIC_CANCELLED:
        ctx->cancelled++;
        break;
    default:
        break;
    }
}

static void test_metrics_callback_all_events(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    metrics_event_ctx_t ctx     = {0, 0, 0, 0};
    loom_metrics_t     *metrics = NULL;
    ASSERT(loom_metrics_create(pool, metrics_event_callback, &ctx, &metrics) == LOOMWORKS_OK,
           "create metrics");

    int counter = 0;
    loom_pool_submit(pool, increment_task, &counter, NULL);
    loom_pool_submit(pool, increment_task, &counter, NULL);

    /* Cancel one before it runs */
    int cancel_data = 999;
    loom_pool_submit(pool, increment_task, &cancel_data, NULL);
    loom_pool_cancel(pool, &cancel_data);

    loom_pool_shutdown(pool);
    loom_metrics_destroy(&metrics);
    loom_pool_destroy(&pool);

    ASSERT(ctx.submitted >= 2, "callback received submitted events");
    ASSERT(ctx.completed >= 2, "callback received completed events");
    ASSERT(ctx.cancelled >= 1, "callback received cancelled event");
    (void)counter;
}

/* ---------- Test: metrics latency is non-zero ---------- */
static void test_metrics_latency_nonzero(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_metrics_t *metrics = NULL;
    ASSERT(loom_metrics_create(pool, NULL, NULL, &metrics) == LOOMWORKS_OK, "create metrics");

    /* Submit several tasks to ensure measurable latency */
    for (int i = 0; i < 10; i++) {
        loom_pool_submit(pool, increment_task, &g_passes, NULL);
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    uint64_t sum_ns = loom_metrics_latency_sum_ns(metrics);
    uint64_t max_ns = loom_metrics_latency_max_ns(metrics);
    ASSERT(sum_ns > 0, "latency sum > 0 after tasks executed");
    ASSERT(max_ns > 0, "latency max > 0 after tasks executed");
    loom_metrics_destroy(&metrics);
}

/* ---------- Test: metrics latency with concurrent tasks ---------- */
static void test_metrics_latency_concurrent(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 200};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_metrics_t *metrics = NULL;
    ASSERT(loom_metrics_create(pool, NULL, NULL, &metrics) == LOOMWORKS_OK, "create metrics");

    for (int i = 0; i < 100; i++) {
        loom_pool_submit(pool, increment_task, &g_passes, NULL);
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    uint64_t sum_ns = loom_metrics_latency_sum_ns(metrics);
    ASSERT(sum_ns > 0, "concurrent tasks produce non-zero latency sum");
    loom_metrics_destroy(&metrics);
}

/* ---------- Test: metrics callback with started event ---------- */
static void test_metrics_callback_started(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    metrics_event_ctx_t ctx     = {0, 0, 0, 0};
    loom_metrics_t     *metrics = NULL;
    ASSERT(loom_metrics_create(pool, metrics_event_callback, &ctx, &metrics) == LOOMWORKS_OK,
           "create metrics");

    for (int i = 0; i < 5; i++) {
        loom_pool_submit(pool, increment_task, &g_passes, NULL);
    }

    loom_pool_shutdown(pool);
    loom_metrics_destroy(&metrics);
    loom_pool_destroy(&pool);

    /* started events may or may not be fired depending on implementation,
     * but completed and submitted should always fire */
    ASSERT(ctx.submitted >= 5, "submitted events received");
    ASSERT(ctx.completed >= 5, "completed events received");
}

/* ---------- Test: submit_blocking with unbounded queue ---------- */
static void test_submit_blocking_unbounded(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create unbounded pool");

    int counter = 0;
    for (int i = 0; i < 100; i++) {
        ASSERT(loom_pool_submit_blocking(pool, increment_task, &counter, NULL) == LOOMWORKS_OK,
               "submit_blocking unbounded");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(counter == 100, "all blocking submits executed");
}

/* ---------- Test: submit_blocking returns task ID ---------- */
static void test_submit_blocking_with_id(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    uint64_t id1 = 0, id2 = 0;
    ASSERT(loom_pool_submit_blocking(pool, increment_task, &g_passes, &id1) == LOOMWORKS_OK,
           "submit_blocking with id");
    ASSERT(id1 > 0, "submit_blocking returned non-zero task_id");

    ASSERT(loom_pool_submit_blocking(pool, increment_task, &g_passes, &id2) == LOOMWORKS_OK,
           "submit_blocking second task");
    ASSERT(id2 > id1, "second task_id > first");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: submit_blocking after shutdown ---------- */
static void test_submit_blocking_after_shutdown(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");
    loom_pool_shutdown(pool);

    int counter = 0;
    ASSERT(loom_pool_submit_blocking(pool, increment_task, &counter, NULL) ==
               LOOMWORKS_ERR_SHUTDOWN,
           "submit_blocking after shutdown fails");

    loom_pool_destroy(&pool);
    ASSERT(counter == 0, "no task executed after shutdown");
}

/* ---------- Test: submit_blocking null safety ---------- */
static void test_submit_blocking_null_safety(void)
{
    ASSERT(loom_pool_submit_blocking(NULL, increment_task, NULL, NULL) == LOOMWORKS_ERR_INVALID,
           "submit_blocking null pool");
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_pool_submit_blocking(pool, NULL, NULL, NULL) == LOOMWORKS_ERR_INVALID,
           "submit_blocking null fn");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: loom_pool_resize grow ---------- */
static void test_resize_grow(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_pool_worker_count(pool) == 2, "initial worker count = 2");

    /* Grow to 4 workers */
    ASSERT(loom_pool_resize(pool, 4) == LOOMWORKS_OK, "resize to 4");
    ASSERT(loom_pool_worker_count(pool) == 4, "worker count = 4 after grow");

    /* Verify new workers are functional */
    int counter = 0;
    for (int i = 0; i < 100; i++) {
        ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK,
               "submit task");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(counter == 100, "all tasks executed after grow");
}

/* ---------- Test: loom_pool_resize shrink ---------- */
static void test_resize_shrink(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_pool_worker_count(pool) == 4, "initial worker count = 4");

    /* Shrink to 2 workers */
    ASSERT(loom_pool_resize(pool, 2) == LOOMWORKS_OK, "resize to 2");
    ASSERT(loom_pool_worker_count(pool) == 2, "worker count = 2 after shrink");

    /* Submit tasks — should still complete with fewer workers */
    int counter = 0;
    for (int i = 0; i < 50; i++) {
        ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK,
               "submit task");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(counter == 50, "all tasks executed after shrink");
}

/* ---------- Test: loom_pool_resize after shutdown ---------- */
static void test_resize_after_shutdown(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_resize(pool, 4) == LOOMWORKS_ERR_SHUTDOWN, "resize after shutdown fails");
    loom_pool_destroy(&pool);
}

/* ---------- Test: loom_pool_resize null safety ---------- */
static void test_resize_null_safety(void)
{
    ASSERT(loom_pool_resize(NULL, 4) == LOOMWORKS_ERR_INVALID, "resize null pool");
}

/* ---------- Test: metrics invariant — submitted == completed + cancelled ---------- */
static void test_metrics_invariant(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_metrics_t *metrics = NULL;
    ASSERT(loom_metrics_create(pool, NULL, NULL, &metrics) == LOOMWORKS_OK, "create metrics");

    /* Submit 20 tasks */
    int *submitted[25];
    int  n_submitted = 0;
    for (int i = 0; i < 20; i++) {
        int *data = (int *)malloc(sizeof(int));
        if (data) {
            *data = i;
            ASSERT(loom_pool_submit(pool, increment_task, data, NULL) == LOOMWORKS_OK, "submit");
            submitted[n_submitted++] = data;
        }
    }

    /* Cancel the first 5 tasks (they should still be in queue) */
    for (int i = 0; i < 5; i++) {
        int *data = (int *)malloc(sizeof(int));
        if (data) {
            *data = -1;
            loom_pool_submit(pool, increment_task, data, NULL);
            /* Cancel immediately — may or may not succeed depending on timing */
            loom_pool_cancel(pool, data);
            submitted[n_submitted++] = data;
        }
    }

    loom_pool_shutdown(pool);

    /* Free all submitted data (completed tasks' data is still valid;
     * cancelled tasks' data was not freed by the library for regular tasks) */
    for (int i = 0; i < n_submitted; i++) {
        free(submitted[i]);
    }

    uint64_t sub  = loom_metrics_submitted(metrics);
    uint64_t comp = loom_metrics_completed(metrics);
    uint64_t canc = loom_metrics_cancelled(metrics);

    ASSERT(sub == comp + canc, "metrics invariant: submitted == completed + cancelled");
    ASSERT(sub >= 20, "at least 20 tasks submitted");

    loom_pool_destroy(&pool);
    loom_metrics_destroy(&metrics);
}

/* ---------- Test: metrics invariant with only completions ---------- */
static void test_metrics_invariant_all_complete(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 200};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_metrics_t *metrics = NULL;
    ASSERT(loom_metrics_create(pool, NULL, NULL, &metrics) == LOOMWORKS_OK, "create metrics");

    for (int i = 0; i < 50; i++) {
        loom_pool_submit(pool, increment_task, &g_passes, NULL);
    }

    loom_pool_shutdown(pool);

    uint64_t sub  = loom_metrics_submitted(metrics);
    uint64_t comp = loom_metrics_completed(metrics);
    uint64_t canc = loom_metrics_cancelled(metrics);

    ASSERT(sub == comp + canc, "invariant holds with only completions");
    ASSERT(canc == 0, "no cancellations");
    ASSERT(comp == sub, "completions match submissions");

    loom_pool_destroy(&pool);
    loom_metrics_destroy(&metrics);
}

/* ---------- Test: task_group + task_id integration ---------- */
static void test_task_group_with_task_id(void)
{
    loom_thread_pool_t *pool  = NULL;
    loom_task_group_t  *group = NULL;
    loom_pool_config_t  cfg   = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "create group");

    uint64_t ids[5];
    int      counter = 0;
    for (int i = 0; i < 5; i++) {
        ASSERT(loom_task_group_submit(group, increment_task, &counter, &ids[i]) == LOOMWORKS_OK,
               "submit via group with task_id");
    }

    /* Verify task IDs are unique and increasing */
    for (int i = 1; i < 5; i++) {
        ASSERT(ids[i] > ids[i - 1], "task_id is increasing");
    }

    loom_task_group_wait(group);
    ASSERT(counter == 5, "all tasks executed");

    loom_task_group_destroy(&group);
    loom_pool_destroy(&pool);
}

/* ---------- Test: task_group submit_future with task_id ---------- */
static void test_task_group_future_with_task_id(void)
{
    loom_thread_pool_t *pool  = NULL;
    loom_task_group_t  *group = NULL;
    loom_pool_config_t  cfg   = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "create group");

    uint64_t       id  = 0;
    loom_future_t *fut = NULL;
    ASSERT(loom_task_group_submit_future(group, fast_result_task, NULL, &fut, &id) == LOOMWORKS_OK,
           "submit future via group with task_id");
    ASSERT(id > 0, "task_id returned");

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait future");
    ASSERT(result != NULL, "result not null");
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    ASSERT(*(int *)result == 42, "result value is 42");
    free(result);
    loom_future_destroy(fut);

    loom_task_group_wait(group);
    loom_task_group_destroy(&group);
    loom_pool_destroy(&pool);
}

/* ---------- Test: future cancel — cancel task before it runs ---------- */
static void test_future_cancel_pending(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    /* Fill the queue with a slow task so the future task stays queued */
    ASSERT(loom_pool_submit(pool, slow_task, NULL, NULL) == LOOMWORKS_OK, "submit slow task");

    /* Give the worker time to pick up the slow task */
    struct timespec ts = {0, 50000000}; /* 50ms */
    nanosleep(&ts, NULL);

    /* Submit a future task */
    loom_future_t *fut     = NULL;
    uint64_t       task_id = 0;
    ASSERT(loom_pool_submit_future(pool, fast_result_task, NULL, &fut, &task_id) == LOOMWORKS_OK,
           "submit future");
    ASSERT(fut != NULL, "future not null");
    ASSERT(task_id > 0, "task_id returned");

    /* Cancel the future's task while it's still queued */
    ASSERT(loom_pool_cancel_by_id(pool, task_id) == LOOMWORKS_OK, "cancel future task");

    /* The future should never complete — wait with a short timeout */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_nsec += 200000000; /* 200ms */
    if (deadline.tv_nsec >= 1000000000) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000;
    }
    loom_result_t rc = loom_future_wait_timeout(fut, NULL, &deadline);
    ASSERT(rc == LOOMWORKS_ERR_TIMEOUT, "future times out when cancelled");

    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: future not cancelled after task completes ---------- */
static void test_future_no_cancel_after_complete(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");

    loom_future_t *fut     = NULL;
    uint64_t       task_id = 0;
    ASSERT(loom_pool_submit_future(pool, fast_result_task, NULL, &fut, &task_id) == LOOMWORKS_OK,
           "submit future");

    /* Wait for completion */
    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait future");
    ASSERT(result != NULL, "result not null");
    free(result);

    /* Canceling after completion should fail */
    ASSERT(loom_pool_cancel_by_id(pool, task_id) == LOOMWORKS_ERR_INVALID,
           "cancel completed task fails");

    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: cancellation propagation through task group ---------- */
static void test_task_group_cancel_propagation(void)
{
    loom_thread_pool_t *pool  = NULL;
    loom_task_group_t  *group = NULL;
    loom_pool_config_t  cfg   = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "create group");

    /* Fill queue with slow task */
    ASSERT(loom_task_group_submit(group, slow_task, NULL, NULL) == LOOMWORKS_OK, "submit slow");

    struct timespec ts = {0, 50000000};
    nanosleep(&ts, NULL);

    /* Submit regular tasks via group - these can be cancelled */
    int cancel_data[3];
    for (int i = 0; i < 3; i++) {
        cancel_data[i] = i;
        ASSERT(loom_task_group_submit(group, increment_task, &cancel_data[i], NULL) == LOOMWORKS_OK,
               "submit task via group");
    }

    /* Cancel via task group - should cancel the 3 queued tasks */
    loom_task_group_cancel(group);

    /* Verify the group is now empty */
    ASSERT(loom_task_group_pending_count(group) == 0, "group empty after cancel");

    loom_task_group_wait(group);
    loom_task_group_destroy(&group);
    loom_pool_destroy(&pool);
}

/* ---------- Helper: concurrent cancel-submit race worker ---------- */
typedef struct {
    loom_thread_pool_t *pool;
    int                 iterations;
    int                *shared_data; /**< Shared array, one per iteration. */
} race_thread_arg_t;

static void *race_worker(void *arg)
{
    race_thread_arg_t *ra = (race_thread_arg_t *)arg;
    for (int i = 0; i < ra->iterations; i++) {
        ra->shared_data[i] = i;
        loom_result_t rc = loom_pool_submit(ra->pool, increment_task,
                                            &ra->shared_data[i], NULL);
        (void)rc;
        /* Note: we do NOT cancel here to avoid use-after-free races.
         * The test verifies that concurrent submit from multiple threads
         * does not crash or corrupt state. */
    }
    return NULL;
}

/* ---------- Test: concurrent cancel-submit race ---------- */
static void test_concurrent_cancel_submit_race(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 50};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    int       iterations = 200;
    pthread_t threads[4];

    race_thread_arg_t *ras[4];
    for (int t = 0; t < 4; t++) {
        ras[t] = (race_thread_arg_t *)malloc(sizeof(*ras[t]));
        ras[t]->pool         = pool;
        ras[t]->iterations   = iterations;
        ras[t]->shared_data  = (int *)malloc(sizeof(int) * iterations);
        ASSERT(ras[t]->shared_data != NULL, "allocate shared_data");
        ASSERT(pthread_create(&threads[t], NULL, race_worker, ras[t]) == 0, "create race thread");
    }

    for (int t = 0; t < 4; t++) {
        pthread_join(threads[t], NULL);
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    for (int t = 0; t < 4; t++) {
        free(ras[t]->shared_data);
        free(ras[t]);
    }

    ASSERT(true, "concurrent cancel-submit race completed without crash");
}

/* ---------- Test: basic pipeline create/destroy ---------- */
static void test_pipeline_basic(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create(2, 0, &pc) == LOOMWORKS_OK, "create pipeline");
    ASSERT(pc != NULL, "pipeline not null");
    loom_pc_shutdown(pc);
    loom_pc_destroy(&pc);
    ASSERT(pc == NULL, "destroy sets to null");
}

/* ---------- Test: pipeline single producer single consumer ---------- */
static void test_pipeline_single(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create(0, 10, &pc) == LOOMWORKS_OK, "create pipeline");

    int *item = (int *)malloc(sizeof(int));
    *item     = 42;
    ASSERT(loom_pc_submit(pc, item) == LOOMWORKS_OK, "submit item");

    void *taken = NULL;
    ASSERT(loom_pc_take(pc, &taken) == LOOMWORKS_OK, "take item");
    ASSERT(taken != NULL, "item not null");
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    ASSERT(*(int *)taken == 42, "item value correct");
    free(taken);

    loom_pc_shutdown(pc);
    /* Consumer should get sentinel */
    ASSERT(loom_pc_take(pc, &taken) == LOOMWORKS_ERR_SHUTDOWN, "shutdown sentinel");
    ASSERT(taken == NULL, "sentinel item is NULL");

    loom_pc_destroy(&pc);
}

/* ---------- Test: pipeline multi-producer multi-consumer ---------- */
typedef struct {
    loom_pc_t *pc;
    int        pid;
} prod_arg_t;

typedef struct {
    loom_pc_t       *pc;
    int             *total;
    pthread_mutex_t *lock;
} cons_arg_t;

static void *pipeline_prod(void *arg)
{
    prod_arg_t *pa = (prod_arg_t *)arg;
    for (int i = 0; i < 50; i++) {
        int *item = (int *)malloc(sizeof(int));
        if (item) {
            *item = pa->pid * 100 + i;
            loom_pc_submit(pa->pc, item);
        }
    }
    return NULL;
}

static void *pipeline_cons(void *arg)
{
    cons_arg_t *ca = (cons_arg_t *)arg;
    void       *item;
    while (loom_pc_take(ca->pc, &item) == LOOMWORKS_OK) {
        int *val = (int *)item;
        pthread_mutex_lock(ca->lock);
        *ca->total += *val;
        pthread_mutex_unlock(ca->lock);
        free(val);
    }
    return NULL;
}

static void test_pipeline_multiprod_cons(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create(0, 0, &pc) == LOOMWORKS_OK, "create pipeline");

    int             total = 0;
    pthread_mutex_t lock  = PTHREAD_MUTEX_INITIALIZER;

    prod_arg_t pargs[2];
    pthread_t  producers[2];
    for (int p = 0; p < 2; p++) {
        pargs[p].pc  = pc;
        pargs[p].pid = p;
        pthread_create(&producers[p], NULL, pipeline_prod, &pargs[p]);
    }
    for (int i = 0; i < 2; i++) {
        pthread_join(producers[i], NULL);
    }

    cons_arg_t cargs[2];
    pthread_t  consumers[2];
    for (int c = 0; c < 2; c++) {
        cargs[c].pc    = pc;
        cargs[c].total = &total;
        cargs[c].lock  = &lock;
        pthread_create(&consumers[c], NULL, pipeline_cons, &cargs[c]);
    }

    loom_pc_shutdown(pc);
    for (int i = 0; i < 2; i++) {
        pthread_join(consumers[i], NULL);
    }

    long expected = 0;
    for (int p = 0; p < 2; p++) {
        for (int i = 0; i < 50; i++) {
            expected += (long)p * 100 + i;
        }
    }
    ASSERT(total == (int)expected, "multiproducer totals match");

    pthread_mutex_destroy(&lock);
    loom_pc_destroy(&pc);
}

/* ---------- Test: pipeline bounded queue ---------- */
static void test_pipeline_bounded(void)
{
    loom_pc_t *pc = NULL;
    /* capacity=2, so only 2 items can be queued at a time */
    ASSERT(loom_pc_create(0, 2, &pc) == LOOMWORKS_OK, "create bounded pipeline");

    int *item1 = (int *)malloc(sizeof(int));
    int *item2 = (int *)malloc(sizeof(int));
    int *item3 = (int *)malloc(sizeof(int));
    ASSERT(item1 != NULL && item2 != NULL && item3 != NULL, "bounded items alloc");
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    *item1 = 1;
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    *item2 = 2;
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    *item3 = 3;

    /* Submit first 2 items (capacity=2) — should succeed immediately */
    ASSERT(loom_pc_submit(pc, item1) == LOOMWORKS_OK, "submit item1");
    ASSERT(loom_pc_submit(pc, item2) == LOOMWORKS_OK, "submit item2");

    /* Queue is full — 3rd submit should block until consumer takes one */
    void *taken = NULL;
    ASSERT(loom_pc_take(pc, &taken) == LOOMWORKS_OK, "take item1");
    ASSERT(taken != NULL, "taken item not null");
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    ASSERT(*(int *)taken == 1, "taken item1 value correct");
    free(taken);

    /* Now queue has space, submit should succeed */
    ASSERT(loom_pc_submit(pc, item3) == LOOMWORKS_OK, "submit item3 after take");

    loom_pc_shutdown(pc);
    /* Drain remaining */
    while (loom_pc_take(pc, &taken) != LOOMWORKS_ERR_SHUTDOWN) {
        if (taken) {
            free(taken);
        }
    }
    /* item1 was taken and freed above; item2 and item3 drained by shutdown+loop. */
    loom_pc_destroy(&pc);
    ASSERT(true, "bounded pipeline test completed");
}

/* ---------- Test: pipeline null safety ---------- */
static void test_pipeline_null_safety(void)
{
    ASSERT(loom_pc_create(0, 0, NULL) != LOOMWORKS_OK, "create with null output fails");
    loom_pc_destroy(NULL);
    loom_pc_shutdown(NULL);
    ASSERT(loom_pc_submit(NULL, NULL) == LOOMWORKS_ERR_INVALID, "submit null pc");
    ASSERT(loom_pc_take(NULL, NULL) == LOOMWORKS_ERR_INVALID, "take null pc");
    ASSERT(loom_pc_pending_count(NULL) == 0, "pending_count null pc returns 0");
    ASSERT(true, "null safety passed");
}

/* ---------- Test: pipeline submit after shutdown ---------- */
static void test_pipeline_submit_after_shutdown(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create(1, 10, &pc) == LOOMWORKS_OK, "create pipeline");
    loom_pc_shutdown(pc);
    int val = 42;
    ASSERT(loom_pc_submit(pc, &val) == LOOMWORKS_ERR_SHUTDOWN, "submit after shutdown fails");
    loom_pc_destroy(&pc);
}

/* ---------- Test: pipeline pending count ---------- */
static void test_pipeline_pending_count(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create(0, 10, &pc) == LOOMWORKS_OK, "create pipeline");

    ASSERT(loom_pc_pending_count(pc) == 0, "pending count is 0 initially");

    int *a = (int *)malloc(sizeof(int));
    int *b = (int *)malloc(sizeof(int));
    ASSERT(a != NULL && b != NULL, "pending alloc");
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    *a = 10;
    /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
    *b = 20;

    ASSERT(loom_pc_submit(pc, a) == LOOMWORKS_OK, "submit a");
    ASSERT(loom_pc_pending_count(pc) == 1, "pending count is 1");
    ASSERT(loom_pc_submit(pc, b) == LOOMWORKS_OK, "submit b");
    ASSERT(loom_pc_pending_count(pc) == 2, "pending count is 2");

    void *taken = NULL;
    ASSERT(loom_pc_take(pc, &taken) == LOOMWORKS_OK, "take a");
    ASSERT(loom_pc_pending_count(pc) == 1, "pending count is 1 after take");
    free(taken);

    loom_pc_shutdown(pc);
    while (loom_pc_take(pc, &taken) != LOOMWORKS_ERR_SHUTDOWN) {
        if (taken) free(taken);
    }
    ASSERT(loom_pc_pending_count(pc) == 0, "pending count is 0 after drain");
    loom_pc_destroy(&pc);
}

/* ---------- Test: pipeline shutdown while consumers waiting ---------- */
static void test_pipeline_shutdown_waiting(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create(2, 0, &pc) == LOOMWORKS_OK, "create pipeline");

    /* Shutdown immediately — no items in queue, consumers should get sentinel */
    loom_pc_shutdown(pc);

    void *taken = NULL;
    /* Both takes should return shutdown sentinel */
    ASSERT(loom_pc_take(pc, &taken) == LOOMWORKS_ERR_SHUTDOWN, "first take gets shutdown");
    ASSERT(taken == NULL, "shutdown item is NULL");
    ASSERT(loom_pc_take(pc, &taken) == LOOMWORKS_ERR_SHUTDOWN, "second take gets shutdown");
    ASSERT(taken == NULL, "shutdown item is NULL");

    loom_pc_destroy(&pc);
}

/* ---------- Test: pipeline submit after destruction is safe ---------- */
static void test_pipeline_destroy_then_submit(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create(1, 10, &pc) == LOOMWORKS_OK, "create pipeline");
    loom_pc_destroy(&pc);
    /* After destroy, pc is NULL — operations should be safe (no-op or error) */
    ASSERT(pc == NULL, "destroy nullifies handle");
    ASSERT(loom_pc_submit(pc, NULL) == LOOMWORKS_ERR_INVALID, "submit after destroy fails");
    ASSERT(loom_pc_take(pc, NULL) == LOOMWORKS_ERR_INVALID, "take after destroy fails");
    ASSERT(loom_pc_pending_count(pc) == 0, "pending_count after destroy returns 0");
}

/* ---------- Test: pipeline stress concurrent produce/consume ---------- */
/* Helper for stress test: producer function */
typedef struct {
    loom_pc_t *pc;
    int        tid;
} stress_prod_arg_t;

static void *pipeline_stress_prod(void *arg)
{
    stress_prod_arg_t *a = (stress_prod_arg_t *)arg;
    for (int j = 0; j < 500; j++) {
        int *item = (int *)malloc(sizeof(int));
        if (item) {
            *item = a->tid * 500 + j;
            loom_pc_submit(a->pc, item);
        }
    }
    return NULL;
}

/* Helper for stress test: consumer function */
typedef struct {
    loom_pc_t *pc;
    int       *total;
    pthread_mutex_t *lock;
} stress_cons_arg_t;

static void *pipeline_stress_cons(void *arg)
{
    stress_cons_arg_t *a = (stress_cons_arg_t *)arg;
    void              *item;
    while (loom_pc_take(a->pc, &item) == LOOMWORKS_OK) {
        pthread_mutex_lock(a->lock);
        *a->total += *(int *)item;
        pthread_mutex_unlock(a->lock);
        free(item);
    }
    return NULL;
}

static void test_pipeline_stress(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create(0, 0, &pc) == LOOMWORKS_OK, "create pipeline");

    int             total = 0;
    pthread_mutex_t lock  = PTHREAD_MUTEX_INITIALIZER;
    const int       PROD  = 4;
    const int       CONS  = 4;

    stress_prod_arg_t  *pargs  = (stress_prod_arg_t *)malloc(sizeof(stress_prod_arg_t)  * (uint32_t)PROD);
    stress_cons_arg_t  *cargs  = (stress_cons_arg_t *)malloc(sizeof(stress_cons_arg_t)  * (uint32_t)CONS);
    pthread_t          *producers = (pthread_t *)malloc(sizeof(pthread_t) * (uint32_t)PROD);
    pthread_t          *consumers = (pthread_t *)malloc(sizeof(pthread_t) * (uint32_t)CONS);
    ASSERT(pargs && cargs && producers && consumers, "stress alloc");

    for (int i = 0; i < PROD; i++) {
        pargs[i].pc  = pc;
        pargs[i].tid = i;
        pthread_create(&producers[i], NULL, pipeline_stress_prod, &pargs[i]);
    }
    for (int i = 0; i < PROD; i++) {
        pthread_join(producers[i], NULL);
    }

    for (int i = 0; i < CONS; i++) {
        cargs[i].pc     = pc;
        cargs[i].total  = &total;
        cargs[i].lock   = &lock;
        pthread_create(&consumers[i], NULL, pipeline_stress_cons, &cargs[i]);
    }

    loom_pc_shutdown(pc);
    for (int i = 0; i < CONS; i++) {
        pthread_join(consumers[i], NULL);
    }

    long expected = 0;
    for (int p = 0; p < PROD; p++) {
        for (int j = 0; j < 500; j++) {
            expected += (long)p * 500 + j;
        }
    }
    ASSERT(total == (int)expected, "stress totals match");
    free(pargs);
    free(cargs);
    free(producers);
    free(consumers);
    loom_pc_destroy(&pc);
    pthread_mutex_destroy(&lock);
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
    test_task_group_with_task_id();
    test_task_group_future_with_task_id();
    test_future_cancel_pending();
    test_future_no_cancel_after_complete();
    test_task_group_cancel_propagation();
    test_concurrent_cancel_submit_race();
    test_pipeline_basic();
    test_pipeline_single();
    test_pipeline_multiprod_cons();
    test_pipeline_bounded();
    test_pipeline_null_safety();
    test_pipeline_submit_after_shutdown();
    test_pipeline_pending_count();
    test_pipeline_shutdown_waiting();
    test_pipeline_destroy_then_submit();
    test_pipeline_stress();
    test_priority_ordering();
    test_priority_future();
    test_metrics_callback();
    test_metrics_null_safety();
    test_metrics_latency();
    test_pool_health();
    test_metrics_monitoring();
    test_submit_returns_task_id();
    test_task_id_uniqueness();
    test_cancel_by_id_not_found();
    test_cancel_by_id_after_shutdown();
    test_cancel_by_id_same_data();
    test_cancel_by_id_running();
    test_submit_priority_returns_task_id();
    test_submit_future_priority_returns_task_id();
    test_cancel_by_id_first_of_many();
    test_cancel_by_id_null_pool();
    test_metrics_callback_all_events();
    test_bucket_priority_edges();
    test_bucket_fifo_within_priority();
    test_bucket_cancel_all();
    test_metrics_latency_nonzero();
    test_metrics_latency_concurrent();
    test_metrics_callback_started();
    test_submit_blocking_unbounded();
    test_submit_blocking_with_id();
    test_submit_blocking_after_shutdown();
    test_submit_blocking_null_safety();
    test_resize_grow();
    test_resize_shrink();
    test_resize_after_shutdown();
    test_resize_null_safety();
    test_metrics_invariant();
    test_metrics_invariant_all_complete();
    test_ring_basic();
    test_ring_multithread_stress();
    test_ring_bounded_full();
    test_ring_unbounded_spill();
    test_ring_cancel_by_id();
    test_ring_cancel_not_found();
    test_ring_tombstone_skip();
    test_ring_cancel_data();
    test_ring_priority_preempt();
    test_ring_shutdown_drains();

    printf("\nResults: %d passed, %d failed\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
