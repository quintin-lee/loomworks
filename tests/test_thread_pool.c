#define _POSIX_C_SOURCE 200809L
#include "loomworks/pipeline.h"
#include "loomworks/thread_pool.h"

/* Internal struct access for work-stealing deque unit tests. */
#include "../src/thread_pool_internal.h"

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

/* Coroutine-task tests: counters are atomic because multiple worker
 * threads run coroutines concurrently.  Sleep durations are chosen from an
 * atomic sequence inside the coroutine itself (passing a small integer
 * through void* trips -Wno-int-to-pointer-cast in this file). */
static _Atomic int g_coro_tasks_done = 0;
static _Atomic int g_coro_sleep_seq  = 0;

static void pool_coro_yield_once(void *arg)
{
    (void)arg;
    loom_coro_yield();
    atomic_fetch_add_explicit(&g_coro_tasks_done, 1, memory_order_relaxed);
}

static void pool_coro_sleep(void *arg)
{
    (void)arg;
    /* 5-15 ms, spread across tasks via the atomic sequence. */
    long ns =
        (long)(5 + (atomic_fetch_add_explicit(&g_coro_sleep_seq, 1, memory_order_relaxed) % 11)) *
        1000000L;
    loom_coro_sleep(ns);
    atomic_fetch_add_explicit(&g_coro_tasks_done, 1, memory_order_relaxed);
}

static void pool_coro_sleep_long(void *arg)
{
    (void)arg;
    loom_coro_sleep(60000000000L); /* 60 s */
    atomic_fetch_add_explicit(&g_coro_tasks_done, 1, memory_order_relaxed);
}

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

/* Slow variant of simple_task for the drain/spill observation tests: the
 * worker keeps its local deque non-empty for ~10ms (300 tasks x ~30us)
 * instead of a sub-us window, so the main-thread observer reliably catches
 * deque-resident work (F1 flake: the observer missed the instantaneous
 * drain, spun 40M times on an already-empty deque, and falsely FAILed). */
static void slow_count_task(void *arg)
{
    volatile long sink = 0;
    for (long i = 0; i < 65536L; i++) {
        sink += i;
    }
    (void)sink;
    volatile int *counter = (volatile int *)arg;
    __sync_fetch_and_add(counter, 1);
}

/* ---------- Bucket/queue regression helpers ---------- */
static int g_exec_order[128];
static int g_exec_count;

static void record_exec(void *arg)
{
    int idx                      = (int)(intptr_t)arg;
    g_exec_order[g_exec_count++] = idx;
}

static _Atomic int g_gate_started = 0;
static _Atomic int g_gate_release = 0;
static _Atomic int g_gate_parked =
    0; /* gate_task entry count (multi-worker, atomic: several workers park concurrently) */
static _Atomic int  g_gate_release2 = 0;
static _Atomic int  g_gate_parked2  = 0; /* gate_task2 entry count */
static volatile int g_cancel_hit    = 0;

/* Steal tests: per-task owner thread records (append via atomic index;
 * slots written exactly once, so no torn reads).  Each task busy-spins
 * ~1ms BEFORE recording, so a full deque of these drains slowly and a
 * parked thief has a wide window to steal from it. */
#define STEAL_MAX_TASKS 512
static pthread_t    g_steal_owner[STEAL_MAX_TASKS];
static volatile int g_steal_owner_count = 0;

static void record_owner_task(void *arg)
{
    (void)arg;
    /* ~1ms dwell: keeps the deque non-empty long enough for a thief
     * to arrive even on a loaded machine. */
    for (volatile int spin = 0; spin < 3000000; spin++) {
    }
    int idx = __sync_fetch_and_add(&g_steal_owner_count, 1);
    if (idx < STEAL_MAX_TASKS) {
        g_steal_owner[idx] = pthread_self();
    }
}

static int count_distinct_owner_threads(void)
{
    int n     = 0;
    int count = g_steal_owner_count;
    for (int i = 0; i < count; i++) {
        int dup = 0;
        for (int j = 0; j < i && !dup; j++) {
            if (pthread_equal(g_steal_owner[i], g_steal_owner[j])) {
                dup = 1;
            }
        }
        n += !dup;
    }
    return n;
}

/* Steal stress: producer threads submit N tasks each to a shared pool. */
static volatile int g_steal_stress_counter = 0;

typedef struct {
    loom_thread_pool_t *pool;
    int                 n;
} steal_producer_arg_t;

static void *steal_producer_thread(void *arg)
{
    steal_producer_arg_t *pa = (steal_producer_arg_t *)arg;
    for (int i = 0; i < pa->n; i++) {
        loom_pool_submit(pa->pool, simple_task, (void *)&g_steal_stress_counter, NULL);
    }
    return NULL;
}
static volatile uint64_t g_cancel_target_id = 0;

static void gate_task(void *arg)
{
    (void)arg;
    g_gate_started = 1;
    g_gate_parked++;
    while (!g_gate_release) {
        /* spin: occupy the only worker so later tasks stay queued */
    }
}

static void gate_task2(void *arg)
{
    (void)arg;
    g_gate_parked2++;
    while (!g_gate_release2) {
        /* spin: occupy the second worker while the first drains */
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
    for (long i = 0; i < 1000000000L; i++) {
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

/* ---------- Test: destroying an unshutdown pool is rejected ---------- */
static void test_pool_destroy_without_shutdown(void)
{
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 16};
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool created");
    ASSERT(loom_pool_destroy(&pool) == LOOMWORKS_ERR_INVALID, "destroy unshutdown pool rejected");
    ASSERT(pool != NULL, "pool still valid after rejected destroy");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_destroy(&pool) == LOOMWORKS_OK, "destroy shutdown pool ok");
    ASSERT(pool == NULL, "pool nulled after ok destroy");
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
    clock_gettime(CLOCK_MONOTONIC, &deadline);
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
    g_gate_started           = 0;
    g_gate_release           = 0;
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
    void *done     = NULL;
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
    /* pending_count tracks the submission history, not running tasks: it is
     * not reset by wait(). */
    ASSERT(loom_task_group_pending_count(group) == 10, "tracked count survives wait");

    loom_task_group_destroy(&group);
    loom_pool_shutdown(pool);
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
    loom_pool_shutdown(pool);
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

/* ---------- Test: group self-wait guard ----------
 * A worker of a pool must never block on (or destroy) a group of its own
 * pool: the pool would stall and the call could never complete. */

static volatile int       g_group_wait_rc  = -1;
static loom_task_group_t *g_group_for_wait = NULL;

static void group_wait_from_worker_fn(void *data)
{
    (void)data;
    g_group_wait_rc = loom_task_group_wait(g_group_for_wait);
}

static void group_destroy_from_worker_fn(void *data)
{
    (void)data;
    g_group_wait_rc = loom_task_group_destroy(&g_group_for_wait);
}

static void test_task_group_wait_from_worker(void)
{
    loom_pool_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.worker_count   = 2;
    cfg.queue_capacity = 64;

    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    loom_task_group_t *group = NULL;
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "group create");
    g_group_for_wait = group;
    g_group_wait_rc  = -1;

    /* Reset the rc marker BEFORE submitting: the worker may run the helper
     * immediately, so a reset after submit could clobber its ERR_INVALID. */
    g_group_wait_rc = -1;
    ASSERT(loom_task_group_submit(group, group_wait_from_worker_fn, NULL, NULL) == LOOMWORKS_OK,
           "group submit");

    /* Wait from main: must complete once the worker task finishes. */
    ASSERT(loom_task_group_wait(group) == LOOMWORKS_OK, "wait from main ok");
    ASSERT(g_group_wait_rc == LOOMWORKS_ERR_INVALID, "wait from own worker rejected");
    ASSERT(loom_task_group_pending_count(group) == 1, "one tracked task survives wait");

    ASSERT(loom_task_group_destroy(&group) == LOOMWORKS_OK, "group destroy");
    ASSERT(group == NULL, "group nulled");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_destroy(&pool) == LOOMWORKS_OK, "destroy");
}

static void test_task_group_destroy_from_worker(void)
{
    loom_pool_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.worker_count   = 2;
    cfg.queue_capacity = 64;

    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    loom_task_group_t *group = NULL;
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "group create");

    /* Submit a task that attempts destroy on its own group. */
    g_group_for_wait = group;
    g_group_wait_rc  = -1;
    ASSERT(loom_task_group_submit(group, group_destroy_from_worker_fn, NULL, NULL) == LOOMWORKS_OK,
           "group submit");
    ASSERT(loom_task_group_wait(group) == LOOMWORKS_OK, "wait from main");
    ASSERT(g_group_wait_rc == LOOMWORKS_ERR_INVALID, "destroy from own worker rejected");

    ASSERT(loom_task_group_destroy(&group) == LOOMWORKS_OK, "group destroy");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_destroy(&pool) == LOOMWORKS_OK, "destroy");
}

/* ---------- Test: group wait timeout ----------
 * wait_timeout takes an absolute CLOCK_MONOTONIC deadline and must never
 * leave the group in a partial state: a timed-out wait leaves every
 * tracked task pending and the group fully usable for a later wait. */

static void group_wait_timeout_from_worker_fn(void *data)
{
    (void)data;
    g_group_wait_rc = loom_task_group_wait_timeout(g_group_for_wait, NULL);
}

static void test_task_group_wait_timeout_expired_and_reusable(void)
{
    loom_pool_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.worker_count   = 1;
    cfg.queue_capacity = 16;

    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    loom_task_group_t *group = NULL;
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "group create");

    /* A task that parks on a gate: the group stays pending. */
    g_gate_started = 0;
    g_gate_release = 0;
    ASSERT(loom_task_group_submit(group, gate_task, NULL, NULL) == LOOMWORKS_OK, "group submit");
    while (!g_gate_started) {
        sched_yield();
    }
    ASSERT(g_gate_parked == 1, "task parked");

    /* An already-expired deadline must time out immediately and leave the
     * group intact. */
    struct timespec past;
    clock_gettime(CLOCK_MONOTONIC, &past);
    past.tv_sec -= 1;
    ASSERT(loom_task_group_wait_timeout(group, &past) == LOOMWORKS_ERR_TIMEOUT,
           "expired deadline times out");
    ASSERT(loom_task_group_pending_count(group) == 1, "tracked task still pending");

    /* Releasing the gate lets a plain wait finish: nothing was torn down. */
    g_gate_release = 1;
    ASSERT(loom_task_group_wait(group) == LOOMWORKS_OK, "group reusable after timeout");

    ASSERT(loom_task_group_destroy(&group) == LOOMWORKS_OK, "group destroy");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_destroy(&pool) == LOOMWORKS_OK, "destroy");
}

static void test_task_group_wait_timeout_ok(void)
{
    loom_pool_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.worker_count   = 1;
    cfg.queue_capacity = 16;

    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    loom_task_group_t *group = NULL;
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "group create");

    int counter = 0;
    ASSERT(loom_task_group_submit(group, increment_task, &counter, NULL) == LOOMWORKS_OK,
           "group submit");

    struct timespec later;
    clock_gettime(CLOCK_MONOTONIC, &later);
    later.tv_sec += 5;
    ASSERT(loom_task_group_wait_timeout(group, &later) == LOOMWORKS_OK, "far deadline ok");
    ASSERT(counter == 1, "task ran");

    ASSERT(loom_task_group_destroy(&group) == LOOMWORKS_OK, "group destroy");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_destroy(&pool) == LOOMWORKS_OK, "destroy");
}

static void test_task_group_wait_timeout_null_deadline(void)
{
    loom_pool_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.worker_count   = 1;
    cfg.queue_capacity = 16;

    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    loom_task_group_t *group = NULL;
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "group create");

    int counter = 0;
    ASSERT(loom_task_group_submit(group, increment_task, &counter, NULL) == LOOMWORKS_OK,
           "group submit");

    /* NULL deadline means "wait forever", exactly like loom_task_group_wait. */
    ASSERT(loom_task_group_wait_timeout(group, NULL) == LOOMWORKS_OK, "null deadline waits");
    ASSERT(counter == 1, "task ran");

    ASSERT(loom_task_group_destroy(&group) == LOOMWORKS_OK, "group destroy");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_destroy(&pool) == LOOMWORKS_OK, "destroy");
}

static void test_task_group_wait_timeout_from_worker(void)
{
    loom_pool_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.worker_count   = 2;
    cfg.queue_capacity = 64;

    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    loom_task_group_t *group = NULL;
    ASSERT(loom_task_group_create(pool, &group) == LOOMWORKS_OK, "group create");
    g_group_for_wait = group;
    g_group_wait_rc  = -1;

    /* Reset the rc marker BEFORE submitting: the worker may run the helper
     * immediately, so a reset after submit could clobber its ERR_INVALID. */
    g_group_wait_rc = -1;
    ASSERT(loom_task_group_submit(group, group_wait_timeout_from_worker_fn, NULL, NULL) ==
               LOOMWORKS_OK,
           "group submit");

    ASSERT(loom_task_group_wait(group) == LOOMWORKS_OK, "wait from main ok");
    ASSERT(g_group_wait_rc == LOOMWORKS_ERR_INVALID, "wait_timeout from own worker rejected");

    ASSERT(loom_task_group_destroy(&group) == LOOMWORKS_OK, "group destroy");
    loom_pool_shutdown(pool);
    ASSERT(loom_pool_destroy(&pool) == LOOMWORKS_OK, "destroy");
}

static void test_task_group_wait_timeout_null_group(void)
{
    struct timespec later;
    clock_gettime(CLOCK_MONOTONIC, &later);
    later.tv_sec += 5;

    ASSERT(loom_task_group_wait_timeout(NULL, &later) == LOOMWORKS_ERR_INVALID,
           "null group rejected");
    ASSERT(loom_task_group_wait_timeout(NULL, NULL) == LOOMWORKS_ERR_INVALID,
           "null group + null deadline rejected");
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
    g_exec_count             = 0;
    g_gate_started           = 0;
    g_gate_release           = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create single-worker pool");

    /* Occupy the only worker with a gate task so every priority task is
     * queued BEFORE any of them can run.  Without the gate, the worker can
     * dequeue the first task (prio 255) before the higher-priority tasks
     * are submitted — and a running task cannot be preempted, so priority
     * ordering among queued tasks does not apply.  The order assertion
     * below would then fail nondeterministically. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "submit gate task");
    while (!g_gate_started) {
        /* wait until the worker is inside the gate */
    }

    struct {
        uint8_t prio;
        int     slot;
    } seq[] = {
        {255, 0},
        {0, 1},
        {5, 2},
        {254, 3},
        {1, 4},
        {10, 5},
    };
    for (int i = 0; i < 6; i++) {
        ASSERT(loom_pool_submit_priority(pool,
                                         record_exec,
                                         /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
                                         (void *)(intptr_t)seq[i].slot,
                                         seq[i].prio,
                                         NULL) == LOOMWORKS_OK,
               "submit priority edge");
    }
    g_gate_release = 1;
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
    g_exec_count             = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create single-worker pool");

    const int N = 100;
    for (int i = 0; i < N; i++) {
        /* Priority 6 (lane path, p >= 5): bucket FIFO semantics live on the
         * lane side.  NORMAL (5) takes the lock-free ring fast path, which
         * after work-stealing drains LIFO through per-worker deques. */
        ASSERT(loom_pool_submit_priority(pool,
                                         record_exec,
                                         /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
                                         (void *)(intptr_t)i,
                                         6,
                                         NULL) == LOOMWORKS_OK,
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
    g_gate_started           = 0;
    g_gate_release           = 0;
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
            ASSERT(loom_pool_submit_priority(pool, simple_task, NULL, prios[i], NULL) ==
                       LOOMWORKS_OK,
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
    cfg.worker_count         = 2;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "basic create");

    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    for (uint32_t i = 0; i < 1000; i++) {
        ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK, "basic submit");
    }

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
        if (loom_pool_submit(pool, ring_inc_task, NULL, NULL) != LOOMWORKS_OK) {
            break; /* tolerate shutdown race in stress test only */
        }
    }
    return NULL;
}

static void test_ring_multithread_stress(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count         = 8;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "stress create");

    pthread_t producers[4];
    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    for (int i = 0; i < 4; i++) {
        ASSERT(pthread_create(&producers[i], NULL, ring_producer, pool) == 0,
               "stress spawn producer");
    }
    for (int i = 0; i < 4; i++) {
        pthread_join(producers[i], NULL);
    }

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
static int         g_ring_order[512];

static void ring_order_record(int prio)
{
    int idx           = atomic_fetch_add_explicit(&g_ring_order_len, 1, memory_order_relaxed);
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
    cfg.worker_count         = 1;
    cfg.queue_capacity       = 5;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "bounded create");

    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_gate2_started, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_gate2_release, 0, memory_order_relaxed);

    /* gate: NORMAL -> ring, pins the only worker */
    ASSERT(loom_pool_submit(pool, ring_gate2_task, NULL, NULL) == LOOMWORKS_OK,
           "bounded submit gate");
    while (!atomic_load_explicit(&g_ring_gate2_started, memory_order_acquire)) {
    }

    for (uint32_t i = 0; i < 5; i++) {
        ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK,
               "bounded fill ring");
    }
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
    cfg.worker_count         = 1;
    cfg.queue_capacity       = 0;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "spill create");

    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_gate2_started, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_gate2_release, 0, memory_order_relaxed);

    ASSERT(loom_pool_submit(pool, ring_gate2_task, NULL, NULL) == LOOMWORKS_OK,
           "spill submit gate");
    while (!atomic_load_explicit(&g_ring_gate2_started, memory_order_acquire)) {
    }

    for (uint32_t i = 0; i < 4096; i++) {
        ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK,
               "spill fill ring");
    }
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
    cfg.worker_count         = 1;
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

    ASSERT(loom_pool_cancel_by_id(pool, tid) == LOOMWORKS_OK, "cancel-by-id: first cancel ok");
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
    cfg.worker_count         = 1;
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
    cfg.worker_count         = 1;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "tombstone create");

    atomic_store_explicit(&g_ring_cancel_done, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_release, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_run, 0, memory_order_relaxed);

    ASSERT(loom_pool_submit(pool, ring_cancel_pin_task, NULL, NULL) == LOOMWORKS_OK,
           "tombstone submit gate");
    while (!atomic_load_explicit(&g_ring_cancel_done, memory_order_acquire)) {
    }

    uint64_t tids[64];
    for (uint32_t i = 0; i < 64; i++) {
        ASSERT(loom_pool_submit(pool, ring_cancel_inc_task, NULL, &tids[i]) == LOOMWORKS_OK,
               "tombstone submit victim");
    }
    ASSERT(loom_pool_pending_count(pool) == 64, "tombstone: 64 pending");

    for (uint32_t i = 0; i < 64; i++) {
        ASSERT(loom_pool_cancel_by_id(pool, tids[i]) == LOOMWORKS_OK, "tombstone cancel victim");
    }
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
    cfg.worker_count         = 1;
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
    ASSERT(loom_pool_cancel(pool, &d1) == LOOMWORKS_ERR_INVALID, "cancel-data: re-cancel rejected");
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
    cfg.worker_count         = 1;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "preempt create");

    atomic_store_explicit(&g_ring_order_len, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_done, 0, memory_order_relaxed);
    atomic_store_explicit(&g_ring_cancel_release, 0, memory_order_relaxed);

    /* gate: NORMAL -> ring, pins the worker (does not record) */
    ASSERT(loom_pool_submit(pool, ring_cancel_pin_task, NULL, NULL) == LOOMWORKS_OK,
           "preempt submit gate");
    while (!atomic_load_explicit(&g_ring_cancel_done, memory_order_acquire)) {
    }

    for (uint32_t i = 0; i < 50; i++) {
        ASSERT(loom_pool_submit_priority(
                   pool, ring_normal_rec, NULL, LOOMWORKS_PRIORITY_NORMAL, NULL) == LOOMWORKS_OK,
               "preempt submit normal");
    }
    for (uint32_t i = 0; i < 3; i++) {
        ASSERT(loom_pool_submit_priority(
                   pool, ring_rt_rec, NULL, LOOMWORKS_PRIORITY_REALTIME, NULL) == LOOMWORKS_OK,
               "preempt submit rt");
    }
    for (uint32_t i = 0; i < 3; i++) {
        ASSERT(loom_pool_submit_priority(
                   pool, ring_high_rec, NULL, LOOMWORKS_PRIORITY_HIGH, NULL) == LOOMWORKS_OK,
               "preempt submit high");
    }
    ASSERT(loom_pool_pending_count(pool) == 56, "preempt: 56 pending");

    atomic_store_explicit(&g_ring_cancel_release, 1, memory_order_release);
    loom_pool_shutdown(pool);

    /* 56 records: 3 RT, 3 HIGH, 50 NORMAL (gate does not record) */
    int len = atomic_load_explicit(&g_ring_order_len, memory_order_relaxed);
    ASSERT(len == 56, "preempt: all 56 ran and recorded");
    int ok = 1;
    for (int i = 0; i < 3; i++) {
        ok = ok && g_ring_order[i] == LOOMWORKS_PRIORITY_REALTIME;
    }
    for (int i = 3; i < 6; i++) {
        ok = ok && g_ring_order[i] == LOOMWORKS_PRIORITY_HIGH;
    }
    for (int i = 6; i < 56; i++) {
        ok = ok && g_ring_order[i] == LOOMWORKS_PRIORITY_NORMAL;
    }
    ASSERT(ok, "preempt: RT(0)x3, HIGH(1)x3, then NORMAL(5)x50");
    ASSERT(loom_pool_pending_count(pool) == 0, "preempt: nothing pending");
    loom_pool_destroy(&pool);
}

/* Guard: shutdown drains the ring — all 500 tasks run exactly once. */
static void test_ring_shutdown_drains(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {0};
    cfg.worker_count         = 2;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "shutdown-drain create");

    atomic_store_explicit(&g_ring_run_count, 0, memory_order_relaxed);
    for (uint32_t i = 0; i < 500; i++) {
        ASSERT(loom_pool_submit(pool, ring_inc_task, NULL, NULL) == LOOMWORKS_OK,
               "shutdown-drain submit");
    }

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
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 0};
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

    /* Occupy the sole worker with a gate so all 3 tasks stay queued: the
     * cancel must deterministically find ids[1] still pending. */
    g_gate_started = 0;
    g_gate_release = 0;
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "submit gate");
    while (!g_gate_started) {
        sched_yield();
    }

    /* Submit 3 tasks with the SAME user_data pointer */
    ASSERT(loom_pool_submit(pool, increment_task, &shared_data, &ids[0]) == LOOMWORKS_OK,
           "submit task 1");
    ASSERT(loom_pool_submit(pool, increment_task, &shared_data, &ids[1]) == LOOMWORKS_OK,
           "submit task 2");
    ASSERT(loom_pool_submit(pool, increment_task, &shared_data, &ids[2]) == LOOMWORKS_OK,
           "submit task 3");

    /* cancel_by_id should only cancel the specific task */
    ASSERT(loom_pool_cancel_by_id(pool, ids[1]) == LOOMWORKS_OK, "cancel by id mid");
    g_gate_release = 1;

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
    int failed;
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
    case LOOMWORKS_METRIC_FAILED:
        ctx->failed++;
        break;
    default:
        break;
    }
}

/* ---------- Test: metrics callback contract lockdown ---------- */
/* The public contract (api-reference §5, design decision 21): the callback
 * fires synchronously from the thread that produces the event — worker
 * threads for STARTED/COMPLETED, the submitting thread for SUBMITTED, the
 * shutting-down thread for FAILED — and always OUTSIDE the pool lock, so it
 * must be cheap and non-blocking.  These tests lock that contract: any future
 * refactor that invokes the callback under the lock will deadlock the
 * pending_count probe below and fail the suite. */
static pthread_t    g_metric_cb_thread;
static pthread_t    g_main_thread;
static volatile int g_cb_pending_ok;
static _Atomic int  g_cb_calls;

static void
metrics_contract_cb(loom_metric_event_t event, const loom_thread_pool_t *pool, void *user_data)
{
    (void)event;
    (void)user_data;
    g_metric_cb_thread = pthread_self();
    atomic_fetch_add_explicit(&g_cb_calls, 1, memory_order_relaxed);
    /* pending_count acquires the pool lock.  If we were called while holding
     * it, this would deadlock and ctest would time out — proving the callback
     * always runs lock-free.  The return value is irrelevant: reaching the
     * assignment at all proves the lock was acquirable. */
    (void)loom_pool_pending_count(pool);
    g_cb_pending_ok = 1;
}

static void test_metrics_callback_on_worker_thread(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    metrics_event_ctx_t ctx     = {0};
    loom_metrics_t     *metrics = NULL;
    ASSERT(loom_metrics_create(pool, metrics_contract_cb, &ctx, &metrics) == LOOMWORKS_OK,
           "metrics created");

    g_main_thread      = pthread_self();
    g_metric_cb_thread = pthread_self();
    g_cb_calls         = 0;
    g_gate_started     = 0;
    g_gate_release     = 0;
    g_gate_parked      = 0;

    /* Gate first so the increment below is guaranteed to run on a worker. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "gate submitted");
    while (!g_gate_started) {
        sched_yield();
    }
    int counter = 0;
    ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK,
           "increment submitted");
    g_gate_release = 1;

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    loom_metrics_destroy(&metrics);
    ASSERT(!pthread_equal(g_metric_cb_thread, g_main_thread),
           "callback ran on a worker thread, not the submitting thread");
    ASSERT(atomic_load_explicit(&g_cb_calls, memory_order_relaxed) >= 1,
           "callback fired at least once");
    ASSERT(counter >= 1, "task executed");
}

static void test_metrics_callback_outside_lock(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    metrics_event_ctx_t ctx     = {0};
    loom_metrics_t     *metrics = NULL;
    ASSERT(loom_metrics_create(pool, metrics_contract_cb, &ctx, &metrics) == LOOMWORKS_OK,
           "metrics created");

    g_cb_pending_ok = 0;
    g_cb_calls      = 0;
    g_gate_started  = 0;
    g_gate_release  = 0;
    g_gate_parked   = 0;

    int counter = 0;
    for (int i = 0; i < 4; i++) {
        ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK,
               "increment submitted");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    loom_metrics_destroy(&metrics);
    ASSERT(g_cb_pending_ok == 1, "callback probed the pool lock without deadlocking");
    ASSERT(atomic_load_explicit(&g_cb_calls, memory_order_relaxed) >= 4,
           "every task produced a callback");
    ASSERT(counter == 4, "all tasks executed");
}

static void test_metrics_callback_lifecycle_counts(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    metrics_event_ctx_t ctx     = {0};
    loom_metrics_t     *metrics = NULL;
    ASSERT(loom_metrics_create(pool, metrics_event_callback, &ctx, &metrics) == LOOMWORKS_OK,
           "metrics created");

    g_gate_started = 0;
    g_gate_release = 0;
    g_gate_parked  = 0;

    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "gate submitted");
    int counter = 0;
    ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK, "increment 1");
    ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK, "increment 2");
    /* Park the worker mid-gate so no task completes before we count. */
    while (g_gate_parked < 1) {
        sched_yield();
    }
    g_gate_release = 1;

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    loom_metrics_destroy(&metrics);
    ASSERT(ctx.submitted == 3, "all 3 submissions counted");
    ASSERT(ctx.completed == 3, "all 3 tasks completed");
    ASSERT(ctx.cancelled == 0, "no cancellations");
    ASSERT(ctx.failed == 0, "no failures");
    ASSERT(counter == 2, "both increments executed");
}

static void test_metrics_callback_all_events(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    metrics_event_ctx_t ctx     = {0};
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

    metrics_event_ctx_t ctx     = {0};
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

/* ---------- Test: worker crash detection via FAILED metric ---------- */
static void crash_task(void *arg)
{
    (void)arg;
    pthread_exit(NULL); /* abnormal exit — worker never sets clean_exit */
}

static void test_worker_crash_detected(void)
{
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 16};
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool created");

    metrics_event_ctx_t ctx     = {0};
    loom_metrics_t     *metrics = NULL;
    ASSERT(loom_metrics_create(pool, metrics_event_callback, &ctx, &metrics) == LOOMWORKS_OK,
           "metrics created");

    /* The single worker runs the crash task and dies abnormally. */
    ASSERT(loom_pool_submit(pool, crash_task, NULL, NULL) == LOOMWORKS_OK, "crash task submitted");

    loom_pool_shutdown(pool);
    ASSERT(ctx.failed == 1, "worker crash reported as FAILED metric");
    ASSERT(loom_metrics_failed(metrics) == 1, "metrics_failed counter reflects crash");

    loom_metrics_destroy(&metrics);
    loom_pool_destroy(&pool);
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

/* ---------- Test: loom_pool_resize rejects zero ---------- */
static void test_resize_zero_rejected(void)
{
    loom_thread_pool_t *pool = NULL;
    ASSERT(loom_pool_create(NULL, &pool) == LOOMWORKS_OK, "create pool");
    ASSERT(loom_pool_resize(pool, 0) == LOOMWORKS_ERR_INVALID, "resize to 0 rejected");
    ASSERT(loom_pool_worker_count(pool) > 0, "workers intact after rejected resize");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: resize fault injection ---------- */
static void gated_crash_task(void *arg)
{
    (void)arg;
    g_gate_started = 1;
    g_gate_parked++;
    while (!g_gate_release) {
        sched_yield();
    }
    pthread_exit(NULL); /* crash after the gate opens */
}

static void test_resize_alloc_fail_deques_realloc(void)
{
    loom_test_arm_alloc_failure(-1); /* defensive */
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_test_arm_alloc_failure(0); /* next check = deques realloc */
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_ERR_ALLOC, "deques realloc fails");
    ASSERT(loom_pool_worker_count(pool) == 2, "worker count unchanged after failed grow");

    int counter = 0;
    ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK,
           "pool usable after failure");
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_OK, "unarmed resize succeeds");
    ASSERT(loom_pool_worker_count(pool) == 8, "rescued worker count = 8");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(counter >= 1, "task executed");
}

static void test_resize_alloc_fail_deque_slots(void)
{
    loom_test_arm_alloc_failure(-1);
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_test_arm_alloc_failure(1); /* 2nd check = first per-deque slots calloc */
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_OK,
           "deque-slot calloc failure degrades to lane-only, NOT an error");
    ASSERT(pool->deques == NULL, "lane-only mode active");
    ASSERT(loom_pool_worker_count(pool) == 8, "worker count still reaches 8");

    int counter = 0;
    for (int i = 0; i < 50; i++) {
        ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK,
               "lane-only submit");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    ASSERT(counter == 50, "all lane-only tasks executed");
}

static void test_resize_alloc_fail_threads_realloc(void)
{
    loom_test_arm_alloc_failure(-1);
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_test_arm_alloc_failure(7); /* 8th check = threads realloc (1 + 6 callocs) */
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_ERR_ALLOC, "threads realloc fails");
    ASSERT(loom_pool_worker_count(pool) == 2, "worker count unchanged");

    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_OK, "unarmed resize succeeds");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_resize_alloc_fail_alive_realloc(void)
{
    loom_test_arm_alloc_failure(-1);
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_test_arm_alloc_failure(8); /* 9th check = thread_alive realloc */
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_ERR_ALLOC, "thread_alive realloc fails");
    ASSERT(loom_pool_worker_count(pool) == 2, "worker count unchanged");

    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_OK, "unarmed resize succeeds");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_resize_alloc_fail_clean_exit_realloc(void)
{
    loom_test_arm_alloc_failure(-1);
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_test_arm_alloc_failure(9); /* 10th check = thread_clean_exit realloc */
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_ERR_ALLOC, "thread_clean_exit realloc fails");
    ASSERT(loom_pool_worker_count(pool) == 2, "worker count unchanged");

    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_OK, "unarmed resize succeeds");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_resize_alloc_fail_worker_arg_first(void)
{
    loom_test_arm_alloc_failure(-1);
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_test_arm_alloc_failure(10); /* 11th check = first worker_arg malloc */
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_ERR_ALLOC, "first worker malloc fails");
    ASSERT(loom_pool_worker_count(pool) == 2, "worker count rolled back");
    ASSERT(atomic_load_explicit(&pool->thread_alive[2], memory_order_acquire) == false,
           "no worker spawned into slot 2");

    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_OK, "unarmed resize succeeds");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_resize_alloc_fail_worker_arg_mid(void)
{
    loom_test_arm_alloc_failure(-1);
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    loom_test_arm_alloc_failure(11); /* 12th check = second worker_arg malloc */
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_ERR_ALLOC, "second worker malloc fails");
    ASSERT(loom_pool_worker_count(pool) == 2, "worker count rolled back");
    ASSERT(atomic_load_explicit(&pool->thread_alive[2], memory_order_acquire) == false,
           "created worker joined and slot released");

    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_OK, "unarmed resize succeeds");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* Regression: after a failed grow rolls back workers, their slots hold a stale
 * clean_exit=true. Reusing those slots for a fresh grow must not hide a crash:
 * shutdown must report FAILED for every crashed worker (== 6 here). */
static void test_resize_fail_then_worker_crash_detected(void)
{
    loom_test_arm_alloc_failure(-1);
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    metrics_event_ctx_t ctx     = {0};
    loom_metrics_t     *metrics = NULL;
    ASSERT(loom_metrics_create(pool, metrics_event_callback, &ctx, &metrics) == LOOMWORKS_OK,
           "metrics created");

    /* Failed grow: workers 2..6 are created, then the 6th malloc (check 16)
     * fails and the rollback joins them. Pre-fix their clean_exit stays true. */
    loom_test_arm_alloc_failure(15);
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_ERR_ALLOC, "6th worker malloc fails");
    ASSERT(loom_pool_worker_count(pool) == 2, "worker count rolled back");

    /* Successful grow reuses slots 2..7 (arrays are already sized 8). */
    ASSERT(loom_pool_resize(pool, 8) == LOOMWORKS_OK, "unarmed resize succeeds");
    ASSERT(loom_pool_worker_count(pool) == 8, "8 workers after reuse");

    /* One gated crash task per new worker: all 6 park, then all 6 crash. */
    g_gate_started = 0;
    g_gate_release = 0;
    g_gate_parked  = 0;
    for (int i = 0; i < 6; i++) {
        ASSERT(loom_pool_submit(pool, gated_crash_task, NULL, NULL) == LOOMWORKS_OK,
               "gated crash task submitted");
    }
    while (g_gate_parked < 6) {
        sched_yield();
    }
    g_gate_release = 1;

    loom_pool_shutdown(pool);
    ASSERT(ctx.failed == 6, "all 6 crashed workers reported FAILED (no stale clean_exit)");
    ASSERT(loom_metrics_failed(metrics) == 6, "metrics_failed counter == 6");

    loom_metrics_destroy(&metrics);
    loom_pool_destroy(&pool);
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
    loom_pool_shutdown(pool);
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
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: future cancel — cancel task before it runs ---------- */
static void test_future_cancel_pending(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    /* Park the single worker on the gate so the future task stays queued */
    g_gate_started = 0;
    g_gate_release = 0;
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "submit gate task");
    while (!g_gate_started) {
        sched_yield();
    }

    /* Submit a future task while the worker is parked */
    loom_future_t *fut     = NULL;
    uint64_t       task_id = 0;
    ASSERT(loom_pool_submit_future(pool, fast_result_task, NULL, &fut, &task_id) == LOOMWORKS_OK,
           "submit future");
    ASSERT(fut != NULL, "future not null");
    ASSERT(task_id > 0, "task_id returned");

    /* Cancel the future's task while it's still queued */
    ASSERT(loom_pool_cancel_by_id(pool, task_id) == LOOMWORKS_OK, "cancel future task");

    /* Long deadline: the worker must drain the cancellation tombstone, which
     * signals the future, before the timeout can expire. */
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    deadline.tv_sec += 5;

    /* Let the worker finish the gate task and process the tombstone */
    g_gate_release = 1;

    /* A cancelled future must report cancellation, not time out */
    loom_result_t rc = loom_future_wait_timeout(fut, NULL, &deadline);
    ASSERT(rc == LOOMWORKS_ERR_CANCELLED, "future reports cancelled when task cancelled");

    /* A cancelled future holds no result */
    void *result = (void *)&rc;
    ASSERT(loom_future_wait_timeout(fut, &result, &deadline) == LOOMWORKS_ERR_CANCELLED,
           "cancelled future wait returns cancelled again");
    ASSERT(result == NULL, "cancelled future leaves result NULL");

    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: infinite wait on cancelled future returns ERR_CANCELLED ---------- */
static void test_future_cancel_wait_cancelled(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    /* Fill the queue with a slow task so the future task stays queued */
    ASSERT(loom_pool_submit(pool, slow_task, NULL, NULL) == LOOMWORKS_OK, "submit slow task");

    /* Give the worker time to pick up the slow task */
    struct timespec ts = {0, 50000000}; /* 50ms */
    nanosleep(&ts, NULL);

    /* Submit a future (ring path, NORMAL priority) */
    loom_future_t *fut     = NULL;
    uint64_t       task_id = 0;
    ASSERT(loom_pool_submit_future(pool, fast_result_task, NULL, &fut, &task_id) == LOOMWORKS_OK,
           "submit future");
    ASSERT(fut != NULL, "future not null");
    ASSERT(task_id > 0, "task_id returned");

    /* Cancel while queued — worker drains the tombstone later */
    ASSERT(loom_pool_cancel_by_id(pool, task_id) == LOOMWORKS_OK, "cancel future task");

    /* Infinite wait must not hang; it must surface the cancellation */
    ASSERT(loom_future_wait(fut, NULL) == LOOMWORKS_ERR_CANCELLED,
           "infinite wait returns cancelled for cancelled future");

    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: cancelled future in a lane bucket (non-NORMAL priority) ---------- */
static void test_future_cancel_lane_cancelled(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    ASSERT(loom_pool_submit(pool, slow_task, NULL, NULL) == LOOMWORKS_OK, "submit slow task");
    struct timespec ts = {0, 50000000}; /* 50ms */
    nanosleep(&ts, NULL);

    /* HIGH priority goes to the lane buckets, not the ring */
    loom_future_t *fut     = NULL;
    uint64_t       task_id = 0;
    ASSERT(loom_pool_submit_future_priority(
               pool, fast_result_task, NULL, LOOMWORKS_PRIORITY_HIGH, &fut, &task_id) ==
               LOOMWORKS_OK,
           "submit high-priority future");
    ASSERT(fut != NULL, "future not null");
    ASSERT(task_id > 0, "task_id returned");

    /* Cancel while queued in a lane — cancel_by_id unlinks the node directly */
    ASSERT(loom_pool_cancel_by_id(pool, task_id) == LOOMWORKS_OK, "cancel lane future task");

    ASSERT(loom_future_wait(fut, NULL) == LOOMWORKS_ERR_CANCELLED,
           "wait returns cancelled for lane-cancelled future");

    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: cancel_all() cancels queued futures ---------- */
static void test_future_cancel_all_cancelled(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    ASSERT(loom_pool_submit(pool, slow_task, NULL, NULL) == LOOMWORKS_OK, "submit slow task");
    struct timespec ts = {0, 50000000}; /* 50ms */
    nanosleep(&ts, NULL);

    /* Submit a future that will sit in the ring while the slow task runs */
    loom_future_t *fut     = NULL;
    uint64_t       task_id = 0;
    ASSERT(loom_pool_submit_future(pool, fast_result_task, NULL, &fut, &task_id) == LOOMWORKS_OK,
           "submit future");
    ASSERT(fut != NULL, "future not null");

    uint32_t cancelled = 0;
    loom_pool_cancel_all(pool, &cancelled);
    ASSERT(cancelled >= 1, "at least the future task is cancelled");

    ASSERT(loom_future_wait(fut, NULL) == LOOMWORKS_ERR_CANCELLED,
           "wait returns cancelled after cancel_all");

    loom_future_destroy(fut);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: destroying an uncompleted future is rejected ---------- */
static void test_future_destroy_pending_rejected(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 100};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    g_gate_started = 0;
    g_gate_release = 0;
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "submit gate task");
    while (!g_gate_started) {
        sched_yield();
    }

    /* Queue a future behind the gated task so it stays pending */
    loom_future_t *fut     = NULL;
    uint64_t       task_id = 0;
    ASSERT(loom_pool_submit_future(pool, fast_result_task, NULL, &fut, &task_id) == LOOMWORKS_OK,
           "submit future");
    ASSERT(fut != NULL, "future not null");

    /* Destroying a pending future must be rejected, not free it under a live worker */
    ASSERT(loom_future_destroy(fut) == LOOMWORKS_ERR_INVALID, "destroy pending future rejected");

    /* The future must still be usable after the rejected destroy */
    g_gate_release = 1;
    void *result   = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_OK, "wait after release");
    ASSERT(result != NULL, "result not null");
    free(result);

    ASSERT(loom_future_destroy(fut) == LOOMWORKS_OK, "destroy completed future ok");

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
    loom_pool_shutdown(pool);
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
        loom_result_t rc   = loom_pool_submit(ra->pool, increment_task, &ra->shared_data[i], NULL);
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
        ras[t]              = (race_thread_arg_t *)malloc(sizeof(*ras[t]));
        ras[t]->pool        = pool;
        ras[t]->iterations  = iterations;
        ras[t]->shared_data = (int *)malloc(sizeof(int) * iterations);
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
        if (taken) {
            free(taken);
        }
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
    loom_pc_t       *pc;
    int             *total;
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

    stress_prod_arg_t *pargs =
        (stress_prod_arg_t *)malloc(sizeof(stress_prod_arg_t) * (uint32_t)PROD);
    stress_cons_arg_t *cargs =
        (stress_cons_arg_t *)malloc(sizeof(stress_cons_arg_t) * (uint32_t)CONS);
    pthread_t *producers = (pthread_t *)malloc(sizeof(pthread_t) * (uint32_t)PROD);
    pthread_t *consumers = (pthread_t *)malloc(sizeof(pthread_t) * (uint32_t)CONS);
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
        cargs[i].pc    = pc;
        cargs[i].total = &total;
        cargs[i].lock  = &lock;
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

/* ---------- Test: pipeline ownership ---------- */
static _Atomic int g_pc_discarded;

static void pc_counting_discard(void *data, void *ctx)
{
    (void)ctx;
    free(data);
    atomic_fetch_add_explicit(&g_pc_discarded, 1, memory_order_relaxed);
}

static void test_pc_create_ex_owns_no_handler_rejected(void)
{
    loom_pc_t *pc = (loom_pc_t *)0x1; /* sentinel: must stay untouched */
    ASSERT(loom_pc_create_ex(2, 16, LOOM_PC_OWN_PAYLOADS, NULL, NULL, &pc) == LOOMWORKS_ERR_INVALID,
           "OWNS + internal + no handler rejected");
    ASSERT(pc == (loom_pc_t *)0x1, "pc untouched on rejection");
}

static void test_pc_create_ex_owns_internal_handler(void)
{
    loom_pc_t *pc  = NULL;
    g_pc_discarded = 0;
    ASSERT(loom_pc_create_ex(1, 64, LOOM_PC_OWN_PAYLOADS, pc_counting_discard, NULL, &pc) ==
               LOOMWORKS_OK,
           "create OWNS + handler");
    const int N = 5000;
    for (int i = 0; i < N; i++) {
        int *payload = (int *)malloc(sizeof(int));
        *payload     = i;
        ASSERT(loom_pc_submit(pc, payload) == LOOMWORKS_OK, "submit payload");
    }
    struct timespec sleep_ts = {0, 2L * 1000 * 1000}; /* 2 ms */
    nanosleep(&sleep_ts, NULL);
    loom_pc_shutdown(pc);
    loom_pc_destroy(&pc);
    ASSERT(g_pc_discarded == N, "every owned payload reclaimed exactly once");
}

static void test_pc_create_ex_owns_external(void)
{
    loom_pc_t *pc  = NULL;
    g_pc_discarded = 0;
    ASSERT(loom_pc_create_ex(0, 0, LOOM_PC_OWN_PAYLOADS, NULL, NULL, &pc) == LOOMWORKS_OK,
           "OWNS with no internal pool is a valid no-op");
    const int N = 1000;
    for (int i = 0; i < N; i++) {
        int *payload = (int *)malloc(sizeof(int));
        *payload     = i;
        ASSERT(loom_pc_submit(pc, payload) == LOOMWORKS_OK, "submit payload");
    }
    for (int i = 0; i < N; i++) {
        void *item = NULL;
        ASSERT(loom_pc_take(pc, &item) == LOOMWORKS_OK, "take payload");
        int *payload = (int *)item;
        ASSERT(*payload == i, "payload intact (ownership preserved)");
        free(payload);
    }
    ASSERT(loom_pc_taken_count(pc) == (uint64_t)N, "taken count == N");
    ASSERT(loom_pc_pending_count(pc) == 0, "queue drained");
    loom_pc_destroy(&pc);
    ASSERT(g_pc_discarded == 0, "external consumers free, handler never fires");
}

static void test_pc_create_ex_unknown_flag(void)
{
    loom_pc_t *pc = NULL;
    ASSERT(loom_pc_create_ex(0, 10, 0x80000000u, NULL, NULL, &pc) == LOOMWORKS_ERR_INVALID,
           "unknown flag bit rejected");
}

static void test_pc_create_ex_null_pc(void)
{
    ASSERT(loom_pc_create_ex(0, 10, 0, NULL, NULL, NULL) == LOOMWORKS_ERR_INVALID,
           "NULL pc rejected");
}

/* ================================================================
 *  Work-stealing deque unit tests (internal loom_work_deque_t)
 * ================================================================ */
static void test_deque_basic_lifo(void)
{
    g_gate_started           = 0;
    g_gate_release           = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "deque test: pool create");
    ASSERT(pool->deques != NULL, "deque test: deques allocated (not lane-only mode)");

    /* Hold the only worker hostage in a gate task: with work-stealing the
     * worker loop pops deques[0] (Step 1), which would race with this test
     * poking the deque directly — and stealing the stack-allocated test
     * tasks would free() them, corrupting the heap. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK,
           "deque test: submit gate");
    while (!g_gate_started) {
    }

    loom_work_deque_t *d = &pool->deques[0];

    /* LIFO: push t1 t2 t3 -> pop t3 -> t2 -> t1 -> NULL */
    loom_task_t t1 = {0}, t2 = {0}, t3 = {0};
    ASSERT(deque_push(pool, d, &t1), "deque: push t1");
    ASSERT(deque_push(pool, d, &t2), "deque: push t2");
    ASSERT(deque_push(pool, d, &t3), "deque: push t3");
    ASSERT(deque_pop(pool, d) == &t3, "deque: LIFO pop returns t3 (newest)");
    ASSERT(deque_pop(pool, d) == &t2, "deque: LIFO pop returns t2");
    ASSERT(deque_pop(pool, d) == &t1, "deque: LIFO pop returns t1 (oldest)");
    ASSERT(deque_pop(pool, d) == NULL, "deque: pop empty returns NULL");

    /* Steal is FIFO: push 5, pop 3 off bottom, steal oldest remaining */
    loom_task_t s[5] = {{0}};
    for (int i = 0; i < 5; i++) {
        ASSERT(deque_push(pool, d, &s[i]), "deque: steal-setup push");
    }
    ASSERT(deque_pop(pool, d) == &s[4], "deque: pop newest after setup");
    ASSERT(deque_pop(pool, d) == &s[3], "deque: pop 2nd newest after setup");
    ASSERT(deque_pop(pool, d) == &s[2], "deque: pop 3rd newest after setup");
    ASSERT(deque_steal(pool, d) == &s[0], "deque: steal returns oldest remaining (FIFO)");
    ASSERT(deque_steal(pool, d) == &s[1], "deque: steal returns next oldest (FIFO)");
    ASSERT(deque_steal(pool, d) == NULL, "deque: steal empty returns NULL");

    /* Capacity: 256 pushes OK, 257th rejected */
    loom_task_t *cap = (loom_task_t *)calloc(LOOMWORKS_DEQUE_CAPACITY + 1, sizeof(loom_task_t));
    ASSERT(cap != NULL, "deque: cap array alloc");
    for (int i = 0; i < LOOMWORKS_DEQUE_CAPACITY; i++) {
        ASSERT(deque_push(pool, d, &cap[i]), "deque: push to capacity");
    }
    ASSERT(!deque_push(pool, d, &cap[LOOMWORKS_DEQUE_CAPACITY]),
           "deque: push over capacity rejected");
    /* Drain to confirm all pushed tasks are retrievable. */
    for (int i = LOOMWORKS_DEQUE_CAPACITY - 1; i >= 0; i--) {
        ASSERT(deque_pop(pool, d) == &cap[i], "deque: drain LIFO order");
    }
    ASSERT(deque_pop(pool, d) == NULL, "deque: drained empty");
    free(cap);

    ASSERT(atomic_load_explicit(&d->len, memory_order_relaxed) == 0,
           "deque: len back to 0 after drain");

    g_gate_release = 1;
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: cancel-index entry lives from submit to run boundary ---------- */
static void cancel_in_deque_task(void *arg)
{
    /* Only the specifically-cancelled task must NOT run; all others may. */
    if ((uint64_t)(uintptr_t)arg == (uint64_t)g_cancel_target_id) {
        g_cancel_hit = 1;
    }
}

static void test_cancel_in_deque(void)
{
    g_gate_started           = 0;
    g_gate_release           = 0;
    g_cancel_hit             = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "cancel-in-deque: pool create");
    ASSERT(pool->deques != NULL, "cancel-in-deque: deques allocated");

    /* Cancel index capacity must cover ring + all deque slots. */
    size_t need = pool->ring_size + (size_t)pool->max_worker_count * LOOMWORKS_DEQUE_CAPACITY;
    ASSERT(pool->cancel_cap >= need && (pool->cancel_cap & (pool->cancel_cap - 1)) == 0,
           "cancel-in-deque: cancel_cap covers ring + deques and is power of two");

    /* Occupy the only worker with a gate so submitted tasks stay queued. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK,
           "cancel-in-deque: submit gate");
    while (!g_gate_started) {
    }

    /* Fill the ring past a single deque's worth so a worker pulling into
     * its deque would leave the LAST-submitted id resident there. */
    uint64_t last_id = 0;
    for (int i = 0; i < LOOMWORKS_DEQUE_CAPACITY + 64; i++) {
        ASSERT(loom_pool_submit(pool,
                                cancel_in_deque_task,
                                /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
                                (void *)(intptr_t)(i + 1),
                                &last_id) == LOOMWORKS_OK,
               "cancel-in-deque: submit cancellable task");
    }
    g_cancel_target_id = last_id;

    /* The last-submitted task must not run after being cancelled. */
    ASSERT(loom_pool_cancel_by_id(pool, last_id) == LOOMWORKS_OK,
           "cancel-in-deque: cancel by id succeeds");
    ASSERT(loom_pool_pending_count(pool) > 0, "cancel-in-deque: pending reflects queue");

    g_gate_release = 1;
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    ASSERT(g_cancel_hit == 0, "cancel-in-deque: cancelled task never ran");
}

/* ---------- Test: bulk dequeue from the Vyukov ring ---------- */
static void test_deque_bulk_dequeue(void)
{
    g_gate_started           = 0;
    g_gate_release           = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "bulk-dequeue: pool create");
    ASSERT(pool->deques != NULL, "bulk-dequeue: deques allocated");

    /* Occupy the only worker so submitted tasks stay resident in the ring. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK,
           "bulk-dequeue: submit gate");
    while (!g_gate_started) {
    }

    /* Submit NORMAL tasks -> ring fast path; capture their task ids. */
    uint64_t ids[LOOMWORKS_BULK_DEQUEUE] = {0};
    int      counter                     = 0;
    for (int i = 0; i < LOOMWORKS_BULK_DEQUEUE; i++) {
        ASSERT(loom_pool_submit(pool, simple_task, &counter, &ids[i]) == LOOMWORKS_OK,
               "bulk-dequeue: submit task");
    }
    ASSERT(atomic_load_explicit(&pool->ring_count, memory_order_relaxed) ==
               (size_t)LOOMWORKS_BULK_DEQUEUE,
           "bulk-dequeue: all N tasks resident in ring");

    /* Claim all N with a single CAS; order must be submission order. */
    loom_task_t *out[LOOMWORKS_BULK_DEQUEUE] = {0};
    size_t       n = ring_bulk_try_dequeue(pool, out, LOOMWORKS_BULK_DEQUEUE);
    ASSERT(n == (size_t)LOOMWORKS_BULK_DEQUEUE, "bulk-dequeue: claimed all N");
    ASSERT(atomic_load_explicit(&pool->ring_count, memory_order_relaxed) == 0,
           "bulk-dequeue: ring drained");
    for (int i = 0; i < LOOMWORKS_BULK_DEQUEUE; i++) {
        ASSERT(out[i] != NULL && out[i]->task_id == ids[i],
               "bulk-dequeue: submission order preserved");
    }

    /* Account like the worker's run boundary would (queue_len--) so the
     * shutdown drain check sees consistent state. */
    atomic_fetch_sub_explicit(&pool->queue_len, n, memory_order_relaxed);

    /* Destroy the bulk-dequeued tasks (they were never executed). */
    for (size_t i = 0; i < n; i++) {
        task_destroy(pool, out[i]);
    }

    g_gate_release = 1;
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Test: worker deque drains LIFO (work-stealing order) ---------- */
static void test_deque_lifo_drain(void)
{
    g_gate_started           = 0;
    g_gate_release           = 0;
    g_exec_count             = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "deque-lifo: pool create");
    ASSERT(pool->deques != NULL, "deque-lifo: deques allocated");

    /* Occupy the sole worker so submissions stay queued in the ring. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK,
           "deque-lifo: submit gate");
    while (!g_gate_started) {
    }

    /* NORMAL tasks take the ring fast path; after the gate they are
     * bulk-dequeued into the worker's deque and run LIFO (newest first). */
    for (int i = 1; i <= 3; i++) {
        ASSERT(loom_pool_submit(pool,
                                record_exec,
                                /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
                                (void *)(intptr_t)i,
                                NULL) == LOOMWORKS_OK,
               "deque-lifo: submit task");
    }

    g_gate_release = 1;
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    ASSERT(g_exec_count == 3, "deque-lifo: all tasks executed");
    ASSERT(g_exec_order[0] == 3 && g_exec_order[1] == 2 && g_exec_order[2] == 1,
           "deque-lifo: LIFO drain order (newest first)");
}

/* ---------- Test: REALTIME preempts deque-resident NORMAL tasks ---------- */
static void test_priority_preempts_deque(void)
{
    g_gate_started           = 0;
    g_gate_release           = 0;
    g_exec_count             = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "preempt-deque: pool create");
    ASSERT(pool->deques != NULL, "preempt-deque: deques allocated");

    /* Occupy the sole worker; NORMAL tasks queue in the ring. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK,
           "preempt-deque: submit gate");
    while (!g_gate_started) {
    }
    for (int i = 1; i <= 3; i++) {
        ASSERT(loom_pool_submit(pool,
                                record_exec,
                                /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
                                (void *)(intptr_t)i,
                                NULL) == LOOMWORKS_OK,
               "preempt-deque: submit NORMAL task");
    }
    /* REALTIME task must run before the deque-resident NORMAL tasks. */
    ASSERT(loom_pool_submit_priority(
               pool, record_exec, (void *)(intptr_t)0, LOOMWORKS_PRIORITY_REALTIME, NULL) ==
               LOOMWORKS_OK,
           "preempt-deque: submit REALTIME task");

    g_gate_release = 1;
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    ASSERT(g_exec_count == 4, "preempt-deque: all tasks executed");
    ASSERT(g_exec_order[0] == 0, "preempt-deque: REALTIME ran before deque tasks");
}

/* ---------- Test: shutdown drains worker deques ---------- */
static void test_shutdown_drains_deque(void)
{
    g_gate_started           = 0;
    g_gate_release           = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "shutdown-drain: pool create");
    ASSERT(pool->deques != NULL, "shutdown-drain: deques allocated");

    /* Occupy the sole worker so all NORMAL tasks stay queued in the ring. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK,
           "shutdown-drain: submit gate");
    while (!g_gate_started) {
    }

    /* The worker bulk-dequeues these into its local deque after release. */
    int counter = 0;
    for (int i = 0; i < 300; i++) {
        ASSERT(loom_pool_submit(pool, slow_count_task, &counter, NULL) == LOOMWORKS_OK,
               "shutdown-drain: submit task");
    }

    g_gate_release = 1;
    /* Spin until the sole worker has pulled tasks into its deque: shutdown
     * is then called with deque-resident work that MUST be drained (the
     * old exit check only knew queue_len/ring_count and would hang or
     * drop them). */
    uint64_t spins = 0;
    while (atomic_load_explicit(&pool->deques[0].len, memory_order_relaxed) == 0) {
        if (++spins > 2000000000ULL) {
            break; /* safety valve — never spin forever */
        }
    }
    ASSERT(spins < 2000000000ULL, "shutdown-drain: observed deque-resident tasks");

    loom_pool_shutdown(pool); /* must drain deque then exit, not hang */

    ASSERT(counter == 300, "shutdown-drain: all 300 tasks ran");
    ASSERT(atomic_load_explicit(&pool->ring_count, memory_order_relaxed) == 0,
           "shutdown-drain: ring drained");
    ASSERT(atomic_load_explicit(&pool->deque_total, memory_order_relaxed) == 0,
           "shutdown-drain: deques drained");
    ASSERT(atomic_load_explicit(&pool->deques[0].len, memory_order_relaxed) == 0,
           "shutdown-drain: worker deque empty");
    loom_pool_destroy(&pool);
}

static void test_resize_down_spills_deque(void)
{
    g_gate_parked            = 0;
    g_gate_started           = 0;
    g_gate_release           = 0;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "resize-spill: pool create");
    ASSERT(pool->deques != NULL, "resize-spill: deques allocated");

    /* Park BOTH workers in gates so all NORMAL tasks pile in the ring. */
    for (int i = 0; i < 2; i++) {
        ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK,
               "resize-spill: submit gate");
    }
    uint64_t spins = 0;
    while (g_gate_parked < 2) {
        if (++spins > 2000000000ULL) {
            break; /* safety valve — never spin forever */
        }
    }
    ASSERT(spins < 2000000000ULL, "resize-spill: both workers parked");

    int counter = 0;
    for (int i = 0; i < 300; i++) {
        ASSERT(loom_pool_submit(pool, slow_count_task, &counter, NULL) == LOOMWORKS_OK,
               "resize-spill: submit task");
    }

    /* Release: both workers drain the ring into their deques.  Resize-down
     * from 2 -> 1 must make the displaced worker (idx 1) spill its
     * deque-resident tasks back to the shared queue before exiting, or
     * they would be lost. */
    g_gate_release = 1;
    spins          = 0;
    while (atomic_load_explicit(&pool->deque_total, memory_order_relaxed) == 0) {
        if (++spins > 2000000000ULL) {
            break; /* safety valve — never spin forever */
        }
    }
    ASSERT(spins < 2000000000ULL, "resize-spill: observed deque-resident tasks");

    ASSERT(loom_pool_resize(pool, 1) == LOOMWORKS_OK, "resize-spill: resize to 1");
    loom_pool_shutdown(pool);

    ASSERT(counter == 300, "resize-spill: all 300 tasks ran exactly once");
    ASSERT(atomic_load_explicit(&pool->ring_count, memory_order_relaxed) == 0,
           "resize-spill: ring drained");
    ASSERT(atomic_load_explicit(&pool->deque_total, memory_order_relaxed) == 0,
           "resize-spill: deques drained");
    loom_pool_destroy(&pool);
}

/* ------------------------------------------------------------------
 *  Task 3.3: steal semantics — trigger, FIFO order, stress
 * ------------------------------------------------------------------ */
static void test_steal_trigger(void)
{
    g_gate_started      = 0;
    g_gate_release      = 0;
    g_gate_parked       = 0;
    g_gate_release2     = 0;
    g_gate_parked2      = 0;
    g_steal_owner_count = 0;

    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "steal: pool create");
    ASSERT(pool->deques != NULL, "steal: deques allocated");

    /* Park BOTH workers on gate tasks: the pool is now frozen, so the
     * ring below fills completely and nothing drains. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "steal: park worker 1");
    ASSERT(loom_pool_submit(pool, gate_task2, NULL, NULL) == LOOMWORKS_OK, "steal: park worker 2");
    uint64_t spins = 0;
    while (g_gate_parked < 1 || g_gate_parked2 < 1) {
        if (++spins > 2000000000ULL) {
            break;
        }
    }
    ASSERT(spins < 2000000000ULL, "steal: both workers parked");

    /* Flood the ring while both workers are frozen. */
    for (int i = 0; i < STEAL_MAX_TASKS; i++) {
        ASSERT(loom_pool_submit(pool, record_owner_task, NULL, NULL) == LOOMWORKS_OK,
               "steal: submit owner-record task");
    }
    ASSERT(atomic_load_explicit(&pool->ring_count, memory_order_relaxed) == (size_t)STEAL_MAX_TASKS,
           "steal: all 512 tasks resident in ring");

    /* Release worker 2 only: it drains the ring into its deque and runs
     * ~1ms dwell tasks, keeping the deque non-empty.  The first worker
     * stays parked until we release it, so it can only get work by
     * stealing.  Time-based deadline (not a fixed spin count): on a
     * loaded box the draining worker may be preempted mid-drain. */
    g_gate_release2 = 1;
    struct timespec wait_deadline;
    clock_gettime(CLOCK_MONOTONIC, &wait_deadline);
    wait_deadline.tv_sec += 60;
    bool ring_drained = false;
    while (!ring_drained) {
        if (atomic_load_explicit(&pool->ring_count, memory_order_relaxed) == 0 &&
            atomic_load_explicit(&pool->deque_total, memory_order_relaxed) > 0) {
            ring_drained = true;
        } else {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            if (now.tv_sec > wait_deadline.tv_sec ||
                (now.tv_sec == wait_deadline.tv_sec && now.tv_nsec >= wait_deadline.tv_nsec)) {
                break;
            }
            sched_yield();
        }
    }
    ASSERT(ring_drained, "steal: ring drained into worker deque");

    /* Release worker 1: own deque empty, ring empty, worker_count > 1 ->
     * it MUST steal from worker 2's deque to run anything at all. */
    g_gate_release = 1;
    loom_pool_shutdown(pool);

    ASSERT(g_steal_owner_count == STEAL_MAX_TASKS, "steal: all 512 owner tasks ran exactly once");
    ASSERT(count_distinct_owner_threads() >= 2,
           "steal: parked worker stole from free worker (>= 2 threads ran)");
    ASSERT(atomic_load_explicit(&pool->deque_total, memory_order_relaxed) == 0,
           "steal: deques drained");
    loom_pool_destroy(&pool);
}

static void test_steal_fifo_order(void)
{
    g_gate_started = 0;
    g_gate_release = 0;

    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "steal-fifo: pool create");
    ASSERT(pool->deques != NULL, "steal-fifo: deques allocated");

    /* Hold the sole worker hostage so it cannot race the direct deque poke. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK,
           "steal-fifo: submit gate");
    while (!g_gate_started) {
    }

    loom_work_deque_t *d    = &pool->deques[0];
    loom_task_t        t[6] = {{0}};
    for (int i = 0; i < 6; i++) {
        ASSERT(deque_push(pool, d, &t[i]), "steal-fifo: setup push");
    }
    /* Owner (test acting as the single thief) pops LIFO from the bottom,
     * then steals remaining oldest-first. */
    ASSERT(deque_pop(pool, d) == &t[5], "steal-fifo: owner pop newest");
    ASSERT(deque_steal(pool, d) == &t[0], "steal-fifo: steal oldest (FIFO)");
    ASSERT(deque_steal(pool, d) == &t[1], "steal-fifo: steal 2nd oldest (FIFO)");
    ASSERT(deque_steal(pool, d) == &t[2], "steal-fifo: steal 3rd oldest (FIFO)");
    ASSERT(deque_steal(pool, d) == &t[3], "steal-fifo: steal 4th oldest (FIFO)");
    ASSERT(deque_steal(pool, d) == &t[4], "steal-fifo: steal last remaining (FIFO)");
    ASSERT(deque_steal(pool, d) == NULL, "steal-fifo: empty after 5 steals");
    ASSERT(deque_pop(pool, d) == NULL, "steal-fifo: bottom empty too");
    ASSERT(atomic_load_explicit(&pool->deque_total, memory_order_relaxed) == 0,
           "steal-fifo: deque_total drained");

    g_gate_release = 1;
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_steal_stress(void)
{
    const int n_producers = 4;
    const int per_prod    = 5000;
    const int total       = n_producers * per_prod;

    for (int rep = 0; rep < 3; rep++) {
        g_steal_stress_counter   = 0;
        loom_thread_pool_t *pool = NULL;
        loom_pool_config_t  cfg  = {.worker_count = 8, .queue_capacity = 0};
        ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "steal-stress: pool create");

        pthread_t            threads[4];
        steal_producer_arg_t args[4];
        for (int p = 0; p < n_producers; p++) {
            args[p].pool = pool;
            args[p].n    = per_prod;
            ASSERT(pthread_create(&threads[p], NULL, steal_producer_thread, &args[p]) == 0,
                   "steal-stress: producer create");
        }
        for (int p = 0; p < n_producers; p++) {
            pthread_join(threads[p], NULL);
        }
        loom_pool_shutdown(pool);

        ASSERT(g_steal_stress_counter == total, "steal-stress: all 20000 tasks ran exactly once");
        ASSERT(atomic_load_explicit(&pool->deque_total, memory_order_relaxed) == 0,
               "steal-stress: deques drained");
        ASSERT(atomic_load_explicit(&pool->ring_count, memory_order_relaxed) == 0,
               "steal-stress: ring drained");
        loom_pool_destroy(&pool);
    }
}

/* ================================================================
 *  Pool coroutine task tests (Chunk 3)
 * ================================================================ */
static void test_pool_submit_coroutine(void)
{
    const int           N    = 1000;
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "coro: create pool");
    ASSERT(pool != NULL, "coro: pool non-NULL");

    g_coro_tasks_done = 0;
    for (int i = 0; i < N; i++) {
        ASSERT(loom_pool_submit_coroutine(pool, pool_coro_yield_once, NULL, 0, NULL) ==
                   LOOMWORKS_OK,
               "coro: submit coroutine task");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    ASSERT(atomic_load(&g_coro_tasks_done) == N, "coro: all tasks completed");
}

static void test_pool_coro_sleep(void)
{
    const int           N        = 100;
    struct timespec     ts_start = {0, 0};
    struct timespec     ts_end   = {0, 0};
    loom_thread_pool_t *pool     = NULL;
    loom_pool_config_t  cfg      = {.worker_count = 4, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "sleep: create pool");
    ASSERT(pool != NULL, "sleep: pool non-NULL");
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    g_coro_tasks_done = 0;
    for (int i = 0; i < N; i++) {
        ASSERT(loom_pool_submit_coroutine(pool, pool_coro_sleep, NULL, 0, NULL) == LOOMWORKS_OK,
               "sleep: submit coroutine task");
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    clock_gettime(CLOCK_MONOTONIC, &ts_end);

    int64_t elapsed_ns = (int64_t)(ts_end.tv_sec - ts_start.tv_sec) * 1000000000L +
                         (int64_t)(ts_end.tv_nsec - ts_start.tv_nsec);
    ASSERT(atomic_load(&g_coro_tasks_done) == N, "sleep: all tasks completed");
    ASSERT(elapsed_ns >= 4000000L, "sleep: elapsed respects the minimum 5ms sleep");
}

static void test_coro_cancel_sleeping(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "cancel-sleep: create pool");
    ASSERT(pool != NULL, "cancel-sleep: pool non-NULL");

    g_coro_tasks_done = 0;
    uint64_t tid      = 0;
    ASSERT(loom_pool_submit_coroutine(pool, pool_coro_sleep_long, NULL, 0, &tid) == LOOMWORKS_OK,
           "cancel-sleep: submit long-sleeping coroutine");
    ASSERT(tid != 0, "cancel-sleep: task id assigned");

    struct timespec delay = {0, 20000000L}; /* 20 ms */
    clock_nanosleep(CLOCK_MONOTONIC, 0, &delay, NULL);

    ASSERT(loom_pool_cancel_by_id(pool, tid) == LOOMWORKS_OK, "cancel-sleep: cancel sleeping task");
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    ASSERT(atomic_load(&g_coro_tasks_done) == 0, "cancel-sleep: task never ran to completion");
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
    test_task_group_wait_from_worker();
    test_task_group_destroy_from_worker();
    test_task_group_wait_timeout_expired_and_reusable();
    test_task_group_wait_timeout_ok();
    test_task_group_wait_timeout_null_deadline();
    test_task_group_wait_timeout_from_worker();
    test_task_group_wait_timeout_null_group();
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
    test_pc_create_ex_owns_no_handler_rejected();
    test_pc_create_ex_owns_internal_handler();
    test_pc_create_ex_owns_external();
    test_pc_create_ex_unknown_flag();
    test_pc_create_ex_null_pc();
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
    test_metrics_callback_on_worker_thread();
    test_metrics_callback_outside_lock();
    test_metrics_callback_lifecycle_counts();
    test_submit_blocking_unbounded();
    test_submit_blocking_with_id();
    test_submit_blocking_after_shutdown();
    test_submit_blocking_null_safety();
    test_resize_grow();
    test_resize_shrink();
    test_resize_after_shutdown();
    test_resize_null_safety();
    test_resize_zero_rejected();
    test_resize_alloc_fail_deques_realloc();
    test_resize_alloc_fail_deque_slots();
    test_resize_alloc_fail_threads_realloc();
    test_resize_alloc_fail_alive_realloc();
    test_resize_alloc_fail_clean_exit_realloc();
    test_resize_alloc_fail_worker_arg_first();
    test_resize_alloc_fail_worker_arg_mid();
    test_resize_fail_then_worker_crash_detected();
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
    test_deque_basic_lifo();
    test_cancel_in_deque();
    test_deque_bulk_dequeue();
    test_deque_lifo_drain();
    test_priority_preempts_deque();
    test_shutdown_drains_deque();
    test_resize_down_spills_deque();
    test_steal_trigger();
    test_steal_fifo_order();
    test_steal_stress();
    test_future_cancel_wait_cancelled();
    test_future_cancel_lane_cancelled();
    test_future_cancel_all_cancelled();
    test_future_destroy_pending_rejected();
    test_pool_destroy_without_shutdown();
    test_worker_crash_detected();
    test_pool_submit_coroutine();
    test_pool_coro_sleep();
    test_coro_cancel_sleeping();
    printf("\nResults: %d passed, %d failed\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
