/**
 * @file cancel_demo.c
 * @brief Cancellation demo — cancel by id, cancel by user-data pointer,
 *        cancel-all, and metrics reconciliation.
 *
 * 10 tasks are submitted to a 2-worker pool; each task sleeps 30 ms, so
 * only the first 2 start running while 8 remain pending.  The demo then
 * cancels one pending task by id, one by user-data pointer, probes a
 * bogus id, and cancels all remaining pending tasks.  It verifies that:
 *   - cancelled tasks never execute (executed flags stay clear),
 *   - cancel of an unknown id returns LOOMWORKS_ERR_INVALID,
 *   - metrics reconcile exactly: submitted == completed + cancelled.
 *
 * Build:
 *   cmake --build build --target example_cancel
 * Run:
 *   ./build/example_cancel
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/metrics.h"
#include "loomworks/thread_pool.h"

#include <stdatomic.h>
#include <stdio.h>
#include <time.h>

enum { N_TASKS = 10, N_WORKERS = 2 };

typedef struct {
    int         index;
    _Atomic int executed;
} task_ctx_t;

static task_ctx_t g_ctx[N_TASKS];
static uint64_t   g_ids[N_TASKS];

static void sleep_ms(long ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

static void slow_task(void *arg)
{
    task_ctx_t *ctx = (task_ctx_t *)arg;
    atomic_store_explicit(&ctx->executed, 1, memory_order_relaxed);
    sleep_ms(30);
}

int main(void)
{
    for (int i = 0; i < N_TASKS; i++) {
        g_ctx[i].index    = i;
        g_ctx[i].executed = 0;
    }

    loom_pool_config_t  cfg  = {.worker_count = N_WORKERS};
    loom_thread_pool_t *pool = NULL;
    if (loom_pool_create(&cfg, &pool) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: pool create\n");
        return 1;
    }
    loom_metrics_t *metrics = NULL;
    if (loom_metrics_create(pool, NULL, NULL, &metrics) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: metrics create\n");
        loom_pool_shutdown(pool);
        loom_pool_destroy(&pool);
        return 1;
    }

    /* Submit all tasks fast.  Wait until both workers have dequeued, so
     * exactly N_TASKS - N_WORKERS tasks are pending when we cancel. */
    for (int i = 0; i < N_TASKS; i++) {
        if (loom_pool_submit(pool, slow_task, &g_ctx[i], &g_ids[i]) != LOOMWORKS_OK) {
            fprintf(stderr, "FAIL: submit %d\n", i);
            return 1;
        }
    }
    for (int i = 0; i < 100 && loom_pool_active_count(pool) < N_WORKERS; i++) {
        sleep_ms(1);
    }
    if (loom_pool_active_count(pool) != N_WORKERS) {
        fprintf(stderr, "FAIL: workers did not become active\n");
        return 1;
    }

    /* Cancel one pending task by id. */
    if (loom_pool_cancel_by_id(pool, g_ids[3]) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: cancel_by_id(g_ids[3])\n");
        return 1;
    }
    /* Cancelling the same id again must fail — already gone. */
    if (loom_pool_cancel_by_id(pool, g_ids[3]) != LOOMWORKS_ERR_INVALID) {
        fprintf(stderr, "FAIL: double cancel_by_id should be ERR_INVALID\n");
        return 1;
    }
    /* Cancel one pending task by user_data pointer. */
    if (loom_pool_cancel(pool, &g_ctx[5]) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: cancel(&g_ctx[5])\n");
        return 1;
    }
    /* Unknown id → ERR_INVALID. */
    if (loom_pool_cancel_by_id(pool, 999999) != LOOMWORKS_ERR_INVALID) {
        fprintf(stderr, "FAIL: cancel_by_id(bogus) should be ERR_INVALID\n");
        return 1;
    }

    /* Cancel every remaining pending task (indices 2,4,6,7,8,9). */
    uint32_t cancelled_all = 0;
    loom_pool_cancel_all(pool, &cancelled_all);
    if (cancelled_all != 6) {
        fprintf(stderr, "FAIL: cancel_all cancelled %u, expected 6\n", cancelled_all);
        return 1;
    }

    loom_pool_shutdown(pool);

    int ran    = 0;
    int wrong  = 0;
    int failed = 0;
    for (int i = 0; i < N_TASKS; i++) {
        int executed = atomic_load_explicit(&g_ctx[i].executed, memory_order_relaxed);
        if (executed) {
            ran++;
        }
        if (i == 0 || i == 1) {
            if (!executed) {
                fprintf(stderr, "FAIL: task %d should have run\n", i);
                failed = 1;
            }
        } else if (executed) {
            fprintf(stderr, "FAIL: cancelled task %d still executed\n", i);
            failed = 1;
        }
    }
    if (ran != N_WORKERS) {
        fprintf(stderr, "FAIL: %d tasks ran, expected %d\n", ran, N_WORKERS);
        failed = 1;
    }

    /* Metrics must reconcile: submitted == completed + cancelled. */
    uint64_t submitted = loom_metrics_submitted(metrics);
    uint64_t completed = loom_metrics_completed(metrics);
    uint64_t cancelled = loom_metrics_cancelled(metrics);
    if (submitted != N_TASKS || completed + cancelled != submitted) {
        fprintf(stderr,
                "FAIL: metrics submitted=%llu completed=%llu cancelled=%llu "
                "(expected completed+cancelled == %llu)\n",
                (unsigned long long)submitted,
                (unsigned long long)completed,
                (unsigned long long)cancelled,
                (unsigned long long)N_TASKS);
        failed = 1;
    }
    if (wrong) {
        failed = 1;
    }

    loom_metrics_destroy(&metrics);
    loom_pool_destroy(&pool);

    printf("Submitted %d, ran %d, cancelled %d (by id 1, by data 1, cancel_all %u).\n",
           N_TASKS,
           ran,
           (int)(submitted - completed),
           cancelled_all);
    if (!failed) {
        printf("PASS: cancelled tasks never executed; metrics reconcile.\n");
        return 0;
    }
    return 1;
}
