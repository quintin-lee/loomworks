/**
 * @file monitor_demo.c
 * @brief Live pool health + metrics monitoring demo.
 *
 * Submits a large batch of short tasks to a 4-worker pool, polls pool
 * health and metrics every 50 ms, then prints a final consistent snapshot
 * once the queue drains — mirroring the [monitor] output style of
 * pipeline_demo.c.
 *
 * Build:
 *   cmake --build build --target monitor_demo
 * Run:
 *   ./build/monitor_demo
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/metrics.h"
#include "loomworks/thread_pool.h"

#include <stdio.h>
#include <time.h>

static void short_task(void *arg)
{
    (void)arg;
    volatile uint64_t sink = 0;
    for (int i = 0; i < 10000; i++) {
        sink += (uint64_t)i;
    }
    (void)sink;
}

int main(void)
{
    loom_pool_config_t cfg  = {.worker_count = 4};
    loom_thread_pool_t *pool = NULL;
    if (loom_pool_create(&cfg, &pool) != LOOMWORKS_OK) {
        fprintf(stderr, "Failed to create pool\n");
        return 1;
    }
    loom_metrics_t *metrics = NULL;
    if (loom_metrics_create(pool, NULL, NULL, &metrics) != LOOMWORKS_OK) {
        fprintf(stderr, "Failed to create metrics\n");
        loom_pool_destroy(&pool);
        return 1;
    }

    enum { N = 100000 };
    for (int i = 0; i < N; i++) {
        if (loom_pool_submit(pool, short_task, NULL, NULL) != LOOMWORKS_OK) {
            break;
        }
    }
    printf("Submitted up to %d tasks.\n", N);

    struct timespec ts = {0, 50 * 1000000L}; /* 50 ms */
    for (int i = 0; i < 40; i++) {
        nanosleep(&ts, NULL);
        uint32_t  active    = loom_pool_active_count(pool);
        uint32_t  idle      = loom_pool_idle_count(pool);
        uint32_t  pending   = loom_pool_pending_count(pool);
        uint64_t  submitted = loom_metrics_submitted(metrics);
        uint64_t  completed = loom_metrics_completed(metrics);
        printf("  [monitor] active %u idle %u queue %u submitted %llu completed %llu\n",
               active, idle, pending, (unsigned long long)submitted,
               (unsigned long long)completed);
        if (pending == 0 && completed == submitted) {
            break;
        }
    }

    loom_pool_shutdown(pool);

    loom_metrics_snapshot_t snap;
    if (loom_metrics_snapshot(metrics, &snap) == LOOMWORKS_OK) {
        printf("  [monitor] final snapshot: submitted %llu started %llu completed %llu "
               "cancelled %llu failed %llu avg %llu ns max %llu ns\n",
               (unsigned long long)snap.submitted, (unsigned long long)snap.started,
               (unsigned long long)snap.completed, (unsigned long long)snap.cancelled,
               (unsigned long long)snap.failed,
               (unsigned long long)loom_metrics_avg_latency_ns(metrics),
               (unsigned long long)snap.latency_max_ns);
    }

    loom_metrics_destroy(&metrics);
    loom_pool_destroy(&pool);
    printf("Done.\n");
    return 0;
}
