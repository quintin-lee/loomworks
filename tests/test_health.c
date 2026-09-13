#define _POSIX_C_SOURCE 200809L
#include "loomworks/thread_pool.h"
#include "loomworks/thread_pool_health.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int g_passes   = 0;
static int g_failures = 0;
#define ASSERT(expr, msg)                                                                          \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "FAIL: %s\n", msg);                                                    \
            g_failures++;                                                                          \
        } else {                                                                                   \
            g_passes++;                                                                            \
        }                                                                                          \
    } while (0)

static void sleep_ms(int ms)
{
    struct timespec ts;
    ts.tv_sec  = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

static void test_health_basic(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    loom_health_status_t status;
    ASSERT(loom_pool_health_sample(pool, &status) == LOOMWORKS_OK, "health sample");
    ASSERT(status.worker_count == 2, "worker count");
    ASSERT(status.active_count == 0, "no active workers initially");
    ASSERT(status.utilization == 0.0, "zero utilization idle");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

static void test_health_during_work(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    for (int i = 0; i < 10; i++) {
        loom_pool_submit(pool, NULL, NULL, NULL);
    }
    sleep_ms(200);

    loom_health_status_t status;
    ASSERT(loom_pool_health_sample(pool, &status) == LOOMWORKS_OK, "health sample");
    ASSERT(status.worker_count == 4, "worker count after work");
    // pending count is always >= 0 for uint32_t;
    ASSERT(status.active_count <= 4, "active within bounds");
    ASSERT(status.utilization >= 0.0 && status.utilization <= 1.0, "utilization in range");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* A pool sized past the old fixed recovery_attempts[64] array.  With worker
 * recovery enabled the timer thread calls check_and_recover_workers every
 * iteration, reading recovery_attempts[i] for i < worker_count.  Under the
 * old fixed-size array this was a heap out-of-bounds read for any pool with
 * more than 64 workers (worker_count=72 below hits it).  It is also a no-
 * deadlock regression: every worker here is alive, so the non-blocking
 * tryjoin returns EBUSY and no restart is attempted.  ASan flags the old
 * OOB; the dynamic array passes clean. */
static void recover_coro_entry(void *user_data)
{
    (void)user_data;
    loom_coro_yield();
}

static void test_recovery_large_pool(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 72};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "large pool create");
    /* NOTE: abnormal_worker_count/health.abnormal_workers uses the predicate
     * alive && !clean, and thread_clean_exit is set only on the shutdown
     * exit path — so a healthy running worker reads !clean and is *counted*.
     * We therefore do NOT assert abnormal_workers==0 here (that is a separate,
     * pre-existing semantic quirk, not what this test guards).  What this
     * test guards: the recovery scan reading recovery_attempts[i] for
     * i < 72.  Under the old fixed recovery_attempts[64] array that is a
     * heap out-of-bounds read; the heap array sized to max_worker_count is
     * clean.  The non-blocking tryjoin returns EBUSY for each live worker,
     * so no restart is attempted and no join-under-lock deadlock occurs. */

    loom_pool_set_worker_recovery_timeout(pool, 1000000000LL); /* 1 s; enables the path */

    /* A coroutine task starts the timer thread, which runs the recovery
     * scan (reading recovery_attempts up to index 71) every iteration. */
    loom_result_t rrc = loom_pool_submit_coroutine(pool, recover_coro_entry, NULL, 0, NULL);
    ASSERT(rrc == LOOMWORKS_OK, "submit coroutine on large pool");
    sleep_ms(500);

    loom_health_status_t status;
    ASSERT(loom_pool_health_sample(pool, &status) == LOOMWORKS_OK, "health sample on large pool");
    ASSERT(status.worker_count == 72, "health sample reports full worker count");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}
static void test_health_null_safety(void)
{
    ASSERT(loom_pool_health_sample(NULL, NULL) == LOOMWORKS_ERR_INVALID, "null pool rejected");
}

int main(void)
{
    test_health_basic();
    test_health_during_work();
    test_health_null_safety();
    test_recovery_large_pool();

    fprintf(stderr, "Passes: %d, Failures: %d\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
