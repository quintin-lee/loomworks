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

static void test_health_null_safety(void)
{
    ASSERT(loom_pool_health_sample(NULL, NULL) == LOOMWORKS_ERR_INVALID, "null pool rejected");
}

int main(void)
{
    test_health_basic();
    test_health_during_work();
    test_health_null_safety();

    fprintf(stderr, "Passes: %d, Failures: %d\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
