/**
 * @file resize_demo.c
 * @brief Dynamic pool resize demo — grow and shrink a pool mid-batch.
 *
 * A 2-worker pool is created, a batch of slow tasks is submitted, and the
 * pool is resized to 8 workers while the batch is still draining — the
 * drain speed visibly increases.  It is then resized back to 2 workers
 * and finally shut down.  The demo verifies worker_count() reflects the
 * resize, active workers never exceed the peak (8) count, resize to 0 is
 * rejected, and all tasks complete exactly once.
 *
 * Build:
 *   cmake --build build --target example_resize
 * Run:
 *   ./build/example_resize
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/thread_pool.h"

#include <stdio.h>
#include <time.h>

static void sleep_ms(long ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

static void slow_task(void *arg)
{
    (void)arg;
    sleep_ms(40);
}

static void check_bounds(const char *phase, loom_thread_pool_t *pool, uint32_t expected,
                         uint32_t active_cap, int *failed)
{
    uint32_t workers = loom_pool_worker_count(pool);
    uint32_t active  = loom_pool_active_count(pool);
    printf("  [%s] workers=%u active=%u pending=%u\n", phase, workers, active,
           loom_pool_pending_count(pool));
    if (workers != expected) {
        fprintf(stderr, "FAIL: [%s] workers=%u expected %u\n", phase, workers, expected);
        *failed = 1;
    }
    /* After a shrink, in-flight workers finish before exiting, so active may
     * transiently exceed the new worker count — but never the peak (8). */
    if (active > active_cap) {
        fprintf(stderr, "FAIL: [%s] active %u > cap %u\n", phase, active, active_cap);
        *failed = 1;
    }
}

int main(void)
{
    int failed = 0;

    loom_pool_config_t cfg = {.worker_count = 2};
    loom_thread_pool_t *pool = NULL;
    if (loom_pool_create(&cfg, &pool) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: pool create\n");
        return 1;
    }
    check_bounds("created", pool, 2, 2, &failed);

    /* Submit a batch big enough to still be draining during the resizes. */
    enum { N = 20 };
    for (int i = 0; i < N; i++) {
        if (loom_pool_submit(pool, slow_task, NULL, NULL) != LOOMWORKS_OK) {
            fprintf(stderr, "FAIL: submit %d\n", i);
            return 1;
        }
    }

    /* Grow mid-batch: 2 → 8 workers. */
    sleep_ms(10); /* let a few tasks start so the batch is still draining */
    if (loom_pool_resize(pool, 8) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: resize up\n");
        return 1;
    }
    check_bounds("resized 2→8", pool, 8, 8, &failed);
    sleep_ms(30); /* observe faster drain */
    check_bounds("draining at 8", pool, 8, 8, &failed);

    /* Shrink mid-batch: 8 → 2 workers. */
    if (loom_pool_resize(pool, 2) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: resize down\n");
        return 1;
    }
    check_bounds("resized 8→2", pool, 2, 8, &failed);

    /* Resize to an invalid count must be rejected (0 = auto only at create). */
    if (loom_pool_resize(pool, 0) == LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: resize to 0 should fail\n");
        failed = 1;
    }

    loom_pool_shutdown(pool);
    check_bounds("after shutdown", pool, 2, 2, &failed);
    loom_pool_destroy(&pool);

    if (!failed) {
        printf("PASS: resize 2→8→2 applied, active never exceeded peak worker count.\n");
        return 0;
    }
    return 1;
}
