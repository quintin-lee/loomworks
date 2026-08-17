/**
 * @file backpressure_demo.c
 * @brief Bounded-queue backpressure and future timeout demo.
 *
 * Part 1 — bounded queue: a 1-worker pool with queue_capacity = 4.  The
 * worker is kept busy with a slow task, then 4 more tasks are submitted
 * to fill the queue.  A further non-blocking submit must fail with
 * LOOMWORKS_ERR_INVALID (queue full), while loom_pool_submit_blocking()
 * blocks until a worker frees a slot — showing backpressure in action.
 *
 * Part 2 — future timeout: a slow future task is submitted; waiting with
 * a 10 ms deadline returns LOOMWORKS_ERR_TIMEOUT, then the ordinary
 * loom_future_wait() still yields the result once the task completes.
 *
 * Build:
 *   cmake --build build --target example_backpressure
 * Run:
 *   ./build/example_backpressure
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
    sleep_ms(50);
}

static void *slow_result_task(void *arg)
{
    (void)arg;
    sleep_ms(200);
    static int answer = 42; /* caller does not free; static keeps it simple */
    return &answer;
}

int main(void)
{
    int failed = 0;

    /* ---- Part 1: bounded queue backpressure ---- */
    enum { CAP = 4, FILL = 4, EXTRA = 5 };
    loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = CAP};
    loom_thread_pool_t *pool = NULL;
    if (loom_pool_create(&cfg, &pool) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: pool create\n");
        return 1;
    }

    /* Task 1: worker grabs it and sleeps 50 ms — busy the whole demo. */
    if (loom_pool_submit(pool, slow_task, NULL, NULL) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: seed submit\n");
        return 1;
    }
    sleep_ms(10); /* worker definitely running the seed task now */

    /* Fill the queue to capacity. */
    for (int i = 0; i < FILL; i++) {
        if (loom_pool_submit(pool, slow_task, NULL, NULL) != LOOMWORKS_OK) {
            fprintf(stderr, "FAIL: fill submit %d\n", i);
            return 1;
        }
    }
    uint32_t pending = loom_pool_pending_count(pool);
    printf("Queue full: pending=%u (capacity %d)\n", pending, CAP);
    if (pending > CAP) {
        fprintf(stderr, "FAIL: pending %u exceeds capacity %d\n", pending, CAP);
        failed = 1;
    }

    /* Non-blocking submit on a full queue → ERR_INVALID. */
    if (loom_pool_submit(pool, slow_task, NULL, NULL) != LOOMWORKS_ERR_INVALID) {
        fprintf(stderr, "FAIL: full-queue non-blocking submit should be ERR_INVALID\n");
        failed = 1;
    }
    printf("Non-blocking submit on full queue: LOOMWORKS_ERR_INVALID (as expected)\n");

    /* Blocking submits: each waits until a slot frees (worker finishes a
     * task every ~50 ms) — the pool applies backpressure, no task lost. */
    for (int i = 0; i < EXTRA; i++) {
        if (loom_pool_submit_blocking(pool, slow_task, NULL, NULL) != LOOMWORKS_OK) {
            fprintf(stderr, "FAIL: blocking submit %d\n", i);
            return 1;
        }
    }
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    printf("All %d queued tasks drained after backpressure.\n", 1 + FILL + EXTRA);

    /* ---- Part 2: future timeout ---- */
    loom_pool_config_t  cfg2  = {.worker_count = 1};
    loom_thread_pool_t *pool2 = NULL;
    if (loom_pool_create(&cfg2, &pool2) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: pool2 create\n");
        return 1;
    }
    loom_future_t *fut = NULL;
    if (loom_pool_submit_future(pool2, slow_result_task, NULL, &fut, NULL) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: future submit\n");
        return 1;
    }

    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    deadline.tv_nsec += 10 * 1000000L; /* 10 ms from now */
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    void         *result = NULL;
    loom_result_t rc     = loom_future_wait_timeout(fut, &result, &deadline);
    printf("Future wait with 10 ms deadline: %s\n",
           rc == LOOMWORKS_ERR_TIMEOUT ? "LOOMWORKS_ERR_TIMEOUT" : "unexpected OK");
    if (rc != LOOMWORKS_ERR_TIMEOUT) {
        fprintf(stderr, "FAIL: expected LOOMWORKS_ERR_TIMEOUT, got %d\n", rc);
        failed = 1;
    }

    if (loom_future_wait(fut, &result) != LOOMWORKS_OK || result == NULL) {
        fprintf(stderr, "FAIL: future_wait after timeout\n");
        failed = 1;
    } else {
        printf("Future result after completing task: %d\n", *(int *)result);
    }
    loom_future_destroy(fut);
    loom_pool_shutdown(pool2);
    loom_pool_destroy(&pool2);

    if (!failed) {
        printf("PASS: backpressure and future timeout behave as documented.\n");
        return 0;
    }
    return 1;
}
