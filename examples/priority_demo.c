/**
 * @file priority_demo.c
 * @brief Priority scheduling demo — later-submitted high-priority tasks
 *        overtake earlier-submitted low-priority tasks.
 *
 * A single worker drains lanes in strict priority order: REALTIME/HIGH
 * (p < 5) lanes first, then the NORMAL (p == 5) lock-free ring, then the
 * LOW (p >= 5) lanes.  This demo submits 10 LOW tasks, then 10 HIGH
 * tasks, and verifies from the recorded execution order that every HIGH
 * task completes before all but the one LOW task that was already
 * running when the HIGH tasks arrived.
 *
 * Build:
 *   cmake --build build --target example_priority
 * Run:
 *   ./build/example_priority
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/thread_pool.h"

#include <stdatomic.h>
#include <stdio.h>
#include <time.h>

enum { N_LOW = 10, N_HIGH = 10, N_TOTAL = N_LOW + N_HIGH };

/* Execution log: each task pushes its id (0..9 = LOW, 10..19 = HIGH). */
static _Atomic unsigned g_log_pos = 0;
static unsigned         g_log[N_TOTAL];
static int              g_index[N_TOTAL];

static void sleep_ms(long ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

static void low_task(void *arg)
{
    unsigned id = (unsigned)((int *)arg - g_index);
    unsigned p = atomic_fetch_add_explicit(&g_log_pos, 1, memory_order_relaxed);
    g_log[p] = id;
    sleep_ms(30); /* keep the worker busy so the HIGH tasks can overtake */
}

static void high_task(void *arg)
{
    unsigned id = N_LOW + (unsigned)((int *)arg - g_index);
    unsigned p = atomic_fetch_add_explicit(&g_log_pos, 1, memory_order_relaxed);
    g_log[p] = id;
    sleep_ms(5);
}

int main(void)
{
    loom_pool_config_t cfg = {.worker_count = 1};
    loom_thread_pool_t *pool = NULL;
    if (loom_pool_create(&cfg, &pool) != LOOMWORKS_OK) {
        fprintf(stderr, "FAIL: pool create\n");
        return 1;
    }

    /* Submit the LOW batch first, then the HIGH batch. */
    for (int i = 0; i < N_LOW; i++) {
        g_index[i] = i;
        if (loom_pool_submit_priority(pool, low_task, &g_index[i],
                                      LOOMWORKS_PRIORITY_LOW, NULL) != LOOMWORKS_OK) {
            fprintf(stderr, "FAIL: LOW submit %d\n", i);
            return 1;
        }
    }
    for (int i = 0; i < N_HIGH; i++) {
        g_index[N_LOW + i] = i;
        if (loom_pool_submit_priority(pool, high_task, &g_index[N_LOW + i],
                                      LOOMWORKS_PRIORITY_HIGH, NULL) != LOOMWORKS_OK) {
            fprintf(stderr, "FAIL: HIGH submit %d\n", i);
            return 1;
        }
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);

    unsigned executed = atomic_load_explicit(&g_log_pos, memory_order_relaxed);
    printf("Execution order (0-9 = LOW, 10-19 = HIGH):\n  ");
    for (unsigned i = 0; i < executed; i++) {
        printf("%s%u", i ? " " : "", g_log[i]);
    }
    printf("\n");

    if (executed != N_TOTAL) {
        fprintf(stderr, "FAIL: only %u/%d tasks executed\n", executed, N_TOTAL);
        return 1;
    }

    /* All HIGH tasks must complete before every LOW task, except possibly
     * the single LOW task that was already running when the HIGH batch was
     * submitted.  The HIGH block must therefore start at log position 0
     * (HIGHs won the race to the lane) or 1 (one LOW was already running),
     * and run contiguously. */
    int first_high = -1;
    for (unsigned i = 0; i < executed; i++) {
        if (g_log[i] >= N_LOW) {
            first_high = (int)i;
            break;
        }
    }
    if (first_high < 0 || first_high > 1) {
        fprintf(stderr, "FAIL: HIGH block starts at position %d (expected 0 or 1)\n",
                first_high);
        return 1;
    }
    for (unsigned i = 0; i < N_HIGH; i++) {
        unsigned id = g_log[(unsigned)first_high + i];
        if (id < N_LOW) {
            fprintf(stderr, "FAIL: LOW task %u executed inside the HIGH block at %u\n",
                    id, (unsigned)first_high + i);
            return 1;
        }
    }
    printf("PASS: %d HIGH-priority tasks (submitted after %d LOW tasks) all ran "
           "before the pending LOW tasks.\n",
           N_HIGH, N_LOW);
    return 0;
}
