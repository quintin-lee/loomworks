/**
 * @file pipeline_demo.c
 * @brief Producer-consumer pipeline demo with internal consumer pool.
 *
 * Architecture:
 *   - loom_pc_t pipeline creates an internal consumer pool when
 *     worker_count > 0.  Consumer threads are managed automatically
 *     by the pipeline — no external pool or pthreads needed for consumers.
 *   - Producers submit items from the main thread.
 *   - loom_pc_shutdown() wakes internal consumers; loom_pc_destroy()
 *     joins and frees them.
 *
 * Build:
 *   cmake --build build --target example_pipeline
 * Run:
 *   ./build/example_pipeline
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/pipeline.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <time.h>

#define NUM_CONSUMERS  4
#define NUM_ITEMS      200

int main(void)
{
    printf("=== Pipeline Demo (internal pool) ===\n");

    /* Create pipeline: internal consumer pool with NUM_CONSUMERS workers,
     * capacity 32.  The pipeline manages its own consumer threads. */
    loom_pc_t *pc = NULL;
    if (loom_pc_create((uint32_t)NUM_CONSUMERS, 32, &pc) != LOOMWORKS_OK) {
        fprintf(stderr, "Failed to create pipeline\n");
        return 1;
    }

    /* Submit items from the main thread */
    int total_submitted = 0;
    for (int i = 0; i < NUM_ITEMS; i++) {
        int *item = (int *)malloc(sizeof(int));
        if (!item) {
            fprintf(stderr, "malloc failed\n");
            break;
        }
        *item = i;
        if (loom_pc_submit(pc, item) == LOOMWORKS_OK) {
            total_submitted++;
        } else {
            free(item);
            break;
        }
    }
    uint64_t submitted = loom_pc_submitted_count(pc);
    printf("Submitted %llu items.\n", (unsigned long long)submitted);

    /* Signal shutdown — internal consumers drain remaining items
     * and then exit.  Destroy cleans up the internal pool. */
    loom_pc_shutdown(pc);

    /* Wait until consumers truly drained everything: taken_count tracks
     * every item actually dequeued by loom_pc_take(). */
    while (loom_pc_taken_count(pc) < submitted) {
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 1000000}; /* 1 ms */
        nanosleep(&ts, NULL);
    }

    uint64_t consumed = loom_pc_taken_count(pc);
    loom_pc_destroy(&pc);

    if (consumed == submitted && consumed == (uint64_t)total_submitted) {
        printf("Consumed %llu items. PASS.\n", (unsigned long long)consumed);
        return 0;
    }
    fprintf(stderr, "FAIL: consumed %llu of %llu submitted\n",
            (unsigned long long)consumed, (unsigned long long)submitted);
    return 1;
}
