/**
 * @file pipeline_demo.c
 * @brief Producer-consumer pipeline demo.
 *
 * Shows how to use loom_pc_t to implement a simple producer-consumer
 * pattern with multiple producers and consumers.
 *
 * Build:
 *   cmake --build build --target example_pipeline
 * Run:
 *   ./build/example_pipeline
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/pipeline.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#define NUM_PRODUCERS 2
#define NUM_CONSUMERS 4
#define ITEMS_PER_PRODUCER 100

typedef struct {
    loom_pc_t       *pc;
    int             *total;
    pthread_mutex_t *lock;
} consumer_arg_t;

typedef struct {
    loom_pc_t *pc;
    int        producer_id;
    int        count;
} producer_arg_t;

static void *producer(void *arg)
{
    producer_arg_t *pa = (producer_arg_t *)arg;
    loom_pc_t      *pc = pa->pc;

    for (int i = 0; i < pa->count; i++) {
        int *item = (int *)malloc(sizeof(int));
        if (!item) {
            break;
        }
        *item            = pa->producer_id * 1000 + i;
        loom_result_t rc = loom_pc_submit(pc, item);
        if (rc != LOOMWORKS_OK) {
            free(item);
            break;
        }
    }
    return NULL;
}

static void *consumer(void *arg)
{
    consumer_arg_t *ca = (consumer_arg_t *)arg;
    loom_pc_t      *pc = ca->pc;
    void           *item;

    while (loom_pc_take(pc, &item) == LOOMWORKS_OK) {
        int *val = (int *)item;
        pthread_mutex_lock(ca->lock);
        *ca->total += *val;
        pthread_mutex_unlock(ca->lock);
        free(val);
    }
    return NULL;
}

int main(void)
{
    printf("=== Pipeline Demo ===\n");

    loom_pc_t *pc = NULL;
    if (loom_pc_create(4, 32, &pc) != LOOMWORKS_OK) {
        fprintf(stderr, "Failed to create pipeline\n");
        return 1;
    }

    int             global_total = 0;
    pthread_mutex_t total_lock   = PTHREAD_MUTEX_INITIALIZER;

    /* Start consumers */
    pthread_t      consumers[NUM_CONSUMERS];
    consumer_arg_t cargs[NUM_CONSUMERS];
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        cargs[i].pc    = pc;
        cargs[i].total = &global_total;
        cargs[i].lock  = &total_lock;
        pthread_create(&consumers[i], NULL, consumer, &cargs[i]);
    }

    /* Start producers */
    pthread_t      producers[NUM_PRODUCERS];
    producer_arg_t pargs[NUM_PRODUCERS];
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        pargs[i].pc          = pc;
        pargs[i].producer_id = i;
        pargs[i].count       = ITEMS_PER_PRODUCER;
        pthread_create(&producers[i], NULL, producer, &pargs[i]);
    }

    /* Wait for all producers */
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        pthread_join(producers[i], NULL);
    }
    printf("All producers done. Submitted %d items.\n", NUM_PRODUCERS * ITEMS_PER_PRODUCER);

    /* Signal shutdown */
    loom_pc_shutdown(pc);

    /* Wait for all consumers */
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        pthread_join(consumers[i], NULL);
    }
    printf("All consumers done.\n");

    /* Verify totals */
    long expected = 0;
    for (int p = 0; p < NUM_PRODUCERS; p++) {
        for (int i = 0; i < ITEMS_PER_PRODUCER; i++) {
            expected += (long)p * 1000 + i;
        }
    }
    printf("Expected total: %ld, Actual total: %d\n", expected, global_total);

    loom_pc_destroy(&pc);
    pthread_mutex_destroy(&total_lock);

    if (global_total == (int)expected) {
        printf("PASS: totals match!\n");
        return 0;
    } else {
        printf("FAIL: totals do not match!\n");
        return 1;
    }
}
