/**
 * @file pipeline_demo.c
 * @brief Producer-consumer pipeline demo using loom_thread_pool_t.
 *
 * Architecture:
 *   - loom_pc_t pipeline (bounded FIFO queue)
 *   - loom_thread_pool_t manages all worker threads (no raw pthreads)
 *   - Producers are pool tasks that submit items to the pipeline
 *   - Consumers are pool workers that block on loom_pc_take()
 *   - Main thread: submit producers + consumers, wait for drain,
 *     shutdown pipeline, then shutdown the pool.
 *
 * Build:
 *   cmake --build build --target example_pipeline
 * Run:
 *   ./build/example_pipeline
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/pipeline.h"
#include "loomworks/thread_pool.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <time.h>

#define NUM_PRODUCERS  2
#define NUM_CONSUMERS  4
#define ITEMS_PER_PRODUCER 100
#define POOL_WORKERS     (NUM_CONSUMERS + NUM_PRODUCERS + 4)

/* Portable sub-second sleep */
static void msleep(int ms)
{
    struct timespec ts;
    ts.tv_sec  = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------------ */
/*  Producer task: generates items and submits them to the pipeline    */
/* ------------------------------------------------------------------ */
typedef struct {
    loom_pc_t *pc;
    int        producer_id;
    int        count;
} producer_arg_t;

static void producer_task(void *arg)
{
    producer_arg_t *pa = (producer_arg_t *)arg;

    for (int i = 0; i < pa->count; i++) {
        int *item = (int *)malloc(sizeof(int));
        if (!item) {
            fprintf(stderr, "producer %d: malloc failed\n", pa->producer_id);
            break;
        }
        *item = pa->producer_id * 1000 + i;
        loom_result_t rc = loom_pc_submit(pa->pc, item);
        if (rc != LOOMWORKS_OK) {
            free(item);
            break;
        }
    }
}

/* ------------------------------------------------------------------ */
/*  Consumer task: blocks on loom_pc_take() until shutdown sentinel    */
/* ------------------------------------------------------------------ */
typedef struct {
    loom_pc_t    *pc;
    atomic_int   *total;
    atomic_int   *processed;
} consumer_arg_t;

static void consumer_task(void *arg)
{
    consumer_arg_t *ca = (consumer_arg_t *)arg;
    void           *item;

    while (loom_pc_take(ca->pc, &item) == LOOMWORKS_OK) {
        int *val = (int *)item;
        atomic_fetch_add(ca->total, *val);
        atomic_fetch_add(ca->processed, 1);
        free(val);
    }
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */
int main(void)
{
    printf("=== Pipeline Demo (thread pool) ===\n");

    /* Create pipeline: capacity 32 */
    loom_pc_t *pc = NULL;
    if (loom_pc_create(NUM_CONSUMERS, 32, &pc) != LOOMWORKS_OK) {
        fprintf(stderr, "Failed to create pipeline\n");
        return 1;
    }

    /* Create thread pool: consumers run as pool workers */
    loom_pool_config_t cfg = {
        .worker_count   = (uint32_t)POOL_WORKERS,
        .stack_size     = 0,
        .queue_capacity = 0
    };
    loom_thread_pool_t *pool = NULL;
    if (loom_pool_create(&cfg, &pool) != LOOMWORKS_OK) {
        fprintf(stderr, "Failed to create thread pool\n");
        loom_pc_destroy(&pc);
        return 1;
    }

    atomic_int global_total = ATOMIC_VAR_INIT(0);
    atomic_int processed    = ATOMIC_VAR_INIT(0);
    const int  total_items  = NUM_PRODUCERS * ITEMS_PER_PRODUCER;

    /* Submit consumer tasks (workers block on loom_pc_take) */
    consumer_arg_t consumers[NUM_CONSUMERS];
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        consumers[i] = (consumer_arg_t){ pc, &global_total, &processed };
        loom_pool_submit(pool, consumer_task, &consumers[i], NULL);
    }

    /* Submit producer tasks */
    producer_arg_t producers[NUM_PRODUCERS];
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        producers[i] = (producer_arg_t){ pc, i, ITEMS_PER_PRODUCER };
        loom_pool_submit(pool, producer_task, &producers[i], NULL);
    }

    /* Wait until all items have been consumed.
     * We track both the queue (pending_count) and a processed counter
     * to avoid the race where the queue empties but consumers are
     * still adding to the total. */

    /* Spin-wait with a small yield; safe because this is a demo */
    while ((int)atomic_load(&processed) < total_items
           || loom_pc_pending_count(pc) > 0) {
        msleep(1);
    }
    printf("All %d items submitted and consumed.\n", total_items);

    /* Signal pipeline shutdown — consumers receive NULL sentinel */
    loom_pc_shutdown(pc);

    /* Shut down the pool; joins all consumer tasks */
    loom_pool_shutdown(pool);

    printf("All consumers done.\n");

    /* Verify totals */
    long expected = 0;
    for (int p = 0; p < NUM_PRODUCERS; p++) {
        for (int i = 0; i < ITEMS_PER_PRODUCER; i++) {
            expected += (long)p * 1000 + i;
        }
    }
    int actual = atomic_load(&global_total);
    printf("Expected total: %ld, Actual total: %d\n", expected, actual);

    loom_pc_destroy(&pc);
    loom_pool_destroy(&pool);

    if (actual == (int)expected) {
        printf("PASS: totals match!\n");
        return 0;
    } else {
        printf("FAIL: totals do not match!\n");
        return 1;
    }
}
