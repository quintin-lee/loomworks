/**
 * @file pipeline.c
 * @brief Producer-consumer pipeline implementation.
 *
 * A bounded FIFO queue protected by a mutex+condvar.
 * Producers call loom_pc_submit() to enqueue (blocks when full).
 * Consumers call loom_pc_take() to dequeue (blocks when empty).
 * loom_pc_shutdown() signals no more items; consumers receive NULL.
 *
 * When worker_count > 0 at creation, an internal loom_thread_pool_t
 * is created to run consumer tasks.
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/pipeline.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct pc_item {
    void           *data;
    struct pc_item *next;
} pc_item_t;

struct loom_pc {
    loom_thread_pool_t *pool;
    uint32_t            capacity;
    pthread_mutex_t     lock;
    pc_item_t          *head;
    pc_item_t          *tail;
    uint32_t            len;
    pthread_cond_t      cond;
    bool                shutdown;
    _Atomic uint64_t    submitted;
    _Atomic uint64_t    taken;
};

static pc_item_t *pc_item_create(void *data)
{
    pc_item_t *item = (pc_item_t *)malloc(sizeof(*item));
    if (!item) return NULL;
    item->data = data;
    item->next = NULL;
    return item;
}

static void pc_item_destroy(pc_item_t *item)
{
    if (item) free(item);
}

/* Consumer pool task: blocks on loom_pc_take() until shutdown */
static void consumer_pool_task(void *arg)
{
    loom_pc_t *pc = (loom_pc_t *)arg;
    void      *item = NULL;
    while (loom_pc_take(pc, &item) == LOOMWORKS_OK) {
        /* Discard item — internal consumers just keep workers alive */
        free(item);
    }
}

loom_result_t loom_pc_create(uint32_t worker_count, uint32_t capacity, loom_pc_t **pc)
{
    if (!pc) return LOOMWORKS_ERR_INVALID;
    loom_pc_t *p = (loom_pc_t *)calloc(1, sizeof(*p));
    if (!p) return LOOMWORKS_ERR_ALLOC;
    p->pool        = NULL;
    p->capacity    = capacity;
    p->head        = NULL;
    p->tail        = NULL;
    p->len         = 0;
    p->shutdown    = false;
    atomic_store(&p->submitted, 0);
    atomic_store(&p->taken,     0);
    if (pthread_mutex_init(&p->lock, NULL) != 0) { free(p); return LOOMWORKS_ERR_ALLOC; }
    if (pthread_cond_init(&p->cond, NULL) != 0)  { pthread_mutex_destroy(&p->lock); free(p); return LOOMWORKS_ERR_ALLOC; }

    if (worker_count > 0) {
        loom_pool_config_t cfg = {
            .worker_count   = worker_count,
            .stack_size     = 0,
            .queue_capacity = 0
        };
        if (loom_pool_create(&cfg, &p->pool) != LOOMWORKS_OK) {
            pthread_cond_destroy(&p->cond);
            pthread_mutex_destroy(&p->lock);
            free(p);
            return LOOMWORKS_ERR_THREAD;
        }
        for (uint32_t i = 0; i < worker_count; i++) {
            loom_pool_submit(p->pool, consumer_pool_task, p, NULL);
        }
    }

    *pc = p;
    return LOOMWORKS_OK;
}

void loom_pc_destroy(loom_pc_t **pc)
{
    if (!pc || !*pc) return;
    loom_pc_t *p = *pc;

    if (p->pool) {
        /* Signal shutdown BEFORE joining the internal pool so consumer
         * tasks blocked in loom_pc_take() wake and exit. Without this,
         * loom_pool_shutdown() waits forever on workers waiting for
         * an item that will never arrive. */
        pthread_mutex_lock(&p->lock);
        p->shutdown = true;
        pthread_cond_broadcast(&p->cond);
        pthread_mutex_unlock(&p->lock);

        /* Shut down internal pool — joins all consumer tasks so no
         * thread can access the queue while we drain it. */
        loom_pool_shutdown(p->pool);
        loom_pool_destroy(&p->pool);
    }

    /* Now safe to drain: no consumer tasks are running.
     * Free pc_item_t nodes; data pointers are owned by consumers
     * (who freed them) or by the caller (leaked here — acceptable
     * for a demo; production code should track ownership). */
    pc_item_t *item = p->head;
    while (item) {
        pc_item_t *next = item->next;
        pc_item_destroy(item);
        item = next;
    }

    pthread_cond_destroy(&p->cond);
    pthread_mutex_destroy(&p->lock);
    free(p);
    *pc = NULL;
}

loom_result_t loom_pc_submit(loom_pc_t *pc, void *item)
{
    if (!pc) return LOOMWORKS_ERR_INVALID;
    pthread_mutex_lock(&pc->lock);
    if (pc->shutdown) { pthread_mutex_unlock(&pc->lock); return LOOMWORKS_ERR_SHUTDOWN; }
    pc_item_t *pi = pc_item_create(item);
    if (!pi) { pthread_mutex_unlock(&pc->lock); return LOOMWORKS_ERR_ALLOC; }
    if (pc->capacity > 0 && pc->len >= pc->capacity) {
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += 60;
        while (pc->len >= pc->capacity && !pc->shutdown) {
            int rc = pthread_cond_timedwait(&pc->cond, &pc->lock, &deadline);
            if (rc == ETIMEDOUT || pc->shutdown) {
                pc_item_destroy(pi);
                pthread_mutex_unlock(&pc->lock);
                return pc->shutdown ? LOOMWORKS_ERR_SHUTDOWN : LOOMWORKS_ERR_TIMEOUT;
            }
        }
        if (pc->shutdown) {
            pc_item_destroy(pi);
            pthread_mutex_unlock(&pc->lock);
            return LOOMWORKS_ERR_SHUTDOWN;
        }
    }
    if (pc->tail) { pc->tail->next = pi; } else { pc->head = pi; }
    pc->tail = pi;
    pc->len++;
    pthread_cond_broadcast(&pc->cond);
    pthread_mutex_unlock(&pc->lock);
    atomic_fetch_add(&pc->submitted, 1);
    return LOOMWORKS_OK;
}

loom_result_t loom_pc_take(loom_pc_t *pc, void **item)
{
    if (!pc || !item) return LOOMWORKS_ERR_INVALID;
    *item = NULL;
    pthread_mutex_lock(&pc->lock);
    while (pc->head == NULL) {
        if (pc->shutdown) {
            pthread_mutex_unlock(&pc->lock);
            return LOOMWORKS_ERR_SHUTDOWN;
        }
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        ts.tv_nsec += 100000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&pc->cond, &pc->lock, &ts);
    }
    pc_item_t *pi  = pc->head;
    pc->head = pi->next;
    if (pc->head == NULL) pc->tail = NULL;
    pc->len--;
    pthread_cond_broadcast(&pc->cond);
    pthread_mutex_unlock(&pc->lock);
    *item = pi->data;
    pc_item_destroy(pi);
    atomic_fetch_add(&pc->taken, 1);
    return LOOMWORKS_OK;
}

void loom_pc_shutdown(loom_pc_t *pc)
{
    if (!pc) return;
    pthread_mutex_lock(&pc->lock);
    pc->shutdown = true;
    pthread_cond_broadcast(&pc->cond);
    pthread_mutex_unlock(&pc->lock);
    if (pc->pool) {
        loom_pool_broadcast(pc->pool);
    }
}

uint32_t loom_pc_pending_count(const loom_pc_t *pc)
{
    if (!pc) return 0;
    pthread_mutex_lock((pthread_mutex_t *)&pc->lock);
    uint32_t n = pc->len;
    pthread_mutex_unlock((pthread_mutex_t *)&pc->lock);
    return n;
}

uint64_t loom_pc_submitted_count(const loom_pc_t *pc)
{
    if (!pc) return 0;
    return atomic_load(&pc->submitted);
}

uint64_t loom_pc_taken_count(const loom_pc_t *pc)
{
    if (!pc) return 0;
    return atomic_load(&pc->taken);
}
