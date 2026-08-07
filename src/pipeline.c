/**
 * @file pipeline.c
 * @brief Producer-consumer pipeline implementation.
 *
 * A bounded FIFO queue protected by a mutex+condvar.
 * Producers call loom_pc_submit() to enqueue (blocks when full).
 * Consumers call loom_pc_take() to dequeue (blocks when empty).
 * loom_pc_shutdown() signals no more items; consumers receive NULL.
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

loom_result_t loom_pc_create(uint32_t worker_count, uint32_t capacity, loom_pc_t **pc)
{
    (void)worker_count;
    if (!pc) return LOOMWORKS_ERR_INVALID;
    loom_pc_t *p = (loom_pc_t *)calloc(1, sizeof(*p));
    if (!p) return LOOMWORKS_ERR_ALLOC;
    p->capacity   = capacity;
    p->head       = NULL;
    p->tail       = NULL;
    p->len        = 0;
    p->shutdown   = false;
    atomic_store(&p->submitted, 0);
    atomic_store(&p->taken,     0);
    if (pthread_mutex_init(&p->lock, NULL) != 0) { free(p); return LOOMWORKS_ERR_ALLOC; }
    if (pthread_cond_init(&p->cond, NULL) != 0)  { pthread_mutex_destroy(&p->lock); free(p); return LOOMWORKS_ERR_ALLOC; }
    *pc = p;
    return LOOMWORKS_OK;
}

void loom_pc_destroy(loom_pc_t **pc)
{
    if (!pc || !*pc) return;
    loom_pc_t *p = *pc;
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

/*
 *  loom_pc_take: retry loop with cond_timedwait.
 *
 *  Uses cond_timedwait with a 100 ms timeout to recover from the
 *  missed-broadcast race: if shutdown() broadcasts before a consumer
 *  enters cond_wait, the timed-wait loop re-checks shutdown on the
 *  next iteration and returns LOOMWORKS_ERR_SHUTDOWN.
 */
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
        ts.tv_nsec += 100000000L; /* 100 ms */
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&pc->cond, &pc->lock, &ts);
        /* Loop back: re-checks head==NULL and shutdown on every
         * iteration so a missed broadcast is recovered within 100 ms. */
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
}

uint32_t loom_pc_pending_count(const loom_pc_t *pc)
{
    if (!pc) return 0;
    pthread_mutex_lock((pthread_mutex_t *)&pc->lock);
    uint32_t n = pc->len;
    pthread_mutex_unlock((pthread_mutex_t *)&pc->lock);
    return n;
}
