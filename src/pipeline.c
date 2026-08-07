/**
 * @file pipeline.c
 * @brief Producer-consumer pipeline implementation.
 *
 * Internally backed by a loom_thread_pool_t with a task queue.
 * Each item submitted by loom_pc_submit() is wrapped in a task
 * and enqueued.  Each loom_pc_take() call registers a consumer
 * waiting on a condvar; when an item is enqueued the condvar is
 * signalled and the consumer wakes to receive the item.
 *
 * Shutdown flow:
 *   1. loom_pc_shutdown() sets the shutdown flag and broadcasts
 *      on the condvar to wake all waiting consumers.
 *   2. Each consumer that wakes sees shutdown == true and returns
 *      LOOMWORKS_ERR_SHUTDOWN with *item = NULL.
 *   3. Consumers loop until they receive the NULL sentinel.
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/pipeline.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================
 *  Internal structures
 * ================================================================ */

/**
 * @brief One item in the pipeline queue.
 */
typedef struct pc_item {
    void           *data; /**< Opaque item pointer. */
    struct pc_item *next; /**< Next item in the FIFO queue. */
} pc_item_t;

/**
 * @brief Per-consumer wait context — one per loom_pc_take() call.
 *
 * Consumers block on this condvar until an item is available or
 * the pipeline is shut down.
 */
typedef struct pc_waiter {
    void            **item_out; /**< Output pointer filled by loom_pc_take(). */
    bool              done;     /**< true once the item has been delivered. */
    pthread_cond_t    cond;
    pthread_mutex_t   lock;
    struct pc_waiter *next;
} pc_waiter_t;

/**
 * @brief Pipeline — bounded producer-consumer channel.
 */
struct loom_pc {
    loom_thread_pool_t *pool;     /**< Backing thread pool. */
    uint32_t            capacity; /**< 0 = unbounded. */

    /* Queue state — protected by queue_lock */
    pthread_mutex_t queue_lock;
    pc_item_t      *queue_head;
    pc_item_t      *queue_tail;
    uint32_t        queue_len;
    pthread_cond_t  cond; /**< Signalled when item enqueued or shutdown. */

    /* Waiter list — protected by queue_lock */
    pc_waiter_t *waiters;

    /* Shutdown flag — protected by queue_lock */
    bool shutdown;

    /* Metrics */
    _Atomic uint64_t submitted;
    _Atomic uint64_t taken;
};

/* ================================================================
 *  Helpers
 * ================================================================ */
static pc_item_t *pc_item_create(void *data)
{
    pc_item_t *item = (pc_item_t *)malloc(sizeof(*item));
    if (!item) {
        return NULL;
    }
    item->data = data;
    item->next = NULL;
    return item;
}

static void pc_item_destroy(pc_item_t *item)
{
    if (item) {
        free(item);
    }
}

static pc_waiter_t *pc_waiter_create(void **item_out)
{
    pc_waiter_t *w = (pc_waiter_t *)malloc(sizeof(*w));
    if (!w) {
        return NULL;
    }
    w->item_out = item_out;
    w->done     = false;
    w->next     = NULL;
    pthread_mutex_init(&w->lock, NULL);
    pthread_cond_init(&w->cond, NULL);
    return w;
}

static void pc_waiter_destroy(pc_waiter_t *w)
{
    if (!w) {
        return;
    }
    pthread_cond_destroy(&w->cond);
    pthread_mutex_destroy(&w->lock);
    free(w);
}

/* ================================================================
 *  loom_pc_create
 * ================================================================ */
loom_result_t loom_pc_create(uint32_t worker_count, uint32_t capacity, loom_pc_t **pc)
{
    if (!pc) {
        return LOOMWORKS_ERR_INVALID;
    }
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = worker_count, .queue_capacity = 0};
    loom_result_t       rc   = loom_pool_create(&cfg, &pool);
    if (rc != LOOMWORKS_OK) {
        return rc;
    }

    loom_pc_t *p = (loom_pc_t *)calloc(1, sizeof(*p));
    if (!p) {
        loom_pool_destroy(&pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    p->pool       = pool;
    p->capacity   = capacity;
    p->queue_head = NULL;
    p->queue_tail = NULL;
    p->queue_len  = 0;
    p->waiters    = NULL;
    p->shutdown   = false;
    atomic_store(&p->submitted, 0);
    atomic_store(&p->taken, 0);

    if (pthread_mutex_init(&p->queue_lock, NULL) != 0) {
        loom_pool_destroy(&pool);
        free(p);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&p->cond, NULL) != 0) {
        pthread_mutex_destroy(&p->queue_lock);
        loom_pool_destroy(&pool);
        free(p);
        return LOOMWORKS_ERR_ALLOC;
    }
    *pc = p;
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_pc_destroy
 * ================================================================ */
void loom_pc_destroy(loom_pc_t **pc)
{
    if (!pc || !*pc) {
        return;
    }
    loom_pc_t *p = *pc;

    /* Drain remaining items in the queue */
    pc_item_t *item = p->queue_head;
    while (item) {
        pc_item_t *next = item->next;
        pc_item_destroy(item);
        item = next;
    }

    /* Free remaining waiters */
    pc_waiter_t *w = p->waiters;
    while (w) {
        pc_waiter_t *next = w->next;
        pc_waiter_destroy(w);
        w = next;
    }

    pthread_cond_destroy(&p->cond);
    pthread_mutex_destroy(&p->queue_lock);
    /* Shutdown the backing pool before destroying it,
     * so worker threads exit cleanly instead of hanging. */
    loom_pool_shutdown(p->pool);
    loom_pool_destroy(&p->pool);
    free(p);
    *pc = NULL;
}

/* ================================================================
 *  Internal: enqueue an item or signal a waiting consumer.
 *
 *  Must be called with p->queue_lock held.
 * ================================================================ */
static void pc_enqueue_unlocked(loom_pc_t *p, pc_item_t *item)
{
    if (p->waiters != NULL) {
        /* There is a waiter — deliver the item directly */
        pc_waiter_t *w = p->waiters;
        p->waiters     = w->next;
        pthread_mutex_lock(&w->lock);
        w->item_out[0] = item->data;
        w->done        = true;
        pthread_mutex_unlock(&w->lock);
        pthread_cond_signal(&w->cond);
        pc_item_destroy(item);
        return;
    }
    /* No waiter — append to queue */
    if (p->queue_tail) {
        p->queue_tail->next = item;
    } else {
        p->queue_head = item;
    }
    p->queue_tail = item;
    p->queue_len++;
}

/* ================================================================
 *  loom_pc_submit
 * ================================================================ */
loom_result_t loom_pc_submit(loom_pc_t *pc, void *item)
{
    if (!pc) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&pc->queue_lock);
    if (pc->shutdown) {
        pthread_mutex_unlock(&pc->queue_lock);
        return LOOMWORKS_ERR_SHUTDOWN;
    }

    pc_item_t *pi = pc_item_create(item);
    if (!pi) {
        pthread_mutex_unlock(&pc->queue_lock);
        return LOOMWORKS_ERR_ALLOC;
    }

    if (pc->capacity > 0 && pc->queue_len >= pc->capacity) {
        /* Bounded queue full — wait for space or shutdown */
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += 60; /* 60s timeout */
        while (pc->queue_len >= pc->capacity && !pc->shutdown) {
            int rc = pthread_cond_timedwait(&pc->cond, &pc->queue_lock, &deadline);
            if (rc == ETIMEDOUT || pc->shutdown) {
                pc_item_destroy(pi);
                pthread_mutex_unlock(&pc->queue_lock);
                return pc->shutdown ? LOOMWORKS_ERR_SHUTDOWN : LOOMWORKS_ERR_TIMEOUT;
            }
        }
        if (pc->shutdown) {
            pc_item_destroy(pi);
            pthread_mutex_unlock(&pc->queue_lock);
            return LOOMWORKS_ERR_SHUTDOWN;
        }
    }

    pc_enqueue_unlocked(pc, pi);
    pthread_cond_broadcast(&pc->cond);
    pthread_mutex_unlock(&pc->queue_lock);
    atomic_fetch_add(&pc->submitted, 1);
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_pc_take
 * ================================================================ */
loom_result_t loom_pc_take(loom_pc_t *pc, void **item)
{
    if (!pc || !item) {
        return LOOMWORKS_ERR_INVALID;
    }
    *item = NULL;

    pthread_mutex_lock(&pc->queue_lock);

    if (pc->shutdown && pc->queue_len == 0) {
        /* Shutdown with nothing left — sentinel */
        pthread_mutex_unlock(&pc->queue_lock);
        return LOOMWORKS_ERR_SHUTDOWN;
    }

    if (pc->queue_head != NULL) {
        /* Item available immediately */
        pc_item_t *pi  = pc->queue_head;
        pc->queue_head = pi->next;
        if (pc->queue_head == NULL) {
            pc->queue_tail = NULL;
        }
        pc->queue_len--;
        pthread_mutex_unlock(&pc->queue_lock);
        *item = pi->data;
        pc_item_destroy(pi);
        atomic_fetch_add(&pc->taken, 1);
        return LOOMWORKS_OK;
    }

    /* Queue empty — register as waiter */
    pc_waiter_t *w = pc_waiter_create(item);
    if (!w) {
        pthread_mutex_unlock(&pc->queue_lock);
        return LOOMWORKS_ERR_ALLOC;
    }
    w->next     = pc->waiters;
    pc->waiters = w;
    pthread_mutex_unlock(&pc->queue_lock);

    /* Wait for delivery */
    pthread_mutex_lock(&w->lock);
    /* Re-check shutdown: it may have been set between queue_lock unlock
     * (above) and our own lock acquisition.  Without this, the consumer
     * misses the sentinel and blocks forever. */
    if (pc->shutdown && pc->queue_len == 0) {
        w->done        = true;
        w->item_out[0] = NULL;
        pthread_cond_signal(&w->cond);
        pthread_mutex_unlock(&w->lock);
        pc_waiter_destroy(w);
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    while (!w->done) {
        pthread_cond_wait(&w->cond, &w->lock);
    }
    pthread_mutex_unlock(&w->lock);
    pc_waiter_destroy(w);

    if (*item == NULL && pc->shutdown) {
        /* Sentinel — no more items */
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    atomic_fetch_add(&pc->taken, 1);
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_pc_shutdown
 * ================================================================ */
void loom_pc_shutdown(loom_pc_t *pc)
{
    if (!pc) {
        return;
    }
    pthread_mutex_lock(&pc->queue_lock);
    pc->shutdown = true;

    /* Wake all waiters so they can drain and exit */
    pc_waiter_t *w = pc->waiters;
    while (w) {
        pthread_mutex_lock(&w->lock);
        w->done = true;
        pthread_cond_signal(&w->cond);
        pthread_mutex_unlock(&w->lock);
        w = w->next;
    }
    pc->waiters = NULL;
    /* Also wake the backing pool's workers so they see shutdown and exit */
    if (pc->pool) {
        /* Set pool shutdown flag and broadcast to wake all workers */
        loom_pool_broadcast(pc->pool);
    }
    pthread_cond_broadcast(&pc->cond);
    pthread_mutex_unlock(&pc->queue_lock);
}

/* ================================================================
 *  loom_pc_pending_count
 * ================================================================ */
uint32_t loom_pc_pending_count(const loom_pc_t *pc)
{
    if (!pc) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&pc->queue_lock);
    uint32_t n = pc->queue_len;
    pthread_mutex_unlock((pthread_mutex_t *)&pc->queue_lock);
    return n;
}
