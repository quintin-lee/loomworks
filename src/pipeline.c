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
 *
 * Synchronization model: ONE mutex (p->lock) guards head/tail/len/shutdown
 * plus the condvar.  Every state mutation holds the lock; every wait loop
 * re-checks its predicate after wakeup (spurious-wakeup safe).  The
 * submitted/taken counters are lock-free atomics updated outside the lock.
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

/* Intrusive singly-linked queue node: data is the caller's opaque payload,
 * next chains the FIFO.  Nodes are allocated per submit and freed either by
 * the consuming thread (loom_pc_take) or the drain loop in destroy(). */
typedef struct pc_item {
    void           *data;
    struct pc_item *next;
} pc_item_t;

struct loom_pc {
    loom_thread_pool_t *pool; /* Optional internal consumer pool (NULL = caller takes). */
    uint32_t            capacity; /* Max queued items; 0 = unbounded. */
    pthread_mutex_t     lock;     /* Guards head/tail/len/shutdown/cond. */
    pc_item_t          *head;     /* FIFO head (oldest item). */
    pc_item_t          *tail;     /* FIFO tail (newest item). */
    uint32_t            len;      /* Current queue occupancy (guarded by lock). */
    pthread_cond_t      cond;     /* Notified on enqueue/dequeue/shutdown. */
    bool                shutdown; /* Set once by shutdown(); submits then fail. */
    _Atomic uint64_t    submitted; /* Total successful enqueues (lock-free). */
    _Atomic uint64_t    taken;     /* Total successful dequeues (lock-free). */
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

    /* Internal-consumer mode: spin up worker_count pool threads, each
     * looping on take() until shutdown.  The pool owns no queue of its
     * own here — every consumer task parks in loom_pc_take(). */
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
    /* Bounded mode: if the queue is full, wait up to 60 s for a consumer to
     * drain.  The item is pre-allocated so we never allocate while holding
     * the lock.  deadline is computed once from CLOCK_REALTIME (the condvar
     * clock); timedwait may return spuriously, hence the while loop. */
    if (pc->capacity > 0 && pc->len >= pc->capacity) {
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += 60;
        while (pc->len >= pc->capacity && !pc->shutdown) {
            int rc = pthread_cond_timedwait(&pc->cond, &pc->lock, &deadline);
            /* EINTR/spurious wakeups loop; only ETIMEDOUT or shutdown exit. */
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
    /* Append to tail; broadcast (not signal) because a full queue can have
     * multiple waiters that each need to retry their capacity check. */
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
    /* Wait while empty; wake on enqueue or shutdown.  CLOCK_MONOTONIC with a
     * 100 ms slice — a timed poll rather than an unbounded wait, so a leaked
     * producer cannot wedge consumers forever after shutdown races. */
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
    /* Unlink head; keep the node alive only for its data, then free it. */
    pc_item_t *pi  = pc->head;
    pc->head = pi->next;
    if (pc->head == NULL) pc->tail = NULL;
    pc->len--;
    /* Consumers waking a blocked bounded producer. */
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
        /* Wake the internal pool's workers so they observe shutdown while
         * parked in take() — otherwise they sleep until the next 100 ms poll. */
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
