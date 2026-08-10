/**
 * @file thread_pool.c
 * @brief Thread pool implementation.
 *
 * A fixed-size worker pool backed by a singly-linked FIFO task queue.
 * Workers block on a condition variable until a task is available or the
 * pool is shut down.  Shutdown is idempotent: calling
 * loom_pool_shutdown() multiple times is safe.
 *
 * Queue capacity enforcement:
 *   When queue_capacity > 0, loom_pool_submit() rejects tasks with
 *   LOOMWORKS_ERR_INVALID once the queue is full.  This is a non-blocking
 *   check — callers must retry or handle the error.
 *
 * Future support:
 *   loom_pool_submit_future() wraps the user function in
 *   future_task_wrapper(), which stores the return value into a
 *   loom_future_t protected by its own mutex+condvar.  The caller
 *   blocks on loom_future_wait() until the worker signals readiness.
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/thread_pool.h"
#include "loomworks/coroutine.h"
#include "loomworks/metrics.h"
#include "thread_pool_internal.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ================================================================
 *  Metrics helper — invoke callback if metrics is attached.
 * ================================================================ */
#define METRICS_CB(pool, event)                                                                    \
    do {                                                                                           \
        if ((pool)->metrics) {                                                                     \
            loom_metric_fn _cb   = ((loom_metrics_t *)(pool)->metrics)->cb;                        \
            void          *_data = ((loom_metrics_t *)(pool)->metrics)->user_data;                 \
            if (_cb) {                                                                             \
                _cb(event, pool, _data);                                                           \
            }                                                                                      \
        }                                                                                          \
    } while (0)

/* ================================================================
 *  Forward declarations
 * ================================================================ */

static loom_result_t pool_init(loom_thread_pool_t *pool);
static void          pool_destroy_internal(loom_thread_pool_t *pool);
static void         *worker_entry(void *arg);
static loom_task_t *
task_create(loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint8_t priority);
static void task_destroy(loom_thread_pool_t *pool, loom_task_t *task);
static void future_task_wrapper(void *arg);
static bool lane_has_priority(loom_thread_pool_t *pool, unsigned max_priority);
static loom_task_t *
dequeue_lowest_priority_unlocked(loom_thread_pool_t *pool, unsigned max_priority);
static loom_task_t *ring_try_dequeue(loom_thread_pool_t *pool);
static bool ring_has_work(loom_thread_pool_t *pool);

/* ================================================================
 *  pool_init — initialise locks, defaults, and worker thread array
 *
 *  worker_count == 0 → auto (sysconf(_SC_NPROCESSORS_ONLN) * 2,
 *  capped at 128).
 *  stack_size == 0   → LOOMWORKS_DEFAULT_STACK_SIZE.
 *  queue_capacity > LOOMWORKS_MAX_QUEUE_CAPACITY → clamped down.
 * ================================================================ */
static void metrics_fire(loom_thread_pool_t *pool, loom_metric_event_t event)
{
    if (!pool) {
        return;
    }
    if (pool->metrics) {
        loom_metrics_fire((loom_metrics_t *)pool->metrics, event);
    }
}

/* Smallest power of two >= v (1 when v == 0). */
static uint64_t next_pow2_u64(uint32_t v)
{
    uint64_t n = 1;
    while (n < v) {
        n <<= 1;
    }
    return n;
}

static loom_result_t pool_init(loom_thread_pool_t *pool)
{
    if (pool->worker_count == 0) {
        long n = sysconf(_SC_NPROCESSORS_ONLN);
        if (n < 1) {
            n = 1;
        }
        if (n > 64) {
            n = 64;
        }
        pool->worker_count = (uint32_t)n * 2;
    }
    if (pool->stack_size == 0) {
        pool->stack_size = LOOMWORKS_DEFAULT_STACK_SIZE;
    }
    if (pool->queue_capacity > LOOMWORKS_MAX_QUEUE_CAPACITY) {
        pool->queue_capacity = LOOMWORKS_MAX_QUEUE_CAPACITY;
    }

    if (pthread_mutex_init(&pool->lock, NULL) != 0) {
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&pool->drain_cond, NULL) != 0) {
        pthread_mutex_destroy(&pool->lock);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (sem_init(&pool->work_sem, 0, 0) != 0) {
        pthread_mutex_destroy(&pool->lock);
        pthread_cond_destroy(&pool->drain_cond);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&pool->space_cond, NULL) != 0) {
        sem_destroy(&pool->work_sem);
        pthread_mutex_destroy(&pool->lock);
        pthread_cond_destroy(&pool->drain_cond);
        return LOOMWORKS_ERR_ALLOC;
    }

    atomic_store_explicit(&pool->shutdown, false, memory_order_relaxed);
    pool->draining = false;
    pool->joined   = false;
    for (int b = 0; b < 256; b++) {
        pool->buckets_head[b] = NULL;
        pool->buckets_tail[b] = NULL;
    }
    for (int w = 0; w < 4; w++) {
        atomic_store_explicit(&pool->nonempty_bits[w], 0, memory_order_relaxed);
    }
    atomic_store_explicit(&pool->queue_len, 0, memory_order_relaxed);
    atomic_store_explicit(&pool->active_workers, 0, memory_order_relaxed);
    pool->metric_cb        = NULL;
    pool->metric_user_data = NULL;
    pool->metrics          = NULL;
    atomic_store_explicit(&pool->next_task_id, 1, memory_order_relaxed);

    /* --- Lock-free ring + cancel index + node pool --- */
    pool->ring_size = (pool->queue_capacity == 0)
                          ? LOOMWORKS_RING_DEFAULT_SLOTS
                          : next_pow2_u64(pool->queue_capacity);
    pool->ring_mask = pool->ring_size - 1;
    pool->ring      = (ring_cell_t *)calloc(pool->ring_size, sizeof(ring_cell_t));
    if (pool->ring != NULL) {
        for (uint64_t i = 0; i < pool->ring_size; i++) {
            atomic_store_explicit(&pool->ring[i].seq, i, memory_order_relaxed);
        }
        pool->cancel_cap  = pool->ring_size * 2;
        pool->cancel_slots = (cancel_slot_t *)calloc(pool->cancel_cap, sizeof(cancel_slot_t));
        if (pool->cancel_slots == NULL) {
            free(pool->ring);
            pool->ring = NULL;
        }
    }
    if (pool->ring == NULL) {
        /* Lane-only fallback: every NORMAL task takes the mutex lane. */
        pool->ring_size  = 0;
        pool->ring_mask  = 0;
        pool->cancel_cap = 0;
    }
    atomic_store_explicit(&pool->ring_head, 0, memory_order_relaxed);
    atomic_store_explicit(&pool->ring_tail, 0, memory_order_relaxed);
    atomic_store_explicit(&pool->ring_count, 0, memory_order_relaxed);
    pool->node_pool     = (loom_task_t *)calloc(LOOMWORKS_NODE_POOL_CAP, sizeof(loom_task_t));
    pool->node_pool_cap = pool->node_pool != NULL ? LOOMWORKS_NODE_POOL_CAP : 0;
    atomic_store_explicit(&pool->node_stack, 0, memory_order_relaxed);

    pool->max_worker_count = pool->worker_count;
    pool->threads          = (pthread_t *)calloc(pool->max_worker_count, sizeof(pthread_t));
    if (!pool->threads) {
        pool_destroy_internal(pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    return LOOMWORKS_OK;
}

/* ================================================================
 *  pool_destroy_internal — free every resource owned by the pool.
 *
 *  Does NOT touch pool->threads beyond freeing the
 *  array; the join loop lives in loom_pool_shutdown().
 *  Queue task nodes are freed one by one.
 * ================================================================ */
static void pool_destroy_internal(loom_thread_pool_t *pool)
{
    if (!pool) {
        return;
    }
    /* Free any tasks still pending across all priority buckets. */
    for (int b = 0; b < 256; b++) {
        loom_task_t *t = pool->buckets_head[b];
        while (t) {
            loom_task_t *n = t->next;
            free(t);
            t = n;
        }
    }
    /* Free lock-free ring structures and the node pool array. */
    free(pool->cancel_slots);
    free(pool->ring);
    free(pool->node_pool);
    pthread_cond_destroy(&pool->drain_cond);
    pthread_cond_destroy(&pool->space_cond);
    sem_destroy(&pool->work_sem);
    pthread_mutex_destroy(&pool->lock);

    /* Clear metrics so no worker accesses freed pool via metric_cb / metrics.
     * The metrics object itself is owned by the caller and destroyed separately. */
    pool->metric_cb        = NULL;
    pool->metric_user_data = NULL;
    pool->metrics          = NULL;

    free(pool->threads);
    free(pool);
}

/* ================================================================
 *  worker_entry — per-worker loop.
 *
 *  Blocks on pool->work_sem while the queue is empty and the pool is not
 *  shutting down.  On wakeup:
 *    1. If shutdown is set and the queue is empty → exit thread.
 *    2. Otherwise dequeue one task (under lock), run it outside the
 *       lock to maximise concurrency.
 * ================================================================ */
typedef struct {
    loom_thread_pool_t *pool;
    uint32_t            index;
} worker_arg_t;

static void *worker_entry(void *arg)
{
    worker_arg_t       *wa   = (worker_arg_t *)arg;
    loom_thread_pool_t *pool = wa->pool;
    uint32_t            idx  = wa->index;
    free(wa);
    while (1) {
        pthread_mutex_lock(&pool->lock);
        /* If resized down, exit when our index is beyond the new count. */
        if (idx >= pool->worker_count && !pool->shutdown) {
            pthread_mutex_unlock(&pool->lock);
            break;
        }
        /* All workers exit once shutdown is set and nothing is pending —
         * including cancelled ring tasks still awaiting a tombstone drain. */
        if (pool->shutdown && pool->queue_len == 0 &&
            atomic_load_explicit(&pool->ring_count, memory_order_relaxed) == 0) {
            pthread_mutex_unlock(&pool->lock);
            break;
        }
        loom_coro_exit();
        loom_task_t *task      = NULL;
        bool         from_ring = false;
        /* Step 1: lane priorities below NORMAL (p < 5) — under lock. */
        if (lane_has_priority(pool, 4)) {
            task = dequeue_lowest_priority_unlocked(pool, 4);
        }
        /* Step 2: the lock-free ring. */
        if (task == NULL && pool->ring != NULL) {
            task      = ring_try_dequeue(pool);
            from_ring = (task != NULL);
        }
        /* Step 3: only when the ring has no work, fall back to p >= 5 lanes. */
        if (task == NULL && (pool->ring == NULL || !ring_has_work(pool))) {
            task = dequeue_lowest_priority_unlocked(pool, 255);
        }
        if (task == NULL) {
            /* No work: wait without holding the lock.  The counting
             * semaphore guarantees no lost wakeup — every successful
             * enqueue posts exactly one token.  Spurious wakeups are
             * harmless: the loop re-checks shutdown and the queue. */
            pthread_mutex_unlock(&pool->lock);
            while (sem_wait(&pool->work_sem) != 0 && errno == EINTR) {
            }
            continue;
        }
        if (task->cancelled) {
            /* Tombstone won: the canceller already accounted queue_len;
             * release the node (and any owned data) and continue. */
            if (task->free_data && task->user_data) {
                free(task->user_data);
            }
            task_destroy(pool, task);
            if (pool->queue_capacity > 0) {
                pthread_cond_signal(&pool->space_cond);
            }
            pthread_mutex_unlock(&pool->lock);
            continue;
        }
        loom_task_fn fn   = task->fn;
        void        *data = task->user_data;
        /* Return the node to the pool under the lock (the free-list is
         * lock-protected); the task function runs outside the lock. */
        task_destroy(pool, task);
        if (from_ring) {
            /* ring_try_dequeue does not touch queue_len — account it now. */
            atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
        }
        /* Space freed: wake a blocked bounded submitter, if any. */
        if (pool->queue_capacity > 0) {
            pthread_cond_signal(&pool->space_cond);
        }
        pthread_mutex_unlock(&pool->lock);
        if (task) {
            atomic_fetch_add(&pool->active_workers, 1);
            metrics_fire(pool, LOOMWORKS_METRIC_STARTED);
            if (pool->metrics) {
                struct timespec ts_start;
                clock_gettime(CLOCK_MONOTONIC, &ts_start);
                fn(data);
                struct timespec ts_end;
                clock_gettime(CLOCK_MONOTONIC, &ts_end);
                uint64_t latency_ns = (uint64_t)(ts_end.tv_sec - ts_start.tv_sec) * 1000000000u +
                                      (uint64_t)(ts_end.tv_nsec - ts_start.tv_nsec);
                loom_metrics_record_latency((loom_metrics_t *)pool->metrics, latency_ns);
            } else {
                fn(data);
            }
            metrics_fire(pool, LOOMWORKS_METRIC_COMPLETED);
            atomic_fetch_sub(&pool->active_workers, 1);
        }
    }
    return NULL;
}

/* --- Node pool: lock-free tagged bounded stack -----------------------------
 * node_stack: low 32 bits = top index, high 32 bits = ABA tag (incremented on
 * every successful CAS).  The pool array is a fixed cap; the stack can never
 * overflow (cap == array cap).  Underflow (empty stack) falls back to malloc,
 * and pushing a malloc'd node frees it — same pooling semantics as before. */

static bool node_from_pool(loom_thread_pool_t *pool, const loom_task_t *node)
{
    if (pool->node_pool == NULL || pool->node_pool_cap == 0)
        return false;
    uintptr_t base = (uintptr_t)pool->node_pool;
    uintptr_t p    = (uintptr_t)node;
    return p >= base && p < base + (uintptr_t)pool->node_pool_cap * sizeof(loom_task_t);
}

static loom_task_t *node_stack_pop(loom_thread_pool_t *pool)
{
    uint64_t stack = atomic_load_explicit(&pool->node_stack, memory_order_relaxed);
    for (;;) {
        uint32_t top = (uint32_t)stack;
        if (top == 0)
            return NULL; /* empty: caller falls back to malloc */
        uint32_t next = top - 1;
        uint64_t updated = ((uint64_t)((uint32_t)(stack >> 32) + 1) << 32) | next;
        if (atomic_compare_exchange_weak_explicit(&pool->node_stack, &stack, updated,
                                                  memory_order_acquire,
                                                  memory_order_relaxed))
            return &pool->node_pool[top - 1];
    }
}

static void node_stack_push(loom_thread_pool_t *pool, loom_task_t *node)
{
    if (node_from_pool(pool, node)) {
        uint64_t stack = atomic_load_explicit(&pool->node_stack, memory_order_relaxed);
        for (;;) {
            uint32_t top = (uint32_t)stack;
            uint32_t next = top + 1;
            uint64_t updated = ((uint64_t)((uint32_t)(stack >> 32) + 1) << 32) | next;
            if (atomic_compare_exchange_weak_explicit(&pool->node_stack, &stack, updated,
                                                      memory_order_release,
                                                      memory_order_relaxed))
                return;
        }
    } else {
        free(node); /* malloc'd node: return to the heap */
    }
}

/* ================================================================
 *  task_create / task_destroy — allocate and free a single task node.
 * ================================================================ */
static loom_task_t *
task_create(loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint8_t priority)
{
    loom_task_t *t = node_stack_pop(pool);
    if (!t) {
        t = (loom_task_t *)malloc(sizeof(*t));
        if (!t) {
            return NULL;
        }
    }
    t->fn        = fn;
    t->user_data = data;
    t->task_id =
        (uint64_t)atomic_fetch_add_explicit(&pool->next_task_id, 1, memory_order_relaxed) + 1;
    t->priority  = priority;
    t->cancelled = false;
    t->free_data = false;
    t->next      = NULL;
    return t;
}

static void task_destroy(loom_thread_pool_t *pool, loom_task_t *t)
{
    if (!t) {
        return;
    }
    if (pool) {
        node_stack_push(pool, t);
    } else {
        free(t);
    }
}

/* ================================================================
 *  Cancel index — lock-free open addressing map (task_id → task).
 *
 *  Lets ring tasks be cancelled without the lane lock.  slot->task_id:
 *  0 = EMPTY, 1 = TOMBSTONE, task_id+1 = occupied.  Task ids start at 2
 *  (task_create does fetch_add(...)+1), so id+1 never collides with
 *  EMPTY or TOMBSTONE.
 * ================================================================ */
static uint64_t cancel_index_hash(uint64_t id, uint64_t cap)
{
    return id & (cap - 1);
}

static void cancel_index_insert(loom_thread_pool_t *pool, loom_task_t *task)
{
    uint64_t cap = pool->cancel_cap;
    if (cap == 0 || pool->cancel_slots == NULL) {
        return;
    }
    uint64_t want = task->task_id + 1;
    uint64_t h    = cancel_index_hash(want, cap);
    for (uint64_t i = 0; i < cap; i++) {
        cancel_slot_t *slot = &pool->cancel_slots[h];
        uint64_t       cur  = atomic_load_explicit(&slot->task_id, memory_order_relaxed);
        if (cur == 0 || cur == 1) { /* EMPTY or TOMBSTONE: reusable */
            slot->task = task;      /* plain stores — visible via release publish */
            slot->data = task->user_data;
            atomic_store_explicit(&slot->task_id, want, memory_order_release);
            return;
        }
        h = (h + 1) & (cap - 1);
    }
    /* Bounded probe: index saturated with occupied slots.  Cannot happen
     * while ring_size live entries < cap and removes tombstone (not empty);
     * if it ever does, the task runs unindexed — cancel-by-id then falls
     * back to the lane walk and reports not-found, but nothing hangs. */
}

/* Clear a dequeued task's index slot.  Runs under pool->lock, and every
 * claim (cancel_by_id / cancel_all) also runs under pool->lock, so this
 * cannot race a claim: either the slot is still occupied (we tombstone it)
 * or it is already a tombstone (the canceller won — the worker skips the
 * node via task->cancelled and does NOT re-account queue_len, the
 * canceller already did).  A slot not found in the index belongs to a
 * spilled task (lane path), which is never inserted.
 *
 * We write TOMBSTONE (1), NOT EMPTY (0): in an open-addressing probe chain
 * an EMPTY hole in front of a live entry makes that entry unreachable for
 * any later probe, so its slot leaks forever.  Under sustained load the
 * collisions are guaranteed (task ids span far more than cap values), the
 * leaked slots accumulate until the index is fully occupied, and every
 * unbounded probe then wraps forever — the CI hang.  TOMBSTONE keeps the
 * chain intact so colliding entries stay reachable. */
static void cancel_index_remove(loom_thread_pool_t *pool, loom_task_t *task)
{
    uint64_t cap = pool->cancel_cap;
    if (cap == 0 || pool->cancel_slots == NULL) {
        return; /* no index (lane-only pool) */
    }
    uint64_t want = task->task_id + 1;
    uint64_t h    = cancel_index_hash(want, cap);
    for (uint64_t i = 0; i < cap; i++) {
        cancel_slot_t *slot = &pool->cancel_slots[h];
        uint64_t       cur  = atomic_load_explicit(&slot->task_id, memory_order_relaxed);
        if (cur == want) {
            atomic_compare_exchange_strong_explicit(&slot->task_id, &cur, 1,
                                                    memory_order_release,
                                                    memory_order_acquire);
            return;
        }
        if (cur == 0) { /* not found: spilled task (lane path), never inserted */
            return;
        }
        h = (h + 1) & (cap - 1);
    }
    /* Bounded probe: entry absent (index fully touched, no EMPTY left to
     * terminate on).  Nothing to clear — spilled/unindexed tasks never
     * occupy a slot. */
}

/* Locate a live ring task by id (used by the cancel paths).  Returns NULL
 * when the id is not currently occupied in the index — the caller then
 * falls back to the lane walk. */
static loom_task_t *cancel_index_find(loom_thread_pool_t *pool, uint64_t task_id)
{
    uint64_t cap = pool->cancel_cap;
    if (cap == 0 || pool->cancel_slots == NULL) {
        return NULL;
    }
    uint64_t want = task_id + 1;
    uint64_t h    = cancel_index_hash(want, cap);
    for (uint64_t i = 0; i < cap; i++) {
        cancel_slot_t *slot = &pool->cancel_slots[h];
        uint64_t       cur  = atomic_load_explicit(&slot->task_id, memory_order_acquire);
        if (cur == want) {
            return slot->task;
        }
        if (cur == 0) { /* not found: already removed by the worker (running) */
            return NULL;
        }
        h = (h + 1) & (cap - 1); /* tombstone: keep probing */
    }
    return NULL; /* bounded probe: absent (index fully touched, no EMPTY left) */
}

/* Steal an occupied slot as a tombstone.  Returns true when this caller won
 * the claim (the task is now cancelled and will be skipped by the worker);
 * false when a worker dequeued it concurrently (task is running or done). */
static bool cancel_index_claim(loom_thread_pool_t *pool, loom_task_t *task)
{
    uint64_t cap = pool->cancel_cap;
    if (cap == 0 || pool->cancel_slots == NULL) {
        return false;
    }
    uint64_t want = task->task_id + 1;
    uint64_t h    = cancel_index_hash(want, cap);
    for (uint64_t i = 0; i < cap; i++) {
        cancel_slot_t *slot = &pool->cancel_slots[h];
        uint64_t       cur  = atomic_load_explicit(&slot->task_id, memory_order_relaxed);
        if (cur == want) { /* occupied → steal it as a tombstone */
            uint64_t tgt = want;
            if (atomic_compare_exchange_strong_explicit(&slot->task_id, &tgt, 1,
                                                        memory_order_release,
                                                        memory_order_acquire)) {
                slot->task = NULL;
                return true;
            }
            return false; /* worker removed it: task is running (or done) */
        }
        if (cur == 0) { /* not found: already removed by the worker (running) */
            return false;
        }
        h = (h + 1) & (cap - 1);
    }
    return false; /* bounded probe: absent (index fully touched, no EMPTY left) */
}

/* ================================================================
 *  Lock-free Vyukov ring — NORMAL-priority fast path.
 *
 *  Canonical sequence numbers (see thread_pool_internal.h):
 *  seq == pos             → empty, available for the producer at pos
 *  seq == pos + 1         → full: task published, consumer may take
 *  seq == pos + ring_size → released by the consumer (producer may reuse)
 * ================================================================ */
static bool ring_try_enqueue(loom_thread_pool_t *pool, loom_task_t *task)
{
    uint64_t tail = atomic_load_explicit(&pool->ring_tail, memory_order_relaxed);
    for (;;) {
        uint64_t pos = tail & pool->ring_mask;
        if (atomic_load_explicit(&pool->ring[pos].seq, memory_order_acquire) != tail) {
            return false; /* full or claimed: do NOT claim (spill instead) */
        }
        uint64_t want = tail;
        if (atomic_compare_exchange_weak_explicit(&pool->ring_tail, &tail, tail + 1,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
            /* position claimed at want */
            atomic_store_explicit(&pool->ring[want & pool->ring_mask].task, task,
                                  memory_order_relaxed);
            cancel_index_insert(pool, task); /* between store and publish */
            atomic_store_explicit(&pool->ring[want & pool->ring_mask].seq, want + 1,
                                  memory_order_release);
            atomic_fetch_add_explicit(&pool->ring_count, 1, memory_order_relaxed);
            return true;
        }
        /* tail moved; retry with the fresh value */
    }
}

static loom_task_t *ring_try_dequeue(loom_thread_pool_t *pool)
{
    uint64_t head = atomic_load_explicit(&pool->ring_head, memory_order_relaxed);
    for (;;) {
        uint64_t pos = head & pool->ring_mask;
        if (atomic_load_explicit(&pool->ring[pos].seq, memory_order_acquire) != head + 1) {
            return NULL; /* empty at this position */
        }
        uint64_t want = head;
        if (atomic_compare_exchange_weak_explicit(&pool->ring_head, &head, head + 1,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
            loom_task_t *task = (loom_task_t *)atomic_load_explicit(
                &pool->ring[want & pool->ring_mask].task, memory_order_relaxed);
            cancel_index_remove(pool, task);
            atomic_store_explicit(&pool->ring[want & pool->ring_mask].seq,
                                  want + pool->ring_size, memory_order_release);
            atomic_fetch_sub_explicit(&pool->ring_count, 1, memory_order_relaxed);
            return task;
        }
    }
}

static bool ring_has_work(loom_thread_pool_t *pool)
{
    uint64_t head = atomic_load_explicit(&pool->ring_head, memory_order_relaxed);
    uint64_t pos  = head & pool->ring_mask;
    return atomic_load_explicit(&pool->ring[pos].seq, memory_order_acquire) == head + 1;
}

/* ================================================================
 *  loom_enqueue_unlocked — append @p task to its priority bucket.
 *
 *  Must be called with pool->lock held.
 * ================================================================ */
void loom_enqueue_unlocked(loom_thread_pool_t *pool, loom_task_t *task)
{
    uint8_t b = task->priority;
    if (pool->buckets_tail[b]) {
        pool->buckets_tail[b]->next = task;
    } else {
        pool->buckets_head[b] = task;
    }
    pool->buckets_tail[b] = task;
    task->next            = NULL;
    atomic_fetch_or_explicit(&pool->nonempty_bits[b / 64], (uint64_t)1u << (b % 64),
                             memory_order_relaxed);
    atomic_fetch_add_explicit(&pool->queue_len, 1, memory_order_relaxed);
}

/* ================================================================
 *  dequeue_lowest_priority_unlocked — remove and return the
 *  lowest-priority pending task at or below @p max_priority
 *  (bucket index == priority; numerically smaller first).
 *
 *  Must be called with pool->lock held.
 *  Returns NULL when no matching task is queued.
 * ================================================================ */
static loom_task_t *
dequeue_lowest_priority_unlocked(loom_thread_pool_t *pool, unsigned max_priority)
{
    /* Find the lowest non-empty bucket via the 256-bit bitmap.
     * Bucket index == priority; numerically smaller runs first. */
    for (int w = 0; w < 4; w++) {
        uint64_t bits = atomic_load_explicit(&pool->nonempty_bits[w], memory_order_relaxed);
        if (bits != 0) {
            int b = w * 64 + __builtin_ctzll(bits);
            if ((unsigned)b > max_priority) {
                return NULL; /* this word and all later words exceed the bound */
            }
            loom_task_t *t        = pool->buckets_head[b];
            pool->buckets_head[b] = t->next;
            if (pool->buckets_head[b] == NULL) {
                pool->buckets_tail[b] = NULL;
                atomic_fetch_and_explicit(&pool->nonempty_bits[w],
                                          ~((uint64_t)1u << (b % 64)),
                                          memory_order_relaxed);
            }
            t->next = NULL;
            atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
            return t;
        }
    }
    return NULL;
}

/* True when any lane bucket with priority <= max_priority is non-empty.
 * Lock-free peek; lets the worker skip the p >= 5 lanes while the ring
 * still has work (strict NORMAL FIFO drain order). */
static bool lane_has_priority(loom_thread_pool_t *pool, unsigned max_priority)
{
    for (int w = 0; w < 4; w++) {
        uint64_t bits = atomic_load_explicit(&pool->nonempty_bits[w], memory_order_acquire);
        if (bits != 0) {
            int b = w * 64 + __builtin_ctzll(bits);
            /* The lowest set bit is the smallest priority in this word;
             * if it exceeds the bound, every other bit does too. */
            return (unsigned)b <= max_priority;
        }
    }
    return false;
}

/* ================================================================
 *  loom_dequeue_unlocked — remove and return the lowest-priority
 *  pending task (any priority).
 *
 *  Must be called with pool->lock held.
 *  Returns NULL when the queue is empty.
 * ================================================================ */
loom_task_t *loom_dequeue_unlocked(loom_thread_pool_t *pool)
{
    return dequeue_lowest_priority_unlocked(pool, 255);
}

/* ================================================================
 *  future_task_wrapper — executed by a worker thread.
 *
 *  Invokes the user-provided function, then stores the result into
 *  the associated loom_future_t under its mutex.  The future's
 *  condition variable is signalled so that loom_future_wait() can
 *  return.
 * ================================================================ */
static void future_task_wrapper(void *arg)
{
    future_task_ctx_t *ctx    = (future_task_ctx_t *)arg;
    loom_future_t     *fut    = ctx->future;
    void              *result = ctx->fn(ctx->data);
    pthread_mutex_lock(&fut->mutex);
    fut->result     = result;
    fut->has_result = true;
    fut->ready      = true;
    pthread_cond_signal(&fut->cond);
    pthread_mutex_unlock(&fut->mutex);
    free(ctx);
}

/* ================================================================
 *  loom_pool_create — allocate and start the pool.
 *
 *  @param config  Configuration (NULL → defaults).  Zero-valued
 *                 fields are filled in by pool_init().
 *  @param pool    Output pointer; set only on success.
 *  @return        LOOMWORKS_OK on success, error code otherwise.
 *
 *  If any pthread_create() fails, already-created worker threads are
 *  joined and all resources are freed before returning the error.
 * ================================================================ */
loom_result_t loom_pool_create(const loom_pool_config_t *config, loom_thread_pool_t **pool)
{
    if (!pool) {
        return LOOMWORKS_ERR_INVALID;
    }
    /* struct loom_thread_pool embeds LOOMWORKS_CACHELINE_ALIGN (aligned(64))
     * members, so it must itself be 64-byte aligned; plain calloc only
     * guarantees max_align_t (16). Use posix_memalign + memset. */
    loom_thread_pool_t *p = NULL;
    if (posix_memalign((void **)&p, 64, sizeof(*p)) != 0) {
        return LOOMWORKS_ERR_ALLOC;
    }
    memset(p, 0, sizeof(*p));
    if (config) {
        p->worker_count   = config->worker_count;
        p->stack_size     = config->stack_size;
        p->queue_capacity = config->queue_capacity;
    }
    loom_result_t rc = pool_init(p);
    if (rc != LOOMWORKS_OK) {
        free(p);
        return rc;
    }
    for (uint32_t i = 0; i < p->worker_count; i++) {
        worker_arg_t *wa = (worker_arg_t *)malloc(sizeof(*wa));
        if (!wa) {
            p->shutdown = true;
            for (uint32_t j = 0; j < i; j++) {
                sem_post(&p->work_sem);
            }
            for (uint32_t j = 0; j < i; j++) {
                pthread_join(p->threads[j], NULL);
            }
            pool_destroy_internal(p);
            return LOOMWORKS_ERR_ALLOC;
        }
        wa->pool  = p;
        wa->index = i;
        int rc2   = pthread_create(&p->threads[i], NULL, worker_entry, wa);
        if (rc2 != 0) {
            fprintf(stderr, "loomworks: pthread_create failed: %s\n", strerror(rc2));
            p->shutdown = true;
            for (uint32_t j = 0; j < i; j++) {
                sem_post(&p->work_sem);
            }
            for (uint32_t j = 0; j < i; j++) {
                pthread_join(p->threads[j], NULL);
            }
            pool_destroy_internal(p);
            return LOOMWORKS_ERR_THREAD;
        }
    }
    *pool = p;
    return LOOMWORKS_OK;
}

/* ================================================================
 *  Submit funnel — single path for every public submit entry point.
 *
 *  NORMAL-priority tasks take the lock-free Vyukov ring when it is
 *  available; everything else goes to the per-priority lane buckets.
 *  On ring overflow the task spills to the NORMAL lane — the ring
 *  never rejects an enqueue that passed the capacity check, a lane
 *  slot is guaranteed (re-checked under the lock for the concurrent
 *  race).  On failure the caller retains ownership of @p data; a
 *  task node created on the failed path is returned to the pool.
 * ================================================================ */
static loom_result_t wait_for_space(loom_thread_pool_t *pool)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 60;
    pthread_mutex_lock(&pool->lock);
    while (pool->queue_len >= pool->queue_capacity && !pool->shutdown) {
        int rc = pthread_cond_timedwait(&pool->space_cond, &pool->lock, &deadline);
        if (rc == ETIMEDOUT || pool->shutdown || pool->draining) {
            pthread_mutex_unlock(&pool->lock);
            return pool->shutdown ? LOOMWORKS_ERR_SHUTDOWN : LOOMWORKS_ERR_TIMEOUT;
        }
    }
    pthread_mutex_unlock(&pool->lock);
    return LOOMWORKS_OK;
}

static loom_result_t enqueue_lane_task(loom_thread_pool_t *pool, loom_task_fn fn, void *data,
                                       uint8_t priority, bool free_data, uint64_t *task_id)
{
    pthread_mutex_lock(&pool->lock);
    if (pool->shutdown || pool->draining) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    if (pool->queue_capacity > 0 && pool->queue_len >= pool->queue_capacity) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    loom_task_t *task = task_create(pool, fn, data, priority);
    if (!task) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_ALLOC;
    }
    task->free_data = free_data;
    loom_enqueue_unlocked(pool, task);
    if (task_id) {
        *task_id = task->task_id;
    }
    sem_post(&pool->work_sem);
    pthread_mutex_unlock(&pool->lock);
    metrics_fire(pool, LOOMWORKS_METRIC_SUBMITTED);
    return LOOMWORKS_OK;
}

static loom_result_t spill_to_normal_lane(loom_thread_pool_t *pool, loom_task_t *task,
                                          uint64_t *task_id)
{
    /* Not visible to workers yet (ring refused it; the lane enqueue below
     * is pending the lock), so reading the id now is race-free. */
    uint64_t id = task->task_id;
    pthread_mutex_lock(&pool->lock);
    if (pool->shutdown || pool->draining) {
        pthread_mutex_unlock(&pool->lock);
        task_destroy(pool, task);
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    if (pool->queue_capacity != 0 &&
        atomic_load_explicit(&pool->queue_len, memory_order_relaxed) >= pool->queue_capacity) {
        pthread_mutex_unlock(&pool->lock);
        task_destroy(pool, task);
        return LOOMWORKS_ERR_INVALID; /* exact capacity: no slot */
    }
    loom_enqueue_unlocked(pool, task);
    pthread_mutex_unlock(&pool->lock);
    if (task_id) {
        *task_id = id;
    }
    sem_post(&pool->work_sem);
    metrics_fire(pool, LOOMWORKS_METRIC_SUBMITTED);
    return LOOMWORKS_OK;
}

static loom_result_t enqueue_ring_task(loom_thread_pool_t *pool, loom_task_fn fn, void *data,
                                       uint8_t priority, bool free_data, uint64_t *task_id)
{
    loom_task_t *task = task_create(pool, fn, data, priority);
    if (task == NULL) {
        return LOOMWORKS_ERR_ALLOC;
    }
    task->free_data = free_data;
    /* Read the id before publishing: a worker may run and free the node as
     * soon as it is visible in the ring. */
    uint64_t id = task->task_id;
    atomic_fetch_add_explicit(&pool->queue_len, 1, memory_order_relaxed);
    if (ring_try_enqueue(pool, task)) {
        if (task_id) {
            *task_id = id;
        }
        sem_post(&pool->work_sem);
        metrics_fire(pool, LOOMWORKS_METRIC_SUBMITTED);
        return LOOMWORKS_OK;
    }
    /* Ring full → spill to the NORMAL lane (capacity was pre-checked; a
     * lane slot is guaranteed — re-checked under the lock for the race). */
    atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
    return spill_to_normal_lane(pool, task, task_id);
}

static loom_result_t enqueue_task(loom_thread_pool_t *pool, loom_task_fn fn, void *data,
                                  uint8_t priority, bool free_data, uint64_t *task_id, bool block)
{
    if (atomic_load_explicit(&pool->shutdown, memory_order_relaxed)) {
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    if (pool->queue_capacity != 0 &&
        atomic_load_explicit(&pool->queue_len, memory_order_relaxed) >= pool->queue_capacity) {
        if (block) {
            loom_result_t rc = wait_for_space(pool);
            if (rc != LOOMWORKS_OK) {
                return rc;
            }
        } else {
            return LOOMWORKS_ERR_INVALID;
        }
    }
    if (priority == LOOMWORKS_PRIORITY_NORMAL && pool->ring != NULL) {
        return enqueue_ring_task(pool, fn, data, priority, free_data, task_id);
    }
    return enqueue_lane_task(pool, fn, data, priority, free_data, task_id);
}

/* ================================================================
 *  loom_pool_submit — enqueue a fire-and-forget task.
 *
 *  Rejects the task (LOOMWORKS_ERR_INVALID) when the queue is at
 *  capacity (queue_capacity > 0).  Rejects with LOOMWORKS_ERR_SHUTDOWN
 *  when the pool is shutting down or has already shut down.
 *
 *  @return  LOOMWORKS_OK on success.
 * ================================================================ */
loom_result_t
loom_pool_submit(loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint64_t *task_id)
{
    if (!pool || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    return enqueue_task(pool, fn, data, LOOMWORKS_PRIORITY_NORMAL, false, task_id, false);
}

/* ================================================================
 *  loom_pool_submit_blocking — enqueue with backpressure.
 *
 *  Blocks until there is queue space or the pool shuts down.
 *  Times out after 60 s with LOOMWORKS_ERR_TIMEOUT.
 * ================================================================ */
loom_result_t
loom_pool_submit_blocking(loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint64_t *task_id)
{
    if (!pool || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    return enqueue_task(pool, fn, data, LOOMWORKS_PRIORITY_NORMAL, false, task_id, true);
}

/* ================================================================
 *  loom_pool_submit_future — enqueue a task whose result can be
 *  retrieved via a loom_future_t.
 *
 *  A fresh future and its associated mutex/condvar are allocated
 *  before the task is submitted.  If submission fails the future is
 *  freed and NULL is stored in @p future.
 *
 *  @return  LOOMWORKS_OK on success.
 * ================================================================ */
loom_result_t loom_pool_submit_future(loom_thread_pool_t *pool,
                                      loom_task_fn_result fn,
                                      void               *data,
                                      loom_future_t     **future,
                                      uint64_t           *task_id)
{
    if (!pool || !fn || !future) {
        return LOOMWORKS_ERR_INVALID;
    }
    loom_future_t *fut = (loom_future_t *)calloc(1, sizeof(*fut));
    if (!fut) {
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_mutex_init(&fut->mutex, NULL) != 0) {
        free(fut);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&fut->cond, NULL) != 0) {
        pthread_mutex_destroy(&fut->mutex);
        free(fut);
        return LOOMWORKS_ERR_ALLOC;
    }
    fut->ready             = false;
    fut->has_result        = false;
    fut->result            = NULL;
    future_task_ctx_t *ctx = (future_task_ctx_t *)malloc(sizeof(*ctx));
    if (!ctx) {
        pthread_mutex_destroy(&fut->mutex);
        pthread_cond_destroy(&fut->cond);
        free(fut);
        return LOOMWORKS_ERR_ALLOC;
    }
    ctx->fn     = fn;
    ctx->data   = data;
    ctx->future = fut;
    /* Ownership of ctx transfers to the pool only on success; on failure
     * we free both the future and the context. */
    loom_result_t rc = enqueue_task(pool, future_task_wrapper, ctx, LOOMWORKS_PRIORITY_NORMAL,
                                    true, task_id, false);
    if (rc != LOOMWORKS_OK) {
        pthread_mutex_destroy(&fut->mutex);
        pthread_cond_destroy(&fut->cond);
        free(fut);
        free(ctx);
        return rc;
    }
    *future = fut;
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_pool_submit_priority — enqueue a fire-and-forget task with priority.
 *
 *  Same as loom_pool_submit() but accepts an explicit priority level.
 *  Lower priority values are executed first.
 * ================================================================ */
loom_result_t loom_pool_submit_priority(
    loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint8_t priority, uint64_t *task_id)
{
    if (!pool || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    return enqueue_task(pool, fn, data, priority, false, task_id, false);
}

/* ================================================================
 *  loom_pool_submit_blocking — enqueue with backpressure.
 *
 *  Blocks until there is queue space or the pool shuts down.
 *  Times out after 60 s with LOOMWORKS_ERR_TIMEOUT.
 * ================================================================ */
/* ================================================================
 *  loom_pool_submit_future_priority — enqueue a priority result task.
 *
 *  Same as loom_pool_submit_future() but accepts an explicit priority.
 * ================================================================ */
loom_result_t loom_pool_submit_future_priority(loom_thread_pool_t *pool,
                                               loom_task_fn_result fn,
                                               void               *data,
                                               uint8_t             priority,
                                               loom_future_t     **future,
                                               uint64_t           *task_id)
{
    if (!pool || !fn || !future) {
        return LOOMWORKS_ERR_INVALID;
    }
    loom_future_t *fut = (loom_future_t *)calloc(1, sizeof(*fut));
    if (!fut) {
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_mutex_init(&fut->mutex, NULL) != 0) {
        free(fut);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&fut->cond, NULL) != 0) {
        pthread_mutex_destroy(&fut->mutex);
        free(fut);
        return LOOMWORKS_ERR_ALLOC;
    }
    fut->ready             = false;
    fut->has_result        = false;
    fut->result            = NULL;
    future_task_ctx_t *ctx = (future_task_ctx_t *)malloc(sizeof(*ctx));
    if (!ctx) {
        pthread_mutex_destroy(&fut->mutex);
        pthread_cond_destroy(&fut->cond);
        free(fut);
        return LOOMWORKS_ERR_ALLOC;
    }
    ctx->fn     = fn;
    ctx->data   = data;
    ctx->future = fut;
    /* Ownership of ctx transfers to the pool only on success; on failure
     * we free both the future and the context. */
    loom_result_t rc =
        enqueue_task(pool, future_task_wrapper, ctx, priority, true, task_id, false);
    if (rc != LOOMWORKS_OK) {
        pthread_mutex_destroy(&fut->mutex);
        pthread_cond_destroy(&fut->cond);
        free(fut);
        free(ctx);
        return rc;
    }
    *future = fut;
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_future_wait — block until the future's task has completed.
 *
 *  The caller receives the result pointer through @p result (which may
 *  be NULL if the caller does not need it).  Ownership of the result
 *  pointer transfers to the caller; the pool does not free it.
 *
 *  @return  LOOMWORKS_OK on success.
 * ================================================================ */
loom_result_t loom_future_wait(loom_future_t *future, void **result)
{
    if (!future) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&future->mutex);
    while (!future->ready) {
        pthread_cond_wait(&future->cond, &future->mutex);
    }
    if (result) {
        *result = future->result;
    }
    pthread_mutex_unlock(&future->mutex);
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_future_wait_timeout
 *
 *  Same semantics as loom_future_wait() but returns LOOMWORKS_ERR_TIMEOUT
 *  if the timeout expires before the task completes.
 *
 *  @param future   The future handle.
 *  @param result   Output pointer for the result (may be NULL).
 *  @param deadline Absolute time (timespec) to wait until.
 *  @return         LOOMWORKS_OK on success, LOOMWORKS_ERR_TIMEOUT on expiry.
 * ================================================================ */
loom_result_t
loom_future_wait_timeout(loom_future_t *future, void **result, const struct timespec *deadline)
{
    if (!future || !deadline) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&future->mutex);
    if (future->ready) {
        if (result) {
            *result = future->result;
        }
        pthread_mutex_unlock(&future->mutex);
        return LOOMWORKS_OK;
    }
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    long remaining_ns = (long)(deadline->tv_sec - now.tv_sec) * 1000000000L +
                        (long)(deadline->tv_nsec - now.tv_nsec);
    if (remaining_ns <= 0) {
        pthread_mutex_unlock(&future->mutex);
        return LOOMWORKS_ERR_TIMEOUT;
    }
    int rc = pthread_cond_timedwait(&future->cond, &future->mutex, deadline);
    if (rc == ETIMEDOUT) {
        pthread_mutex_unlock(&future->mutex);
        return LOOMWORKS_ERR_TIMEOUT;
    }
    if (result) {
        *result = future->result;
    }
    pthread_mutex_unlock(&future->mutex);
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_future_destroy — free a future's synchronisation primitives.
 *
 *  The result pointer stored in the future is NOT freed; the caller
 *  is responsible for freeing it after loom_future_wait().
 * ================================================================ */
void loom_future_destroy(loom_future_t *future)
{
    if (!future) {
        return;
    }
    pthread_mutex_lock(&future->mutex);
    future->result     = NULL;
    future->has_result = false;
    pthread_mutex_unlock(&future->mutex);
    pthread_mutex_destroy(&future->mutex);
    pthread_cond_destroy(&future->cond);
    free(future);
}

/* ================================================================
 *  loom_pool_shutdown — signal all workers to exit and wait for them.
 *
 *  Idempotent: a second call returns immediately because the workers
 *  have already been joined (see pool->joined).
 *
 *  Sequence:
 *    1. Set shutdown=true and post work_sem once per worker to wake all
 *       sleeping workers.
 *    2. pthread_join each worker thread.
 *    3. Set draining=false and broadcast on drain_cond so that any
 *       external waiter (e.g. pending_count observers) is notified.
 * ================================================================ */
void loom_pool_shutdown(loom_thread_pool_t *pool)
{
    if (!pool) {
        return;
    }
    pthread_mutex_lock(&pool->lock);
    /* Already joined → nothing to do. */
    if (pool->joined) {
        pthread_mutex_unlock(&pool->lock);
        return;
    }
    pool->shutdown = true;
    pool->draining = true;
    /* Wake every sleeping worker; extras are harmless. */
    for (uint32_t i = 0; i < pool->worker_count; i++) {
        sem_post(&pool->work_sem);
    }
    pthread_mutex_unlock(&pool->lock);
    for (uint32_t i = 0; i < pool->max_worker_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    pthread_mutex_lock(&pool->lock);
    pool->draining = false;
    pool->joined   = true;
    pthread_cond_broadcast(&pool->drain_cond);
    pthread_mutex_unlock(&pool->lock);
}

/* ================================================================
 *  loom_pool_cancel -- cancel a single pending task by user_data pointer.
 *
 *  Searches the queue for a task with matching user_data.  If found
 *  before the task starts executing, removes and frees it.
 *
 *  @return  LOOMWORKS_OK if cancelled, LOOMWORKS_ERR_INVALID if not found.
 * ================================================================ */
loom_result_t loom_pool_cancel(loom_thread_pool_t *pool, void *data)
{
    if (!pool || !data) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&pool->lock);
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    /* Ring fast path: scan the cancel index for a task whose user_data
     * matches and claim its slot as a tombstone — the worker then skips
     * the node and frees it; we account queue_len here. */
    if (pool->cancel_slots != NULL && pool->cancel_cap != 0) {
        for (uint64_t i = 0; i < pool->cancel_cap; i++) {
            cancel_slot_t *slot = &pool->cancel_slots[i];
            uint64_t       cur  = atomic_load_explicit(&slot->task_id, memory_order_acquire);
            if (cur <= 1) { /* EMPTY or TOMBSTONE */
                continue;
            }
            loom_task_t *task = slot->task;
            if (task == NULL || task->cancelled || task->user_data != data) {
                continue;
            }
            task->cancelled = true;
            if (cancel_index_claim(pool, task)) {
                atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
                pthread_mutex_unlock(&pool->lock);
                metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
                return LOOMWORKS_OK;
            }
        }
    }
    /* Walk every bucket; the match set is expected to be tiny. */
    for (int b = 0; b < 256; b++) {
        loom_task_t *prev = NULL;
        loom_task_t *cur  = pool->buckets_head[b];
        while (cur) {
            if (cur->user_data == data && !cur->cancelled) {
                if (prev) {
                    prev->next = cur->next;
                } else {
                    pool->buckets_head[b] = cur->next;
                }
                if (cur == pool->buckets_tail[b]) {
                    pool->buckets_tail[b] = prev;
                }
                if (pool->buckets_head[b] == NULL) {
                    atomic_fetch_and_explicit(&pool->nonempty_bits[b / 64],
                                              ~((uint64_t)1u << (b % 64)),
                                              memory_order_relaxed);
                }
                atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
                loom_task_t *to_free = cur;
                if (to_free->free_data && to_free->user_data) {
                    free(to_free->user_data);
                }
                task_destroy(pool, to_free);
                pthread_mutex_unlock(&pool->lock);
                metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
                return LOOMWORKS_OK;
            }
            prev = cur;
            cur  = cur->next;
        }
    }
    pthread_mutex_unlock(&pool->lock);
    return LOOMWORKS_ERR_INVALID;
}

/* ================================================================
 *  loom_pool_cancel_by_id -- cancel a single pending task by its unique ID.
 *
 *  Searches the queue for a task whose task_id matches @p task_id.
 *  If found before the task starts executing, removes and frees it.
 *  This is safer than loom_pool_cancel() when multiple tasks share
 *  the same user_data pointer.
 *
 *  @return  LOOMWORKS_OK if cancelled, LOOMWORKS_ERR_INVALID if not found.
 * ================================================================ */
loom_result_t loom_pool_cancel_by_id(loom_thread_pool_t *pool, uint64_t task_id)
{
    if (!pool) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&pool->lock);
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    /* Ring fast path: probe the lock-free cancel index first. */
    loom_task_t *task = cancel_index_find(pool, task_id);
    if (task != NULL) {
        if (task->cancelled) {
            pthread_mutex_unlock(&pool->lock);
            return LOOMWORKS_ERR_INVALID;
        }
        task->cancelled = true; /* published to the worker via the tombstone release */
        if (cancel_index_claim(pool, task)) {
            atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
            pthread_mutex_unlock(&pool->lock);
            metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
            return LOOMWORKS_OK;
        }
        /* Worker dequeued it concurrently: task is running (or done). */
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    for (int b = 0; b < 256; b++) {
        loom_task_t *prev = NULL;
        loom_task_t *cur  = pool->buckets_head[b];
        while (cur) {
            if (cur->task_id == task_id && !cur->cancelled) {
                if (prev) {
                    prev->next = cur->next;
                } else {
                    pool->buckets_head[b] = cur->next;
                }
                if (cur == pool->buckets_tail[b]) {
                    pool->buckets_tail[b] = prev;
                }
                if (pool->buckets_head[b] == NULL) {
                    atomic_fetch_and_explicit(&pool->nonempty_bits[b / 64],
                                              ~((uint64_t)1u << (b % 64)),
                                              memory_order_relaxed);
                }
                atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
                loom_task_t *to_free = cur;
                if (to_free->free_data && to_free->user_data) {
                    free(to_free->user_data);
                }
                task_destroy(pool, to_free);
                pthread_mutex_unlock(&pool->lock);
                metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
                return LOOMWORKS_OK;
            }
            prev = cur;
            cur  = cur->next;
        }
    }
    pthread_mutex_unlock(&pool->lock);
    return LOOMWORKS_ERR_INVALID;
}

/* ================================================================
 *  loom_pool_cancel_all -- cancel every task still in the queue.
 *
 *  Tasks already being executed are NOT interrupted.  Only queued
 *  tasks are removed and freed.
 *
 *  @param count  Output pointer for the number of cancelled tasks (may be NULL).
 * ================================================================ */
void loom_pool_cancel_all(loom_thread_pool_t *pool, uint32_t *count)
{
    if (!pool) {
        return;
    }
    uint32_t cancelled = 0;
    pthread_mutex_lock(&pool->lock);
    if (!pool->shutdown) {
        /* Ring tasks first: claim every occupied cancel-index slot as a
         * tombstone.  The worker later drains and frees each node. */
        if (pool->cancel_slots != NULL && pool->cancel_cap != 0) {
            for (uint64_t i = 0; i < pool->cancel_cap; i++) {
                cancel_slot_t *slot = &pool->cancel_slots[i];
                uint64_t       cur  = atomic_load_explicit(&slot->task_id,
                                                           memory_order_acquire);
                if (cur <= 1) { /* EMPTY or TOMBSTONE */
                    continue;
                }
                loom_task_t *task = slot->task;
                if (task == NULL || task->cancelled) {
                    continue;
                }
                task->cancelled = true;
                uint64_t tgt    = cur;
                if (atomic_compare_exchange_strong_explicit(&slot->task_id, &tgt, 1,
                                                            memory_order_release,
                                                            memory_order_acquire)) {
                    slot->task = NULL;
                    atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
                    cancelled++;
                    metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
                }
            }
        }
        for (int b = 0; b < 256; b++) {
            loom_task_t *cur      = pool->buckets_head[b];
            pool->buckets_head[b] = NULL;
            pool->buckets_tail[b] = NULL;
            while (cur) {
                loom_task_t *next = cur->next;
                if (cur->free_data && cur->user_data) {
                    free(cur->user_data);
                }
                task_destroy(pool, cur);
                cur = next;
                cancelled++;
                atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
                metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
            }
        }
        for (int w = 0; w < 4; w++) {
            atomic_store_explicit(&pool->nonempty_bits[w], 0, memory_order_relaxed);
        }
    }
    pthread_mutex_unlock(&pool->lock);
    if (count) {
        *count = cancelled;
    }
}

/* ================================================================
 *  loom_pool_set_metrics_callback — register/unregister metrics callback.
 * ================================================================ */
void loom_pool_set_metrics_callback(loom_thread_pool_t *pool, loom_metric_fn cb, void *user_data)
{
    if (!pool) {
        return;
    }
    if (cb == NULL) {
        /* Unregister only — do not clobber an existing callback. */
        pool->metric_user_data = NULL;
        return;
    }
    union {
        loom_metric_fn f;
        void (*p)(void *, void *, void *);
    } u;
    u.f                    = cb;
    pool->metric_cb        = u.p;
    pool->metric_user_data = user_data;
}

/* ================================================================
 *  loom_pool_set_metrics — attach/detach a metrics collector.
 * ================================================================ */
void loom_pool_set_metrics(loom_thread_pool_t *pool, loom_metrics_t *metrics)
{
    if (!pool) {
        return;
    }
    pool->metrics = metrics;
}

/* ================================================================
 *  loom_pool_destroy — free the pool and set the handle to NULL.
 *
 *  Must be called after loom_pool_shutdown().  Calling this on a
 *  non-shutdown pool will leak worker threads.
 *
 *  Safe to call with a NULL handle or a handle already set to NULL.
 * ================================================================ */
void loom_pool_destroy(loom_thread_pool_t **pool)
{
    if (!pool || !*pool) {
        return;
    }
    pool_destroy_internal(*pool);
    *pool = NULL;
}

/* ================================================================
 *  loom_pool_resize — dynamically adjust the number of worker threads.
 *
 *  Growing: new threads are created and appended to the pool.
 *  Shrinking: excess threads (beyond the new count) exit when idle.
 *            Workers currently executing tasks are NOT interrupted.
 *
 *  Must not be called after loom_pool_shutdown().
 *
 *  @param pool     The pool handle.
 *  @param count    New number of worker threads.
 *  @return         LOOMWORKS_OK on success, error code otherwise.
 * ================================================================ */
loom_result_t loom_pool_resize(loom_thread_pool_t *pool, uint32_t count)
{
    if (!pool) {
        return LOOMWORKS_ERR_INVALID;
    }
    if (count == pool->worker_count) {
        return LOOMWORKS_OK;
    }
    pthread_mutex_lock(&pool->lock);
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    if (count > pool->max_worker_count) {
        /* Need to grow the threads array. */
        pthread_t *new_threads = (pthread_t *)realloc(pool->threads, count * sizeof(pthread_t));
        if (!new_threads) {
            pthread_mutex_unlock(&pool->lock);
            return LOOMWORKS_ERR_ALLOC;
        }
        pool->threads          = new_threads;
        pool->max_worker_count = count;
    }
    uint32_t old_count = pool->worker_count;
    pool->worker_count = count;
    if (count > old_count) {
        /* Start new worker threads. */
        for (uint32_t i = old_count; i < count; i++) {
            worker_arg_t *wa = (worker_arg_t *)malloc(sizeof(*wa));
            if (!wa) {
                /* Roll back: restore old count and break. */
                pool->worker_count = old_count;
                pthread_mutex_unlock(&pool->lock);
                return LOOMWORKS_ERR_ALLOC;
            }
            wa->pool  = pool;
            wa->index = i;
            int rc    = pthread_create(&pool->threads[i], NULL, worker_entry, wa);
            if (rc != 0) {
                free(wa);
                pool->worker_count = old_count;
                pthread_mutex_unlock(&pool->lock);
                fprintf(stderr, "loomworks: pthread_create failed: %s\n", strerror(rc));
                return LOOMWORKS_ERR_THREAD;
            }
        }
    }
    /* Wake idle workers so they can re-check worker_count. */
    for (uint32_t i = 0; i < pool->worker_count; i++) {
        sem_post(&pool->work_sem);
    }
    pthread_mutex_unlock(&pool->lock);
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_pool_worker_count — return the number of worker threads.
 *
 *  Returns 0 when @p pool is NULL (safe null-pointer query).
 * ================================================================ */
uint32_t loom_pool_worker_count(const loom_thread_pool_t *pool)
{
    if (!pool) {
        return 0;
    }
    return pool->worker_count;
}

/* ================================================================
 *  loom_pool_pending_count — return the number of tasks in the queue.
 *
 *  Acquires the lock briefly for a consistent snapshot.  Returns 0
 *  when @p pool is NULL.
 * ================================================================ */
uint32_t loom_pool_pending_count(const loom_thread_pool_t *pool)
{
    if (!pool) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&pool->lock);
    uint32_t n = pool->queue_len;
    pthread_mutex_unlock((pthread_mutex_t *)&pool->lock);
    return n;
}

/* ================================================================
 *  loom_pool_active_count — number of workers executing a task.
 *
 *  Lock-free (relaxed atomic load); may lag briefly.  Returns 0
 *  when @p pool is NULL.
 * ================================================================ */
uint32_t loom_pool_active_count(const loom_thread_pool_t *pool)
{
    if (!pool) {
        return 0;
    }
    return atomic_load(&pool->active_workers);
}

/* ================================================================
 *  loom_pool_idle_count — number of idle workers (worker_count - active).
 *
 *  Returns 0 when @p pool is NULL.
 * ================================================================ */
uint32_t loom_pool_idle_count(const loom_thread_pool_t *pool)
{
    if (!pool) {
        return 0;
    }
    uint32_t wc     = pool->worker_count;
    uint32_t active = atomic_load(&pool->active_workers);
    return active >= wc ? 0 : wc - active;
}

/* ================================================================
 *  loom_pool_utilization — active / worker_count in [0.0, 1.0].
 *
 *  Returns 0.0 when @p pool is NULL or has no workers.
 * ================================================================ */
double loom_pool_utilization(const loom_thread_pool_t *pool)
{
    if (!pool || pool->worker_count == 0) {
        return 0.0;
    }
    return (double)atomic_load(&pool->active_workers) / (double)pool->worker_count;
}

/* ================================================================
 *  loom_pool_broadcast — wake all workers blocked on the pool wait.
 *
 *  Safe to call from any thread; acquires the lock briefly.
 * ================================================================ */
void loom_pool_broadcast(loom_thread_pool_t *pool)
{
    if (!pool) {
        return;
    }
    pthread_mutex_lock(&pool->lock);
    for (uint32_t i = 0; i < pool->worker_count; i++) {
        sem_post(&pool->work_sem);
    }
    pthread_mutex_unlock(&pool->lock);
}
