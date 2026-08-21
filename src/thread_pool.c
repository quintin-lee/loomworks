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
#include "portability.h" /* must precede system headers: owns _GNU_SOURCE */
#include "loomworks/thread_pool.h"
#include "coroutine_internal.h"
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

/* Set by worker_entry; lets task-group code detect a self-deadlock when a
 * worker waits on its own pool's group. */
static _Thread_local loom_thread_pool_t *g_current_pool = NULL;

static loom_result_t pool_init(loom_thread_pool_t *pool);
static void          pool_destroy_internal(loom_thread_pool_t *pool);
static void         *worker_entry(void *arg);
static loom_task_t *
task_create(loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint8_t priority);
void                 task_destroy(loom_thread_pool_t *pool, loom_task_t *task);
static void          future_task_wrapper(void *arg);
static bool          lane_has_priority(loom_thread_pool_t *pool, unsigned max_priority);
static loom_task_t  *dequeue_lowest_priority_unlocked(loom_thread_pool_t *pool,
                                                      unsigned            max_priority);
static bool          ring_has_work(loom_thread_pool_t *pool);
size_t               ring_bulk_try_dequeue(loom_thread_pool_t *pool, loom_task_t **out, size_t max);
static void          cancel_index_remove(loom_thread_pool_t *pool, loom_task_t *task);
static void          future_mark_cancelled(future_task_ctx_t *ctx);
static void         *timer_thread_fn(void *arg);
static loom_result_t ensure_timer_thread(loom_thread_pool_t *pool);
static void          coro_sleep_reg_hook(void *ctx, uint64_t task_id, int64_t deadline_ns);

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

/* Mark a cancelled future terminal under its mutex. Called with pool->lock
 * held (lock order: pool->lock -> future->mutex), so waiters wake up and
 * loom_future_wait() can return LOOMWORKS_ERR_CANCELLED instead of hanging
 * forever on a task that will never run. */
static void future_mark_cancelled(future_task_ctx_t *ctx)
{
    loom_future_t *fut = ctx->future;
    pthread_mutex_lock(&fut->mutex);
    fut->cancelled  = true;
    fut->ready      = true;
    fut->has_result = false;
    pthread_cond_broadcast(&fut->cond);
    pthread_mutex_unlock(&fut->mutex);
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

/* CLOCK_MONOTONIC is immune to wall-clock jumps; all timeout waits in this
 * module pair a monotonic deadline with a monotonic-clocked condvar. */
static pthread_once_t     g_condattr_once = PTHREAD_ONCE_INIT;
static pthread_condattr_t g_condattr_mono;

static void init_monotonic_condattr(void)
{
    pthread_condattr_init(&g_condattr_mono);
    pthread_condattr_setclock(&g_condattr_mono, CLOCK_MONOTONIC);
}

/* Allocate the work-stealing deques array with the alignment the struct
 * requires.  loom_work_deque_t embeds _Alignas(64) members (bottom/top),
 * so the whole struct needs 64-byte alignment; calloc()/realloc() only
 * guarantee max_align_t (16 bytes), which UBSan flags as misaligned
 * member access.  posix_memalign is used instead of aligned_alloc for
 * the macOS 10.12 platform floor (aligned_alloc is 10.15+). */
static loom_work_deque_t *deques_alloc(size_t count)
{
    loom_work_deque_t *p = NULL;
    if (posix_memalign(
            (void **)&p, _Alignof(loom_work_deque_t), count * sizeof(loom_work_deque_t)) != 0) {
        return NULL;
    }
    memset(p, 0, count * sizeof(loom_work_deque_t));
    return p;
}

/* Free the slot arrays of deques [from, to) and NULL them.  Used to roll
 * back a partially-grown deques array when a later allocation in
 * loom_pool_resize fails: max_worker_count stays unchanged, so the next
 * successful resize would re-enter the grow path and memset the tail to
 * zero — orphaning those committed slot arrays (a 12288-byte leak). */
static void rollback_deques_tail(loom_thread_pool_t *pool, uint32_t from, uint32_t to)
{
    if (pool->deques == NULL) {
        return;
    }
    for (uint32_t i = from; i < to; i++) {
        free((void *)pool->deques[i].slots);
        pool->deques[i].slots = NULL;
    }
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
    pthread_once(&g_condattr_once, init_monotonic_condattr);
    if (pthread_cond_init(&pool->space_cond, &g_condattr_mono) != 0) {
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
    pool->ring_size = (pool->queue_capacity == 0) ? LOOMWORKS_RING_DEFAULT_SLOTS
                                                  : next_pow2_u64(pool->queue_capacity);
    pool->ring_mask = pool->ring_size - 1;
    pool->ring      = (ring_cell_t *)calloc(pool->ring_size, sizeof(ring_cell_t));
    if (pool->ring != NULL) {
        for (uint64_t i = 0; i < pool->ring_size; i++) {
            atomic_store_explicit(&pool->ring[i].seq, i, memory_order_relaxed);
        }
        /* Cancel index capacity must cover every task that can hold an
         * index entry: ring slots + all per-worker deques (a task's entry
         * now lives from submit until its run boundary). */
        size_t deque_slots = (size_t)pool->worker_count * LOOMWORKS_DEQUE_CAPACITY;
        size_t need        = pool->ring_size + deque_slots;
        pool->cancel_cap   = 1;
        while (pool->cancel_cap < need) {
            pool->cancel_cap <<= 1;
        }
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

    /* --- Work-stealing deques (one Chase-Lev deque per worker slot) --- */
    pool->deques = deques_alloc(pool->max_worker_count);
    if (pool->deques != NULL) {
        bool ok = true;
        for (uint32_t i = 0; i < pool->max_worker_count; i++) {
            pool->deques[i].capacity = LOOMWORKS_DEQUE_CAPACITY;
            pool->deques[i].mask     = LOOMWORKS_DEQUE_CAPACITY - 1;
            pool->deques[i].bottom   = 0;
            atomic_store_explicit(&pool->deques[i].top, 0, memory_order_relaxed);
            atomic_store_explicit(&pool->deques[i].len, 0, memory_order_relaxed);
            pool->deques[i].slots =
                (loom_task_t **)calloc(LOOMWORKS_DEQUE_CAPACITY, sizeof(loom_task_t *));
            if (pool->deques[i].slots == NULL) {
                ok = false;
                break;
            }
        }
        if (!ok) {
            for (uint32_t i = 0; i < pool->max_worker_count; i++) {
                free((void *)pool->deques[i].slots);
            }
            free(pool->deques);
            pool->deques = NULL; /* lane-only mode */
        }
    }
    atomic_store_explicit(&pool->deque_total, 0, memory_order_relaxed);

    /* Per-worker coroutine ready FIFO + timer heap for sleeping coroutines.
     * The timer heap is lazily allocated by the timer thread.  On failure,
     * roll back what was initialised before pool_destroy_internal() runs:
     * that function only tears down the coro/timer locks when coro_ready
     * is non-NULL, which here means every lock below was initialised. */
    pool->coro_ready =
        (struct loom_coro_ready **)calloc(pool->max_worker_count, sizeof(*pool->coro_ready));
    if (!pool->coro_ready) {
        pool_destroy_internal(pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_mutex_init(&pool->coro_lock, NULL) != 0) {
        free((void *)pool->coro_ready);
        pool->coro_ready = NULL;
        pool_destroy_internal(pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_mutex_init(&pool->timer_lock, NULL) != 0) {
        pthread_mutex_destroy(&pool->coro_lock);
        free((void *)pool->coro_ready);
        pool->coro_ready = NULL;
        pool_destroy_internal(pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (sem_init(&pool->timer_sem, 0, 0) != 0) {
        pthread_mutex_destroy(&pool->coro_lock);
        pthread_mutex_destroy(&pool->timer_lock);
        free((void *)pool->coro_ready);
        pool->coro_ready = NULL;
        pool_destroy_internal(pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    pool->timer_heap         = NULL;
    pool->timer_len          = 0;
    pool->timer_cap          = 0;
    pool->timer_created      = false;
    pool->timer_thread_alive = false;

    pool->threads = (pthread_t *)calloc(pool->max_worker_count, sizeof(pthread_t));
    if (!pool->threads) {
        pool_destroy_internal(pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    pool->thread_alive = (_Atomic bool *)calloc(pool->max_worker_count, sizeof(_Atomic bool));
    if (!pool->thread_alive) {
        pool_destroy_internal(pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    pool->thread_clean_exit = (_Atomic bool *)calloc(pool->max_worker_count, sizeof(_Atomic bool));
    if (!pool->thread_clean_exit) {
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

    /* Free work-stealing deques and their per-deque slot arrays. */
    if (pool->deques != NULL) {
        for (uint32_t i = 0; i < pool->max_worker_count; i++) {
            free((void *)pool->deques[i].slots);
        }
        free(pool->deques);
    }

    /* Drain per-worker coroutine ready FIFOs and tear down the timer heap.
     * coro_ready non-NULL implies coro_lock/timer_lock/timer_sem were all
     * initialised (see pool_init rollback). */
    if (pool->coro_ready != NULL) {
        for (uint32_t i = 0; i < pool->max_worker_count; i++) {
            struct loom_coro_ready *r = pool->coro_ready[i];
            while (r) {
                struct loom_coro_ready *n = r->next;
                free(r);
                r = n;
            }
        }
        free((void *)pool->coro_ready);
        pthread_mutex_destroy(&pool->coro_lock);
        pthread_mutex_destroy(&pool->timer_lock);
        sem_destroy(&pool->timer_sem);
    }
    free(pool->timer_heap);

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
    free(pool->thread_alive);
    free(pool->thread_clean_exit);
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

/* --- Coroutine timer thread ---------------------------------------------
 * Wakes sleeping coroutines exactly once their deadline passes.  It never
 * resumes a coroutine itself (affinity is preserved): it only moves the
 * task node from the timer heap into the owner worker's ready FIFO and
 * posts a work token.  The owner worker picks it up in Step 0. */
static void *timer_thread_fn(void *arg)
{
    loom_thread_pool_t *pool = (loom_thread_pool_t *)arg;
    /* Drain-until-empty: keep running after timer_thread_alive goes false so
     * shutdown still wakes every sleeping coroutine (their owners then resume
     * and finish them).  timer_len is read under timer_lock below. */
    for (;;) {
        struct timespec abs;
        clock_gettime(CLOCK_MONOTONIC, &abs);
        int64_t now = (int64_t)abs.tv_sec * 1000000000 + (int64_t)abs.tv_nsec;
        /* Sleep until the earliest deadline, or one second if the heap is
         * empty (polling fallback — a push may race our peek). */
        int64_t deadline = 0;
        bool    has      = false;
        bool    alive    = atomic_load_explicit(&pool->timer_thread_alive, memory_order_acquire);
        pthread_mutex_lock(&pool->timer_lock);
        if (pool->timer_len > 0) {
            has      = true;
            deadline = pool->timer_heap[0].deadline_ns;
        }
        bool empty = (pool->timer_len == 0);
        pthread_mutex_unlock(&pool->timer_lock);
        if (!alive && empty) {
            break;
        }
        if (has && deadline > now) {
            int64_t wait_ns = deadline - now;
            abs.tv_sec += wait_ns / 1000000000;
            abs.tv_nsec = (long)(wait_ns % 1000000000);
            if (abs.tv_nsec >= 1000000000) {
                abs.tv_sec += 1;
                abs.tv_nsec -= 1000000000;
            }
        } else {
            abs.tv_sec += 1;
        }
        /* sem_timedwait returns EINTR on signals; retry unless shutting
         * down.  A spurious timeout just re-checks the heap. */
        while (sem_timedwait(&pool->timer_sem, &abs) != 0 && errno == EINTR) {
            if (!atomic_load_explicit(&pool->timer_thread_alive, memory_order_acquire)) {
                break;
            }
        }
        /* Drain every entry whose deadline has passed. */
        clock_gettime(CLOCK_MONOTONIC, &abs);
        now = (int64_t)abs.tv_sec * 1000000000 + (int64_t)abs.tv_nsec;
        for (;;) {
            pthread_mutex_lock(&pool->timer_lock);
            bool due = (pool->timer_len > 0) && (pool->timer_heap[0].deadline_ns <= now);
            if (!due) {
                pthread_mutex_unlock(&pool->timer_lock);
                break;
            }
            loom_timer_entry_t e;
            loom_timer_pop_due(pool, &e);
            struct loom_coro_ready *rn =
                (struct loom_coro_ready *)calloc(1, sizeof(struct loom_coro_ready));
            if (rn == NULL) {
                /* OOM: re-arm the entry so the next pass retries it.  A
                 * sleeping coroutine must never be lost silently. */
                loom_timer_push(pool, e);
                pthread_mutex_unlock(&pool->timer_lock);
                sem_post(&pool->timer_sem);
                break;
            }
            rn->coro    = (struct loom_coroutine *)e.coro_ctx;
            rn->task_id = e.task_id;
            rn->task    = (loom_task_t *)e.task;
            rn->next    = NULL;
            if (e.worker_idx < pool->max_worker_count) {
                /* Push into the ready FIFO while still holding timer_lock:
                 * a shrinking worker's exit check reads timer_len under
                 * timer_lock and then the ready FIFO; if the ready node
                 * landed between those two reads (after pop_due released
                 * timer_lock) the worker could exit with a runnable
                 * coroutine stranded in its FIFO.  One critical section
                 * makes pop+push atomic w.r.t. that exit check. */
                pthread_mutex_lock(&pool->coro_lock);
                struct loom_coro_ready **tail = &pool->coro_ready[e.worker_idx];
                while (*tail != NULL) {
                    tail = &(*tail)->next;
                }
                *tail = rn;
                pthread_mutex_unlock(&pool->coro_lock);
                sem_post(&pool->work_sem);
            } else {
                free(rn); /* worker slot vanished after resize-down */
            }
            pthread_mutex_unlock(&pool->timer_lock);
        }
    }
    return NULL;
}

/* Lazily start the pool's timer thread on first coroutine submission. */
static loom_result_t ensure_timer_thread(loom_thread_pool_t *pool)
{
    pthread_mutex_lock(&pool->timer_lock);
    if (pool->timer_created) {
        pthread_mutex_unlock(&pool->timer_lock);
        return LOOMWORKS_OK;
    }
    atomic_store_explicit(&pool->timer_thread_alive, true, memory_order_release);
    if (pthread_create(&pool->timer_thread, NULL, timer_thread_fn, pool) != 0) {
        atomic_store_explicit(&pool->timer_thread_alive, false, memory_order_release);
        pthread_mutex_unlock(&pool->timer_lock);
        return LOOMWORKS_ERR_ALLOC;
    }
    pool->timer_created = true;
    pthread_mutex_unlock(&pool->timer_lock);
    return LOOMWORKS_OK;
}

/* Register a sleeping pool coroutine with the timer heap.  Invoked from
 * inside the coroutine itself (via loom_coro_sleep_until); ctx is the
 * coroutine, whose sleep_reg_ctx points back at the pool. */
static void coro_sleep_reg_hook(void *ctx, uint64_t task_id, int64_t deadline_ns)
{
    struct loom_coroutine *coro = (struct loom_coroutine *)ctx;
    loom_thread_pool_t    *pool = (loom_thread_pool_t *)coro->sleep_reg_ctx;
    if (!pool) {
        return;
    }
    loom_timer_entry_t e;
    e.deadline_ns = deadline_ns;
    e.task_id     = task_id;
    e.worker_idx  = coro->worker_idx;
    e.coro_ctx    = coro;
    e.task        = coro->task_node;
    pthread_mutex_lock(&pool->timer_lock);
    loom_timer_push(pool, e);
    pthread_mutex_unlock(&pool->timer_lock);
    sem_post(&pool->timer_sem);
}

static void *worker_entry(void *arg)
{
    worker_arg_t       *wa   = (worker_arg_t *)arg;
    loom_thread_pool_t *pool = wa->pool;
    uint32_t            idx  = wa->index;
    free(wa);
    g_current_pool = pool;
    while (1) {
        pthread_mutex_lock(&pool->lock);
        /* Step C0: resume this worker's ready coroutine FIFO (head first).
         * Runs before every other step so sleeping/suspended coroutines make
         * progress even while shutdown is draining.  The coroutine body may
         * call pool APIs, so resume must never run under pool->lock. */
        struct loom_coro_ready *head = NULL;
        pthread_mutex_lock(&pool->coro_lock);
        if (pool->coro_ready != NULL && pool->coro_ready[idx] != NULL) {
            head                  = pool->coro_ready[idx];
            pool->coro_ready[idx] = head->next;
            head->next            = NULL;
        }
        pthread_mutex_unlock(&pool->coro_lock);
        if (head != NULL) {
            pthread_mutex_unlock(&pool->lock);
            loom_coroutine_t *coro  = head->coro;
            loom_task_t      *ctask = head->task;
            if (ctask != NULL && atomic_load_explicit(&ctask->cancelled, memory_order_relaxed)) {
                loom_coro_terminate(coro);
                metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
                loom_coro_destroy(&coro);
                if (ctask->free_data && ctask->user_data) {
                    free(ctask->user_data);
                }
                task_destroy(pool, ctask);
                free(head);
                continue;
            }
            loom_coro_result_t crc = loom_coro_resume(coro);
            if (crc == LOOMWORKS_CORO_OK) {
                loom_coro_state_t st = loom_coro_state(coro);
                if (st == LOOMWORKS_CORO_SUSPENDED) {
                    pthread_mutex_lock(&pool->coro_lock);
                    head->next                    = NULL;
                    struct loom_coro_ready **tail = &pool->coro_ready[idx];
                    while (*tail) {
                        tail = &(*tail)->next;
                    }
                    *tail = head;
                    pthread_mutex_unlock(&pool->coro_lock);
                    continue;
                }
                if (st == LOOMWORKS_CORO_SLEEPING) {
                    /* The coroutine re-slept; sleep_reg re-armed the timer
                     * heap, so the ready node is consumed — the timer thread
                     * will push a fresh one at the new deadline. */
                    free(head);
                    continue;
                }
                if (st == LOOMWORKS_CORO_DONE) {
                    metrics_fire(pool, LOOMWORKS_METRIC_COMPLETED);
                }
            } else if (crc != LOOMWORKS_CORO_ERR_RUNNING) {
                metrics_fire(pool, LOOMWORKS_METRIC_FAILED);
            }
            loom_coro_destroy(&coro);
            free(head);
            task_destroy(pool, ctask);
            if (pool->queue_capacity > 0) {
                pthread_mutex_lock(&pool->lock);
                pthread_cond_signal(&pool->space_cond);
                pthread_mutex_unlock(&pool->lock);
            }
            continue;
        }
        /* Re-read each iteration: loom_pool_resize may realloc the deques
         * array (grow beyond the initial max), moving it in memory. */
        loom_work_deque_t *my = (pool->deques != NULL) ? &pool->deques[idx] : NULL;
        /* If resized down, exit when our index is beyond the new count.
         * Spill any deque-resident work back to the shared queue first. */
        if (idx >= pool->worker_count && !pool->shutdown) {
            /* Terminate any coroutines still owned by this (vanishing) slot:
             * ready FIFO first, then timer-heap entries registered to us.
             * Resize-down is an explicit shrink; abandoning runnable
             * coroutines is the documented contract. */
            pthread_mutex_lock(&pool->coro_lock);
            struct loom_coro_ready *r = NULL;
            if (pool->coro_ready != NULL) {
                r                     = pool->coro_ready[idx];
                pool->coro_ready[idx] = NULL;
            }
            pthread_mutex_unlock(&pool->coro_lock);
            while (r) {
                struct loom_coro_ready *n = r->next;
                loom_coroutine_t       *c = r->coro;
                loom_task_t            *t = r->task;
                loom_coro_terminate(c);
                metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
                loom_coro_destroy(&c);
                if (t->free_data && t->user_data) {
                    free(t->user_data);
                }
                task_destroy(pool, t);
                free(r);
                r = n;
            }
            for (;;) {
                pthread_mutex_lock(&pool->timer_lock);
                uint64_t victim = 0;
                bool     found  = false;
                for (size_t i = 0; i < pool->timer_len; i++) {
                    if (pool->timer_heap[i].worker_idx == idx) {
                        victim = pool->timer_heap[i].task_id;
                        found  = true;
                        break;
                    }
                }
                if (!found) {
                    pthread_mutex_unlock(&pool->timer_lock);
                    break;
                }
                loom_timer_entry_t e;
                loom_timer_remove_by_task(pool, victim, &e);
                pthread_mutex_unlock(&pool->timer_lock);
                loom_coroutine_t *c = (struct loom_coroutine *)e.coro_ctx;
                loom_coro_terminate(c);
                metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
                loom_coro_destroy(&c);
                loom_task_t *t = (loom_task_t *)e.task;
                if (t && t->free_data && t->user_data) {
                    free(t->user_data);
                }
                task_destroy(pool, t);
            }
            if (my != NULL) {
                loom_task_t *t;
                while ((t = deque_pop(pool, my)) != NULL) {
                    /* Deque tasks are ring-accounted (queue_len++ at submit);
                     * undo that so the lane enqueue counts them exactly once.
                     * They were also cancel-index-inserted at ring submit —
                     * drop the entry now, else the slot leaks forever. */
                    cancel_index_remove(pool, t);
                    atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
                    loom_enqueue_unlocked(pool, t);
                }
            }
            pthread_mutex_unlock(&pool->lock);
            /* Wake another worker: the spilled lane tasks need a token. */
            sem_post(&pool->work_sem);
            /* Do NOT clear thread_alive here — the joining side (resize
             * shrink / shutdown) clears it after pthread_join, so no
             * displaced worker is ever skipped by a stale false read. */
            atomic_store_explicit(&pool->thread_clean_exit[idx], true, memory_order_release);
            break;
        }
        /* All workers exit once shutdown is set and nothing is pending —
         * including cancelled ring tasks still awaiting a tombstone drain,
         * tasks still resident in per-worker deques, and sleeping coroutines
         * still registered in the timer heap (their owners may not exit
         * until the timer thread has woken every one of them). */
        if (pool->shutdown && pool->queue_len == 0 &&
            atomic_load_explicit(&pool->ring_count, memory_order_relaxed) == 0 &&
            atomic_load_explicit(&pool->deque_total, memory_order_relaxed) == 0) {
            pthread_mutex_lock(&pool->timer_lock);
            bool timer_empty = (pool->timer_len == 0);
            pthread_mutex_unlock(&pool->timer_lock);
            pthread_mutex_lock(&pool->coro_lock);
            bool ready_empty = (pool->coro_ready == NULL) || (pool->coro_ready[idx] == NULL);
            pthread_mutex_unlock(&pool->coro_lock);
            if (!timer_empty || !ready_empty) {
                /* A sleeping coroutine still needs this worker: the timer
                 * thread will push it into our ready FIFO and post a token.
                 * Fall through to the wait below (may also be woken by a
                 * new submission in the meantime — harmless). */
            } else {
                pthread_mutex_unlock(&pool->lock);
                /* thread_alive stays true until shutdown joins this worker. */
                atomic_store_explicit(&pool->thread_clean_exit[idx], true, memory_order_release);
                break;
            }
        }
        loom_coro_exit();
        loom_task_t *task      = NULL;
        bool         from_ring = false;
        /* Step 0: REALTIME/HIGH (p <= 4) — lock-free peek, then locked
         * dequeue.  Priority first: a full local deque must never starve
         * REALTIME/HIGH work. */
        if (lane_has_priority(pool, 4)) {
            task = dequeue_lowest_priority_unlocked(pool, 4);
        }
        /* Step 1: own deque, LIFO (newest first — cache friendly). */
        if (task == NULL && my != NULL) {
            task      = deque_pop(pool, my);
            from_ring = (task != NULL);
        }
        /* Step 2: bulk-dequeue from the ring into the deque, then pop one. */
        if (task == NULL && my != NULL && pool->ring != NULL) {
            loom_task_t *batch[LOOMWORKS_BULK_DEQUEUE];
            size_t       n = ring_bulk_try_dequeue(pool, batch, LOOMWORKS_BULK_DEQUEUE);
            for (size_t i = 0; i < n; i++) {
                if (!deque_push(pool, my, batch[i])) {
                    /* deque full: spill back to the shared queue.  Ring
                     * tasks are queue_len++ at submit; undo that so the
                     * lane enqueue counts them exactly once.  They were
                     * also cancel-index-inserted at ring submit — drop
                     * the entry now, else the slot leaks forever. */
                    if (i == 0) {
                        task      = batch[i];
                        from_ring = true;
                    } else {
                        cancel_index_remove(pool, batch[i]);
                        atomic_fetch_sub_explicit(&pool->queue_len, 1, memory_order_relaxed);
                        loom_enqueue_unlocked(pool, batch[i]);
                    }
                }
            }
            if (task == NULL) {
                task      = deque_pop(pool, my);
                from_ring = (task != NULL);
            }
        }
        /* Step 3: steal FIFO (oldest) from random victims when idle. */
        if (task == NULL && my != NULL && pool->worker_count > 1) {
            /* The try*2 stride covers only every other victim (parity):
             * with an even worker count the owner can stay unreachable and
             * its tasks strand, burning the wake budget and wedging
             * shutdown.  Probe opportunistically while the ring still has
             * work; once the ring is dry, runnable work lives only in the
             * deques, so visit every other worker exactly once. */
            if (pool->ring != NULL && ring_has_work(pool)) {
                for (uint32_t try = 0; try < LOOMWORKS_STEAL_TRIES && task == NULL; try++) {
                    uint32_t victim = (uint32_t)((idx + 1 + try * 2) % pool->worker_count);
                    if (victim == idx) {
                        continue;
                    }
                    task      = deque_steal(pool, &pool->deques[victim]);
                    from_ring = (task != NULL);
                }
            } else {
                for (uint32_t v = 0; v < pool->worker_count - 1 && task == NULL; v++) {
                    uint32_t victim = (uint32_t)((idx + 1 + v) % pool->worker_count);
                    if (victim == idx) {
                        continue;
                    }
                    task      = deque_steal(pool, &pool->deques[victim]);
                    from_ring = (task != NULL);
                }
            }
        }
        /* Step 4: p >= 5 lanes — only when the ring has no work. */
        if (task == NULL && (pool->ring == NULL || !ring_has_work(pool))) {
            task = dequeue_lowest_priority_unlocked(pool, 255);
        }
        if (task == NULL) {
            /* No work: wait without holding the lock.  The counting
             * semaphore guarantees no lost wakeup — every successful
             * enqueue posts exactly one token.  Spurious wakeups are
             * harmless: the loop re-checks shutdown and the queue. */
            pthread_mutex_unlock(&pool->lock);
            if (pool->shutdown) {
                /* Shutdown posted exactly worker_count tokens — one per
                 * worker.  Any worker that sleeps again steals another
                 * token, exhausting the supply so a leftover sleeping
                 * worker can never wake up to see queue_len hit 0, and
                 * shutdown joins forever.  Never re-sleep after shutdown:
                 * no new work can arrive, so either an idle pass finds a
                 * stranded task to drain or queue_len == 0 lets the exit
                 * check at the top of the loop fire. */
                sched_yield();
                continue;
            }
            while (sem_wait(&pool->work_sem) != 0 && errno == EINTR) {
            }
            continue;
        }
        /* Run boundary: remove the cancel-index entry (deferred from the
         * ring->deque transfer).  After this, cancel()/cancel_by_id()
         * probing the index can no longer find the task; it is committed
         * to run (or was already cancelled -> the check below catches it).
         * Only ring-sourced tasks (from_ring) were ever inserted: lane
         * tasks (Step 0/Step 4) skip this, else the probe would scan the
         * dense index for a key that is not there on every lane task.
         * Lock contract: cancel_index_remove assumes pool->lock is held. */
        if (from_ring) {
            cancel_index_remove(pool, task);
        }
        if (task->cancelled) {
            /* Tombstone won: the canceller already accounted queue_len;
             * release the node (and any owned data) and continue. */
            if (task->is_future) {
                future_mark_cancelled((future_task_ctx_t *)task->user_data);
            }
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
        if (task->is_coro) {
            /* Pool coroutine task: the worker owns the coroutine for its
             * whole lifetime (affinity).  The task node's user_data becomes
             * the coroutine handle so Step C0 can recycle it; free_data is
             * always false for coroutine tasks.  queue_len was already
             * accounted at lane dequeue — never touch it here. */
            pthread_mutex_unlock(&pool->lock);
            metrics_fire(pool, LOOMWORKS_METRIC_STARTED);
            loom_coroutine_t  *coro = NULL;
            loom_coro_result_t crc =
                loom_coro_create(task->coro_fn, task->user_data, task->stack_size, &coro);
            if (crc != LOOMWORKS_CORO_OK) {
                metrics_fire(pool, LOOMWORKS_METRIC_FAILED);
                /* task->user_data still holds the original caller pointer
                 * (task->free_data == false for pool-coroutine tasks).
                 * Save it here before we overwrite task->user_data below,
                 * so we can free it on this failure path. */
                void *saved_ud = task->user_data;
                task_destroy(pool, task);
                if (saved_ud) {
                    free(saved_ud);
                }
                if (pool->queue_capacity > 0) {
                    pthread_mutex_lock(&pool->lock);
                    pthread_cond_signal(&pool->space_cond);
                    pthread_mutex_unlock(&pool->lock);
                }
                continue;
            }
            coro->task_id       = task->task_id;
            coro->sleep_reg     = coro_sleep_reg_hook;
            coro->sleep_reg_ctx = pool;
            coro->task_node     = task;
            coro->worker_idx    = idx;
            task->user_data     = coro;
            crc                 = loom_coro_resume(coro);
            if (crc == LOOMWORKS_CORO_OK) {
                loom_coro_state_t st = loom_coro_state(coro);
                if (st == LOOMWORKS_CORO_SUSPENDED) {
                    struct loom_coro_ready *rn = (struct loom_coro_ready *)calloc(1, sizeof(*rn));
                    if (rn != NULL) {
                        rn->coro    = coro;
                        rn->task_id = task->task_id;
                        rn->task    = task;
                        pthread_mutex_lock(&pool->coro_lock);
                        struct loom_coro_ready **tail = &pool->coro_ready[idx];
                        while (*tail) {
                            tail = &(*tail)->next;
                        }
                        *tail = rn;
                        pthread_mutex_unlock(&pool->coro_lock);
                        continue;
                    }
                    /* calloc failed: fall through to recycle. */
                } else if (st == LOOMWORKS_CORO_SLEEPING) {
                    /* sleep_reg already registered the timer entry; the timer
                     * thread will push a fresh ready node at the deadline. */
                    continue;
                } else if (st == LOOMWORKS_CORO_DONE) {
                    metrics_fire(pool, LOOMWORKS_METRIC_COMPLETED);
                }
            } else if (crc != LOOMWORKS_CORO_ERR_RUNNING) {
                metrics_fire(pool, LOOMWORKS_METRIC_FAILED);
            }
            loom_coro_destroy(&coro);
            task_destroy(pool, task);
            if (pool->queue_capacity > 0) {
                pthread_mutex_lock(&pool->lock);
                pthread_cond_signal(&pool->space_cond);
                pthread_mutex_unlock(&pool->lock);
            }
            continue;
        }
        loom_task_fn fn   = task->fn;
        void        *data = task->user_data;
        /* Return the node to the pool under the lock (the free-list is
         * lock-protected); the task function runs outside the lock. */
        task_destroy(pool, task);
        if (from_ring) {
            /* Ring tasks were queue_len++ at submit; account them now.
             * (ring_count was already decremented inside the bulk dequeue.) */
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
    if (pool->node_pool == NULL || pool->node_pool_cap == 0) {
        return false;
    }
    uintptr_t base = (uintptr_t)pool->node_pool;
    uintptr_t p    = (uintptr_t)node;
    return p >= base && p < base + (uintptr_t)pool->node_pool_cap * sizeof(loom_task_t);
}

static loom_task_t *node_stack_pop(loom_thread_pool_t *pool)
{
    uint64_t stack = atomic_load_explicit(&pool->node_stack, memory_order_relaxed);
    for (;;) {
        uint32_t top = (uint32_t)stack;
        if (top == 0) {
            return NULL; /* empty: caller falls back to malloc */
        }
        uint32_t next    = top - 1;
        uint64_t updated = ((uint64_t)((uint32_t)(stack >> 32) + 1) << 32) | next;
        if (atomic_compare_exchange_weak_explicit(
                &pool->node_stack, &stack, updated, memory_order_acquire, memory_order_relaxed)) {
            return &pool->node_pool[top - 1];
        }
    }
}

static void node_stack_push(loom_thread_pool_t *pool, loom_task_t *node)
{
    if (node_from_pool(pool, node)) {
        uint64_t stack = atomic_load_explicit(&pool->node_stack, memory_order_relaxed);
        for (;;) {
            uint32_t top     = (uint32_t)stack;
            uint32_t next    = top + 1;
            uint64_t updated = ((uint64_t)((uint32_t)(stack >> 32) + 1) << 32) | next;
            if (atomic_compare_exchange_weak_explicit(&pool->node_stack,
                                                      &stack,
                                                      updated,
                                                      memory_order_release,
                                                      memory_order_relaxed)) {
                return;
            }
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
    t->priority   = priority;
    t->cancelled  = false;
    t->free_data  = false;
    t->is_future  = (fn == future_task_wrapper);
    t->is_coro    = false;
    t->coro_fn    = NULL;
    t->stack_size = 0;
    t->next       = NULL;
    return t;
}

void task_destroy(loom_thread_pool_t *pool, loom_task_t *t)
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
 *  Chase-Lev work-stealing deque (Le, Popa, Amarasinghe — SPAA'05).
 *
 *  Owner thread pushes and pops at the BOTTOM (LIFO, cache-friendly,
 *  recent-first).  Idle thieves CAS the TOP to steal the OLDEST task
 *  (FIFO — a task sits longest at the top, so it is the fairest to
 *  redistribute).  bottom is owner-private; top is shared.
 * ================================================================ */
bool deque_push(loom_thread_pool_t *pool, loom_work_deque_t *d, loom_task_t *task)
{
    size_t b = atomic_load_explicit(&d->bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&d->top, memory_order_relaxed);
    if (b - t >= d->capacity) {
        return false; /* full */
    }
    d->slots[b & d->mask] = task;
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&d->bottom, b + 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&d->len, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&pool->deque_total, 1, memory_order_relaxed);
    return true;
}

loom_task_t *deque_pop(loom_thread_pool_t *pool, loom_work_deque_t *d)
{
    size_t b = atomic_load_explicit(&d->bottom, memory_order_relaxed);
    if (b == 0) {
        return NULL;
    }
    size_t t = atomic_load_explicit(&d->top, memory_order_acquire);
    if (b - 1 > t) {
        /* Fast path (no fence): two or more elements.  A thief can only
         * take the OLDEST element (index t); with b-1 > t the element we
         * pop at b-1 is out of any thief's reach, so no seq_cst fence is
         * needed to order slots vs top — the slot was already published
         * by push()'s release fence before bottom was incremented. */
        b -= 1;
        atomic_store_explicit(&d->bottom, b, memory_order_relaxed);
        loom_task_t *task = d->slots[b & d->mask];
        atomic_fetch_sub_explicit(&d->len, 1, memory_order_relaxed);
        atomic_fetch_sub_explicit(&pool->deque_total, 1, memory_order_relaxed);
        return task;
    }
    /* Slow path: possibly the last element (b-1 == t) — must fence so a
     * thief reading bottom sees our decrement, then resolve the race. */
    b -= 1;
    atomic_store_explicit(&d->bottom, b, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    t = atomic_load_explicit(&d->top, memory_order_relaxed);
    if (b < t) {
        /* Thief took our element (top moved past us): resync bottom. */
        atomic_store_explicit(&d->bottom, t, memory_order_relaxed);
        return NULL;
    }
    loom_task_t *task = d->slots[b & d->mask];
    if (b > t) {
        /* More than one element left: no thief race, safe LIFO pop. */
        atomic_fetch_sub_explicit(&d->len, 1, memory_order_relaxed);
        atomic_fetch_sub_explicit(&pool->deque_total, 1, memory_order_relaxed);
        return task;
    }
    /* b == t: last element — race with a thief. */
    if (atomic_compare_exchange_strong_explicit(
            &d->top, &t, t + 1, memory_order_seq_cst, memory_order_relaxed)) {
        atomic_store_explicit(&d->bottom, t + 1, memory_order_relaxed);
        atomic_fetch_sub_explicit(&d->len, 1, memory_order_relaxed);
        atomic_fetch_sub_explicit(&pool->deque_total, 1, memory_order_relaxed);
        return task;
    }
    /* Lost to a thief; we own the slot but it is gone. */
    atomic_store_explicit(&d->bottom, t, memory_order_relaxed);
    return NULL;
}

loom_task_t *deque_steal(loom_thread_pool_t *pool, loom_work_deque_t *d)
{
    size_t t = atomic_load_explicit(&d->top, memory_order_acquire);
    atomic_thread_fence(memory_order_seq_cst);
    size_t b = atomic_load_explicit(&d->bottom, memory_order_acquire);
    if (t >= b) {
        return NULL; /* empty */
    }
    loom_task_t *task = d->slots[t & d->mask];
    if (atomic_compare_exchange_strong_explicit(
            &d->top, &t, t + 1, memory_order_seq_cst, memory_order_relaxed)) {
        atomic_fetch_sub_explicit(&d->len, 1, memory_order_relaxed);
        atomic_fetch_sub_explicit(&pool->deque_total, 1, memory_order_relaxed);
        return task;
    }
    return NULL;
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
            atomic_compare_exchange_strong_explicit(
                &slot->task_id, &cur, 1, memory_order_release, memory_order_acquire);
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
            if (atomic_compare_exchange_strong_explicit(
                    &slot->task_id, &tgt, 1, memory_order_release, memory_order_acquire)) {
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
        if (atomic_compare_exchange_weak_explicit(
                &pool->ring_tail, &tail, tail + 1, memory_order_relaxed, memory_order_relaxed)) {
            /* position claimed at want */
            atomic_store_explicit(
                &pool->ring[want & pool->ring_mask].task, task, memory_order_relaxed);
            cancel_index_insert(pool, task); /* between store and publish */
            atomic_store_explicit(
                &pool->ring[want & pool->ring_mask].seq, want + 1, memory_order_release);
            atomic_fetch_add_explicit(&pool->ring_count, 1, memory_order_relaxed);
            return true;
        }
        /* tail moved; retry with the fresh value */
    }
}

/**
 * @brief Bulk-dequeue up to @p max consecutive tasks from the Vyukov ring
 * with a single CAS on ring_head.
 *
 * The producer claims slots sequentially (tail + n), so slots [head, head+n)
 * hold tasks in submission order; the consumer claims a whole run in one
 * CAS and publishes each slot with the standard released sequence number.
 *
 * NOTE: the cancel-index entry is NOT removed here — it lives from submit
 * until the worker's run boundary so tasks sitting in a per-worker deque
 * stay cancellable.  The worker removes it (cancel_index_remove) right
 * before running the task.
 *
 * @return The number of tasks actually claimed (0 if the head is empty or
 *         the CAS lost).  The caller is responsible for ring_count/queue_len
 *         accounting.
 */
size_t ring_bulk_try_dequeue(loom_thread_pool_t *pool, loom_task_t **out, size_t max)
{
    uint64_t head = atomic_load_explicit(&pool->ring_head, memory_order_relaxed);
    size_t   n    = 0;
    for (; n < max; n++) {
        uint64_t pos = (head + n) & pool->ring_mask;
        if (atomic_load_explicit(&pool->ring[pos].seq, memory_order_acquire) != head + n + 1) {
            break; /* empty at this position */
        }
        out[n] = (loom_task_t *)atomic_load_explicit(&pool->ring[pos].task, memory_order_relaxed);
    }
    if (n == 0) {
        return 0;
    }
    uint64_t want = head;
    if (!atomic_compare_exchange_weak_explicit(
            &pool->ring_head, &head, head + n, memory_order_acq_rel, memory_order_relaxed)) {
        return 0; /* another consumer won; retry on next loop iteration */
    }
    for (size_t k = 0; k < n; k++) {
        /* Canonical release: slot at absolute position (want + k) is
         * released to seq = (want + k) + ring_size, so after one full
         * revolution the producer's empty-check (seq == tail) matches
         * again.  Adding +1 here would make the next consumer round
         * ghost-claim the slot and double-free the stale task node. */
        atomic_store_explicit(&pool->ring[(want + k) & pool->ring_mask].seq,
                              want + k + pool->ring_size,
                              memory_order_release);
    }
    atomic_fetch_sub_explicit(&pool->ring_count, n, memory_order_relaxed);
    return n;
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
    atomic_fetch_or_explicit(
        &pool->nonempty_bits[b / 64], (uint64_t)1u << (b % 64), memory_order_relaxed);
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
static loom_task_t *dequeue_lowest_priority_unlocked(loom_thread_pool_t *pool,
                                                     unsigned            max_priority)
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
                atomic_fetch_and_explicit(
                    &pool->nonempty_bits[w], ~((uint64_t)1u << (b % 64)), memory_order_relaxed);
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
        atomic_store_explicit(&p->thread_alive[i], true, memory_order_release);
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
    clock_gettime(CLOCK_MONOTONIC, &deadline);
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

static loom_result_t enqueue_lane_task(loom_thread_pool_t *pool,
                                       loom_task_fn        fn,
                                       void               *data,
                                       uint8_t             priority,
                                       bool                free_data,
                                       uint64_t           *task_id)
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

static loom_result_t
spill_to_normal_lane(loom_thread_pool_t *pool, loom_task_t *task, uint64_t *task_id)
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

static loom_result_t enqueue_ring_task(loom_thread_pool_t *pool,
                                       loom_task_fn        fn,
                                       void               *data,
                                       uint8_t             priority,
                                       bool                free_data,
                                       uint64_t           *task_id)
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

static loom_result_t enqueue_task(loom_thread_pool_t *pool,
                                  loom_task_fn        fn,
                                  void               *data,
                                  uint8_t             priority,
                                  bool                free_data,
                                  uint64_t           *task_id,
                                  bool                block)
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
    /* NORMAL tasks take the ring only when per-worker deques exist — the
     * ring is drained exclusively via the deques (worker Step 1/2/3).  In
     * lane-only mode (deques == NULL) the ring is unreachable, so route
     * NORMAL tasks to the lane buckets that workers actually consume. */
    if (priority == LOOMWORKS_PRIORITY_NORMAL && pool->ring != NULL && pool->deques != NULL) {
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
 *  loom_pool_submit_coroutine — enqueue a stackful coroutine task.
 *
 *  Coroutine tasks are routed through the lane buckets (never the ring)
 *  so the owning worker picks them up and resumes them on its own stack.
 *  Yields and sleeps park the coroutine; the owner worker resumes it from
 *  its per-worker ready FIFO (yield) or the timer thread pushes a ready
 *  node once the sleep deadline expires (sleep).  The pool lazily spawns
 *  a timer thread on the first coroutine submission.
 *
 *  @return  LOOMWORKS_OK on success.
 * ================================================================ */
loom_result_t loom_pool_submit_coroutine(
    loom_thread_pool_t *pool, loom_coro_fn fn, void *arg, size_t stack_size, uint64_t *task_id)
{
    if (!pool || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    if (atomic_load_explicit(&pool->shutdown, memory_order_relaxed)) {
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    loom_result_t rc = ensure_timer_thread(pool);
    if (rc != LOOMWORKS_OK) {
        return rc;
    }
    pthread_mutex_lock(&pool->lock);
    if (pool->shutdown || pool->draining) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    if (pool->queue_capacity > 0 && pool->queue_len >= pool->queue_capacity) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    loom_task_t *task = task_create(pool, (loom_task_fn)fn, arg, LOOMWORKS_PRIORITY_NORMAL);
    if (!task) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_ALLOC;
    }
    task->is_coro    = true;
    task->coro_fn    = fn;
    task->stack_size = stack_size;
    task->free_data  = false; /* user_data is reused as the coroutine handle */
    loom_enqueue_unlocked(pool, task);
    if (task_id) {
        *task_id = task->task_id;
    }
    sem_post(&pool->work_sem);
    pthread_mutex_unlock(&pool->lock);
    metrics_fire(pool, LOOMWORKS_METRIC_SUBMITTED);
    return LOOMWORKS_OK;
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
    pthread_once(&g_condattr_once, init_monotonic_condattr);
    if (pthread_cond_init(&fut->cond, &g_condattr_mono) != 0) {
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
    loom_result_t rc = enqueue_task(
        pool, future_task_wrapper, ctx, LOOMWORKS_PRIORITY_NORMAL, true, task_id, false);
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
    pthread_once(&g_condattr_once, init_monotonic_condattr);
    if (pthread_cond_init(&fut->cond, &g_condattr_mono) != 0) {
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
    loom_result_t rc = enqueue_task(pool, future_task_wrapper, ctx, priority, true, task_id, false);
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
    if (future->cancelled) {
        if (result) {
            *result = NULL;
        }
        pthread_mutex_unlock(&future->mutex);
        return LOOMWORKS_ERR_CANCELLED;
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
        if (future->cancelled) {
            if (result) {
                *result = NULL;
            }
            pthread_mutex_unlock(&future->mutex);
            return LOOMWORKS_ERR_CANCELLED;
        }
        if (result) {
            *result = future->result;
        }
        pthread_mutex_unlock(&future->mutex);
        return LOOMWORKS_OK;
    }
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
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
    if (future->cancelled) {
        if (result) {
            *result = NULL;
        }
        pthread_mutex_unlock(&future->mutex);
        return LOOMWORKS_ERR_CANCELLED;
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
 *  Only a completed future (ready == true) may be destroyed. Destroying
 *  a pending future would free the mutex/cond while the worker that runs
 *  the task may still signal them, so it is rejected with ERR_INVALID.
 *  The result pointer stored in the future is NOT freed; the caller
 *  is responsible for freeing it after loom_future_wait().
 * ================================================================ */
loom_result_t loom_future_destroy(loom_future_t *future)
{
    if (!future) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&future->mutex);
    if (!future->ready) {
        pthread_mutex_unlock(&future->mutex);
        return LOOMWORKS_ERR_INVALID;
    }
    future->result     = NULL;
    future->has_result = false;
    pthread_mutex_unlock(&future->mutex);
    pthread_mutex_destroy(&future->mutex);
    pthread_cond_destroy(&future->cond);
    free(future);
    return LOOMWORKS_OK;
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

    /* The timer thread must outlive the workers: a coroutine that reaches
     * sleep_until during this drain registers a heap entry from inside the
     * task (coro_sleep_reg_hook).  Tearing the timer down first would strand
     * that entry — no thread remains to fire it, the owning worker spins on
     * its non-empty exit condition forever, and the joins below never
     * return.  Only after every worker has exited can no new entry appear. */
    for (uint32_t i = 0; i < pool->max_worker_count; i++) {
        if (atomic_load_explicit(&pool->thread_alive[i], memory_order_acquire)) {
            int jrc = pthread_join(pool->threads[i], NULL);
            /* A worker that exited via the clean-exit path sets
             * thread_clean_exit before leaving; a worker that crashed
             * (pthread_exit mid-task, SIGSEGV, ...) never does.  Report
             * any abnormal exit as a FAILED metric so callers can detect
             * worker crashes that would otherwise look like normal
             * shutdown. */
            if (jrc != 0 ||
                !atomic_load_explicit(&pool->thread_clean_exit[i], memory_order_acquire)) {
                metrics_fire(pool, LOOMWORKS_METRIC_FAILED);
            }
            atomic_store_explicit(&pool->thread_alive[i], false, memory_order_release);
        }
    }

    if (pool->timer_created) {
        atomic_store_explicit(&pool->timer_thread_alive, false, memory_order_release);
        sem_post(&pool->timer_sem);
        pthread_join(pool->timer_thread, NULL);
        pool->timer_created = false;
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
                if (to_free->is_future) {
                    future_mark_cancelled((future_task_ctx_t *)to_free->user_data);
                }
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
    /* Coroutine fast path: a sleeping coroutine task lives in the timer
     * heap (removed from the lane at its worker's dequeue), so neither the
     * cancel index nor the lane buckets can find it.  Mark it cancelled and
     * hand a ready node to the owning worker's FIFO — Step C0 terminates it. */
    loom_timer_entry_t te;
    pthread_mutex_lock(&pool->timer_lock);
    bool in_timer = loom_timer_remove_by_task(pool, task_id, &te);
    pthread_mutex_unlock(&pool->timer_lock);
    if (in_timer) {
        loom_task_t *ctask = (loom_task_t *)te.task;
        atomic_store_explicit(&ctask->cancelled, true, memory_order_release);
        struct loom_coro_ready *rn = (struct loom_coro_ready *)calloc(1, sizeof(*rn));
        if (rn != NULL) {
            rn->coro    = (struct loom_coroutine *)te.coro_ctx;
            rn->task_id = task_id;
            rn->task    = ctask;
            pthread_mutex_lock(&pool->coro_lock);
            struct loom_coro_ready **tail = &pool->coro_ready[te.worker_idx];
            while (*tail) {
                tail = &(*tail)->next;
            }
            *tail = rn;
            pthread_mutex_unlock(&pool->coro_lock);
            sem_post(&pool->work_sem);
        } else {
            /* Extremely rare: leave the coroutine sleeping (no ready node);
             * it will terminate at its deadline via the drain path instead. */
        }
        pthread_mutex_unlock(&pool->lock);
        metrics_fire(pool, LOOMWORKS_METRIC_CANCELLED);
        return LOOMWORKS_OK;
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
                if (to_free->is_future) {
                    future_mark_cancelled((future_task_ctx_t *)to_free->user_data);
                }
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
                uint64_t       cur  = atomic_load_explicit(&slot->task_id, memory_order_acquire);
                if (cur <= 1) { /* EMPTY or TOMBSTONE */
                    continue;
                }
                loom_task_t *task = slot->task;
                if (task == NULL || task->cancelled) {
                    continue;
                }
                task->cancelled = true;
                uint64_t tgt    = cur;
                if (atomic_compare_exchange_strong_explicit(
                        &slot->task_id, &tgt, 1, memory_order_release, memory_order_acquire)) {
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
                if (cur->is_future) {
                    future_mark_cancelled((future_task_ctx_t *)cur->user_data);
                }
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
 *  non-shutdown pool is rejected with LOOMWORKS_ERR_INVALID and the
 *  handle is left untouched: freeing a pool whose workers may still
 *  be running would let them trample freed memory, so the caller
 *  must shut the pool down first.
 *
 *  Safe to call with a NULL handle or a handle already set to NULL
 *  (returns LOOMWORKS_OK without touching anything).
 * ================================================================ */
loom_result_t loom_pool_destroy(loom_thread_pool_t **pool)
{
    if (!pool || !*pool) {
        return LOOMWORKS_OK;
    }

    if (!atomic_load_explicit(&(*pool)->shutdown, memory_order_acquire)) {
        return LOOMWORKS_ERR_INVALID;
    }

    pool_destroy_internal(*pool);
    *pool = NULL;
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_pool_current — return the pool whose worker is executing
 *  on this thread, or NULL when not running inside a worker.
 *
 *  Internal accessor used by task_group wait/destroy guards to
 *  reject self-deadlocking calls from a worker of the same pool.
 * ================================================================ */
loom_thread_pool_t *loom_pool_current(void)
{
    return g_current_pool;
}

/* ==== Test-only allocation fault injection ============================
 * One-shot armed counter consumed by loom_pool_resize's guarded call
 * sites. Armed with n, the (n+1)-th check fires once then auto-disarms,
 * so a single misfire cannot cascade into later sites (the lane-only
 * degrade test depends on that). Unarmed (-1) is a single negative
 * branch — zero behavior change. */
static _Atomic long g_test_alloc_fail_at = -1;

static bool test_alloc_fail_next(void)
{
    long v = atomic_load_explicit(&g_test_alloc_fail_at, memory_order_relaxed);
    if (v < 0) {
        return false;
    }
    if (v == 0) {
        atomic_store_explicit(&g_test_alloc_fail_at, -1, memory_order_relaxed);
        return true;
    }
    atomic_store_explicit(&g_test_alloc_fail_at, v - 1, memory_order_relaxed);
    return false;
}

void loom_test_arm_alloc_failure(long n)
{
    atomic_store_explicit(&g_test_alloc_fail_at, n, memory_order_relaxed);
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
    if (count == 0) {
        /* Zero is auto-detect only in loom_pool_create(); here it would
         * stop every worker and permanently break the pool. */
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
        /* Need to grow the threads array.  The deques array must grow in
         * lockstep: workers index it by slot id, and realloc may move it,
         * so do it FIRST — on failure nothing else has been touched yet. */
        uint32_t old_max = pool->max_worker_count;
        if (pool->deques != NULL) {
            /* Grow the deques array with proper 64-byte alignment
             * (realloc cannot preserve it — see deques_alloc). */
            loom_work_deque_t *new_deques = test_alloc_fail_next() ? NULL : deques_alloc(count);
            if (!new_deques) {
                pthread_mutex_unlock(&pool->lock);
                return LOOMWORKS_ERR_ALLOC;
            }
            memcpy(new_deques, pool->deques, old_max * sizeof(loom_work_deque_t));
            free(pool->deques);
            pool->deques = new_deques;
            /* Initialize the freshly-extended deques (mirror pool_init).
             * deques_alloc already zeroed the whole array, so the tail
             * slots pointers read NULL until initialized here. */
            bool slots_failed = false;
            for (uint32_t i = old_max; i < count && !slots_failed; i++) {
                pool->deques[i].capacity = LOOMWORKS_DEQUE_CAPACITY;
                pool->deques[i].mask     = LOOMWORKS_DEQUE_CAPACITY - 1;
                pool->deques[i].bottom   = 0;
                atomic_store_explicit(&pool->deques[i].top, 0, memory_order_relaxed);
                atomic_store_explicit(&pool->deques[i].len, 0, memory_order_relaxed);
                pool->deques[i].slots =
                    test_alloc_fail_next()
                        ? NULL
                        : (loom_task_t **)calloc(LOOMWORKS_DEQUE_CAPACITY, sizeof(loom_task_t *));
                if (pool->deques[i].slots == NULL) {
                    slots_failed = true;
                }
            }
            if (slots_failed) {
                /* Fall back to lane-only mode (same policy as pool_init):
                 * free every slots array (old AND new — the old ones below
                 * old_max are about to be abandoned too, so freeing only the
                 * new tail would leak them), then the array itself.
                 * Existing workers below old_max already hold `my` pointing
                 * into this array — worker_entry re-reads `my` under the
                 * lock each iteration, so NULL here is safe for them too. */
                for (uint32_t i = 0; i < count; i++) {
                    free((void *)pool->deques[i].slots);
                    pool->deques[i].slots = NULL;
                }
                free(pool->deques);
                pool->deques = NULL;
            }
        }
        pthread_t *new_threads =
            test_alloc_fail_next() ? NULL
                                   : (pthread_t *)realloc(pool->threads, count * sizeof(pthread_t));
        if (!new_threads) {
            rollback_deques_tail(pool, old_max, count);
            pthread_mutex_unlock(&pool->lock);
            return LOOMWORKS_ERR_ALLOC;
        }
        pool->threads = new_threads;
        _Atomic bool *new_alive =
            test_alloc_fail_next()
                ? NULL
                : (_Atomic bool *)realloc(pool->thread_alive, count * sizeof(_Atomic bool));
        if (!new_alive) {
            rollback_deques_tail(pool, old_max, count);
            pthread_mutex_unlock(&pool->lock);
            return LOOMWORKS_ERR_ALLOC;
        }
        /* Zero the newly-extended tail: fresh slots must read as not-alive. */
        memset(new_alive + old_max, 0, (count - old_max) * sizeof(_Atomic bool));
        pool->thread_alive = new_alive;
        _Atomic bool *new_clean_exit =
            test_alloc_fail_next()
                ? NULL
                : (_Atomic bool *)realloc(pool->thread_clean_exit, count * sizeof(_Atomic bool));
        if (!new_clean_exit) {
            rollback_deques_tail(pool, old_max, count);
            pthread_mutex_unlock(&pool->lock);
            return LOOMWORKS_ERR_ALLOC;
        }
        /* Zero the newly-extended tail: fresh slots must read as not-exited. */
        memset(new_clean_exit + old_max, 0, (count - old_max) * sizeof(_Atomic bool));
        pool->thread_clean_exit = new_clean_exit;
        if (pool->coro_ready != NULL) {
            /* Switch under coro_lock: the timer thread reads the array
             * inside coro_lock to push ready nodes, so replacing the
             * pointer without the lock could free memory it is about to
             * dereference. */
            pthread_mutex_lock(&pool->coro_lock);
            struct loom_coro_ready **new_coro_ready =
                (struct loom_coro_ready **)calloc(count, sizeof(*new_coro_ready));
            if (!new_coro_ready) {
                pthread_mutex_unlock(&pool->coro_lock);
                rollback_deques_tail(pool, old_max, count);
                pthread_mutex_unlock(&pool->lock);
                return LOOMWORKS_ERR_ALLOC;
            }
            memcpy((void *)new_coro_ready,
                   (const void *)pool->coro_ready,
                   old_max * sizeof(*new_coro_ready));
            free((void *)pool->coro_ready);
            pool->coro_ready = new_coro_ready;
            pthread_mutex_unlock(&pool->coro_lock);
        }
        pool->max_worker_count = count;
    }
    uint32_t old_count = pool->worker_count;
    pool->worker_count = count;
    if (count > old_count) {
        /* Start new worker threads into free slots. */
        for (uint32_t i = old_count; i < count; i++) {
            if (atomic_load_explicit(&pool->thread_alive[i], memory_order_acquire)) {
                continue; /* slot still has a live worker (should not happen) */
            }
            worker_arg_t *wa = test_alloc_fail_next() ? NULL : (worker_arg_t *)malloc(sizeof(*wa));
            if (!wa) {
                /* Roll back: restore old count and join any workers created
                 * in this call before returning. */
                pool->worker_count = old_count;
                pthread_mutex_unlock(&pool->lock);
                for (uint32_t j = old_count; j < i; j++) {
                    if (atomic_load_explicit(&pool->thread_alive[j], memory_order_acquire)) {
                        /* A freshly spawned worker may never have woken and is
                         * blocked on work_sem; a plain pthread_join would
                         * deadlock. Re-post wake tokens until its exit check
                         * runs: worker_entry sets thread_clean_exit[j] right
                         * before breaking out on every exit path, so polling
                         * the flag is the portable wait-for-exit primitive
                         * (pthread_tryjoin_np is a GNU extension). */
                        while (!atomic_load_explicit(&pool->thread_clean_exit[j],
                                                     memory_order_acquire)) {
                            sem_post(&pool->work_sem);
                            sched_yield();
                        }
                        pthread_join(pool->threads[j], NULL);
                        atomic_store_explicit(&pool->thread_alive[j], false, memory_order_release);
                        atomic_store_explicit(
                            &pool->thread_clean_exit[j], false, memory_order_release);
                    }
                }
                return LOOMWORKS_ERR_ALLOC;
            }
            wa->pool  = pool;
            wa->index = i;
            int rc    = pthread_create(&pool->threads[i], NULL, worker_entry, wa);
            if (rc != 0) {
                free(wa);
                pool->worker_count = old_count;
                pthread_mutex_unlock(&pool->lock);
                for (uint32_t j = old_count; j < i; j++) {
                    if (atomic_load_explicit(&pool->thread_alive[j], memory_order_acquire)) {
                        /* A freshly spawned worker may never have woken and is
                         * blocked on work_sem; a plain pthread_join would
                         * deadlock. Re-post wake tokens until its exit check
                         * runs: worker_entry sets thread_clean_exit[j] right
                         * before breaking out on every exit path, so polling
                         * the flag is the portable wait-for-exit primitive
                         * (pthread_tryjoin_np is a GNU extension). */
                        while (!atomic_load_explicit(&pool->thread_clean_exit[j],
                                                     memory_order_acquire)) {
                            sem_post(&pool->work_sem);
                            sched_yield();
                        }
                        pthread_join(pool->threads[j], NULL);
                        atomic_store_explicit(&pool->thread_alive[j], false, memory_order_release);
                        atomic_store_explicit(
                            &pool->thread_clean_exit[j], false, memory_order_release);
                    }
                }
                fprintf(stderr, "loomworks: pthread_create failed: %s\n", strerror(rc));
                return LOOMWORKS_ERR_THREAD;
            }
            atomic_store_explicit(&pool->thread_alive[i], true, memory_order_release);
        }
    }
    /* Wake ALL existing workers — including displaced ones on shrink, so they
     * re-check worker_count and exit instead of sleeping forever.  Count on
     * old_count: every live worker may be blocked on work_sem right now. */
    for (uint32_t i = 0; i < old_count; i++) {
        sem_post(&pool->work_sem);
    }
    pthread_mutex_unlock(&pool->lock);

    if (count < old_count) {
        /* Displaced workers (idx in [count, old_count)) observe idx >=
         * worker_count on their next wake and exit on their own.  Join them
         * so their thread handles are reclaimed and their slots become
         * reusable for a later grow.  The wake above posts exactly
         * old_count tokens, but surviving workers (idx < count) that wake
         * and find no work sleep again — consuming another token each cycle
         * in a thundering-herd race — so a displaced worker can starve with
         * no wake and its exit check never runs, blocking the join forever.
         * Keep posting tokens until its exit check runs: worker_entry sets
         * thread_clean_exit[i] before breaking out on every exit path, so
         * polling the flag is the portable wait-for-exit primitive. */
        for (uint32_t i = count; i < old_count; i++) {
            while (!atomic_load_explicit(&pool->thread_clean_exit[i], memory_order_acquire)) {
                sem_post(&pool->work_sem);
                sched_yield();
            }
            pthread_join(pool->threads[i], NULL);
            atomic_store_explicit(&pool->thread_alive[i], false, memory_order_release);
            atomic_store_explicit(&pool->thread_clean_exit[i], false, memory_order_release);
        }
    }
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
