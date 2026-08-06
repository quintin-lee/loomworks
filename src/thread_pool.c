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
#include "thread_pool_internal.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

/* ================================================================
 *  Forward declarations
 * ================================================================ */
static loom_result_t pool_init(loom_thread_pool_t *pool);
static void pool_destroy_internal(loom_thread_pool_t *pool);
static void *worker_entry(void *arg);
static loom_task_t *task_create(loom_task_fn fn, void *data);
static void task_destroy(loom_task_t *task);
static void future_task_wrapper(void *arg);

/* ================================================================
 *  pool_init — initialise locks, defaults, and worker thread array
 *
 *  worker_count == 0 → auto (sysconf(_SC_NPROCESSORS_ONLN) * 2,
 *  capped at 128).
 *  stack_size == 0   → LOOMWORKS_DEFAULT_STACK_SIZE.
 *  queue_capacity > LOOMWORKS_MAX_QUEUE_CAPACITY → clamped down.
 * ================================================================ */
static loom_result_t pool_init(loom_thread_pool_t *pool)
{
    if (pool->worker_count == 0) {
        long n = sysconf(_SC_NPROCESSORS_ONLN);
        if (n < 1) n = 1;
        if (n > 64) n = 64;
        pool->worker_count = (uint32_t)n * 2;
    }
    if (pool->stack_size == 0) pool->stack_size = LOOMWORKS_DEFAULT_STACK_SIZE;
    if (pool->queue_capacity > LOOMWORKS_MAX_QUEUE_CAPACITY)
        pool->queue_capacity = LOOMWORKS_MAX_QUEUE_CAPACITY;

    if (pthread_mutex_init(&pool->lock, NULL) != 0) return LOOMWORKS_ERR_ALLOC;
    if (pthread_cond_init(&pool->cond, NULL) != 0) {
        pthread_mutex_destroy(&pool->lock); return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&pool->drain_cond, NULL) != 0) {
        pthread_mutex_destroy(&pool->lock);
        pthread_cond_destroy(&pool->cond);
        return LOOMWORKS_ERR_ALLOC;
    }

    pool->shutdown = false;
    pool->draining = false;
    pool->joined = false;
    pool->queue_head = NULL;
    pool->queue_tail = NULL;
    pool->queue_len = 0;

    pool->workers = (loom_worker_ctx_t *)calloc(
        pool->worker_count, sizeof(loom_worker_ctx_t));
    if (!pool->workers) { pool_destroy_internal(pool); return LOOMWORKS_ERR_ALLOC; }

    pool->threads = (pthread_t *)calloc(
        pool->worker_count, sizeof(pthread_t));
    if (!pool->threads) {
        free(pool->workers); pool_destroy_internal(pool);
        return LOOMWORKS_ERR_ALLOC;
    }
    return LOOMWORKS_OK;
}

/* ================================================================
 *  pool_destroy_internal — free every resource owned by the pool.
 *
 *  Does NOT touch pool->threads or pool->workers beyond freeing the
 *  arrays; the join loop lives in loom_pool_shutdown().
 *  Queue task nodes are freed one by one.
 * ================================================================ */
static void pool_destroy_internal(loom_thread_pool_t *pool)
{
    if (!pool) return;
    /* Drain any tasks still pending in the queue. */
    loom_task_t *t = pool->queue_head;
    while (t) {
        loom_task_t *n = t->next;
        free(t);
        t = n;
    }
    pthread_cond_destroy(&pool->drain_cond);
    pthread_cond_destroy(&pool->cond);
    pthread_mutex_destroy(&pool->lock);
    free(pool->threads);
    free(pool->workers);
    free(pool);
}

/* ================================================================
 *  worker_entry — per-worker loop.
 *
 *  Blocks on pool->cond while the queue is empty and the pool is not
 *  shutting down.  On wakeup:
 *    1. If shutdown is set and the queue is empty → exit thread.
 *    2. Otherwise dequeue one task (under lock), run it outside the
 *       lock to maximise concurrency.
 * ================================================================ */
static void *worker_entry(void *arg)
{
    loom_thread_pool_t *pool = (loom_thread_pool_t *)arg;
    while (1) {
        pthread_mutex_lock(&pool->lock);
        /* Spin until work arrives or shutdown is signalled. */
        while (pool->queue_len == 0 && !pool->shutdown) {
            pthread_cond_wait(&pool->cond, &pool->lock);
        }
        /* All workers exit once shutdown is set and the queue is empty. */
        if (pool->shutdown && pool->queue_len == 0) {
            pthread_mutex_unlock(&pool->lock); break;
        }
        loom_task_t *task = loom_dequeue_unlocked(pool);
        pthread_mutex_unlock(&pool->lock);
        if (task) {
            loom_task_fn fn = task->fn;
            void *data = task->user_data;
            task_destroy(task);
            fn(data);
        }
    }
    return NULL;
}

/* ================================================================
 *  task_create / task_destroy — allocate and free a single task node.
 * ================================================================ */
static loom_task_t *task_create(loom_task_fn fn, void *data)
{
    loom_task_t *t = (loom_task_t *)malloc(sizeof(*t));
    if (!t) return NULL;
    t->fn        = fn;
    t->user_data = data;
    t->next      = NULL;
    return t;
}

static void task_destroy(loom_task_t *t)
{
    if (t) free(t);
}

/* ================================================================
 *  loom_enqueue_unlocked — append @p task to the tail of the queue.
 *
 *  Must be called with pool->lock held.
 * ================================================================ */
void loom_enqueue_unlocked(loom_thread_pool_t *pool, loom_task_t *task)
{
    if (pool->queue_tail) {
        pool->queue_tail->next = task;
    } else {
        pool->queue_head = task;
    }
    pool->queue_tail = task;
    pool->queue_len++;
}

/* ================================================================
 *  loom_dequeue_unlocked — remove and return the head task.
 *
 *  Must be called with pool->lock held.
 *  Returns NULL when the queue is empty.
 * ================================================================ */
loom_task_t *loom_dequeue_unlocked(loom_thread_pool_t *pool)
{
    if (!pool->queue_head) {
        return NULL;
    }
    loom_task_t *t = pool->queue_head;
    pool->queue_head = t->next;
    if (pool->queue_head == NULL) {
        pool->queue_tail = NULL;
    }
    t->next = NULL;
    pool->queue_len--;
    return t;
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
    future_task_ctx_t *ctx = (future_task_ctx_t *)arg;
    loom_future_t *fut = ctx->future;
    void *result = ctx->fn(ctx->data);
    pthread_mutex_lock(&fut->mutex);
    fut->result = result;
    fut->has_result = true;
    fut->ready = true;
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
loom_result_t loom_pool_create(const loom_pool_config_t *config,
                                    loom_thread_pool_t **pool)
{
    if (!pool) return LOOMWORKS_ERR_INVALID;
    loom_thread_pool_t *p =
        (loom_thread_pool_t *)calloc(1, sizeof(*p));
    if (!p) return LOOMWORKS_ERR_ALLOC;
    if (config) {
        p->worker_count = config->worker_count;
        p->stack_size = config->stack_size;
        p->queue_capacity = config->queue_capacity;
    }
    loom_result_t rc = pool_init(p);
    if (rc != LOOMWORKS_OK) { free(p); return rc; }
    for (uint32_t i = 0; i < p->worker_count; i++) {
        int rc2 = pthread_create(&p->threads[i], NULL, worker_entry, p);
        if (rc2 != 0) {
            fprintf(stderr, "loomworks: pthread_create failed: %s\n", strerror(rc2));
            p->shutdown = true;
            pthread_cond_broadcast(&p->cond);
            for (uint32_t j = 0; j < i; j++) pthread_join(p->threads[j], NULL);
            pool_destroy_internal(p);
            return LOOMWORKS_ERR_THREAD;
        }
    }
    *pool = p;
    return LOOMWORKS_OK;
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
loom_result_t loom_pool_submit(loom_thread_pool_t *pool,
                                    loom_task_fn fn, void *data)
{
    if (!pool || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&pool->lock);
    /* Reject submissions once shutdown has started. */
    if (pool->shutdown || pool->draining) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_SHUTDOWN;
    }
    if (pool->queue_capacity > 0 && pool->queue_len >= pool->queue_capacity) {
        pthread_mutex_unlock(&pool->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    loom_task_t *task = task_create(fn, data);
    if (!task) { pthread_mutex_unlock(&pool->lock); return LOOMWORKS_ERR_ALLOC; }
    loom_enqueue_unlocked(pool, task);
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
    return LOOMWORKS_OK;
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
                                           void *data,
                                           loom_future_t **future)
{
    if (!pool || !fn || !future) {
        return LOOMWORKS_ERR_INVALID;
    }
    loom_future_t *fut = (loom_future_t *)calloc(1, sizeof(*fut));
    if (!fut) return LOOMWORKS_ERR_ALLOC;
    if (pthread_mutex_init(&fut->mutex, NULL) != 0) {
        free(fut);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&fut->cond, NULL) != 0) {
        pthread_mutex_destroy(&fut->mutex); free(fut); return LOOMWORKS_ERR_ALLOC;
    }
    fut->ready = false; fut->has_result = false; fut->result = NULL;
    future_task_ctx_t *ctx =
        (future_task_ctx_t *)malloc(sizeof(*ctx));
    if (!ctx) { pthread_mutex_destroy(&fut->mutex); pthread_cond_destroy(&fut->cond); free(fut); return LOOMWORKS_ERR_ALLOC; }
    ctx->fn = fn; ctx->data = data; ctx->future = fut;
    loom_result_t rc = loom_pool_submit(pool, future_task_wrapper, ctx);
    if (rc != LOOMWORKS_OK) {
        pthread_mutex_destroy(&fut->mutex); pthread_cond_destroy(&fut->cond);
        free(fut); free(ctx); return rc;
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
    if (!future) return LOOMWORKS_ERR_INVALID;
    pthread_mutex_lock(&future->mutex);
    while (!future->ready) pthread_cond_wait(&future->cond, &future->mutex);
    if (result) *result = future->result;
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
    if (!future) return;
    pthread_mutex_lock(&future->mutex);
    future->result = NULL; future->has_result = false;
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
 *    1. Set shutdown=true and broadcast on pool->cond to wake all
 *       sleeping workers.
 *    2. pthread_join each worker thread.
 *    3. Set draining=false and broadcast on drain_cond so that any
 *       external waiter (e.g. pending_count observers) is notified.
 * ================================================================ */
void loom_pool_shutdown(loom_thread_pool_t *pool)
{
    if (!pool) return;
    pthread_mutex_lock(&pool->lock);
    /* Already joined → nothing to do. */
    if (pool->joined) {
        pthread_mutex_unlock(&pool->lock);
        return;
    }
    pool->shutdown = true; pool->draining = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
    for (uint32_t i = 0; i < pool->worker_count; i++)
        pthread_join(pool->threads[i], NULL);
    pthread_mutex_lock(&pool->lock);
    pool->draining = false;
    pool->joined = true;
    pthread_cond_broadcast(&pool->drain_cond);
    pthread_mutex_unlock(&pool->lock);
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
    if (!pool || !*pool) return;
    pool_destroy_internal(*pool);
    *pool = NULL;
}

/* ================================================================
 *  loom_pool_worker_count — return the number of worker threads.
 *
 *  Returns 0 when @p pool is NULL (safe null-pointer query).
 * ================================================================ */
uint32_t loom_pool_worker_count(const loom_thread_pool_t *pool)
{
    if (!pool) return 0;
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
    if (!pool) return 0;
    pthread_mutex_lock((pthread_mutex_t *)&pool->lock);
    uint32_t n = pool->queue_len;
    pthread_mutex_unlock((pthread_mutex_t *)&pool->lock);
    return n;
}
