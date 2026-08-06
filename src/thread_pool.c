#define _POSIX_C_SOURCE 200809L
#include "ctpool/thread_pool.h"
#include "thread_pool_internal.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

static ctpool_result_t pool_init(ctpool_thread_pool_t *pool);
static void pool_destroy_internal(ctpool_thread_pool_t *pool);
static void *worker_entry(void *arg);
static ctpool_task_t *task_create(ctpool_task_fn fn, void *data);
static void task_destroy(ctpool_task_t *task);
static void future_task_wrapper(void *arg);

static ctpool_result_t pool_init(ctpool_thread_pool_t *pool)
{
    if (pool->worker_count == 0) {
        long n = sysconf(_SC_NPROCESSORS_ONLN);
        if (n < 1) n = 1;
        if (n > 64) n = 64;
        pool->worker_count = (uint32_t)n * 2;
    }
    if (pool->stack_size == 0) pool->stack_size = CTPPOOL_DEFAULT_STACK_SIZE;
    if (pool->queue_capacity > CTPPOOL_MAX_QUEUE_CAPACITY)
        pool->queue_capacity = CTPPOOL_MAX_QUEUE_CAPACITY;

    if (pthread_mutex_init(&pool->lock, NULL) != 0) return CTPPOOL_ERR_ALLOC;
    if (pthread_cond_init(&pool->cond, NULL) != 0) {
        pthread_mutex_destroy(&pool->lock); return CTPPOOL_ERR_ALLOC;
    }
    if (pthread_cond_init(&pool->drain_cond, NULL) != 0) {
        pthread_mutex_destroy(&pool->lock);
        pthread_cond_destroy(&pool->cond);
        return CTPPOOL_ERR_ALLOC;
    }

    pool->shutdown = false;
    pool->draining = false;
    pool->queue_head = NULL;
    pool->queue_tail = NULL;
    pool->queue_len = 0;

    pool->workers = (ctpool_worker_ctx_t *)calloc(
        pool->worker_count, sizeof(ctpool_worker_ctx_t));
    if (!pool->workers) { pool_destroy_internal(pool); return CTPPOOL_ERR_ALLOC; }

    pool->threads = (pthread_t *)calloc(
        pool->worker_count, sizeof(pthread_t));
    if (!pool->threads) {
        free(pool->workers); pool_destroy_internal(pool);
        return CTPPOOL_ERR_ALLOC;
    }
    return CTPPOOL_OK;
}

static void pool_destroy_internal(ctpool_thread_pool_t *pool)
{
    if (!pool) return;
    ctpool_task_t *t = pool->queue_head;
    while (t) { ctpool_task_t *n = t->next; free(t); t = n; }
    pthread_cond_destroy(&pool->drain_cond);
    pthread_cond_destroy(&pool->cond);
    pthread_mutex_destroy(&pool->lock);
    free(pool->threads);
    free(pool->workers);
    free(pool);
}

static void *worker_entry(void *arg)
{
    ctpool_thread_pool_t *pool = (ctpool_thread_pool_t *)arg;
    while (1) {
        pthread_mutex_lock(&pool->lock);
        while (pool->queue_len == 0 && !pool->shutdown)
            pthread_cond_wait(&pool->cond, &pool->lock);
        if (pool->shutdown && pool->queue_len == 0) {
            pthread_mutex_unlock(&pool->lock); break;
        }
        ctpool_task_t *task = ctpool_dequeue_unlocked(pool);
        pthread_mutex_unlock(&pool->lock);
        if (task) {
            ctpool_task_fn fn = task->fn;
            void *data = task->user_data;
            task_destroy(task);
            fn(data);
        }
    }
    return NULL;
}

static ctpool_task_t *task_create(ctpool_task_fn fn, void *data)
{
    ctpool_task_t *t = (ctpool_task_t *)malloc(sizeof(*t));
    if (!t) return NULL;
    t->fn = fn; t->user_data = data; t->next = NULL;
    return t;
}

static void task_destroy(ctpool_task_t *t) { if (t) free(t); }

void ctpool_enqueue_unlocked(ctpool_thread_pool_t *pool, ctpool_task_t *task)
{
    if (pool->queue_tail) pool->queue_tail->next = task;
    else pool->queue_head = task;
    pool->queue_tail = task;
    pool->queue_len++;
}

ctpool_task_t *ctpool_dequeue_unlocked(ctpool_thread_pool_t *pool)
{
    if (!pool->queue_head) return NULL;
    ctpool_task_t *t = pool->queue_head;
    pool->queue_head = t->next;
    if (!pool->queue_head) pool->queue_tail = NULL;
    t->next = NULL;
    pool->queue_len--;
    return t;
}

static void future_task_wrapper(void *arg)
{
    future_task_ctx_t *ctx = (future_task_ctx_t *)arg;
    ctpool_future_t *fut = ctx->future;
    void *result = ctx->fn(ctx->data);
    pthread_mutex_lock(&fut->mutex);
    fut->result = result;
    fut->has_result = true;
    fut->ready = true;
    pthread_cond_signal(&fut->cond);
    pthread_mutex_unlock(&fut->mutex);
    free(ctx);
}

/* ---------- Public API ---------- */

ctpool_result_t ctpool_pool_create(const ctpool_pool_config_t *config,
                                    ctpool_thread_pool_t **pool)
{
    if (!pool) return CTPPOOL_ERR_INVALID;
    ctpool_thread_pool_t *p =
        (ctpool_thread_pool_t *)calloc(1, sizeof(*p));
    if (!p) return CTPPOOL_ERR_ALLOC;
    if (config) {
        p->worker_count = config->worker_count;
        p->stack_size = config->stack_size;
        p->queue_capacity = config->queue_capacity;
    }
    ctpool_result_t rc = pool_init(p);
    if (rc != CTPPOOL_OK) { free(p); return rc; }
    for (uint32_t i = 0; i < p->worker_count; i++) {
        int rc2 = pthread_create(&p->threads[i], NULL, worker_entry, p);
        if (rc2 != 0) {
            fprintf(stderr, "ctpool: pthread_create failed: %s\n", strerror(rc2));
            p->shutdown = true;
            pthread_cond_broadcast(&p->cond);
            for (uint32_t j = 0; j < i; j++) pthread_join(p->threads[j], NULL);
            pool_destroy_internal(p);
            return CTPPOOL_ERR_THREAD;
        }
    }
    *pool = p;
    return CTPPOOL_OK;
}

ctpool_result_t ctpool_pool_submit(ctpool_thread_pool_t *pool,
                                    ctpool_task_fn fn, void *data)
{
    if (!pool || !fn) return CTPPOOL_ERR_INVALID;
    pthread_mutex_lock(&pool->lock);
    if (pool->shutdown || pool->draining) {
        pthread_mutex_unlock(&pool->lock);
        return CTPPOOL_ERR_SHUTDOWN;
    }
    if (pool->queue_capacity > 0 && pool->queue_len >= pool->queue_capacity) {
        pthread_mutex_unlock(&pool->lock);
        return CTPPOOL_ERR_INVALID;
    }
    ctpool_task_t *task = task_create(fn, data);
    if (!task) { pthread_mutex_unlock(&pool->lock); return CTPPOOL_ERR_ALLOC; }
    ctpool_enqueue_unlocked(pool, task);
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
    return CTPPOOL_OK;
}

ctpool_result_t ctpool_pool_submit_future(ctpool_thread_pool_t *pool,
                                           ctpool_task_fn_result fn,
                                           void *data,
                                           ctpool_future_t **future)
{
    if (!pool || !fn || !future) return CTPPOOL_ERR_INVALID;
    ctpool_future_t *fut = (ctpool_future_t *)calloc(1, sizeof(*fut));
    if (!fut) return CTPPOOL_ERR_ALLOC;
    if (pthread_mutex_init(&fut->mutex, NULL) != 0) { free(fut); return CTPPOOL_ERR_ALLOC; }
    if (pthread_cond_init(&fut->cond, NULL) != 0) {
        pthread_mutex_destroy(&fut->mutex); free(fut); return CTPPOOL_ERR_ALLOC;
    }
    fut->ready = false; fut->has_result = false; fut->result = NULL;
    future_task_ctx_t *ctx =
        (future_task_ctx_t *)malloc(sizeof(*ctx));
    if (!ctx) { pthread_mutex_destroy(&fut->mutex); pthread_cond_destroy(&fut->cond); free(fut); return CTPPOOL_ERR_ALLOC; }
    ctx->fn = fn; ctx->data = data; ctx->future = fut;
    ctpool_result_t rc = ctpool_pool_submit(pool, future_task_wrapper, ctx);
    if (rc != CTPPOOL_OK) {
        pthread_mutex_destroy(&fut->mutex); pthread_cond_destroy(&fut->cond);
        free(fut); free(ctx); return rc;
    }
    *future = fut;
    return CTPPOOL_OK;
}

ctpool_result_t ctpool_future_wait(ctpool_future_t *future, void **result)
{
    if (!future) return CTPPOOL_ERR_INVALID;
    pthread_mutex_lock(&future->mutex);
    while (!future->ready) pthread_cond_wait(&future->cond, &future->mutex);
    if (result) *result = future->result;
    pthread_mutex_unlock(&future->mutex);
    return CTPPOOL_OK;
}

void ctpool_future_destroy(ctpool_future_t *future)
{
    if (!future) return;
    pthread_mutex_lock(&future->mutex);
    future->result = NULL; future->has_result = false;
    pthread_mutex_unlock(&future->mutex);
    pthread_mutex_destroy(&future->mutex);
    pthread_cond_destroy(&future->cond);
    free(future);
}

void ctpool_pool_shutdown(ctpool_thread_pool_t *pool)
{
    if (!pool) return;
    pthread_mutex_lock(&pool->lock);
    pool->shutdown = true; pool->draining = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
    for (uint32_t i = 0; i < pool->worker_count; i++)
        pthread_join(pool->threads[i], NULL);
    pthread_mutex_lock(&pool->lock);
    pool->draining = false;
    pthread_cond_broadcast(&pool->drain_cond);
    pthread_mutex_unlock(&pool->lock);
}

void ctpool_pool_destroy(ctpool_thread_pool_t **pool)
{
    if (!pool || !*pool) return;
    pool_destroy_internal(*pool);
    *pool = NULL;
}

uint32_t ctpool_pool_worker_count(const ctpool_thread_pool_t *pool)
{
    if (!pool) return 0;
    return pool->worker_count;
}

uint32_t ctpool_pool_pending_count(const ctpool_thread_pool_t *pool)
{
    if (!pool) return 0;
    pthread_mutex_lock((pthread_mutex_t *)&pool->lock);
    uint32_t n = pool->queue_len;
    pthread_mutex_unlock((pthread_mutex_t *)&pool->lock);
    return n;
}
