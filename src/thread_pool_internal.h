#ifndef CTPPOOL_THREAD_POOL_INTERNAL_H
#define CTPPOOL_THREAD_POOL_INTERNAL_H

#include "ctpool/thread_pool.h"
#include <pthread.h>
#include <stdbool.h>

#define CTPPOOL_CACHELINE_ALIGN  __attribute__((aligned(64)))
#define CTPPOOL_MAX_QUEUE_CAPACITY 1024 * 1024

/* ---------- Internal task node ---------- */
typedef struct ctpool_task {
    ctpool_task_fn         fn;
    void                  *user_data;
    struct ctpool_task    *next;
} ctpool_task_t;

/* ---------- Internal future context for wrapper task ---------- */
typedef struct {
    ctpool_task_fn_result fn;
    void                 *data;
    ctpool_future_t      *future;
} future_task_ctx_t;

/* ---------- Full future definition ---------- */
struct ctpool_future {
    void        *result;
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
    bool          ready;
    bool          has_result;
};

/* ---------- Per-worker context (cache-line separated) ---------- */
typedef struct ctpool_worker_ctx {
    uint64_t padding[7];
    ctpool_task_t *task_queue_head;
    ctpool_task_t *task_queue_tail;
    uint32_t       task_queue_len;
    uint64_t padding2[7];
} ctpool_worker_ctx_t;

/* ---------- Full pool definition ---------- */
struct ctpool_thread_pool {
    uint32_t                worker_count;
    size_t                  stack_size;
    uint32_t                queue_capacity;

    pthread_mutex_t         lock CTPPOOL_CACHELINE_ALIGN;
    pthread_cond_t          cond CTPPOOL_CACHELINE_ALIGN;
    pthread_cond_t          drain_cond CTPPOOL_CACHELINE_ALIGN;
    bool                    shutdown;
    bool                    draining;
    bool                    joined;

    ctpool_task_t          *queue_head;
    ctpool_task_t          *queue_tail;
    uint32_t                queue_len;

    ctpool_worker_ctx_t   *workers;
    pthread_t              *threads;
};

void *ctpool_worker_run(void *arg);
void ctpool_enqueue_unlocked(ctpool_thread_pool_t *pool, ctpool_task_t *task);
ctpool_task_t *ctpool_dequeue_unlocked(ctpool_thread_pool_t *pool);

#endif /* CTPPOOL_THREAD_POOL_INTERNAL_H */
