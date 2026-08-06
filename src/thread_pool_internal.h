#ifndef CTPPOOL_THREAD_POOL_INTERNAL_H
#define CTPPOOL_THREAD_POOL_INTERNAL_H

#include "ctpool/thread_pool.h"
#include <pthread.h>
#include <stdbool.h>

/* One cache line on modern x86-64.  Hot fields (mutex, condvars) are
 * aligned to avoid false sharing between worker threads. */
#define CTPPOOL_CACHELINE_ALIGN  __attribute__((aligned(64)))

/* Hard upper bound on queue_capacity to prevent unbounded allocation. */
#define CTPPOOL_MAX_QUEUE_CAPACITY (1024 * 1024)

/* ================================================================
 *  Task node — singly-linked list element in the pool queue.
 * ================================================================ */
/**
 * @brief Internal task queue node.
 *
 * Each pending task is wrapped in a ctpool_task_t and appended to the
 * tail of a singly-linked list.  Workers dequeue from the head.
 */
typedef struct ctpool_task {
    ctpool_task_fn         fn;        /**< Task function to execute. */
    void                  *user_data; /**< Opaque argument passed to @p fn. */
    struct ctpool_task    *next;      /**< Next node in the queue. */
} ctpool_task_t;

/* ================================================================
 *  Future task context — bridges a ctpool_future_t to the pool's
 *  fire-and-forget task mechanism.
 * ================================================================ */
/**
 * @brief Internal context passed to future_task_wrapper().
 *
 * The wrapper runs the user function and stores the result in the
 * associated ctpool_future_t under its mutex.
 */
typedef struct {
    ctpool_task_fn_result fn;       /**< User function that returns a result. */
    void                 *data;     /**< Opaque argument passed to @p fn. */
    ctpool_future_t      *future;   /**< Future to signal when complete. */
} future_task_ctx_t;

/* ================================================================
 *  Future — holds the deferred result of a submitted task.
 * ================================================================ */
/**
 * @brief Opaque future structure (visible here for internal use).
 *
 * Protected by @p mutex.  ctpool_future_wait() blocks on @p cond until
 * @p ready is set by the worker that executed the task.
 */
struct ctpool_future {
    void        *result;      /**< Result pointer (caller must free). */
    pthread_mutex_t mutex;    /**< Guards @p ready and @p result. */
    pthread_cond_t  cond;     /**< Signalled when @p ready becomes true. */
    bool          ready;      /**< true once the task has completed. */
    bool          has_result; /**< true if result is non-NULL. */
};

/* ================================================================
 *  Worker context — per-worker bookkeeping (cache-line padded).
 * ================================================================ */
/**
 * @brief Per-worker context structure.
 *
 * Currently unused as each worker shares the central queue; the struct
 * is reserved for future per-worker task affinity or load-balancing.
 */
typedef struct ctpool_worker_ctx {
    uint64_t padding[7];           /**< Pad to cache-line boundary. */
    ctpool_task_t *task_queue_head; /**< Reserved for per-worker queues. */
    ctpool_task_t *task_queue_tail; /**< Reserved for per-worker queues. */
    uint32_t       task_queue_len;  /**< Reserved for per-worker queues. */
    uint64_t padding2[7];          /**< Pad to cache-line boundary. */
} ctpool_worker_ctx_t;

/* ================================================================
 *  Thread pool — central data structure.
 * ================================================================ */
/**
 * @brief Opaque thread pool structure (visible here for internal use).
 *
 * @p lock protects the entire queue and the three bool flags.  The
 * condition variables are cache-line aligned to avoid false sharing
 * with the mutex on contended paths.
 *
 * @p joined tracks whether ctpool_pool_shutdown() has already called
 * pthread_join on every worker, making subsequent calls a no-op.
 */
struct ctpool_thread_pool {
    uint32_t                worker_count;   /**< Number of worker threads. */
    size_t                  stack_size;     /**< Stack size per worker (bytes). */
    uint32_t                queue_capacity; /**< Max pending tasks (0 = unbounded). */

    pthread_mutex_t         lock CTPPOOL_CACHELINE_ALIGN; /**< Guard queue + flags. */
    pthread_cond_t          cond CTPPOOL_CACHELINE_ALIGN; /**< Signal when task enqueued. */
    pthread_cond_t          drain_cond CTPPOOL_CACHELINE_ALIGN; /**< Signal when draining done. */
    bool                    shutdown;       /**< true once shutdown() has been called. */
    bool                    draining;       /**< true while workers are finishing tasks. */
    bool                    joined;         /**< true once all threads have been joined. */

    ctpool_task_t          *queue_head;     /**< Head of the FIFO task queue. */
    ctpool_task_t          *queue_tail;     /**< Tail of the FIFO task queue. */
    uint32_t                queue_len;      /**< Current number of pending tasks. */

    ctpool_worker_ctx_t   *workers;         /**< Per-worker context array. */
    pthread_t              *threads;        /**< pthread_t array (one per worker). */
};

/**
 * @brief Enqueue a task onto the pool queue (caller must hold pool->lock).
 */
void ctpool_enqueue_unlocked(ctpool_thread_pool_t *pool, ctpool_task_t *task);

/**
 * @brief Dequeue a task from the pool queue (caller must hold pool->lock).
 * @return The dequeued task, or NULL if the queue is empty.
 */
ctpool_task_t *ctpool_dequeue_unlocked(ctpool_thread_pool_t *pool);

#endif /* CTPPOOL_THREAD_POOL_INTERNAL_H */
