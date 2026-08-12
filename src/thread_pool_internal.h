#ifndef LOOMWORKS_THREAD_POOL_INTERNAL_H
#define LOOMWORKS_THREAD_POOL_INTERNAL_H

#include "loomworks/thread_pool.h"
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdatomic.h>

/* One cache line on modern x86-64.  Hot fields (mutex, condvars) are
 * aligned to avoid false sharing between worker threads. */
#define LOOMWORKS_CACHELINE_ALIGN __attribute__((aligned(64)))

/* Hard upper bound on queue_capacity to prevent unbounded allocation. */
#define LOOMWORKS_MAX_QUEUE_CAPACITY (1024 * 1024)

/* Max task nodes retained on the pool free-list (prevents unbounded
 * residency after bursts; excess nodes are freed normally). */
#define LOOMWORKS_NODE_POOL_CAP 4096

/* Ring size for unbounded-mode pools (queue_capacity == 0). */
#define LOOMWORKS_RING_DEFAULT_SLOTS 4096u

/* Work-stealing scheduler tuning. */
#define LOOMWORKS_BULK_DEQUEUE 8    /* ring -> local deque batch size */
#define LOOMWORKS_DEQUE_CAPACITY 256 /* per-worker Chase-Lev deque slots (pow2) */
#define LOOMWORKS_STEAL_TRIES 3     /* random victims tried before blocking */

/* ================================================================
 *  Task node — element of a per-priority FIFO bucket in the queue.
 * ================================================================ */
/**
 * @brief Internal task queue node.
 *
 * Each pending task is wrapped in a loom_task_t and appended to the
 * tail of its priority bucket (buckets_tail[priority]).  Workers
 * dequeue from the head of the lowest non-empty bucket.
 */
typedef struct loom_task {
    loom_task_fn      fn;        /**< Task function to execute. */
    void             *user_data; /**< Opaque argument passed to @p fn. */
    _Atomic bool      cancelled; /**< true if task was cancelled before execution. */
    uint64_t          task_id;   /**< Unique task identifier (assigned on submission). */
    uint8_t           priority;  /**< Task priority (lower = higher). */
    struct loom_task *next;      /**< Next node in the queue. */
    bool              free_data; /**< true if user_data should be freed in task_destroy. */
} loom_task_t;

/* Ring cell — canonical Vyukov sequence numbers.  seq == pos is "empty for
 * producer pos"; seq == pos+1 is "full"; seq == pos+ring_size is "released
 * by the consumer at pos" (free for the producer at pos+ring_size). */
typedef struct ring_cell {
    _Atomic uint64_t    seq;       /* position-relative sequence (see plan) */
    _Atomic(loom_task_t *) task;   /* stable while in the ring */
} ring_cell_t;

/* Cancel index slot — open addressing, linear probe, hash = id & (cap-1).
 * task_id: 0 = EMPTY, 1 = TOMBSTONE, id+1 = occupied. */
typedef struct cancel_slot {
    _Atomic uint64_t task_id; /* 0 EMPTY / 1 TOMBSTONE / id+1 occupied */
    loom_task_t     *task;    /* owning task (for the cancelled flag) */
    void            *data;    /* task user_data (for loom_pool_cancel) */
} cancel_slot_t;

/* Per-worker Chase-Lev work-stealing deque.  Owner thread pushes/pops at
 * the bottom (LIFO, cache-friendly); idle thieves CAS the top for FIFO
 * steal.  bottom is private to the owner; top is shared. */
typedef struct loom_work_deque {
    _Alignas(64) _Atomic size_t bottom; /* owner-private write index */
    _Alignas(64) _Atomic size_t top;    /* shared read index (CAS by thieves) */
    loom_task_t           **slots;      /* ring buffer, capacity slots */
    size_t                  capacity;  /* power of two */
    size_t                  mask;      /* capacity - 1 */
    _Atomic size_t          len;       /* tasks currently resident */
} loom_work_deque_t;

/* ================================================================
 *  Future task context — bridges a loom_future_t to the pool's
 *  fire-and-forget task mechanism.
 * ================================================================ */
/**
 * @brief Internal context passed to future_task_wrapper().
 *
 * The wrapper runs the user function and stores the result in the
 * associated loom_future_t under its mutex.
 */
typedef struct {
    loom_task_fn_result fn;     /**< User function that returns a result. */
    void               *data;   /**< Opaque argument passed to @p fn. */
    loom_future_t      *future; /**< Future to signal when complete. */
} future_task_ctx_t;

/* ================================================================
 *  Future — holds the deferred result of a submitted task.
 * ================================================================ */
/**
 * @brief Opaque future structure (visible here for internal use).
 *
 * Protected by @p mutex.  loom_future_wait() blocks on @p cond until
 * @p ready is set by the worker that executed the task.
 */
struct loom_future {
    void           *result;     /**< Result pointer (caller must free). */
    pthread_mutex_t mutex;      /**< Guards @p ready and @p result. */
    pthread_cond_t  cond;       /**< Signalled when @p ready becomes true. */
    bool            ready;      /**< true once the task has completed. */
    bool            has_result; /**< true if result is non-NULL. */
};

/* ================================================================
 *  Worker context — per-worker bookkeeping (cache-line padded).
 * ================================================================ */
/* ================================================================
 *  Thread pool — central data structure.
 * ================================================================ */
/**
 * @brief Opaque thread pool structure (visible here for internal use).
 *
 * @p lock protects the lane buckets, @p draining/@p joined, and the
 * condition variables.  The lock-free Vyukov ring and the cancel index
 * are accessed without @p lock.  The condition variables are cache-line
 * aligned to avoid false sharing with the mutex on contended paths.
 *
 * @p joined tracks whether loom_pool_shutdown() has already called
 * pthread_join on every worker, making subsequent calls a no-op.
 */
struct loom_thread_pool {
    uint32_t worker_count;   /**< Number of worker threads. */
    size_t   stack_size;     /**< Stack size per worker (bytes). */
    uint32_t queue_capacity; /**< Max pending tasks (0 = unbounded). */

    pthread_mutex_t lock       LOOMWORKS_CACHELINE_ALIGN; /**< Guard lane queue + flags. */
    pthread_cond_t  drain_cond LOOMWORKS_CACHELINE_ALIGN; /**< Signal when draining done. */
    sem_t           work_sem   LOOMWORKS_CACHELINE_ALIGN; /**< Task-available wakeup (lock-free). */
    pthread_cond_t  space_cond LOOMWORKS_CACHELINE_ALIGN; /**< Space-available (blocking submit). */
    _Atomic bool               shutdown; /**< true once shutdown() has been called. */
    bool                       draining; /**< true while workers are finishing tasks. */
    bool                       joined;   /**< true once all threads have been joined. */

    /* Per-priority FIFO buckets: bucket[b] holds tasks with priority == b.
     * Numerically smaller priority runs first.  Enqueue appends to the
     * bucket tail (O(1)); dequeue pops the lowest non-empty bucket via the
     * occupancy bitmap (O(1), ~4 loads + ctz). */
    loom_task_t *buckets_head[256]; /**< Per-priority bucket heads. */
    loom_task_t *buckets_tail[256]; /**< Per-priority bucket tails. */
    _Atomic uint64_t nonempty_bits[4]; /**< Bit b set <=> buckets_head[b] != NULL. */
    _Atomic uint32_t queue_len;        /**< Current pending tasks (ring + lanes). */

    /* Lock-free Vyukov ring (NORMAL fast path). */
    _Atomic uint64_t ring_head;  /**< Consumer position (monotonic). */
    _Atomic uint64_t ring_tail;  /**< Producer position (monotonic). */
    ring_cell_t     *ring;       /**< NULL when allocation failed (lane-only mode). */
    uint64_t         ring_size;  /**< Power of two. */
    uint64_t         ring_mask;  /**< ring_size - 1. */
    _Atomic uint32_t ring_count; /**< Tasks currently in the ring. */

    /* Lock-free task_id → slot index for ring cancel (open addressing). */
    cancel_slot_t *cancel_slots; /**< NULL when ring is NULL. */
    uint64_t       cancel_cap;   /**< Power of two >= 2 * ring_size. */

    /* Lock-free node pool (tagged bounded stack; the sole task-node allocator). */
    loom_task_t      *node_pool;     /**< Preallocated node array (never freed individually). */
    uint32_t          node_pool_cap; /**< == LOOMWORKS_NODE_POOL_CAP. */
    _Atomic uint64_t  node_stack;    /**< low32: top, high32: ABA tag. */

    _Atomic uint32_t active_workers; /**< Workers currently executing a task. */

    /* Work-stealing: per-worker Chase-Lev deques + aggregate resident count. */
    loom_work_deque_t *deques;         /**< Array sized max_worker_count. NULL = lane-only mode. */
    _Atomic size_t     deque_total;    /**< Total tasks resident in all deques. */

    pthread_t       *threads;          /**< pthread_t array (one per worker). */
    uint32_t         max_worker_count; /**< Max capacity of threads array. */
    _Atomic uint64_t next_task_id;     /**< Monotonically increasing task ID counter. */
    void            *metrics;          /**< Optional metrics collector (loom_metrics_t*). */
    /* Inline metrics callback fields to avoid circular dependency */
    void (*metric_cb)(void *, void *, void *);
    void *metric_user_data;
};

/**
 * @brief Enqueue a task onto the pool queue (caller must hold pool->lock).
 */
void loom_enqueue_unlocked(loom_thread_pool_t *pool, loom_task_t *task);

/**
 * @brief Dequeue a task from the pool queue (caller must hold pool->lock).
 * @return The dequeued task, or NULL if the queue is empty.
 */
loom_task_t *loom_dequeue_unlocked(loom_thread_pool_t *pool);

/* ================================================================
 *  Chase-Lev work-stealing deque operations.
 *  Owner thread: deque_push / deque_pop (LIFO at the bottom).
 *  Idle thieves: deque_steal (FIFO from the top).
 * ================================================================ */

/**
 * @brief Push a task onto the owner's deque bottom (LIFO side).
 * @return false if the deque is full (caller must spill to shared queues).
 */
bool deque_push(loom_work_deque_t *d, loom_task_t *task);

/**
 * @brief Pop the most-recent task from the owner's bottom (LIFO).
 * @return The task, or NULL if the deque is empty.
 */
loom_task_t *deque_pop(loom_work_deque_t *d);

/**
 * @brief Steal the oldest task from @p d's top (FIFO side).  Called by
 * idle workers; CAS-protected against the owner and other thieves.
 * @return The task, or NULL if nothing could be stolen.
 */
loom_task_t *deque_steal(loom_work_deque_t *d);

#endif /* LOOMWORKS_THREAD_POOL_INTERNAL_H */
