#ifndef LOOMWORKS_THREAD_POOL_H
#define LOOMWORKS_THREAD_POOL_H

/**
 * @file thread_pool.h
 * @brief Industrial-grade thread pool with opaque API.
 *
 * Features:
 *   - Configurable worker count
 *   - Flexible task submission (fire-and-forget, future-based)
 *   - Graceful shutdown with task drain
 *   - Cache-line aligned internal structures to prevent false sharing
 *   - Pure C11 with POSIX threading
 */

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque thread pool handle. */
typedef struct loom_thread_pool loom_thread_pool_t;

/** Opaque future handle for deferred result retrieval. */
typedef struct loom_future loom_future_t;

/**
 * @brief Result codes for thread pool operations.
 */
typedef enum {
    LOOMWORKS_OK = 0,        /**< Operation succeeded. */
    LOOMWORKS_ERR_ALLOC,     /**< Memory allocation failed. */
    LOOMWORKS_ERR_THREAD,    /**< Thread creation failed. */
    LOOMWORKS_ERR_INVALID,   /**< Invalid argument or handle. */
    LOOMWORKS_ERR_SHUTDOWN,  /**< Pool is shutting down or shut down. */
    LOOMWORKS_ERR_TIMEOUT,   /**< Operation timed out. */
    LOOMWORKS_ERR_CANCELLED, /**< Operation was cancelled. */
} loom_result_t;

/**
 * @brief Task function signature.
 *
 * @param user_data  Opaque pointer passed at submission time.
 */
typedef void (*loom_task_fn)(void *user_data);

/**
 * @brief Task function signature for tasks returning a result.
 *
 * @param user_data  Opaque pointer passed at submission time.
 * @return           Pointer to result (caller owns the memory; pool does not free it).
 */
typedef void *(*loom_task_fn_result)(void *user_data);

/**
 * @brief Default stack size for worker threads.
 */
#define LOOMWORKS_DEFAULT_STACK_SIZE ((size_t)(128 * 1024)) /* 128 KiB */

/**
 * @brief Default number of worker threads (0 = hardware_concurrency * 2, clamped).
 */
#define LOOMWORKS_DEFAULT_WORKER_COUNT 0

/**
 * @brief Thread pool configuration.
 */
typedef struct {
    uint32_t worker_count;   /**< Number of worker threads (0 = auto). */
    size_t   stack_size;     /**< Stack size per worker (0 = default). */
    uint32_t queue_capacity; /**< Max pending tasks before blocking submit (0 = unbounded). */
} loom_pool_config_t;

/**
 * @brief Task priority levels (lower number = higher priority).
 */
typedef enum {
    LOOMWORKS_PRIORITY_LOW      = 10, /**< Default low priority. */
    LOOMWORKS_PRIORITY_NORMAL   = 5,  /**< Normal priority. */
    LOOMWORKS_PRIORITY_HIGH     = 1,  /**< High priority. */
    LOOMWORKS_PRIORITY_REALTIME = 0,  /**< Realtime / critical priority. */
} loom_task_priority_t;

/**
 * @brief Create a thread pool.
 *
 * @param config   Configuration for the pool (pass NULL for defaults).
 * @param pool     Output pointer for the created pool handle.
 * @return         LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_pool_create(const loom_pool_config_t *config, loom_thread_pool_t **pool);

/**
 * @brief Submit a fire-and-forget task to the pool.
 *
 * The task is executed by an available worker. This function blocks only
 * if the internal queue is at capacity (when queue_capacity > 0).
 *
 * @param pool     The pool handle.
 * @param fn       Task function.
 * @param data     Opaque user data passed to the task.
 * @return         LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t
loom_pool_submit(loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint64_t *task_id);

/**
 * @brief Submit a fire-and-forget task, blocking if the queue is full.
 *
 * Same as loom_pool_submit() but blocks (with timeout) when the queue
 * is at capacity instead of returning LOOMWORKS_ERR_INVALID.
 * Times out after 60 seconds, returning LOOMWORKS_ERR_TIMEOUT.
 *
 * @param pool     The pool handle.
 * @param fn       Task function.
 * @param data     Opaque user data passed to the task.
 * @return         LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t
loom_pool_submit_blocking(loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint64_t *task_id);

/**
 * @brief Submit a task that returns a result, with a future for retrieval.
 *
 * @param pool     The pool handle.
 * @param fn       Task function returning a result pointer.
 * @param data     Opaque user data passed to the task.
 * @param future   Output pointer for the future handle (caller must free it).
 * @return         LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_pool_submit_future(loom_thread_pool_t *pool,
                                      loom_task_fn_result fn,
                                      void               *data,
                                      loom_future_t     **future,
                                      uint64_t           *task_id);

/**
 * @brief Submit a fire-and-forget task with explicit priority.
 *
 * @param pool     The pool handle.
 * @param fn       Task function.
 * @param data     Opaque user data.
 * @param priority Task priority (see loom_task_priority_t).
 * @return         LOOMWORKS_OK on success.
 */
loom_result_t loom_pool_submit_priority(
    loom_thread_pool_t *pool, loom_task_fn fn, void *data, uint8_t priority, uint64_t *task_id);

/**
 * @brief Submit a result task with explicit priority.
 *
 * @param pool     The pool handle.
 * @param fn       Task function returning a result pointer.
 * @param data     Opaque user data.
 * @param priority Task priority.
 * @param future   Output pointer for the future handle.
 * @return         LOOMWORKS_OK on success.
 */
loom_result_t loom_pool_submit_future_priority(loom_thread_pool_t *pool,
                                               loom_task_fn_result fn,
                                               void               *data,
                                               uint8_t             priority,
                                               loom_future_t     **future,
                                               uint64_t           *task_id);

/**
 * @brief Wait for a future's result. Blocks until the task completes.
 *
 * @param future   The future handle.
 * @param result   Output pointer for the result (may be NULL).
 * @return         LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_future_wait(loom_future_t *future, void **result);

/**
 * @brief Destroy a future, freeing associated resources.
 *
 * Must be called after loom_future_wait() has returned.
 *
 * @param future   The future handle (NULL-safe).
 */
void loom_future_destroy(loom_future_t *future);

/**
 * @brief Gracefully shut down the pool, draining all pending tasks.
 *
 * Blocks until all submitted tasks have completed execution.
 * No new tasks may be submitted after this call.
 *
 * @param pool  The pool handle.
 */
void loom_pool_shutdown(loom_thread_pool_t *pool);

/**
 * @brief Destroy the pool and release all resources.
 *
 * The pool must have been shut down first via loom_pool_shutdown().
 *
 * @param pool  Pointer to the pool handle (set to NULL on return).
 */
void loom_pool_destroy(loom_thread_pool_t **pool);

/**
 * @brief Get the number of worker threads in the pool.
 *
 * @param pool  The pool handle.
 * @return      Worker count, or 0 if pool is invalid.
 */
uint32_t loom_pool_worker_count(const loom_thread_pool_t *pool);

/**
 * @brief Get the number of pending tasks in the queue.
 *
 * @param pool  The pool handle.
 * @return      Pending task count, or 0 if pool is invalid.
 */
uint32_t loom_pool_pending_count(const loom_thread_pool_t *pool);

/**
 * @brief Get the number of workers currently executing a task.
 *
 * @param pool  The pool handle.
 * @return      Active worker count, or 0 if pool is invalid.
 */
uint32_t loom_pool_active_count(const loom_thread_pool_t *pool);

/**
 * @brief Get the number of idle workers (worker_count - active).
 *
 * @param pool  The pool handle.
 * @return      Idle worker count, or 0 if pool is invalid.
 */
uint32_t loom_pool_idle_count(const loom_thread_pool_t *pool);

/**
 * @brief Get pool utilization as active / worker_count (0.0 when worker_count == 0).
 *
 * @param pool  The pool handle.
 * @return      Utilization in [0.0, 1.0], or 0.0 if pool is invalid.
 */
double loom_pool_utilization(const loom_thread_pool_t *pool);

/**
 * @brief Wake all worker threads blocked on the pool condition variable.
 *
 * Use this when external state changes require workers to re-check conditions
 * (e.g., pipeline shutdown signals the backing pool).  Safe to call on any valid pool.
 *
 * @param pool  The pool handle.
 */
void loom_pool_broadcast(loom_thread_pool_t *pool);

/**
 * @brief Cancel a task that is still in the queue (not yet started).
 *
 * Searches the pending queue for a task whose user_data pointer matches
 * @p data. If found and not yet begun execution, the task is removed and freed.
 *
 * @param pool  The pool handle.
 * @param data  Opaque pointer that was passed to loom_pool_submit().
 * @return      LOOMWORKS_OK if a matching pending task was cancelled,
 *              LOOMWORKS_ERR_INVALID if not found or already running.
 */
loom_result_t loom_pool_cancel(loom_thread_pool_t *pool, void *data);

/**
 * @brief Cancel a pending task by its unique task ID.
 *
 * Searches the queue for a task whose assigned ID matches @p task_id.
 * If found and the task has not yet started executing, it is removed
 * and freed.  This is safer than loom_pool_cancel() when multiple
 * tasks share the same user_data pointer.
 *
 * @param pool     The pool handle.
 * @param task_id  The task identifier assigned by the pool.
 * @return         LOOMWORKS_OK if cancelled, LOOMWORKS_ERR_INVALID if not found.
 */
loom_result_t loom_pool_cancel_by_id(loom_thread_pool_t *pool, uint64_t task_id);

/**
 * @brief Dynamically resize the pool to @p count worker threads.
 *
 * Growing adds new threads; shrinking stops excess idle workers.
 * Workers currently executing tasks are NOT interrupted.
 * Must not be called after loom_pool_shutdown().
 *
 * @param pool  The pool handle.
 * @param count New number of worker threads.  Must be >= 1; 0 is rejected
 *              with LOOMWORKS_ERR_INVALID (auto-detection applies only to
 *              loom_pool_create()).
 * @return      LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_pool_resize(loom_thread_pool_t *pool, uint32_t count);

/**
 * @brief Cancel all tasks currently waiting in the queue.
 *
 * Removed tasks are NOT executed. The number of cancelled tasks is
 * returned via @p count (may be NULL).
 *
 * @param pool  The pool handle.
 * @param count Output pointer for the number of cancelled tasks (optional).
 */
void loom_pool_cancel_all(loom_thread_pool_t *pool, uint32_t *count);

/**
 * @brief Wait for a future with a timeout.
 *
 * @param future   The future handle.
 * @param result   Output pointer for the result (may be NULL).
 * @param deadline Absolute time (timespec) after which to give up waiting.
 * @return         LOOMWORKS_OK on success, LOOMWORKS_ERR_TIMEOUT on expiry.
 */
loom_result_t
loom_future_wait_timeout(loom_future_t *future, void **result, const struct timespec *deadline);

#ifdef __cplusplus
}
#endif

#endif /* LOOMWORKS_THREAD_POOL_H */
