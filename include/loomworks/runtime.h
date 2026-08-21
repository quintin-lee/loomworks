#ifndef LOOMWORKS_RUNTIME_H
#define LOOMWORKS_RUNTIME_H

/**
 * @file runtime.h
 * @brief Unified thread + coroutine runtime — single entry point.
 *
 * Submit either a plain thread task or a stackful coroutine task through the
 * same API.  The runtime internally routes to the appropriate path based on
 * the flag passed at submit time.
 *
 * Thread tasks (LOOM_SUBMIT_THREAD):
 *   Run to completion on a worker thread.  Standard priority / cancel /
 *   future semantics apply (same as loom_pool_submit_*).
 *
 * Coroutine tasks (LOOM_SUBMIT_CORO):
 *   Run on the owning worker via the per-worker coroutine ready FIFO.
 *   The worker multiplexes multiple coroutines: when one yields or sleeps
 *   the next coroutine in the FIFO runs (M:N scheduling).
 */

#include <stddef.h>
#include <stdint.h>

#include "loomworks/coroutine.h"
#include "loomworks/thread_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque runtime handle. */
typedef struct loom_runtime loom_runtime_t;

/**
 * @brief Submission flag — selects the execution path.
 */
typedef enum {
    LOOM_SUBMIT_THREAD = 0, /**< Route to thread-pool worker (loom_task_fn). */
    LOOM_SUBMIT_CORO   = 1, /**< Route to per-worker coroutine scheduler.     */
} loom_submit_flag_t;

/**
 * @brief Union type for passing either a thread or coroutine function.
 *
 * Use .thread_fn for LOOM_SUBMIT_THREAD and .coro_fn for LOOM_SUBMIT_CORO.
 */
typedef union {
    loom_task_fn thread_fn;
    loom_coro_fn coro_fn;
    void        *ptr;
} loom_fn_union_t;

/**
 * @brief Runtime configuration.
 *
 * Fields documented against loom_pool_config_t in thread_pool.h.
 * coro_stack_size is passed verbatim to each loom_coro_create() call
 * submitted via LOOM_SUBMIT_CORO; 0 selects the coroutine default (64 KiB).
 */
typedef struct {
    uint32_t worker_count;    /**< 0 = auto-detect (same formula as pool).           */
    size_t   stack_size;      /**< Per-worker stack size in bytes (0 = default).     */
    uint32_t queue_capacity;  /**< Max pending tasks before blocking submit (0 = unbounded). */
    uint32_t coro_stack_size; /**< Per-coroutine stack size (0 = default 64 KiB).    */
} loom_runtime_config_t;

/**
 * @brief Create a runtime instance.
 *
 * Allocates and initialises the backing thread pool (auto-selected worker
 * count if config->worker_count is 0).  Returns LOOMWORKS_ERR_ALLOC if the
 * underlying pool creation fails; *out is set to NULL in that case.
 *
 * @param cfg  Configuration (NULL = all defaults).
 * @param out  Output pointer for the created runtime handle.
 * @return     LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_runtime_create(const loom_runtime_config_t *cfg, loom_runtime_t **out);

/**
 * @brief Destroy a runtime and release all resources.
 *
 * Calls shutdown() internally (draining all pending tasks), joins all
 * workers, then frees the handle.  Idempotent: calling destroy() on an
 * already-destroyed runtime (out == NULL) is a safe no-op.
 *
 * @param rt  Pointer to the runtime handle (set to NULL on return).
 */
void loom_runtime_destroy(loom_runtime_t **rt);

/**
 * @brief Submit a task to the runtime.
 *
 * If flag == LOOM_SUBMIT_THREAD, fn.thread_fn is called as a normal thread task.
 * If flag == LOOM_SUBMIT_CORO, fn.coro_fn is called as a coroutine entry point;
 * the worker running this task multiplexes coroutines via the ready FIFO.
 *
 * @param rt       The runtime handle.
 * @param fn       Task entry function union (use .thread_fn or .coro_fn).
 * @param data     Opaque argument passed to fn.
 * @param flag     LOOM_SUBMIT_THREAD or LOOM_SUBMIT_CORO.
 * @param priority Task priority (used only when flag == LOOM_SUBMIT_THREAD;
 *                 ignored for CORO).  Range 0–255.
 * @param task_id  Output pointer for the assigned task ID (may be NULL).
 * @return         LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_runtime_submit(loom_runtime_t    *rt,
                                  loom_fn_union_t    fn,
                                  void              *data,
                                  loom_submit_flag_t flag,
                                  uint8_t            priority,
                                  uint64_t          *task_id);

/**
 * @brief Cancel a pending task by its task ID.
 *
 * Only tasks that have not yet begun execution are removed.  Running tasks
 * are left to complete normally.
 *
 * @param rt       The runtime handle.
 * @param task_id  The task ID returned by loom_runtime_submit().
 * @return         LOOMWORKS_OK if the task was cancelled,
 *                 LOOMWORKS_ERR_INVALID if not found or already running.
 */
loom_result_t loom_runtime_cancel(loom_runtime_t *rt, uint64_t task_id);

/**
 * @brief Cancel all pending tasks.
 *
 * @param rt     The runtime handle.
 * @param count  Output pointer for the number of cancelled tasks (may be NULL).
 */
void loom_runtime_cancel_all(loom_runtime_t *rt, uint32_t *count);

/**
 * @brief Get the number of worker threads.
 */
uint32_t loom_runtime_worker_count(const loom_runtime_t *rt);

/**
 * @brief Get the number of pending (not-yet-started) tasks.
 */
uint32_t loom_runtime_pending_count(const loom_runtime_t *rt);

/**
 * @brief Get the number of workers currently executing a task.
 */
uint32_t loom_runtime_active_count(const loom_runtime_t *rt);

/**
 * @brief Get the number of idle workers.
 */
uint32_t loom_runtime_idle_count(const loom_runtime_t *rt);

/**
 * @brief Get pool utilization as active / worker_count (0.0 when worker_count == 0).
 */
double loom_runtime_utilization(const loom_runtime_t *rt);

/**
 * @brief Gracefully shut down the runtime, draining all pending tasks.
 *
 * No new submissions are accepted after this call.
 */
void loom_runtime_shutdown(loom_runtime_t *rt);

#ifdef __cplusplus
}
#endif

#endif /* LOOMWORKS_RUNTIME_H */
