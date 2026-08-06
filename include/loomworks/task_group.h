#ifndef LOOMWORKS_TASK_GROUP_H
#define LOOMWORKS_TASK_GROUP_H

/**
 * @file task_group.h
 * @brief Task group — batch management of related tasks.
 *
 * A task group ties a set of submitted tasks to a common lifecycle.
 * - Submit tasks via loom_task_group_submit() / loom_task_group_submit_future().
 * - Cancel all pending tasks in the group via loom_task_group_cancel().
 * - Wait for every task in the group to finish via loom_task_group_wait().
 * - Destroy the group when done.
 *
 * Internally each submitted task's user_data pointer is recorded so that
 * loom_pool_cancel() can target individual tasks, and loom_task_group_cancel()
 * can walk the list and cancel them all at once.
 */

#include "loomworks/thread_pool.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque task-group handle. */
typedef struct loom_task_group loom_task_group_t;

/**
 * @brief Create a task group attached to a thread pool.
 *
 * @param pool   The thread pool to submit tasks into.
 * @param group  Output pointer for the created group handle.
 * @return       LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_task_group_create(loom_thread_pool_t *pool, loom_task_group_t **group);

/**
 * @brief Destroy a task group.
 *
 * Cancels any pending tasks still in the queue and frees group resources.
 * Tasks already running are NOT interrupted.
 *
 * @param group  Pointer to the group handle (NULL-safe).
 */
void loom_task_group_destroy(loom_task_group_t **group);

/**
 * @brief Submit a fire-and-forget task to the group.
 *
 * The task is enqueued on the group's backing pool.  If the group has
 * already been destroyed or the pool shut down, LOOMWORKS_ERR_INVALID
 * is returned.
 *
 * @param group  The task group.
 * @param fn     Task function.
 * @param data   Opaque user data passed to the task.
 * @return       LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_task_group_submit(loom_task_group_t *group, loom_task_fn fn, void *data);

/**
 * @brief Submit a result-producing task to the group.
 *
 * Same semantics as loom_task_group_submit() but returns a future.
 *
 * @param group  The task group.
 * @param fn     Task function returning a result pointer.
 * @param data   Opaque user data passed to the task.
 * @param future Output pointer for the future handle (caller must free it).
 * @return       LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_task_group_submit_future(loom_task_group_t  *group,
                                            loom_task_fn_result fn,
                                            void               *data,
                                            loom_future_t     **future);

/**
 * @brief Cancel all pending tasks in the group.
 *
 * Walks the internal tracking list and calls loom_pool_cancel() for
 * each enqueued task.  Tasks already being executed are NOT interrupted.
 */
void loom_task_group_cancel(loom_task_group_t *group);

/**
 * @brief Wait for all tasks in the group to complete.
 *
 * Blocks until the pool's draining phase finishes (i.e. until
 * loom_pool_shutdown() has been called and all workers have exited).
 * This is a convenience that calls loom_pool_shutdown() on the backing
 * pool if not already shut down.
 *
 * @param group  The task group.
 */
void loom_task_group_wait(loom_task_group_t *group);

/**
 * @brief Get the number of pending (not yet started) tasks in the group.
 *
 * @param group  The task group.
 * @return       Pending task count, or 0 if group is NULL.
 */
uint32_t loom_task_group_pending_count(const loom_task_group_t *group);

#ifdef __cplusplus
}
#endif

#endif /* LOOMWORKS_TASK_GROUP_H */
