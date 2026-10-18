#ifndef LOOMWORKS_DAG_H
#define LOOMWORKS_DAG_H

/**
 * @file dag.h
 * @brief Task DAG — dependency-aware task layer on top of a thread pool.
 *
 * The user builds a directed acyclic graph (DAG) of task nodes; each node
 * runs exactly once, when all of its predecessors have completed.  The DAG
 * is a client of a loom_thread_pool_t: it introduces no new threads and no
 * polling.  Execution is engine-driven — a node's wrapper is submitted to
 * the pool only when its last predecessor completes, so no thread ever
 * blocks waiting on a predecessor.
 *
 * Lifecycle (single-use):
 *   loom_dag_create() → loom_dag_add_node() / loom_dag_add_edge()
 *   → loom_dag_submit() → loom_dag_wait() / loom_dag_wait_timeout()
 *   → loom_dag_destroy()
 *
 * Failure policy is report-only: a node's task returns a loom_result_t and
 * its dependents run regardless; the user inspects per-node futures or
 * loom_dag_node_status() to observe individual outcomes.  Only a *release*
 * failure (pool submit hook error) or a cycle detected at submit freezes
 * the DAG.
 *
 * The pool must outlive the DAG: it must not be shut down or destroyed
 * before loom_dag_destroy().  If it is, loom_dag_wait() may block
 * indefinitely and node futures may never become ready.
 */

#include "loomworks/thread_pool.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Task function executed by a DAG node.
 *
 * @param user_data  The user_data pointer given at loom_dag_add_node().
 * @return            A loom_result_t; report-only — an error does NOT
 *                    prevent dependents from running.
 */
typedef loom_result_t (*loom_dag_task_fn)(void *user_data);

/** Opaque DAG node handle (returned by loom_dag_add_node()). */
typedef struct loom_dag_node loom_dag_node_t;

/** Opaque DAG handle. */
typedef struct loom_dag loom_dag_t;

/**
 * @brief Create a DAG with room for up to @p node_capacity nodes.
 *
 * @param pool          The pool that executes the DAG's tasks; must outlive
 *                      the DAG.
 * @param node_capacity Maximum number of add_node() calls; must be > 0.
 *                      Node ids are user-chosen indices in [0, node_capacity).
 * @param out           Output pointer for the DAG handle.
 * @return              LOOMWORKS_OK on success, LOOMWORKS_ERR_INVALID for
 *                      NULL pool/out or zero capacity, LOOMWORKS_ERR_ALLOC
 *                      on allocation failure.
 */
loom_result_t loom_dag_create(loom_thread_pool_t *pool, uint32_t node_capacity, loom_dag_t **out);

/**
 * @brief Destroy a DAG.
 *
 * Blocks until every started runner has drained (same join as
 * loom_dag_wait()).  NULL-safe (a NULL handle is a no-op returning
 * LOOMWORKS_OK).  The run outcome is not propagated: a successful teardown
 * returns LOOMWORKS_OK regardless of per-node results - observe those through
 * loom_dag_wait() / loom_dag_node_status() before destroying.  Using a handle
 * after destroy is undefined behavior.
 */
loom_result_t loom_dag_destroy(loom_dag_t *dag);

/**
 * @brief Add a node to the DAG.
 *
 * @param dag        The DAG (must be in its build phase).
 * @param node_id    User-chosen id in [0, node_capacity); duplicates are
 *                   rejected.
 * @param fn          Node task function; must be non-NULL.
 * @param user_data   Opaque argument passed to fn.
 * @param priority    A loom_task_priority_t value (uint8_t, lower runs
 *                    first).
 * @param out_node    Optional output pointer for the node handle; may be
 *                    NULL when the caller does not need it.
 * @return            LOOMWORKS_OK on success, LOOMWORKS_ERR_INVALID for
 *                    NULL dag, NULL fn, an out-of-range or
 *                    duplicate id, or a mutation after submit,
 *                    LOOMWORKS_ERR_ALLOC if the node array could not be
 *                    allocated.
 */
loom_result_t loom_dag_add_node(loom_dag_t       *dag,
                                uint32_t          node_id,
                                loom_dag_task_fn  fn,
                                void             *user_data,
                                uint8_t           priority,
                                loom_dag_node_t **out_node);

/**
 * @brief Add an edge: @p to runs after @p from.
 *
 * @param dag   The DAG (build phase).
 * @param from  Predecessor node id.
 * @param to    Successor node id.
 * @return      LOOMWORKS_OK on success, LOOMWORKS_ERR_INVALID for NULL
 *              dag, unknown ids, a self-edge, a duplicate edge, or a
 *              mutation after submit, LOOMWORKS_ERR_ALLOC on allocation
 *              failure.  Acyclicity is NOT checked here; it is detected
 *              lazily at submit.
 */
loom_result_t loom_dag_add_edge(loom_dag_t *dag, uint32_t from, uint32_t to);

/**
 * @brief Submit the DAG: release every zero-in-degree node to the pool.
 *
 * One-shot — the graph is frozen afterwards (add_node/add_edge return
 * LOOMWORKS_ERR_INVALID).  Acyclicity is checked here with a Kahn
 * topological pass; a cycle makes submit return LOOMWORKS_ERR_INVALID and
 * the DAG remains usable for inspection but never re-submittable.  An
 * empty DAG (0 nodes) succeeds immediately, so wait() returns OK right
 * after submit.
 *
 * @return LOOMWORKS_OK, or the recorded failure (LOOMWORKS_ERR_INVALID for
 *         a cycle or a full bounded queue mid-release; the pool's error,
 *         e.g. LOOMWORKS_ERR_SHUTDOWN, on a pool submit failure) which is
 *         also what a later loom_dag_wait() returns.
 */
loom_result_t loom_dag_submit(loom_dag_t *dag);

/**
 * @brief Wait until the graph has fully released.
 *
 * When it returns LOOMWORKS_OK every node's future is ready and
 * loom_dag_node_status() is readable.  A DAG whose release path recorded a
 * failure returns that recorded error after the started runners drain.
 *
 * @return LOOMWORKS_OK, the recorded fail_error of a failed DAG, or
 *         LOOMWORKS_ERR_INVALID if the DAG was never submitted (never
 *         blocks on unreleased work).
 */
loom_result_t loom_dag_wait(loom_dag_t *dag);

/**
 * @brief Wait for the DAG up to an absolute CLOCK_MONOTONIC deadline.
 *
 * On expiry returns LOOMWORKS_ERR_TIMEOUT and leaves the DAG fully usable —
 * a later loom_dag_wait() resumes where this call left off.
 *
 * @param dag       The DAG.
 * @param deadline  Absolute CLOCK_MONOTONIC deadline; NULL means "wait
 *                  forever" (equivalent to loom_dag_wait()).
 * @return           LOOMWORKS_OK when drained, LOOMWORKS_ERR_TIMEOUT on
 *                   expiry, the recorded fail_error of a failed DAG, or
 *                   LOOMWORKS_ERR_INVALID if never submitted.
 */
loom_result_t loom_dag_wait_timeout(loom_dag_t *dag, const struct timespec *deadline);

/**
 * @brief Get the node's future.
 *
 * @param node  The node (obtained from loom_dag_add_node()).
 * @return       A pointer to the node's embedded future once the node was
 *               released (started), or NULL if it was never released.
 *               The DAG owns its futures: callers must not call
 *               loom_future_destroy() on them.
 */
loom_future_t *loom_dag_node_future(loom_dag_node_t *node);

/**
 * @brief Get the node's task return value.
 *
 * @param node  The node.
 * @return       The task's loom_result_t once the node's future is ready
 *               (i.e. after loom_dag_wait() returned for this DAG), or
 *               LOOMWORKS_ERR_INVALID while the future is not yet ready.
 */
loom_result_t loom_dag_node_status(loom_dag_node_t *node);

#ifdef __cplusplus
}
#endif

#endif /* LOOMWORKS_DAG_H */
