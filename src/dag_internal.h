#ifndef LOOMWORKS_DAG_INTERNAL_H
#define LOOMWORKS_DAG_INTERNAL_H

/**
 * @file dag_internal.h
 * @brief Internal structures for the task DAG subsystem.
 *
 * Not a public header: only src/dag.c (and the DAG test suite, which
 * includes internal headers to assert on pool lane occupancy) see these.
 */

#include "loomworks/dag.h"
#include "thread_pool_internal.h" /* loom_future_t by value + pool struct */

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * @brief One DAG node: a user task plus its graph bookkeeping.
 *
 * `fn != NULL` is the "added" marker (R5): the node array is calloc'd and
 * `fn` is the only field the build phase sets on a live node, so scanning
 * for `fn == NULL` finds exactly the ids that were never added.  The
 * runner needs `dag` + `node_id` to reach its own record (R2), hence the
 * back-pointer.
 *
 * `in_degree` is the immutable build-phase predecessor count (never written
 * after submit).  `pending_preds` is the live countdown seeded from it at
 * submit and atomically decremented once per completed predecessor; the Kahn
 * pass works on a private `in_degree` copy so it cannot perturb the release
 * counting.
 */
struct loom_dag_node {
    uint32_t         node_id; /* user id; stamped at add_node */
    loom_dag_t      *dag;     /* back-pointer for the runner (R2) */
    loom_dag_task_fn fn;      /* NULL = id not added (R5) */
    void            *user_data;
    int              priority;      /* loom_task_priority_t value */
    uint32_t         in_degree;     /* build-phase in-degree; IMMUTABLE after submit.
                                     * Kept separate from pending_preds: Kahn and the
                                     * root scan read it while runners concurrently
                                     * decrement pending_preds, so it must not change. */
    loom_future_t    future;        /* embedded; init'd at release (D1) */
    loom_result_t    status;        /* fn's return (or a release error, D6) */
    _Atomic uint32_t pending_preds; /* live predecessor countdown; seeded from
                                     * in_degree at submit, decremented once per
                                     * completed predecessor. */
    _Atomic bool future_owner;      /* embedded future initialised (D6); read by
                                     * node_future()/node_status()/destroy. */
    loom_dag_node_t **succs;        /* dynamic array of successor node ptrs */
    uint32_t          succs_cap;
    uint32_t          succs_len;
};

/**
 * @brief The DAG: graph state plus the drain bookkeeping (D4/D5).
 *
 * `lock` guards the build phase and the one-shot submit only; the hot
 * release path never takes it.  `done_mu`/`done_cv` guard the drain
 * wait: the final runner to complete (or the release path that stops on
 * `failed`) signals it, and every false→true flip of `dag_drained()`
 * happens while holding `done_mu` so a joiner can never miss it (D5).
 */
struct loom_dag {
    loom_thread_pool_t *pool;
    loom_dag_node_t    *nodes; /* [node_capacity]; lazily alloc'd on first add_node */
    uint32_t            node_capacity;
    pthread_mutex_t     lock;           /* build phase + one-shot submit */
    _Atomic bool        submitted;      /* frozen after a successful submit OR a cycle (R6) */
    bool                destroyed;      /* under d->lock */
    _Atomic bool        failed;         /* D3: a release failed (or cycle at submit) */
    loom_result_t       fail_error;     /* written before failed=true; read after acquire (R3) */
    _Atomic uint32_t    in_flight;      /* started runners not yet drained (D4) */
    _Atomic uint32_t    released_total; /* successful releases so far (D5) */
    _Atomic uint32_t    added_count;    /* added nodes; stamped at submit (D5) */
    pthread_cond_t      done_cv; /* wait sleep; guarded by done_mu; CLOCK_MONOTONIC condattr */
    pthread_mutex_t     done_mu;
};

/**
 * @brief The DAG has fully drained.
 *
 * Evaluated under done_mu only.  Two monotone counters plus the sticky
 * `failed` flag:
 *
 *   - in_flight       == 0 : no started runner is queued or running
 *   - released_total  == added_count : every added node has been released
 *     (success or failure recorded), so nothing is left to start — or
 *     `failed` short-circuited the release path, in which case the second
 *     term is impossible and the first alone settles it.
 *
 * A latched `all_done` flag is NOT used: with independent roots,
 * in_flight transiently hits 0 while submit is still releasing later
 * roots, and a latch would let wait() return (and destroy() free) while
 * the graph is live.
 */
static inline bool dag_drained(const struct loom_dag *d)
{
    return atomic_load_explicit(&d->in_flight, memory_order_acquire) == 0 &&
           (atomic_load_explicit(&d->failed, memory_order_acquire) ||
            atomic_load_explicit(&d->released_total, memory_order_acquire) ==
                atomic_load_explicit(&d->added_count, memory_order_acquire));
}

/**
 * @brief The per-release wrapper submitted to the pool (D2: free_data=false).
 *
 * The DAG owns the ctx (malloc'd in dag_release_node) and frees it after
 * the pool's task epilogue has finished with it — the pool never frees
 * user_data for DAG tasks.
 */
typedef struct {
    loom_dag_t *dag;
    uint32_t    node_id;
} dag_runner_ctx_t;

#endif /* LOOMWORKS_DAG_INTERNAL_H */
