#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/* ================================================================
 * loomworks - task DAG engine (src/dag.c)
 *
 * See include/loomworks/dag.h for the public API and
 * docs/superpowers/specs/2026-09-21-task-dag-design.md for the design.
 *
 * Engine invariants (spec D3/D4/D5):
 *   - in_flight counts started runners (queued or running).  EVERY release
 *     increments it under done_mu BEFORE the pool submit; the runner's
 *     epilogue decrements it (under done_mu) and signals done_cv when the
 *     drain predicate flips.
 *   - drained(d) = in_flight == 0 && (failed || released_total ==
 *     added_count).  Both counters are monotone; every false->true flip of
 *     the predicate happens under done_mu with a signal, so a joiner can
 *     never miss it.
 *   - released_total is bumped once per release attempt (success or
 *     failure) under done_mu; it reaches added_count when the release path
 *     terminates for any reason.
 *   - A node is released at most once: roots once from submit, every other
 *     node exactly once by the predecessor whose fetch_sub reaches 1.
 * ================================================================ */

#include "loomworks/dag.h"

#include "dag_internal.h"
#include "thread_pool_internal.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static void dag_runner_fn(void *arg);

/* ------------------------------------------------------------
 * dag_record_release_failure - sticky failure bookkeeping.
 *
 * Called under done_mu by a release failure.  The FIRST failure wins
 * fail_error (D3); later failures keep the flag but do not overwrite.
 * Always bumps released_total and signals if the predicate flipped.
 * ------------------------------------------------------------ */
static void dag_record_release_failure(loom_dag_t *dag, loom_result_t rc)
{
    if (!atomic_load_explicit(&dag->failed, memory_order_acquire)) {
        dag->fail_error = rc;
        atomic_store_explicit(&dag->failed, true, memory_order_release);
    }
    atomic_fetch_add_explicit(&dag->released_total, 1, memory_order_relaxed);
    if (dag_drained(dag)) {
        pthread_cond_signal(&dag->done_cv);
    }
}

/* ------------------------------------------------------------
 * dag_release_node - submit one node's wrapper to the pool.
 *
 * Called once per node: from the submit thread (roots) or from a runner
 * (the predecessor whose fetch_sub reaches 1).  Never entered
 * concurrently for the same node.
 *
 * D4: the in_flight increment happens here, under done_mu, BEFORE the
 * submit, so roots and successors are accounted identically.
 * D6: a failure BEFORE the future is initialised (ctx malloc fail or
 * future-init fail) leaves node_future() == NULL; a failure AFTER init
 * completes the embedded future with the error before recording failure.
 * ------------------------------------------------------------ */
static loom_result_t dag_release_node(loom_dag_t *dag, loom_dag_node_t *node)
{
    loom_result_t     rc  = LOOMWORKS_OK;
    dag_runner_ctx_t *ctx = (dag_runner_ctx_t *)malloc(sizeof(*ctx));
    if (!ctx) {
        rc = LOOMWORKS_ERR_ALLOC;
        goto pre_init_failure;
    }
    rc = loom_future_init(&node->future);
    if (rc != LOOMWORKS_OK) {
        free(ctx);
        goto pre_init_failure;
    }
    atomic_store_explicit(&node->future_owner, true, memory_order_release);
    ctx->dag     = dag;
    ctx->node_id = node->node_id;

    /* D4: count this runner before it can start. */
    pthread_mutex_lock(&dag->done_mu);
    atomic_fetch_add_explicit(&dag->in_flight, 1, memory_order_relaxed);
    pthread_mutex_unlock(&dag->done_mu);

    loom_result_t subrc = loom_pool_submit_internal(dag->pool, (loom_task_fn)dag_runner_fn, ctx,
                                                    (uint8_t)node->priority);
    if (subrc != LOOMWORKS_OK) {
        /* D6: the future exists, so complete it with the release error. */
        node->status = subrc;
        loom_future_complete(&node->future, subrc);
        free(ctx);
        pthread_mutex_lock(&dag->done_mu);
        /* Undo this release's increment: the runner never started. */
        atomic_fetch_sub_explicit(&dag->in_flight, 1, memory_order_relaxed);
        dag_record_release_failure(dag, subrc);
        pthread_mutex_unlock(&dag->done_mu);
        return subrc;
    }

    /* Success: the runner owns ctx and the in_flight increment. */
    pthread_mutex_lock(&dag->done_mu);
    atomic_fetch_add_explicit(&dag->released_total, 1, memory_order_relaxed);
    if (dag_drained(dag)) {
        pthread_cond_signal(&dag->done_cv);
    }
    pthread_mutex_unlock(&dag->done_mu);
    return LOOMWORKS_OK;

pre_init_failure:
    /* No future exists yet (D6): node_future() stays NULL for this node. */
    pthread_mutex_lock(&dag->done_mu);
    dag_record_release_failure(dag, rc);
    pthread_mutex_unlock(&dag->done_mu);
    return rc;
}

/* ------------------------------------------------------------
 * dag_runner_fn - the per-release wrapper submitted to the pool.
 *
 * Runs on a pool worker: executes the user task, records its status,
 * completes the node's embedded future, releases successors whose last
 * predecessor just completed, then drains the in_flight counter.
 *
 * The pool does NOT free the ctx (free_data=false); the runner frees it.
 * ------------------------------------------------------------ */
static void dag_runner_fn(void *arg)
{
    dag_runner_ctx_t *ctx  = (dag_runner_ctx_t *)arg;
    loom_dag_t       *dag  = ctx->dag;
    loom_dag_node_t  *node = &dag->nodes[ctx->node_id];

    /* Report-only: a task error does not stop successors; it only feeds
     * the node's status/future. */
    loom_result_t status = node->fn ? node->fn(node->user_data) : LOOMWORKS_OK;
    node->status         = status;
    loom_future_complete(&node->future, status);

    for (uint32_t i = 0; i < node->succs_len; i++) {
        if (atomic_load_explicit(&dag->failed, memory_order_acquire)) {
            break; /* release path is frozen (D3) */
        }
        loom_dag_node_t *succ = node->succs[i];
        if (atomic_fetch_sub_explicit(&succ->pending_preds, 1, memory_order_acq_rel) == 1) {
            (void)dag_release_node(dag, succ); /* failure recorded inside */
        }
    }

    pthread_mutex_lock(&dag->done_mu);
    atomic_fetch_sub_explicit(&dag->in_flight, 1, memory_order_relaxed);
    if (dag_drained(dag)) {
        pthread_cond_signal(&dag->done_cv);
    }
    pthread_mutex_unlock(&dag->done_mu);

    free(ctx);
}

/* ------------------------------------------------------------
 * dag_wait_internal - block until drained (shared by wait/destroy).
 * Returns fail_error if the DAG failed, else LOOMWORKS_OK.
 * ------------------------------------------------------------ */
static loom_result_t dag_wait_internal(loom_dag_t *dag, const struct timespec *deadline)
{
    if (!atomic_load_explicit(&dag->submitted, memory_order_acquire)) {
        return LOOMWORKS_ERR_INVALID; /* never submitted: no block */
    }
    pthread_mutex_lock(&dag->done_mu);
    while (!dag_drained(dag)) {
        if (deadline) {
            int rc = pthread_cond_timedwait(&dag->done_cv, &dag->done_mu, deadline);
            if (rc == ETIMEDOUT) {
                pthread_mutex_unlock(&dag->done_mu);
                return LOOMWORKS_ERR_TIMEOUT;
            }
        } else {
            pthread_cond_wait(&dag->done_cv, &dag->done_mu);
        }
    }
    loom_result_t rc =
        atomic_load_explicit(&dag->failed, memory_order_acquire) ? dag->fail_error : LOOMWORKS_OK;
    pthread_mutex_unlock(&dag->done_mu);
    return rc;
}

/* ------------------------------------------------------------
 * loom_dag_create
 * ------------------------------------------------------------ */
loom_result_t loom_dag_create(loom_thread_pool_t *pool, uint32_t node_capacity, loom_dag_t **out)
{
    if (!pool || !out || node_capacity == 0) {
        return LOOMWORKS_ERR_INVALID;
    }
    loom_dag_t *dag = (loom_dag_t *)calloc(1, sizeof(*dag));
    if (!dag) {
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_mutex_init(&dag->lock, NULL) != 0) {
        free(dag);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_mutex_init(&dag->done_mu, NULL) != 0) {
        pthread_mutex_destroy(&dag->lock);
        free(dag);
        return LOOMWORKS_ERR_ALLOC;
    }
    /* CLOCK_MONOTONIC condattr so wait_timeout takes a monotonic deadline.
     * condattr_init failure and setclock failure are handled separately:
     * init must be followed by destroy exactly once. */
    pthread_condattr_t cattr;
    if (pthread_condattr_init(&cattr) != 0) {
        pthread_mutex_destroy(&dag->done_mu);
        pthread_mutex_destroy(&dag->lock);
        free(dag);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_condattr_setclock(&cattr, CLOCK_MONOTONIC) != 0) {
        pthread_condattr_destroy(&cattr);
        pthread_mutex_destroy(&dag->done_mu);
        pthread_mutex_destroy(&dag->lock);
        free(dag);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&dag->done_cv, &cattr) != 0) {
        pthread_condattr_destroy(&cattr);
        pthread_mutex_destroy(&dag->done_mu);
        pthread_mutex_destroy(&dag->lock);
        free(dag);
        return LOOMWORKS_ERR_ALLOC;
    }
    pthread_condattr_destroy(&cattr);

    dag->pool          = pool;
    dag->node_capacity = node_capacity;
    dag->nodes         = NULL; /* lazily alloc'd on first add_node */
    *out               = dag;
    return LOOMWORKS_OK;
}

/* ------------------------------------------------------------
 * loom_dag_add_node
 * ------------------------------------------------------------ */
loom_result_t loom_dag_add_node(loom_dag_t *dag, uint32_t node_id, loom_dag_task_fn fn,
                                void *user_data, uint8_t priority, loom_dag_node_t **out_node)
{
    if (!dag || !fn || !out_node) {
        return LOOMWORKS_ERR_INVALID;
    }
    if (node_id >= dag->node_capacity) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&dag->lock);
    if (atomic_load_explicit(&dag->submitted, memory_order_acquire) || dag->destroyed) {
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_ERR_INVALID; /* mutation after submit / destroyed */
    }
    if (!dag->nodes) {
        dag->nodes = (loom_dag_node_t *)calloc(dag->node_capacity, sizeof(loom_dag_node_t));
        if (!dag->nodes) {
            pthread_mutex_unlock(&dag->lock);
            return LOOMWORKS_ERR_ALLOC;
        }
    }
    loom_dag_node_t *node = &dag->nodes[node_id];
    if (node->fn != NULL) { /* duplicate id */
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    node->node_id      = node_id;
    node->dag          = dag;
    node->fn           = fn;
    node->user_data    = user_data;
    node->priority     = (int)priority;
    node->in_degree    = 0;
    node->status       = LOOMWORKS_OK;
    node->succs        = NULL;
    node->succs_cap    = 0;
    node->succs_len    = 0;
    atomic_init(&node->pending_preds, 0);
    atomic_init(&node->future_owner, false);
    atomic_fetch_add_explicit(&dag->added_count, 1, memory_order_relaxed);
    *out_node = node;
    pthread_mutex_unlock(&dag->lock);
    return LOOMWORKS_OK;
}

/* ------------------------------------------------------------
 * loom_dag_add_edge
 * ------------------------------------------------------------ */
loom_result_t loom_dag_add_edge(loom_dag_t *dag, uint32_t from, uint32_t to)
{
    if (!dag || from >= dag->node_capacity || to >= dag->node_capacity) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&dag->lock);
    if (atomic_load_explicit(&dag->submitted, memory_order_acquire) || dag->destroyed) {
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    if (!dag->nodes) {
        dag->nodes = (loom_dag_node_t *)calloc(dag->node_capacity, sizeof(loom_dag_node_t));
        if (!dag->nodes) {
            pthread_mutex_unlock(&dag->lock);
            return LOOMWORKS_ERR_ALLOC;
        }
    }
    loom_dag_node_t *f = &dag->nodes[from];
    loom_dag_node_t *t = &dag->nodes[to];
    if (f->fn == NULL || t->fn == NULL) { /* unknown id */
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    if (from == to) { /* self-edge */
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    for (uint32_t i = 0; i < f->succs_len; i++) { /* duplicate edge */
        if (f->succs[i] == t) {
            pthread_mutex_unlock(&dag->lock);
            return LOOMWORKS_ERR_INVALID;
        }
    }
    if (f->succs_len == f->succs_cap) {
        uint32_t          newcap = f->succs_cap ? f->succs_cap * 2 : 4;
        loom_dag_node_t **ns =
            (loom_dag_node_t **)realloc(f->succs, newcap * sizeof(loom_dag_node_t *));
        if (!ns) {
            pthread_mutex_unlock(&dag->lock);
            return LOOMWORKS_ERR_ALLOC;
        }
        f->succs     = ns;
        f->succs_cap = newcap;
    }
    f->succs[f->succs_len++] = t;
    t->in_degree++;
    pthread_mutex_unlock(&dag->lock);
    return LOOMWORKS_OK;
}

/* ------------------------------------------------------------
 * loom_dag_submit
 *
 * One-shot.  Under dag->lock: Kahn cycle-detection pass over the
 * immutable build-phase in-degrees, seed pending_preds, freeze the
 * counts, then release the roots.  A cycle (or a failed root release)
 * records failed + fail_error and freezes the DAG.  An empty DAG is
 * drained by construction.
 * ------------------------------------------------------------ */
loom_result_t loom_dag_submit(loom_dag_t *dag)
{
    if (!dag) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&dag->lock);
    if (atomic_load_explicit(&dag->submitted, memory_order_acquire) || dag->destroyed) {
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_ERR_INVALID; /* resubmit / destroyed */
    }

    uint32_t added = 0;
    if (dag->nodes) {
        for (uint32_t i = 0; i < dag->node_capacity; i++) {
            if (dag->nodes[i].fn != NULL) {
                added++;
            }
        }
    }

    /* Empty DAG (d): nothing to release; it is drained immediately. */
    if (added == 0) {
        atomic_store_explicit(&dag->added_count, 0, memory_order_release);
        atomic_store_explicit(&dag->submitted, true, memory_order_release);
        pthread_mutex_lock(&dag->done_mu);
        if (dag_drained(dag)) {
            pthread_cond_signal(&dag->done_cv);
        }
        pthread_mutex_unlock(&dag->done_mu);
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_OK;
    }

    /* Kahn pass on a node_id-indexed private copy of the in-degrees (e):
     * capacity-sized so sparse ids are safe; queue sized to the node count. */
    uint32_t *indeg = (uint32_t *)calloc(dag->node_capacity, sizeof(uint32_t));
    uint32_t *queue = (uint32_t *)malloc((size_t)added * sizeof(uint32_t));
    if (!indeg || !queue) {
        free(indeg);
        free(queue);
        atomic_store_explicit(&dag->submitted, true, memory_order_release);
        dag->fail_error = LOOMWORKS_ERR_ALLOC;
        atomic_store_explicit(&dag->failed, true, memory_order_release);
        pthread_mutex_lock(&dag->done_mu);
        if (dag_drained(dag)) {
            pthread_cond_signal(&dag->done_cv);
        }
        pthread_mutex_unlock(&dag->done_mu);
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_ERR_ALLOC;
    }
    uint32_t qt = 0;
    for (uint32_t i = 0; i < dag->node_capacity; i++) {
        if (dag->nodes[i].fn != NULL) {
            indeg[i] = dag->nodes[i].in_degree;
            if (indeg[i] == 0) {
                queue[qt++] = i;
            }
        }
    }
    uint32_t qh = 0;
    while (qh < qt) {
        uint32_t         id = queue[qh++];
        loom_dag_node_t *f  = &dag->nodes[id];
        for (uint32_t j = 0; j < f->succs_len; j++) {
            uint32_t sid = f->succs[j]->node_id;
            if (--indeg[sid] == 0) {
                queue[qt++] = sid;
            }
        }
    }
    bool has_cycle = (qt != added);

    if (has_cycle) {
        atomic_store_explicit(&dag->submitted, true, memory_order_release);
        dag->fail_error = LOOMWORKS_ERR_INVALID;
        atomic_store_explicit(&dag->failed, true, memory_order_release);
        pthread_mutex_lock(&dag->done_mu);
        if (dag_drained(dag)) {
            pthread_cond_signal(&dag->done_cv);
        }
        pthread_mutex_unlock(&dag->done_mu);
        free(indeg);
        free(queue);
        pthread_mutex_unlock(&dag->lock);
        return LOOMWORKS_ERR_INVALID;
    }

    /* Seed the live countdown from the immutable in-degree (before any
     * runner can run) and freeze the counts. */
    for (uint32_t i = 0; i < dag->node_capacity; i++) {
        loom_dag_node_t *n = &dag->nodes[i];
        if (n->fn != NULL) {
            atomic_store_explicit(&n->pending_preds, n->in_degree, memory_order_release);
        }
    }
    atomic_store_explicit(&dag->added_count, added, memory_order_release);
    atomic_store_explicit(&dag->submitted, true, memory_order_release);

    /* Release the roots.  dag_release_node owns the in_flight increment
     * (D4), so the root loop must NOT add it again. */
    loom_result_t rc = LOOMWORKS_OK;
    for (uint32_t i = 0; i < dag->node_capacity; i++) {
        loom_dag_node_t *n = &dag->nodes[i];
        if (n->fn != NULL && n->in_degree == 0) {
            loom_result_t r = dag_release_node(dag, n);
            if (r != LOOMWORKS_OK) {
                rc = r; /* release already recorded failed + signalled */
                break;
            }
        }
    }
    free(indeg);
    free(queue);
    pthread_mutex_unlock(&dag->lock);
    return rc;
}

/* ------------------------------------------------------------
 * loom_dag_wait / loom_dag_wait_timeout
 * ------------------------------------------------------------ */
loom_result_t loom_dag_wait(loom_dag_t *dag)
{
    if (!dag) {
        return LOOMWORKS_ERR_INVALID;
    }
    return dag_wait_internal(dag, NULL);
}

loom_result_t loom_dag_wait_timeout(loom_dag_t *dag, const struct timespec *deadline)
{
    if (!dag) {
        return LOOMWORKS_ERR_INVALID;
    }
    return dag_wait_internal(dag, deadline);
}

/* ------------------------------------------------------------
 * loom_dag_node_future / loom_dag_node_status
 * ------------------------------------------------------------ */
loom_future_t *loom_dag_node_future(loom_dag_node_t *node)
{
    if (!node) {
        return NULL;
    }
    if (!atomic_load_explicit(&node->future_owner, memory_order_acquire)) {
        return NULL; /* never released (or failed before future init, D6) */
    }
    return &node->future;
}

loom_result_t loom_dag_node_status(loom_dag_node_t *node)
{
    if (!node) {
        return LOOMWORKS_ERR_INVALID;
    }
    if (!atomic_load_explicit(&node->future_owner, memory_order_acquire)) {
        return LOOMWORKS_ERR_INVALID; /* never released */
    }
    pthread_mutex_lock(&node->future.mutex);
    loom_result_t rc = node->future.ready ? node->status : LOOMWORKS_ERR_INVALID;
    pthread_mutex_unlock(&node->future.mutex);
    return rc;
}

/* ------------------------------------------------------------
 * loom_dag_destroy
 *
 * Joins in-flight work (blocks until drained), then frees every
 * initialized node future and the structure.  NULL-safe.  Per D7 the
 * run outcome is NOT propagated: a successful teardown returns
 * LOOMWORKS_OK (observe outcomes via wait/wait_timeout/node_status).
 * ------------------------------------------------------------ */
loom_result_t loom_dag_destroy(loom_dag_t *dag)
{
    if (!dag) {
        return LOOMWORKS_OK; /* NULL-safe no-op */
    }
    /* Join.  A never-submitted DAG returns immediately (no block). */
    (void)dag_wait_internal(dag, NULL);

    pthread_mutex_lock(&dag->lock);
    dag->destroyed = true;
    pthread_mutex_unlock(&dag->lock);

    /* After drain no runner is alive, so no signal races a destroy; every
     * future_owner node's future is ready (runner completed it, or the
     * release-failure path completed it per D6). */
    if (dag->nodes) {
        for (uint32_t i = 0; i < dag->node_capacity; i++) {
            loom_dag_node_t *n = &dag->nodes[i];
            if (n->fn != NULL) {
                if (atomic_load_explicit(&n->future_owner, memory_order_acquire)) {
                    loom_future_destroy_embedded(&n->future);
                }
                free(n->succs);
            }
        }
        free(dag->nodes);
    }
    pthread_cond_destroy(&dag->done_cv);
    pthread_mutex_destroy(&dag->done_mu);
    pthread_mutex_destroy(&dag->lock);
    free(dag);
    return LOOMWORKS_OK;
}
