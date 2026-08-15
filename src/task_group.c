/**
 * @file task_group.c
 * @brief Task group implementation.
 *
 * A task group tracks submitted tasks via a linked list keyed by task_id.
 * Every submission is wrapped in a completion wrapper so the group knows
 * exactly when each task finished.  loom_task_group_cancel() walks the
 * list and calls loom_pool_cancel_by_id() for each entry.
 *
 * Tracking key: the group records the *task_id* (not the user_data
 * pointer), so tasks submitted with data == NULL are tracked just like
 * any other task and can be cancelled via the group.
 *
 * completion accounting: the `pending` counter is bumped on submit and
 * dropped exactly once per task — either by the completion wrapper (task
 * ran) or by cancel()/destroy() (task was still queued and cancel_by_id()
 * returned OK — the wrapper is then guaranteed never to run).
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/task_group.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================
 *  Internal wrapper — carries one submitted task plus a back-pointer
 *  to its group.  The wrapper is enqueued in place of the user's fn
 *  so that completion (and therefore the group's pending count) can
 *  be observed from inside the worker thread.
 * ================================================================ */
typedef struct task_group_ctx {
    loom_task_group_t  *group;    /**< Owning group (used on completion). */
    loom_task_fn        fn;       /**< Fire-and-forget task (submit path). */
    loom_task_fn_result fn_result;/**< Result task (submit_future path). */
    void               *data;     /**< User data forwarded to fn/fn_result. */
} task_group_ctx_t;

/* ================================================================
 *  Internal node — tracks one submitted task's task_id.
 * ================================================================ */
typedef struct task_group_node {
    uint64_t                task_id; /**< task_id returned by the pool. */
    task_group_ctx_t       *ctx;     /**< Wrapper context (owned by group). */
    struct task_group_node *next;    /**< Next node in the tracked list. */
} task_group_node_t;

/* ================================================================
 *  loom_task_group — internal structure.
 *
 *  The node list is append-mostly: entries are added on submit and
 *  wholesale-removed by cancel()/destroy().  A mutex keeps concurrent
 *  submitters and cancels from racing on the same list.
 * ================================================================ */
struct loom_task_group {
    loom_thread_pool_t *pool;       /**< Backing thread pool. */
    pthread_mutex_t     lock;       /**< Guards node list + pending. */
    pthread_cond_t      done_cond;  /**< Signaled when pending reaches 0. */
    task_group_node_t  *head;       /**< Head of tracked node list. */
    task_group_node_t  *tail;       /**< Tail of tracked node list. */
    uint32_t            node_count; /**< Number of tracked submissions. */
    uint32_t            pending;    /**< Submissions not yet completed. */
    bool                destroyed;  /**< true once destroy() has been called. */
};

/* Forward declarations: the wrappers are defined below the submit
 * functions but referenced by them. */
static void  task_group_wrapper(void *arg);
static void *task_group_future_wrapper(void *arg);

/* ================================================================
 *  task_group_ctx_create — allocate a wrapper context.
 * ================================================================ */
static task_group_ctx_t *task_group_ctx_create(loom_task_group_t *group, void *data)
{
    task_group_ctx_t *ctx = (task_group_ctx_t *)malloc(sizeof(*ctx));
    if (!ctx) {
        return NULL;
    }
    ctx->group     = group;
    ctx->fn        = NULL;
    ctx->fn_result = NULL;
    ctx->data      = data;
    return ctx;
}

/* ================================================================
 *  task_group_node_create — allocate and initialise a node.
 * ================================================================ */
static task_group_node_t *task_group_node_create(void)
{
    task_group_node_t *node = (task_group_node_t *)malloc(sizeof(*node));
    if (!node) {
        return NULL;
    }
    node->task_id = 0;
    node->ctx     = NULL;
    node->next    = NULL;
    return node;
}

/* ================================================================
 *  task_group_append — link node onto the tracked list and bump the
 *  counters.  Caller holds group->lock.
 * ================================================================ */
static void task_group_append(loom_task_group_t *g, task_group_node_t *node)
{
    if (g->tail) {
        g->tail->next = node;
    } else {
        g->head = node;
    }
    g->tail       = node;
    g->node_count++;
    g->pending++;
}

/* ================================================================
 *  loom_task_group_create
 * ================================================================ */
loom_result_t loom_task_group_create(loom_thread_pool_t *pool, loom_task_group_t **group)
{
    if (!pool || !group) {
        return LOOMWORKS_ERR_INVALID;
    }
    loom_task_group_t *g = (loom_task_group_t *)calloc(1, sizeof(*g));
    if (!g) {
        return LOOMWORKS_ERR_ALLOC;
    }
    g->pool       = pool;
    g->head       = NULL;
    g->tail       = NULL;
    g->node_count = 0;
    g->pending    = 0;
    g->destroyed  = false;
    if (pthread_mutex_init(&g->lock, NULL) != 0) {
        free(g);
        return LOOMWORKS_ERR_ALLOC;
    }
    if (pthread_cond_init(&g->done_cond, NULL) != 0) {
        pthread_mutex_destroy(&g->lock);
        free(g);
        return LOOMWORKS_ERR_ALLOC;
    }
    *group = g;
    return LOOMWORKS_OK;
}

/* ================================================================
 *  task_group_cancel_node — cancel one tracked task.  Caller holds
 *  group->lock.
 *
 *  cancel_by_id() returns OK only while the task is still queued; the
 *  wrapper is then guaranteed never to run, so we free the ctx and
 *  drop the pending count ourselves.  ERR_INVALID means the task was
 *  already dequeued: it is either running (its wrapper will decrement
 *  and free) or finished (its wrapper already did).  Either way we leave
 *  the ctx and the pending count to the wrapper — never double-free.
 * ================================================================ */
static void task_group_cancel_node(loom_task_group_t *g, task_group_node_t *node)
{
    loom_result_t rc = loom_pool_cancel_by_id(g->pool, node->task_id);
    if (rc == LOOMWORKS_OK) {
        /* Cancelled while queued: wrapper will never run on this ctx. */
        if (node->ctx) {
            free(node->ctx);
            node->ctx = NULL;
        }
        g->pending--;
    }
    /* INVALID/SHUTDOWN: wrapper owns the ctx now. */
}

/* ================================================================
 *  task_group_cancel_tracked — cancel every tracked task and empty the
 *  node list.  Caller holds group->lock.
 * ================================================================ */
static void task_group_cancel_tracked(loom_task_group_t *g)
{
    task_group_node_t *cur = g->head;
    while (cur) {
        task_group_node_t *next = cur->next;
        task_group_cancel_node(g, cur);
        free(cur);
        cur = next;
    }
    g->head       = NULL;
    g->tail       = NULL;
    g->node_count = 0;
}

/* ================================================================
 *  loom_task_group_destroy
 *
 *  Cancels every queued task, then waits for in-flight tasks (their
 *  wrappers) to finish before releasing the group.  This guarantees no
 *  worker thread ever touches the group after destroy() returns.
 * ================================================================ */
void loom_task_group_destroy(loom_task_group_t **group)
{
    if (!group || !*group) {
        return;
    }
    loom_task_group_t *g = *group;
    pthread_mutex_lock(&g->lock);
    g->destroyed = true;
    task_group_cancel_tracked(g);
    /* In-flight wrappers decrement pending and free their ctx once they
     * finish; wait for them so the group outlives every worker that
     * references it. */
    while (g->pending > 0) {
        pthread_cond_wait(&g->done_cond, &g->lock);
    }
    pthread_mutex_unlock(&g->lock);
    pthread_cond_destroy(&g->done_cond);
    pthread_mutex_destroy(&g->lock);
    free(g);
    *group = NULL;
}

/* ================================================================
 *  loom_task_group_submit
 *
 *  Allocation order matters: ctx and node are allocated BEFORE the task
 *  is enqueued.  If either allocation fails the task is never submitted,
 *  so a pool submission is always tracked and there is no post-submit
 *  tracking failure to clean up.
 * ================================================================ */
loom_result_t
loom_task_group_submit(loom_task_group_t *group, loom_task_fn fn, void *data, uint64_t *task_id)
{
    if (!group || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&group->lock);
    if (group->destroyed) {
        pthread_mutex_unlock(&group->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    task_group_ctx_t *ctx = task_group_ctx_create(group, data);
    if (!ctx) {
        pthread_mutex_unlock(&group->lock);
        return LOOMWORKS_ERR_ALLOC;
    }
    ctx->fn = fn;
    task_group_node_t *node = task_group_node_create();
    if (!node) {
        free(ctx);
        pthread_mutex_unlock(&group->lock);
        return LOOMWORKS_ERR_ALLOC;
    }

    uint64_t      id = 0;
    loom_result_t rc = loom_pool_submit(group->pool, task_group_wrapper, ctx, &id);
    if (rc != LOOMWORKS_OK) {
        free(node);
        free(ctx);
        pthread_mutex_unlock(&group->lock);
        return rc;
    }
    node->task_id = id;
    node->ctx     = ctx;
    task_group_append(group, node);
    if (task_id) {
        *task_id = id;
    }
    pthread_mutex_unlock(&group->lock);
    return LOOMWORKS_OK;
}

/* ================================================================
 *  task_group_wrapper — completion callback for plain submissions.
 *
 *  Runs the user's fn, then decrements the group's pending count under
 *  the group lock and frees itself.  Ownership of the ctx transfers to
 *  the wrapper the moment the pool dequeues the task: from then on the
 *  group must never free this ctx again (cancel_node() returns early for
 *  dequeued tasks — the wrapper is the only freer).
 * ================================================================ */
static void task_group_wrapper(void *arg)
{
    task_group_ctx_t *ctx = (task_group_ctx_t *)arg;
    ctx->fn(ctx->data);

    loom_task_group_t *g = ctx->group;
    pthread_mutex_lock(&g->lock);
    g->pending--;
    if (g->pending == 0) {
        pthread_cond_broadcast(&g->done_cond);
    }
    pthread_mutex_unlock(&g->lock);
    free(ctx);
}

/* ================================================================
 *  task_group_future_wrapper — completion callback for future
 *  submissions.  Same discipline as task_group_wrapper but forwards the
 *  task's result to the pool's future machinery.
 * ================================================================ */
static void *task_group_future_wrapper(void *arg)
{
    task_group_ctx_t *ctx    = (task_group_ctx_t *)arg;
    void             *result = ctx->fn_result(ctx->data);

    loom_task_group_t *g = ctx->group;
    pthread_mutex_lock(&g->lock);
    g->pending--;
    if (g->pending == 0) {
        pthread_cond_broadcast(&g->done_cond);
    }
    pthread_mutex_unlock(&g->lock);
    free(ctx);
    return result;
}

/* ================================================================
 *  loom_task_group_submit_future — same ordering discipline as
 *  loom_task_group_submit.
 * ================================================================ */
loom_result_t loom_task_group_submit_future(loom_task_group_t  *group,
                                            loom_task_fn_result fn,
                                            void               *data,
                                            loom_future_t     **future,
                                            uint64_t           *task_id)
{
    if (!group || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&group->lock);
    if (group->destroyed) {
        pthread_mutex_unlock(&group->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    task_group_ctx_t *ctx = task_group_ctx_create(group, data);
    if (!ctx) {
        pthread_mutex_unlock(&group->lock);
        return LOOMWORKS_ERR_ALLOC;
    }
    ctx->fn_result = fn;
    task_group_node_t *node = task_group_node_create();
    if (!node) {
        free(ctx);
        pthread_mutex_unlock(&group->lock);
        return LOOMWORKS_ERR_ALLOC;
    }

    uint64_t      id = 0;
    loom_result_t rc =
        loom_pool_submit_future(group->pool, task_group_future_wrapper, ctx, future, &id);
    if (rc != LOOMWORKS_OK) {
        free(node);
        free(ctx);
        pthread_mutex_unlock(&group->lock);
        return rc;
    }
    node->task_id = id;
    node->ctx     = ctx;
    task_group_append(group, node);
    if (task_id) {
        *task_id = id;
    }
    pthread_mutex_unlock(&group->lock);
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_task_group_cancel
 * ================================================================ */
void loom_task_group_cancel(loom_task_group_t *group)
{
    if (!group) {
        return;
    }
    pthread_mutex_lock(&group->lock);
    /* Destructive: the tracking list is emptied, so tasks submitted AFTER
     * this call start a fresh tracking set and are NOT cancelled here.
     * Queued tasks are cancelled by task_id; in-flight tasks finish on
     * their own (their wrappers still decrement pending, so wait() and
     * destroy() observe completion correctly). */
    task_group_cancel_tracked(group);
    pthread_mutex_unlock(&group->lock);
}

/* ================================================================
 *  loom_task_group_wait
 *
 *  Waits until every task submitted to the group has completed.  Unlike
 *  the historical behaviour this does NOT shut down the backing pool —
 *  the pool stays fully usable for new submissions after wait().
 * ================================================================ */
void loom_task_group_wait(loom_task_group_t *group)
{
    if (!group) {
        return;
    }
    pthread_mutex_lock(&group->lock);
    while (group->pending > 0) {
        pthread_cond_wait(&group->done_cond, &group->lock);
    }
    pthread_mutex_unlock(&group->lock);
}

/* ================================================================
 *  loom_task_group_pending_count
 * ================================================================ */
uint32_t loom_task_group_pending_count(const loom_task_group_t *group)
{
    if (!group) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&group->lock);
    uint32_t n = group->node_count;
    pthread_mutex_unlock((pthread_mutex_t *)&group->lock);
    return n;
}