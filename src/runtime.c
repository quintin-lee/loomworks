#define _POSIX_C_SOURCE 200809L
#include "loomworks/runtime.h"

#include <stdlib.h>
#include <string.h>

/* ================================================================
 *  Internal layout
 *
 *  loom_runtime_t holds a single backing loom_thread_pool_t.
 *  coro_stack_size is stored here so submit_coro can pass it through
 *  without needing an extra config query path.
 * ================================================================ */
struct loom_runtime {
    loom_thread_pool_t *pool;
    size_t              coro_stack_size; /* 0 = use coroutine default */
};

/* ================================================================
 *  create / destroy
 * ================================================================ */
loom_result_t loom_runtime_create(const loom_runtime_config_t *cfg, loom_runtime_t **out)
{
    if (!out) {
        return LOOMWORKS_ERR_INVALID;
    }

    loom_runtime_t *rt = (loom_runtime_t *)calloc(1, sizeof(*rt));
    if (!rt) {
        *out = NULL;
        return LOOMWORKS_ERR_ALLOC;
    }

    loom_pool_config_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    if (cfg) {
        pcfg.worker_count   = cfg->worker_count;
        pcfg.stack_size     = cfg->stack_size;
        pcfg.queue_capacity = cfg->queue_capacity;
        rt->coro_stack_size = cfg->coro_stack_size;
    } else {
        rt->coro_stack_size = 0;
    }

    loom_result_t rc = loom_pool_create(&pcfg, &rt->pool);
    if (rc != LOOMWORKS_OK) {
        free(rt);
        *out = NULL;
        return rc;
    }

    *out = rt;
    return LOOMWORKS_OK;
}

void loom_runtime_destroy(loom_runtime_t **rt)
{
    if (!rt) {
        return;
    }
    loom_runtime_t *r = *rt;
    if (!r) {
        return; /* idempotent no-op */
    }
    loom_pool_shutdown(r->pool);
    loom_pool_destroy(&r->pool);
    free(r);
    *rt = NULL;
}

/* ================================================================
 *  submit
 * ================================================================ */
loom_result_t loom_runtime_submit(loom_runtime_t    *rt,
                                  loom_fn_union_t    fn,
                                  void              *data,
                                  loom_submit_flag_t flag,
                                  uint8_t            priority,
                                  uint64_t          *task_id)
{
    if (!rt || !rt->pool) {
        return LOOMWORKS_ERR_INVALID;
    }

    if (flag == LOOM_SUBMIT_CORO) {
        return loom_pool_submit_coroutine(rt->pool, fn.coro_fn, data, rt->coro_stack_size, task_id);
    }

    /* LOOM_SUBMIT_THREAD — pass priority to the pool lane system.
     * Thread tasks use priorities 0-7 (REALTIME..LOW); coroutine tasks
     * are routed to the per-worker ready FIFO (priority 8+ layer) at
     * the pool level via loom_pool_submit_coroutine(). */
    return loom_pool_submit_priority(rt->pool, fn.thread_fn, data, priority, task_id);
}

/* ================================================================
 *  cancel / cancel_all
 * ================================================================ */
loom_result_t loom_runtime_cancel(loom_runtime_t *rt, uint64_t task_id)
{
    if (!rt || !rt->pool) {
        return LOOMWORKS_ERR_INVALID;
    }
    return loom_pool_cancel_by_id(rt->pool, task_id);
}

void loom_runtime_cancel_all(loom_runtime_t *rt, uint32_t *count)
{
    if (!rt || !rt->pool) {
        return;
    }
    loom_pool_cancel_all(rt->pool, count);
}

/* ================================================================
 *  queries (thin delegates)
 * ================================================================ */
uint32_t loom_runtime_worker_count(const loom_runtime_t *rt)
{
    return rt ? loom_pool_worker_count(rt->pool) : 0;
}
uint32_t loom_runtime_pending_count(const loom_runtime_t *rt)
{
    return rt ? loom_pool_pending_count(rt->pool) : 0;
}
uint32_t loom_runtime_active_count(const loom_runtime_t *rt)
{
    return rt ? loom_pool_active_count(rt->pool) : 0;
}
uint32_t loom_runtime_idle_count(const loom_runtime_t *rt)
{
    return rt ? loom_pool_idle_count(rt->pool) : 0;
}
double loom_runtime_utilization(const loom_runtime_t *rt)
{
    return rt ? loom_pool_utilization(rt->pool) : 0.0;
}
void loom_runtime_shutdown(loom_runtime_t *rt)
{
    if (rt && rt->pool) {
        loom_pool_shutdown(rt->pool);
    }
}
