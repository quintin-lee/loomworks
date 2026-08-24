#define _POSIX_C_SOURCE 200809L
#include "loomworks/runtime.h"
#include "loomworks/metrics_shm.h"
#include "thread_pool_internal.h"

#include <stdlib.h>
#include <string.h>

/* ================================================================
 *  Internal layout
 *
 *  loom_runtime_t holds a single backing loom_thread_pool_t and an
 *  optional shared-memory metrics region.
 * ================================================================ */
struct loom_runtime {
    loom_thread_pool_t *pool;
    size_t              coro_stack_size; /* 0 = use coroutine default */
    loom_metrics_shm_t *shm;             /* NULL when shm_name was not set */
    char                shm_name[128];   /* copy of user-provided name      */
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

    /* Copy shm_name with bounds checking. */
    if (cfg && cfg->shm_name) {
        size_t len = strlen(cfg->shm_name);
        if (len >= sizeof(rt->shm_name)) {
            len = sizeof(rt->shm_name) - 1;
        }
        memcpy(rt->shm_name, cfg->shm_name, len + 1);
    } else {
        rt->shm_name[0] = '\0';
    }

    loom_result_t rc = loom_pool_create(&pcfg, &rt->pool);
    if (rc != LOOMWORKS_OK) {
        free(rt);
        *out = NULL;
        return rc;
    }

    /* Attach shared-memory metrics region if a name was provided. */
    if (rt->shm_name[0] != '\0') {
        rc = (loom_result_t)loom_metrics_shm_create(rt->shm_name, &rt->shm);
        if (rc != LOOMWORKS_OK) {
            loom_pool_destroy(&rt->pool);
            free(rt);
            *out = NULL;
            return rc;
        }
        loom_pool_attach_metrics_shm(
            rt->pool, rt->shm, (void (*)(loom_metric_event_t, void *))loom_metrics_shm_write);
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
    if (r->shm) {
        loom_metrics_shm_destroy(r->shm_name, r->shm);
    }
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

    /* Thread tasks submit through the priority lane system (REALTIME=0,
     * HIGH=1, NORMAL=5, LOW=10; values 2-4 and 6-9 and 11-255 are free).
     * Coroutine tasks bypass the lane system entirely — each worker owns
     * a dedicated ready-FIFO so there is no priority contention. */
    return loom_pool_submit_priority(rt->pool, fn.thread_fn, data, priority, task_id);
}

loom_result_t loom_runtime_submit_future(loom_runtime_t     *rt,
                                         loom_task_fn_result fn,
                                         void               *data,
                                         loom_future_t     **future,
                                         uint64_t           *task_id)
{
    if (!rt || !rt->pool || !future || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    return loom_pool_submit_future(rt->pool, fn, data, future, task_id);
}

loom_result_t loom_runtime_submit_blocking(loom_runtime_t    *rt,
                                           loom_fn_union_t    fn,
                                           void              *data,
                                           loom_submit_flag_t flag,
                                           uint8_t            priority,
                                           uint64_t          *task_id)
{
    if (!rt || !rt->pool) {
        return LOOMWORKS_ERR_INVALID;
    }
    if (flag != LOOM_SUBMIT_THREAD) {
        return LOOMWORKS_ERR_INVALID;
    }
    (void)priority; /* Blocking submit always uses NORMAL priority. */
    return loom_pool_submit_blocking(rt->pool, fn.thread_fn, data, task_id);
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
 *  resize / metrics
 * ================================================================ */
loom_result_t loom_runtime_resize(loom_runtime_t *rt, uint32_t count)
{
    if (!rt || !rt->pool || count == 0) {
        return LOOMWORKS_ERR_INVALID;
    }
    return loom_pool_resize(rt->pool, count);
}

void loom_runtime_set_metrics_callback(loom_runtime_t *rt, loom_metric_fn cb, void *user_data)
{
    if (!rt || !rt->pool) {
        return;
    }
    loom_pool_set_metrics_callback(rt->pool, cb, user_data);
}

loom_result_t loom_runtime_metrics_snapshot(const loom_runtime_t *rt, loom_metrics_shm_t *out)
{
    if (!rt || !rt->shm || !out) {
        return LOOMWORKS_ERR_INVALID;
    }
    /* memcpy gives an eventually-consistent snapshot — each field is a
     * valid value but they may come from different points in time.
     * This is the documented trade-off and sufficient for monitoring. */
    memcpy(out, rt->shm, sizeof(*out));
    return LOOMWORKS_OK;
}

/* ================================================================
 *  queries (thin delegates)
 * ================================================================ */
/**
 * @brief Return the number of worker threads configured in the runtime.
 */
 uint32_t loom_runtime_worker_count(const loom_runtime_t *rt)
 {
     return rt ? loom_pool_worker_count(rt->pool) : 0;
 }
/**
 * @brief Return the total number of tasks currently queued across all
 *        queues (ring + lanes + per-worker deques).
 */
 uint32_t loom_runtime_pending_count(const loom_runtime_t *rt)
 {
     return rt ? loom_pool_pending_count(rt->pool) : 0;
 }
/**
 * @brief Return the number of workers currently executing a task.
 */
 uint32_t loom_runtime_active_count(const loom_runtime_t *rt)
 {
     return rt ? loom_pool_active_count(rt->pool) : 0;
 }
/**
 * @brief Return the number of idle workers (worker_count - active).
 */
 uint32_t loom_runtime_idle_count(const loom_runtime_t *rt)
 {
     return rt ? loom_pool_idle_count(rt->pool) : 0;
 }
/**
 * @brief Return current utilization as active / worker_count (0.0 when
 *        worker_count is zero).
 */
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
/**
 * @brief Return the backing thread pool handle (useful for low-level
 *        diagnostics or direct pool API usage).
 */
 loom_thread_pool_t *loom_runtime_pool(const loom_runtime_t *rt)
 {
     return rt ? rt->pool : NULL;
 }
/**
 * @brief Return the shared-memory metrics region, or NULL if no name
 *        was configured at creation time.
 */
 loom_metrics_shm_t *loom_runtime_shm(const loom_runtime_t *rt)
 {
     return rt ? rt->shm : NULL;
 }

loom_metrics_shm_t *loom_runtime_shm(const loom_runtime_t *rt)
{
    return rt ? rt->shm : NULL;
}
