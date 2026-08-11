/**
 * @file metrics.c
 * @brief Task execution metrics implementation.
 *
 * Design: worker threads update the counters lock-free via C11 atomics so
 * the hot path never contends; the embedded mutex exists solely to give
 * loom_metrics_snapshot() a consistent cross-section of every counter at
 * once (a single atomic load per counter is not a coherent snapshot).
 * The callback (cb) is invoked by loom_metrics_fire() on the *worker*
 * thread, so it must be cheap and non-blocking.
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/metrics.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>

struct loom_metrics {
    loom_thread_pool_t *pool; /* Pool that generated the events (for callbacks). */
    loom_metric_fn      cb;   /* Optional per-event callback, fired on worker threads. */
    void               *user_data; /* Opaque argument passed to cb. */
    pthread_mutex_t     lock;      /* Guards snapshot() reads only — never the hot path. */
    /* Lock-free counters, updated with relaxed/fetch_add by workers. */
    _Atomic uint64_t submitted;
    _Atomic uint64_t completed;
    _Atomic uint64_t cancelled;
    _Atomic uint64_t started;
    _Atomic uint64_t failed;
    _Atomic uint64_t latency_sum_ns; /* Sum of per-task execution latencies. */
    _Atomic uint64_t latency_max_ns; /* High-water mark, CAS-guarded. */
};

loom_result_t
loom_metrics_create(loom_thread_pool_t *pool, loom_metric_fn cb, void *data, loom_metrics_t **out)
{
    if (!pool || !out) {
        return LOOMWORKS_ERR_INVALID;
    }
    loom_metrics_t *m = (loom_metrics_t *)calloc(1, sizeof(*m));
    if (!m) {
        return LOOMWORKS_ERR_ALLOC;
    }
    m->pool      = pool;
    m->cb        = cb;
    m->user_data = data;
    /* calloc zeroed the counters; the stores keep the intent explicit. */
    atomic_store(&m->submitted, 0);
    atomic_store(&m->completed, 0);
    atomic_store(&m->cancelled, 0);
    atomic_store(&m->latency_sum_ns, 0);
    atomic_store(&m->latency_max_ns, 0);
    /* Register callback on the pool via public API */
    if (pool) {
        loom_pool_set_metrics_callback(pool, cb, data);
        loom_pool_set_metrics(pool, m);
    }
    if (pthread_mutex_init(&m->lock, NULL) != 0) {
        free(m);
        return LOOMWORKS_ERR_ALLOC;
    }
    *out = m;
    return LOOMWORKS_OK;
}

void loom_metrics_destroy(loom_metrics_t **metrics)
{
    if (!metrics || !*metrics) {
        return;
    }
    loom_metrics_t *m = *metrics;
    /* Clear pool reference first so subsequent cleanup calls
     * see a NULL pool and skip the UAF-prone API calls. */
    m->pool = NULL;
    pthread_mutex_destroy(&m->lock);
    free(m);
    *metrics = NULL;
}

uint64_t loom_metrics_submitted(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return atomic_load(&metrics->submitted);
}

uint64_t loom_metrics_completed(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return atomic_load(&metrics->completed);
}

uint64_t loom_metrics_cancelled(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return atomic_load(&metrics->cancelled);
}

uint64_t loom_metrics_latency_sum_ns(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return atomic_load(&metrics->latency_sum_ns);
}

uint64_t loom_metrics_latency_max_ns(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return atomic_load(&metrics->latency_max_ns);
}

uint64_t loom_metrics_started(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return atomic_load(&metrics->started);
}

uint64_t loom_metrics_failed(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return atomic_load(&metrics->failed);
}

uint64_t loom_metrics_avg_latency_ns(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    /* Sum and count may momentarily disagree (two separate atomic loads);
     * that is fine for an average, unlike a strict invariant check. */
    uint64_t completed = atomic_load(&metrics->completed);
    if (completed == 0) {
        return 0;
    }
    return atomic_load(&metrics->latency_sum_ns) / completed;
}

loom_result_t
loom_metrics_snapshot(const loom_metrics_t *metrics, loom_metrics_snapshot_t *out)
{
    if (!metrics || !out) {
        return LOOMWORKS_ERR_INVALID;
    }
    /* NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast) */
    loom_metrics_t *m = (loom_metrics_t *)metrics;
    /* The mutex serializes snapshot() readers against each other.  It does
     * NOT lock out counter updates (workers never take it), so the snapshot
     * is "torn" across events but each field is a valid value; this is the
     * documented trade-off — a fully coherent snapshot would need the lock
     * on every worker update. */
    pthread_mutex_lock(&m->lock);
    out->submitted      = atomic_load(&m->submitted);
    out->started        = atomic_load(&m->started);
    out->completed      = atomic_load(&m->completed);
    out->cancelled      = atomic_load(&m->cancelled);
    out->failed         = atomic_load(&m->failed);
    out->latency_sum_ns = atomic_load(&m->latency_sum_ns);
    out->latency_max_ns = atomic_load(&m->latency_max_ns);
    pthread_mutex_unlock(&m->lock);
    return LOOMWORKS_OK;
}

void loom_metrics_record_latency(loom_metrics_t *metrics, uint64_t latency_ns)
{
    if (!metrics) {
        return;
    }
    /* Sum is a plain monotonic accumulation — no contention concern. */
    atomic_fetch_add(&metrics->latency_sum_ns, latency_ns);
    /* Max is a lock-free high-water mark: optimistic read, then CAS only
     * when we actually beat the current value.  The loop retries on lost
     * races; on success latency_max only ever increases. */
    uint64_t old_max = atomic_load(&metrics->latency_max_ns);
    while (latency_ns > old_max &&
           !atomic_compare_exchange_weak(&metrics->latency_max_ns, &old_max, latency_ns))
        ;
}

void loom_metrics_fire(loom_metrics_t *metrics, loom_metric_event_t event)
{
    if (!metrics) {
        return;
    }
    /* Every event type maps to exactly one counter; the callback (if any)
     * runs synchronously on the firing thread, so keep it short. */
    switch (event) {
    case LOOMWORKS_METRIC_SUBMITTED:
        atomic_fetch_add(&metrics->submitted, 1);
        break;
    case LOOMWORKS_METRIC_COMPLETED:
        atomic_fetch_add(&metrics->completed, 1);
        break;
    case LOOMWORKS_METRIC_CANCELLED:
        atomic_fetch_add(&metrics->cancelled, 1);
        break;
    case LOOMWORKS_METRIC_STARTED:
        atomic_fetch_add(&metrics->started, 1);
        break;
    case LOOMWORKS_METRIC_FAILED:
        atomic_fetch_add(&metrics->failed, 1);
        break;
    default:
        break;
    }
    if (metrics->cb) {
        metrics->cb(event, metrics->pool, metrics->user_data);
    }
}
