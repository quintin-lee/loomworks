/**
 * @file metrics.c
 * @brief Task execution metrics implementation.
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/metrics.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>

struct loom_metrics {
    loom_thread_pool_t *pool;
    loom_metric_fn      cb;
    void               *user_data;
    pthread_mutex_t     lock;
    /* Thread-safe counters, updated via atomic ops in worker threads */
    _Atomic uint64_t submitted;
    _Atomic uint64_t completed;
    _Atomic uint64_t cancelled;
    _Atomic uint64_t latency_sum_ns;
    _Atomic uint64_t latency_max_ns;
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
    /* Clear callback on the pool via public API */
    if (m->pool) {
        loom_pool_set_metrics(m->pool, NULL);
        loom_pool_set_metrics_callback(m->pool, NULL, NULL);
    }
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

void loom_metrics_record_latency(loom_metrics_t *metrics, uint64_t latency_ns)
{
    if (!metrics) {
        return;
    }
    atomic_fetch_add(&metrics->latency_sum_ns, latency_ns);
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
    default:
        break;
    }
    if (metrics->cb) {
        metrics->cb(event, metrics->pool, metrics->user_data);
    }
}
