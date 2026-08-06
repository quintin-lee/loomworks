/**
 * @file metrics.c
 * @brief Task execution metrics implementation.
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/metrics.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>

struct loom_metrics {
    loom_thread_pool_t *pool;
    loom_metric_fn      cb;
    void               *user_data;
    pthread_mutex_t     lock;
    /* Atomically updated counters */
    uint64_t submitted;
    uint64_t completed;
    uint64_t cancelled;
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
    m->submitted = 0;
    m->completed = 0;
    m->cancelled = 0;
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
    return metrics->submitted;
}

uint64_t loom_metrics_completed(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return metrics->completed;
}

uint64_t loom_metrics_cancelled(const loom_metrics_t *metrics)
{
    if (!metrics) {
        return 0;
    }
    return metrics->cancelled;
}

void loom_metrics_fire(loom_metrics_t *metrics, loom_metric_event_t event)
{
    if (!metrics) {
        return;
    }
    switch (event) {
    case LOOMWORKS_METRIC_SUBMITTED:
        metrics->submitted++;
        break;
    case LOOMWORKS_METRIC_COMPLETED:
        metrics->completed++;
        break;
    case LOOMWORKS_METRIC_CANCELLED:
        metrics->cancelled++;
        break;
    default:
        break;
    }
    fprintf(stderr,
            "DEBUG loom_metrics_fire: event=%d submitted=%llu completed=%llu cancelled=%llu\n",
            (int)event,
            (unsigned long long)metrics->submitted,
            (unsigned long long)metrics->completed,
            (unsigned long long)metrics->cancelled);
    if (event == LOOMWORKS_METRIC_SUBMITTED) {
        metrics->submitted++;
    } else if (event == LOOMWORKS_METRIC_COMPLETED) {
        metrics->completed++;
    } else if (event == LOOMWORKS_METRIC_CANCELLED) {
        metrics->cancelled++;
    }
    fprintf(stderr,
            "DEBUG after: submitted=%llu completed=%llu cancelled=%llu\n",
            (unsigned long long)metrics->submitted,
            (unsigned long long)metrics->completed,
            (unsigned long long)metrics->cancelled);
    if (metrics->cb) {
        metrics->cb(event, metrics->pool, metrics->user_data);
    }
}
