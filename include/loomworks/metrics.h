#ifndef LOOMWORKS_METRICS_H
#define LOOMWORKS_METRICS_H

/**
 * @file metrics.h
 * @brief Task execution metrics with optional callbacks.
 *
 * The metrics system tracks per-pool counters (submitted, completed,
 * cancelled, failed) and invokes optional user callbacks on each
 * event.  All counters are atomically updated for thread safety.
 */

#include "loomworks/thread_pool.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Metric event types.
 */
typedef enum {
    LOOMWORKS_METRIC_SUBMITTED, /**< A task was submitted. */
    LOOMWORKS_METRIC_COMPLETED, /**< A task finished successfully. */
    LOOMWORKS_METRIC_CANCELLED, /**< A task was cancelled before execution. */
    LOOMWORKS_METRIC_FAILED,    /**< A task raised an exception (reserved). */
} loom_metric_event_t;

/**
 * @brief Callback invoked on each metric event.
 *
 * @param event      The event type.
 * @param pool       The pool that generated the event.
 * @param user_data  Opaque pointer provided at registration time.
 */
typedef void (*loom_metric_fn)(loom_metric_event_t       event,
                               const loom_thread_pool_t *pool,
                               void                     *user_data);

/**
 * @brief Opaque metrics handle.
 */
typedef struct loom_metrics loom_metrics_t;

/**
 * @brief Create a metrics collector attached to a pool.
 *
 * @param pool   The thread pool to observe.
 * @param cb     Callback function (may be NULL).
 * @param data   Opaque user data passed to the callback.
 * @param out    Output pointer for the metrics handle.
 * @return       LOOMWORKS_OK on success.
 */
loom_result_t
loom_metrics_create(loom_thread_pool_t *pool, loom_metric_fn cb, void *data, loom_metrics_t **out);

/**
 * @brief Destroy a metrics collector.
 *
 * @param metrics  Pointer to the metrics handle (NULL-safe).
 */
void loom_metrics_destroy(loom_metrics_t **metrics);

/**
 * @brief Get the number of submitted tasks.
 */
uint64_t loom_metrics_submitted(const loom_metrics_t *metrics);

/**
 * @brief Get the number of completed tasks.
 */
uint64_t loom_metrics_completed(const loom_metrics_t *metrics);

/**
 * @brief Get the number of cancelled tasks.
 */
uint64_t loom_metrics_cancelled(const loom_metrics_t *metrics);

/**
 * @brief Register a metrics callback on the pool.
 *
 * @param pool       The pool handle.
 * @param cb         Callback function (NULL to unregister).
 * @param user_data  Opaque data passed to the callback.
 */
void loom_pool_set_metrics_callback(loom_thread_pool_t *pool, loom_metric_fn cb, void *user_data);

/**
 * @brief Attach a metrics collector to a pool for counter tracking.
 */
void loom_pool_set_metrics(loom_thread_pool_t *pool, loom_metrics_t *metrics);

/**
 * @brief Fire a metric event through the attached metrics collector.
 */
void loom_metrics_fire(loom_metrics_t *metrics, loom_metric_event_t event);

#ifdef __cplusplus
}
#endif

#endif /* LOOMWORKS_METRICS_H */
