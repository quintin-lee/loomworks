#ifndef LOOMWORKS_THREAD_POOL_BACKPRESSURE_H
#define LOOMWORKS_THREAD_POOL_BACKPRESSURE_H

#include "loomworks/thread_pool.h"

#include <stdint.h>

/** Backpressure events sent to the callback. */
typedef enum {
    LOOM_BACKPRESSURE_QUEUE_HIGH,    /**< Queue depth exceeded threshold */
    LOOM_BACKPRESSURE_QUEUE_BLOCKED, /**< Submit blocked by queue depth */
} loom_backpressure_event_t;

/** Configuration for backpressure thresholds. */
typedef struct loom_backpressure_config {
    double  queue_depth_warn_ratio; /**< Default: 0.8 (80%) */
    int64_t queue_wait_timeout_ns;  /**< Default: 60s */
} loom_backpressure_config_t;

/** Callback signature for backpressure events. */
typedef void (*loom_backpressure_fn)(void *ctx, loom_backpressure_event_t event);

/**
 * @brief Configure backpressure thresholds.
 */
void loom_pool_set_backpressure_config(loom_thread_pool_t               *pool,
                                       const loom_backpressure_config_t *cfg);

/**
 * @brief Register a callback for backpressure events.
 *
 * The callback is invoked from worker threads; it must be async-signal-safe
 * and execute quickly (no blocking, no malloc).
 *
 * @param pool The thread pool handle.
 * @param cb   Callback function (NULL to unregister).
 * @param ctx  User context passed to the callback.
 */
void loom_pool_set_backpressure_callback(loom_thread_pool_t  *pool,
                                         loom_backpressure_fn cb,
                                         void                *ctx);

#endif /* LOOMWORKS_THREAD_POOL_BACKPRESSURE_H */
