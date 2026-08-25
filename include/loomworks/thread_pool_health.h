#ifndef LOOMWORKS_THREAD_POOL_HEALTH_H
#define LOOMWORKS_THREAD_POOL_HEALTH_H

#include "loomworks/thread_pool.h"

#include <stdint.h>
#include <stdlib.h>

/** Health status snapshot returned by loom_pool_health_sample(). */
typedef struct loom_health_status {
    uint32_t worker_count;     /**< Total configured workers. */
    uint32_t active_count;     /**< Workers currently executing a task. */
    uint32_t pending_count;    /**< Tasks waiting in queue. */
    uint32_t abnormal_workers; /**< Workers that exited abnormally. */
    int64_t  uptime_ns;        /**< Pool uptime in nanoseconds (CLOCK_MONOTONIC). */
    double   utilization;      /**< active_count / worker_count (0.0-1.0). */
} loom_health_status_t;

/**
 * @brief Synchronously sample pool health status.
 *
 * Acquires the pool lock briefly for a consistent snapshot.
 * Safe to call from any thread at any time.
 *
 * @param pool The thread pool to sample.
 * @param out  Output pointer for the health status snapshot.
 * @return     LOOMWORKS_OK on success, LOOMWORKS_ERR_INVALID if pool or out is NULL.
 */
loom_result_t loom_pool_health_sample(loom_thread_pool_t *pool, loom_health_status_t *out);

#endif /* LOOMWORKS_THREAD_POOL_HEALTH_H */
