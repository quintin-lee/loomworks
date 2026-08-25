#ifndef LOOMWORKS_THREAD_POOL_HEALTH_H
#define LOOMWORKS_THREAD_POOL_HEALTH_H

#include "loomworks/thread_pool.h"
#include <stdint.h>
#include <stdbool.h>

/** Health status snapshot returned by loom_pool_health_check(). */
typedef struct loom_health_status {
    uint32_t worker_count;          /**< Total configured workers. */
    uint32_t active_count;          /**< Workers currently executing a task. */
    uint32_t pending_count;         /**< Tasks waiting in queue. */
    uint32_t abnormal_workers;      /**< Workers that exited abnormally. */
    int64_t  uptime_ns;             /**< Pool uptime in nanoseconds (CLOCK_MONOTONIC). */
    double   utilization;           /**< active_count / worker_count (0.0–1.0). */
} loom_health_status_t;

/** Opaque health monitor handle. */
typedef struct loom_pool_health loom_pool_health_t;

/**
 * @brief Create a health monitor for a thread pool.
 *
 * Spawns a background monitor thread that samples pool state every 100ms.
 * The monitor is detached and cleaned up when loom_pool_health_destroy()
 * is called (which must happen before loom_pool_destroy()).
 *
 * @param pool The thread pool to monitor.
 * @param out  Output pointer for the created health monitor handle.
 * @return LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_pool_health_create(loom_thread_pool_t *pool,
                                       loom_pool_health_t **out);

/**
 * @brief Get a consistent snapshot of pool health.
 *
 * Returns the last sampled state. Callers can invoke this from any thread
 * at any time; the sample is lock-free.
 *
 * @param h    The health monitor handle.
 * @param out  Output pointer for the health status snapshot.
 * @return LOOMWORKS_OK on success.
 */
loom_result_t loom_pool_health_check(const loom_pool_health_t *h,
                                      loom_health_status_t *out);

/**
 * @brief Destroy a health monitor and stop the background thread.
 *
 * Safe to call multiple times (idempotent). Set @p h to NULL on return.
 *
 * @param h Pointer to the health monitor handle (NULL-safe).
 */
void loom_pool_health_destroy(loom_pool_health_t **h);

#endif /* LOOMWORKS_THREAD_POOL_HEALTH_H */
