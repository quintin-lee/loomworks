#ifndef LOOMWORKS_METRICS_SHM_H
#define LOOMWORKS_METRICS_SHM_H

/**
 * @file metrics_shm.h
 * @brief Lock-free metrics counters mapped into POSIX shared memory.
 *
 * This header defines a fixed-size, pointer-free struct that can be
 * mmap'd with MAP_SHARED onto /dev/shm/loomworks_<name>.  Workers
 * update every field with lock-free C11 atomics; an external analysis
 * tool maps the same region and reads counters directly — no library
 * link required on the reader side.
 *
 * Layout invariant: all counters are `_Atomic uint64_t` laid out
 * consecutively so an external reader can also do a single memcpy of
 * the whole struct for a fast (eventually-consistent) snapshot.
 *
 * Naming:  /dev/shm/loomworks_<name>
 *           where <name> is the user-supplied string from
 *           loom_runtime_config_t.name.
 */

#include <stdint.h>

#include "loomworks/metrics.h" /* loom_metric_event_t */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Fixed-size metrics counters — safe to memcpy for a snapshot.
 *
 * Every field is an `_Atomic uint64_t` updated lock-free by worker
 * threads.  An external reader may load each field individually or
 * memcpy the whole struct; in either case values are always valid
 * (monotonically non-decreasing counters).
 */
typedef struct {
    /* Event counters — all monotonically non-decreasing. */
    _Atomic uint64_t submitted; /**< Tasks submitted to the pool. */
    _Atomic uint64_t started;   /**< Tasks that began executing.     */
    _Atomic uint64_t completed; /**< Tasks finished successfully.  */
    _Atomic uint64_t cancelled; /**< Tasks cancelled before start. */
    _Atomic uint64_t failed;    /**< Workers that exited abnormally.*/

    /* Latency stats (nanoseconds). */
    _Atomic uint64_t latency_sum_ns; /**< Sum of all completed-task latencies. */
    _Atomic uint64_t latency_max_ns; /**< High-water mark across all tasks. */
} loom_metrics_shm_t;

/**
 * @brief Create (or open) the shared-memory metrics segment for @p name.
 *
 * Creates `/dev/shm/loomworks_<name>` (or `/loomworks_<name>` on
 * platforms without /dev/shm) with exactly one mapping per unique name.
 * Calling this multiple times with the same name returns the existing
 * region; calling with a different name creates a new segment.
 *
 * The returned pointer must be passed to every subsequent
 * loom_metrics_shm_* call and eventually to
 * loom_metrics_shm_destroy().
 *
 * @param name   Unique identifier for this runtime instance.
 *               Must be non-NULL and contain only [a-zA-Z0-9_].
 *               Max length: 127 chars (leaves room for prefix + NUL).
 * @param out    Output pointer for the mapped region.
 * @return       LOOMWORKS_OK on success, LOOMWORKS_ERR_ALLOC on failure.
 */
int loom_metrics_shm_create(const char *name, loom_metrics_shm_t **out);

/**
 * @brief Destroy the shared-memory metrics segment.
 *
 * Unmaps the region and calls shm_unlink() so the kernel reclaims the
 * backing store.  Safe to call multiple times (idempotent).
 *
 * @param name   Must match the name used in loom_metrics_shm_create().
 * @param ptr    The pointer returned by loom_metrics_shm_create() (NULL-safe).
 */
void loom_metrics_shm_destroy(const char *name, loom_metrics_shm_t *ptr);

/**
 * @brief Write a single metric event into the shared-memory counters.
 *
 * Called from worker threads; uses lock-free atomics so no locking is
 * needed.  Safe to call concurrently from many workers.
 */
void loom_metrics_shm_write(loom_metric_event_t event, loom_metrics_shm_t *shm);

/**
 * @brief Read a consistent cross-section of all counters.
 *
 * Uses the internal mutex (see metrics.h) to serialise reads.  All
 * fields in @p out are mutually consistent at the instant the lock is
 * released.
 *
 * @param shm    The shared-memory region.
 * @param out    Output snapshot (caller-allocated).
 * @return       0 on success, -1 on NULL args.
 */
int loom_metrics_shm_snapshot(const loom_metrics_shm_t *shm, loom_metrics_shm_t *out);

#ifdef __cplusplus
}
#endif

#endif /* LOOMWORKS_METRICS_SHM_H */
