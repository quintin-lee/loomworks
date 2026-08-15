#ifndef LOOMWORKS_PIPELINE_H
#define LOOMWORKS_PIPELINE_H

/**
 * @file pipeline.h
 * @brief Bounded producer-consumer pipeline backed by the thread pool.
 *
 * This module provides a high-level abstraction for the classic
 * producer-consumer pattern:
 *   - Producers call loom_pc_submit() to enqueue items (blocks when full).
 *   - Consumers call loom_pc_take() to dequeue items (blocks when empty).
 *   - Multiple producers and consumers can share the same pipeline.
 *   - loom_pc_shutdown() signals that no more items will be produced;
 *     consumers receive NULL and should terminate their loop.
 *
 * Usage:
 *   loom_pc_t *pc = NULL;
 *   loom_pc_create(4, 64, &pc);       // 4 workers, capacity 64
 *
 *   // Producer thread:
 *   while (!done) {
 *       item_t *item = produce();
 *       loom_pc_submit(pc, item);
 *   }
 *   loom_pc_shutdown(pc);
 *
 *   // Consumer thread:
 *   void *item;
 *   while (loom_pc_take(pc, &item) == LOOMWORKS_OK) {
 *       process(item);
 *   }
 */

#include "loomworks/thread_pool.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque pipeline handle. */
typedef struct loom_pc loom_pc_t;

/**
 * @brief Create a producer-consumer pipeline.
 *
 * @param worker_count  Number of worker threads (0 = auto).
 * @param capacity      Max items in the queue (0 = unbounded).
 * @param pc            Output pointer for the created pipeline handle.
 * @return              LOOMWORKS_OK on success.
 */
loom_result_t loom_pc_create(uint32_t worker_count, uint32_t capacity, loom_pc_t **pc);

/**
 * @brief Destroy the pipeline and free all resources.
 *
 * Stops the internal consumer pool (if any) and releases the queue.
 * Items still queued at destroy() time are handed to the discard handler
 * registered via loom_pc_set_discard_handler(), if any — this is the hook
 * that lets callers reclaim payloads that would otherwise be dropped.
 *
 * @param pc  Pointer to the pipeline handle (NULL-safe).
 */
void loom_pc_destroy(loom_pc_t **pc);

/**
 * @brief Register a callback for payloads that are dropped without being
 *        dequeued.
 *
 * Destroyed pipelines and internal pool consumers discard queued items.
 * Without a handler, internal consumers free() the payload and destroy()
 * simply drops it (losing the caller's pointer).  Register this handler to
 * take over payload cleanup — e.g. free(), refcount decrement, or a log.
 *
 * Not synchronized with concurrent take()/submit(): install it once, right
 * after create(), before producers start.
 *
 * @param pc       The pipeline handle.
 * @param discard  Callback invoked as discard(data, ctx) for each dropped
 *                 payload.  May be NULL to restore the default behaviour.
 * @param ctx      Opaque context passed verbatim to the callback.
 */
void loom_pc_set_discard_handler(loom_pc_t *pc, void (*discard)(void *data, void *ctx), void *ctx);

/**
 * @brief Submit an item into the pipeline.
 *
 * Blocks until the item is enqueued (when capacity > 0) or the
 * pipeline is shut down.  With a bounded queue, a submit that cannot
 * complete within 60 seconds returns LOOMWORKS_ERR_TIMEOUT.
 *
 * @param pc     The pipeline handle.
 * @param item   Opaque item pointer (owned by caller; returned via loom_pc_take()).
 * @return       LOOMWORKS_OK on success, LOOMWORKS_ERR_SHUTDOWN if closed,
 *               LOOMWORKS_ERR_TIMEOUT if a bounded queue stays full for 60 s.
 */
loom_result_t loom_pc_submit(loom_pc_t *pc, void *item);

/**
 * @brief Take an item out of the pipeline.
 *
 * Blocks until an item is available or the pipeline is shut down.
 * Returns LOOMWORKS_OK with the item, or LOOMWORKS_ERR_SHUTDOWN
 * with *item set to NULL when all producers are done.
 *
 * @param pc     The pipeline handle.
 * @param item   Output pointer for the dequeued item (may be NULL on shutdown).
 * @return       LOOMWORKS_OK on success, LOOMWORKS_ERR_SHUTDOWN on shutdown.
 */
loom_result_t loom_pc_take(loom_pc_t *pc, void **item);

/**
 * @brief Signal that no more items will be produced.
 *
 * Wakes all blocked consumers so they can drain remaining items and
 * then receive a NULL sentinel to terminate.  Must be called exactly
 * once, after all producers are done.
 *
 * @param pc  The pipeline handle.
 */
void loom_pc_shutdown(loom_pc_t *pc);

/**
 * @brief Get the number of items currently waiting in the queue.
 *
 * @param pc  The pipeline handle.
 * @return    Pending item count, or 0 if pc is NULL.
 */
uint32_t loom_pc_pending_count(const loom_pc_t *pc);

/**
 * @brief Get the total number of items successfully enqueued.
 *
 * @param pc  The pipeline handle.
 * @return    Submitted count, or 0 if pc is NULL.
 */
uint64_t loom_pc_submitted_count(const loom_pc_t *pc);

/**
 * @brief Get the total number of items actually dequeued by consumers.
 *
 * Incremented on every successful loom_pc_take(), so it measures real
 * consumption.  Must be read before loom_pc_destroy().
 *
 * @param pc  The pipeline handle.
 * @return    Taken count, or 0 if pc is NULL.
 */
uint64_t loom_pc_taken_count(const loom_pc_t *pc);

#ifdef __cplusplus
}
#endif

#endif /* LOOMWORKS_PIPELINE_H */
