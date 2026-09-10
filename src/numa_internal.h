#ifndef LOOMWORKS_NUMA_INTERNAL_H
#define LOOMWORKS_NUMA_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "loomworks/thread_pool.h"

/**
 * @brief Represents a NUMA domain with its associated CPU core IDs.
 */
typedef struct loom_numa_domain_info {
    uint32_t  domain_id; /**< Logical domain index (0 .. domain_count-1). */
    uint32_t  node_id;   /**< Physical NUMA node id, or virtual id. */
    uint32_t  cpu_count; /**< Number of CPUs belonging to this domain. */
    uint32_t *cpu_ids;   /**< Array of CPU core indices. */
} loom_numa_domain_info_t;

/**
 * @brief Discovered or configured NUMA topology.
 */
typedef struct loom_numa_topology {
    loom_numa_mode_t         mode;
    uint32_t                 domain_count;
    loom_numa_domain_info_t *domains;
    uint32_t                 total_cpus;
} loom_numa_topology_t;

/**
 * @brief Detect host physical NUMA topology via sysfs (/sys/devices/system/node/).
 *        Falls back to a single domain if sysfs is not accessible or single node.
 *
 * @param out Output topology structure (caller frees via loom_numa_topology_free).
 * @return LOOMWORKS_OK on success, error code otherwise.
 */
loom_result_t loom_numa_detect_topology(loom_numa_topology_t **out);

/**
 * @brief Create a virtual NUMA topology partitioning available online CPUs into
 *        @p virtual_domains domains.
 *
 * @param virtual_domains Number of virtual domains (if 0 or > total_cpus, clamped).
 * @param out Output topology structure.
 * @return LOOMWORKS_OK on success.
 */
loom_result_t loom_numa_virtual_topology(uint32_t virtual_domains, loom_numa_topology_t **out);

/**
 * @brief Free a NUMA topology structure.
 */
void loom_numa_topology_free(loom_numa_topology_t *topo);

/**
 * @brief Bind the calling thread to a specific CPU core.
 *
 * @param cpu_id CPU core index.
 * @return true on success, false otherwise (e.g. platform unsupported or invalid core).
 */
bool loom_numa_bind_current_thread(uint32_t cpu_id);

/**
 * @brief Map a worker index to its domain and CPU core in a round-robin / balanced fashion.
 *
 * @param topo Topology structure.
 * @param worker_idx Worker index.
 * @param out_domain_idx Output pointer for domain index (may be NULL).
 * @param out_cpu_id Output pointer for CPU core ID (may be NULL).
 */
void loom_numa_map_worker(const loom_numa_topology_t *topo,
                          uint32_t                    worker_idx,
                          uint32_t                   *out_domain_idx,
                          uint32_t                   *out_cpu_id);

#endif /* LOOMWORKS_NUMA_INTERNAL_H */
