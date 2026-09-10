#define _POSIX_C_SOURCE 200809L
#include "loomworks/thread_pool.h"
#include "numa_internal.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int g_passes   = 0;
static int g_failures = 0;

#define ASSERT(expr, msg)                                                                          \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "FAIL: %s (line %d)\n", msg, __LINE__);                                \
            g_failures++;                                                                          \
            return;                                                                                \
        } else {                                                                                   \
            g_passes++;                                                                            \
        }                                                                                          \
    } while (0)

static void count_task_fn(void *arg)
{
    _Atomic uint32_t *counter = (_Atomic uint32_t *)arg;
    atomic_fetch_add(counter, 1);
}

static void test_numa_detect_topology(void)
{
    loom_numa_topology_t *topo = NULL;
    ASSERT(loom_numa_detect_topology(&topo) == LOOMWORKS_OK, "detect topology");
    ASSERT(topo != NULL, "topo not null");
    ASSERT(topo->domain_count >= 1, "at least 1 domain");
    ASSERT(topo->total_cpus >= 1, "at least 1 cpu");
    for (uint32_t i = 0; i < topo->domain_count; i++) {
        ASSERT(topo->domains[i].domain_id == i, "domain id match");
        ASSERT(topo->domains[i].cpu_count >= 1, "cpu count positive");
        ASSERT(topo->domains[i].cpu_ids != NULL, "cpu ids not null");
    }
    loom_numa_topology_free(topo);
    loom_numa_topology_free(NULL); /* NULL-safe */
}

static void test_numa_virtual_topology(void)
{
    loom_numa_topology_t *topo = NULL;
    ASSERT(loom_numa_virtual_topology(2, &topo) == LOOMWORKS_OK, "virtual topology 2 domains");
    ASSERT(topo != NULL, "topo not null");
    ASSERT(topo->mode == LOOM_NUMA_VIRTUAL, "mode virtual");
    if (topo->total_cpus >= 2) {
        ASSERT(topo->domain_count == 2, "domain count 2");
    } else {
        ASSERT(topo->domain_count == 1, "clamped to 1");
    }

    uint32_t d0 = 99, d1 = 99, d2 = 99;
    uint32_t c0 = 99, c1 = 99, c2 = 99;
    loom_numa_map_worker(topo, 0, &d0, &c0);
    loom_numa_map_worker(topo, 1, &d1, &c1);
    loom_numa_map_worker(topo, 2, &d2, &c2);
    ASSERT(d0 == 0, "worker 0 in domain 0");
    if (topo->domain_count >= 2) {
        ASSERT(d1 == 1, "worker 1 in domain 1");
        ASSERT(d2 == 0, "worker 2 in domain 0");
    }

    loom_numa_topology_free(topo);

    /* Edge cases */
    topo = NULL;
    ASSERT(loom_numa_virtual_topology(0, &topo) == LOOMWORKS_OK, "virtual topology 0 domains auto");
    ASSERT(topo != NULL && topo->domain_count >= 1, "domain count valid");
    loom_numa_topology_free(topo);

    topo = NULL;
    ASSERT(loom_numa_virtual_topology(9999, &topo) == LOOMWORKS_OK, "virtual topology clamped");
    ASSERT(topo != NULL && topo->domain_count <= topo->total_cpus, "clamped count");
    loom_numa_topology_free(topo);
}

static void test_pool_numa_disabled(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .numa_mode = LOOM_NUMA_DISABLED};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create disabled pool");
    ASSERT(loom_pool_numa_domain_count(pool) == 1, "domain count 1");
    ASSERT(loom_pool_worker_domain(pool, 0) == 0, "worker domain 0");
    ASSERT(loom_pool_worker_cpu(pool, 0) == (uint32_t)-1, "worker cpu unbounded");

    _Atomic uint32_t done = 0;
    for (int i = 0; i < 10; i++) {
        ASSERT(loom_pool_submit(pool, count_task_fn, &done, NULL) == LOOMWORKS_OK, "submit task");
    }
    loom_pool_shutdown(pool);
    ASSERT(atomic_load(&done) == 10, "all tasks completed");
    loom_pool_destroy(&pool);
}

static void test_pool_numa_auto(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .numa_mode = LOOM_NUMA_AUTO};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create auto pool");
    uint32_t dcount = loom_pool_numa_domain_count(pool);
    ASSERT(dcount >= 1, "auto domain count >= 1");

    for (uint32_t i = 0; i < 4; i++) {
        uint32_t dom = loom_pool_worker_domain(pool, i);
        uint32_t cpu = loom_pool_worker_cpu(pool, i);
        ASSERT(dom < dcount, "valid worker domain");
        ASSERT(cpu != (uint32_t)-1, "valid assigned cpu");
    }

    _Atomic uint32_t done = 0;
    for (int i = 0; i < 20; i++) {
        ASSERT(loom_pool_submit(pool, count_task_fn, &done, NULL) == LOOMWORKS_OK, "submit task");
    }
    loom_pool_shutdown(pool);
    ASSERT(atomic_load(&done) == 20, "all tasks completed");
    loom_pool_destroy(&pool);
}

static void test_pool_numa_virtual_domains(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {
        .worker_count    = 4,
        .numa_mode       = LOOM_NUMA_VIRTUAL,
        .virtual_domains = 2,
    };
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create virtual pool");
    uint32_t dcount = loom_pool_numa_domain_count(pool);
    if (dcount >= 2) {
        ASSERT(loom_pool_worker_domain(pool, 0) == 0, "worker 0 domain 0");
        ASSERT(loom_pool_worker_domain(pool, 1) == 1, "worker 1 domain 1");
        ASSERT(loom_pool_worker_domain(pool, 2) == 0, "worker 2 domain 0");
        ASSERT(loom_pool_worker_domain(pool, 3) == 1, "worker 3 domain 1");
    }

    _Atomic uint32_t done = 0;
    for (int i = 0; i < 50; i++) {
        ASSERT(loom_pool_submit(pool, count_task_fn, &done, NULL) == LOOMWORKS_OK, "submit task");
    }
    loom_pool_shutdown(pool);
    ASSERT(atomic_load(&done) == 50, "all tasks completed");
    loom_pool_destroy(&pool);
}

static void test_pool_numa_resize(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {
        .worker_count    = 2,
        .numa_mode       = LOOM_NUMA_VIRTUAL,
        .virtual_domains = 2,
    };
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create virtual pool for resize");
    uint32_t dcount = loom_pool_numa_domain_count(pool);

    ASSERT(loom_pool_resize(pool, 6) == LOOMWORKS_OK, "resize to 6 workers");
    if (dcount >= 2) {
        ASSERT(loom_pool_worker_domain(pool, 0) == 0, "worker 0 domain 0");
        ASSERT(loom_pool_worker_domain(pool, 1) == 1, "worker 1 domain 1");
        ASSERT(loom_pool_worker_domain(pool, 2) == 0, "worker 2 domain 0");
        ASSERT(loom_pool_worker_domain(pool, 3) == 1, "worker 3 domain 1");
        ASSERT(loom_pool_worker_domain(pool, 4) == 0, "worker 4 domain 0");
        ASSERT(loom_pool_worker_domain(pool, 5) == 1, "worker 5 domain 1");
    }

    _Atomic uint32_t done = 0;
    for (int i = 0; i < 30; i++) {
        ASSERT(loom_pool_submit(pool, count_task_fn, &done, NULL) == LOOMWORKS_OK, "submit task");
    }
    loom_pool_shutdown(pool);
    ASSERT(atomic_load(&done) == 30, "all tasks completed after resize");
    loom_pool_destroy(&pool);
}

static void test_numa_null_safety(void)
{
    ASSERT(loom_pool_numa_domain_count(NULL) == 1, "null pool domain count");
    ASSERT(loom_pool_worker_domain(NULL, 0) == 0, "null pool worker domain");
    ASSERT(loom_pool_worker_cpu(NULL, 0) == (uint32_t)-1, "null pool worker cpu");

    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 2, .numa_mode = LOOM_NUMA_DISABLED};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "create pool");

    ASSERT(loom_pool_worker_domain(pool, 999) == 0, "out of bounds worker domain");
    ASSERT(loom_pool_worker_cpu(pool, 999) == (uint32_t)-1, "out of bounds worker cpu");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

int main(void)
{
    printf("Running NUMA affinity tests...\n");
    test_numa_detect_topology();
    test_numa_virtual_topology();
    test_pool_numa_disabled();
    test_pool_numa_auto();
    test_pool_numa_virtual_domains();
    test_pool_numa_resize();
    test_numa_null_safety();

    printf("\nNUMA Tests Results: %d passed, %d failed\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
