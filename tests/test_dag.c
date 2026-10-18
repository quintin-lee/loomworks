#define _POSIX_C_SOURCE 200809L
/* Task DAG tests.  Includes the internal header to assert on pool state. */
#include "loomworks/dag.h"

#include "dag_internal.h"
#include "loomworks/thread_pool.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int g_passes   = 0;
static int g_failures = 0;

#define ASSERT(expr, msg)                                                                          \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "FAIL: %s at %s:%d\n", msg, __FILE__, __LINE__);                       \
            g_failures++;                                                                          \
        } else {                                                                                   \
            g_passes++;                                                                            \
        }                                                                                          \
    } while (0)

/* Absolute CLOCK_MONOTONIC deadline ms from now. */
static struct timespec deadline_ms(int64_t ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    ts.tv_sec += ms / 1000;
    ts.tv_nsec += (ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000L;
    }
    return ts;
}

/* A generous "should never expire" deadline (2 s): turns a hang caused by a
 * broken drain predicate into a clean ERR_TIMEOUT failure. */
#define SAFE_DEADLINE() deadline_ms(2000)

static loom_thread_pool_t *make_pool(uint32_t workers, uint32_t cap)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = workers, .queue_capacity = cap};
    if (loom_pool_create(&cfg, &pool) != LOOMWORKS_OK) {
        return NULL;
    }
    return pool;
}

static void kill_pool(loom_thread_pool_t **pool)
{
    if (*pool) {
        loom_pool_shutdown(*pool);
        loom_pool_destroy(pool);
    }
}

/* Node task: atomically bumps the pointed counter and records order. */
static loom_result_t count_task(void *ud)
{
    atomic_int *c = (atomic_int *)ud;
    if (c) {
        atomic_fetch_add(c, 1);
    }
    return LOOMWORKS_OK;
}

/* ============================ category 1: lifecycle ==================== */

static void test_lifecycle_happy(void)
{
    loom_thread_pool_t *pool = make_pool(4, 0);
    ASSERT(pool != NULL, "pool create");
    loom_dag_t *dag = NULL;
    ASSERT(loom_dag_create(pool, 8, &dag) == LOOMWORKS_OK, "dag create");
    atomic_int       ran = 0;
    loom_dag_node_t *a = NULL, *b = NULL, *c = NULL;
    ASSERT(loom_dag_add_node(dag, 0, count_task, &ran, 5, &a) == LOOMWORKS_OK, "add 0");
    ASSERT(loom_dag_add_node(dag, 1, count_task, &ran, 5, &b) == LOOMWORKS_OK, "add 1");
    ASSERT(loom_dag_add_node(dag, 2, count_task, &ran, 5, &c) == LOOMWORKS_OK, "add 2");
    ASSERT(loom_dag_add_edge(dag, 0, 1) == LOOMWORKS_OK, "edge 0->1");
    ASSERT(loom_dag_add_edge(dag, 1, 2) == LOOMWORKS_OK, "edge 1->2");
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_OK, "submit");
    struct timespec dl = SAFE_DEADLINE();
    ASSERT(loom_dag_wait_timeout(dag, &dl) == LOOMWORKS_OK, "wait ok");
    ASSERT(atomic_load(&ran) == 3, "all three ran");
    ASSERT(loom_dag_node_status(a) == LOOMWORKS_OK, "a status OK");
    ASSERT(loom_dag_node_status(c) == LOOMWORKS_OK, "c status OK");
    ASSERT(loom_dag_node_future(a) != NULL, "a future exposed");
    ASSERT(loom_dag_destroy(dag) == LOOMWORKS_OK, "destroy OK");
    kill_pool(&pool);
}

static void test_lifecycle_empty(void)
{
    loom_thread_pool_t *pool = make_pool(2, 0);
    loom_dag_t         *dag  = NULL;
    ASSERT(loom_dag_create(pool, 4, &dag) == LOOMWORKS_OK, "empty dag create");
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_OK, "empty submit OK");
    ASSERT(loom_dag_wait(dag) == LOOMWORKS_OK, "empty wait immediate OK");
    ASSERT(loom_dag_destroy(dag) == LOOMWORKS_OK, "empty destroy OK");
    kill_pool(&pool);
}

static void test_lifecycle_never_submitted_destroy(void)
{
    loom_thread_pool_t *pool = make_pool(2, 0);
    loom_dag_t         *dag  = NULL;
    ASSERT(loom_dag_create(pool, 4, &dag) == LOOMWORKS_OK, "create");
    loom_dag_node_t *n = NULL;
    ASSERT(loom_dag_add_node(dag, 0, count_task, NULL, 5, &n) == LOOMWORKS_OK, "add");
    ASSERT(loom_dag_wait(dag) == LOOMWORKS_ERR_INVALID, "never-submitted wait invalid");
    ASSERT(loom_dag_destroy(dag) == LOOMWORKS_OK, "destroy never-submitted OK");
    kill_pool(&pool);
}

static void test_lifecycle_null_args(void)
{
    ASSERT(loom_dag_destroy(NULL) == LOOMWORKS_OK, "destroy NULL OK");
    ASSERT(loom_dag_wait(NULL) == LOOMWORKS_ERR_INVALID, "wait NULL invalid");
    ASSERT(loom_dag_wait_timeout(NULL, NULL) == LOOMWORKS_ERR_INVALID, "wait_timeout NULL invalid");
    ASSERT(loom_dag_submit(NULL) == LOOMWORKS_ERR_INVALID, "submit NULL invalid");
    ASSERT(loom_dag_node_future(NULL) == NULL, "node_future NULL");
    ASSERT(loom_dag_node_status(NULL) == LOOMWORKS_ERR_INVALID, "node_status NULL invalid");
    loom_thread_pool_t *pool = make_pool(2, 0);
    loom_dag_t         *dag  = NULL;
    ASSERT(loom_dag_create(NULL, 4, &dag) == LOOMWORKS_ERR_INVALID, "create NULL pool");
    ASSERT(loom_dag_create(pool, 0, &dag) == LOOMWORKS_ERR_INVALID, "create zero capacity");
    ASSERT(loom_dag_add_node(NULL, 0, count_task, NULL, 5, NULL) == LOOMWORKS_ERR_INVALID,
           "add_node NULL dag");
    kill_pool(&pool);
}

/* ============================ category 2: topology ===================== */

static void test_topology_diamond(void)
{
    loom_thread_pool_t *pool = make_pool(4, 0);
    loom_dag_t         *dag  = NULL;
    loom_dag_create(pool, 4, &dag);
    atomic_int       top = 0, left = 0, right = 0, join = 0;
    loom_dag_node_t *n[4] = {0};
    loom_dag_add_node(dag, 0, count_task, &top, 5, &n[0]);
    loom_dag_add_node(dag, 1, count_task, &left, 5, &n[1]);
    loom_dag_add_node(dag, 2, count_task, &right, 5, &n[2]);
    loom_dag_add_node(dag, 3, count_task, &join, 5, &n[3]);
    loom_dag_add_edge(dag, 0, 1);
    loom_dag_add_edge(dag, 0, 2);
    loom_dag_add_edge(dag, 1, 3);
    loom_dag_add_edge(dag, 2, 3);
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_OK, "diamond submit");
    struct timespec dl = SAFE_DEADLINE();
    ASSERT(loom_dag_wait_timeout(dag, &dl) == LOOMWORKS_OK, "diamond wait");
    ASSERT(atomic_load(&top) == 1 && atomic_load(&left) == 1 && atomic_load(&right) == 1 &&
               atomic_load(&join) == 1,
           "every diamond node ran exactly once");
    loom_dag_destroy(dag);
    kill_pool(&pool);
}

static void test_topology_deep_chain(void)
{
#ifdef __VALGRIND__
    const uint32_t N = 250; /* valgrind scaling */
#else
    const uint32_t N = 1000;
#endif
    loom_thread_pool_t *pool = make_pool(2, 0);
    loom_dag_t         *dag  = NULL;
    loom_dag_create(pool, N, &dag);
    atomic_int ran = 0;
    for (uint32_t i = 0; i < N; i++) {
        loom_dag_node_t *n = NULL;
        ASSERT(loom_dag_add_node(dag, i, count_task, &ran, 5, &n) == LOOMWORKS_OK, "chain add");
    }
    for (uint32_t i = 0; i + 1 < N; i++) {
        ASSERT(loom_dag_add_edge(dag, i, i + 1) == LOOMWORKS_OK, "chain edge");
    }
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_OK, "chain submit");
    struct timespec dl = SAFE_DEADLINE();
    ASSERT(loom_dag_wait_timeout(dag, &dl) == LOOMWORKS_OK, "chain wait (stack-depth independent)");
    ASSERT(atomic_load(&ran) == (int)N, "chain fully ran");
    loom_dag_destroy(dag);
    kill_pool(&pool);
}

static void test_topology_wide_fan(void)
{
    const uint32_t      fan  = 64;
    loom_thread_pool_t *pool = make_pool(4, 0);
    loom_dag_t         *dag  = NULL;
    loom_dag_create(pool, fan + 2, &dag);
    atomic_int ran = 0;
    loom_dag_add_node(dag, 0, count_task, &ran, 5, NULL);       /* source */
    loom_dag_add_node(dag, fan + 1, count_task, &ran, 5, NULL); /* sink */
    for (uint32_t i = 1; i <= fan; i++) {
        loom_dag_add_node(dag, i, count_task, &ran, 5, NULL);
        loom_dag_add_edge(dag, 0, i);       /* fan-out */
        loom_dag_add_edge(dag, i, fan + 1); /* fan-in */
    }
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_OK, "fan submit");
    struct timespec dl = SAFE_DEADLINE();
    ASSERT(loom_dag_wait_timeout(dag, &dl) == LOOMWORKS_OK, "fan wait");
    ASSERT(atomic_load(&ran) == (int)(fan + 2), "fan fully ran");
    loom_dag_destroy(dag);
    kill_pool(&pool);
}

static void test_topology_wide_then_narrow(void)
{
#ifdef __VALGRIND__
    const uint32_t W = 12500; /* valgrind scaling of 50k */
#else
    const uint32_t W = 50000;
#endif
    loom_thread_pool_t *pool = make_pool(4, 0);
    loom_dag_t         *dag  = NULL;
    ASSERT(loom_dag_create(pool, W + 2, &dag) == LOOMWORKS_OK, "stress create");
    atomic_int ran = 0;
    loom_dag_add_node(dag, 0, count_task, &ran, 5, NULL); /* wide source */
    for (uint32_t i = 1; i <= W; i++) {
        loom_dag_add_node(dag, i, count_task, &ran, 5, NULL);
        ASSERT(loom_dag_add_edge(dag, 0, i) == LOOMWORKS_OK, "stress edge");
    }
    loom_dag_add_node(dag, W + 1, count_task, &ran, 5, NULL); /* narrow sink */
    for (uint32_t i = 1; i <= W; i++) {
        loom_dag_add_edge(dag, i, W + 1);
    }
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_OK, "stress submit");
    struct timespec dl = deadline_ms(30000);
    ASSERT(loom_dag_wait_timeout(dag, &dl) == LOOMWORKS_OK, "stress wait");
    ASSERT(atomic_load(&ran) == (int)(W + 2), "stress fully ran");
    loom_dag_destroy(dag);
    kill_pool(&pool);
}

/* ============================ category 3: errors ====================== */

static void test_errors_cycles_and_self_edge(void)
{
    loom_thread_pool_t *pool = make_pool(2, 0);

    /* 2-cycle */
    loom_dag_t *d1 = NULL;
    loom_dag_create(pool, 2, &d1);
    loom_dag_add_node(d1, 0, count_task, NULL, 5, NULL);
    loom_dag_add_node(d1, 1, count_task, NULL, 5, NULL);
    loom_dag_add_edge(d1, 0, 1);
    loom_dag_add_edge(d1, 1, 0);
    ASSERT(loom_dag_submit(d1) == LOOMWORKS_ERR_INVALID, "2-cycle submit invalid");
    ASSERT(loom_dag_wait(d1) == LOOMWORKS_ERR_INVALID, "2-cycle wait invalid");
    ASSERT(loom_dag_submit(d1) == LOOMWORKS_ERR_INVALID, "frozen after cycle");
    loom_dag_destroy(d1);

    /* 5-cycle */
    loom_dag_t *d2 = NULL;
    loom_dag_create(pool, 5, &d2);
    for (uint32_t i = 0; i < 5; i++) {
        loom_dag_add_node(d2, i, count_task, NULL, 5, NULL);
    }
    for (uint32_t i = 0; i < 5; i++) {
        loom_dag_add_edge(d2, i, (i + 1) % 5);
    }
    ASSERT(loom_dag_submit(d2) == LOOMWORKS_ERR_INVALID, "5-cycle submit invalid");
    loom_dag_destroy(d2);

    /* self-edge */
    loom_dag_t *d3 = NULL;
    loom_dag_create(pool, 2, &d3);
    loom_dag_add_node(d3, 0, count_task, NULL, 5, NULL);
    ASSERT(loom_dag_add_edge(d3, 0, 0) == LOOMWORKS_ERR_INVALID, "self-edge invalid");
    loom_dag_destroy(d3);

    kill_pool(&pool);
}

static void test_errors_unknown_and_duplicate(void)
{
    loom_thread_pool_t *pool = make_pool(2, 0);
    loom_dag_t         *dag  = NULL;
    loom_dag_create(pool, 4, &dag);
    loom_dag_add_node(dag, 0, count_task, NULL, 5, NULL);
    loom_dag_add_node(dag, 1, count_task, NULL, 5, NULL);
    ASSERT(loom_dag_add_node(dag, 0, count_task, NULL, 5, NULL) == LOOMWORKS_ERR_INVALID,
           "duplicate id rejected");
    ASSERT(loom_dag_add_node(dag, 99, count_task, NULL, 5, NULL) == LOOMWORKS_ERR_INVALID,
           "out-of-range id rejected");
    ASSERT(loom_dag_add_edge(dag, 0, 3) == LOOMWORKS_ERR_INVALID, "unknown successor id");
    ASSERT(loom_dag_add_edge(dag, 3, 1) == LOOMWORKS_ERR_INVALID, "unknown predecessor id");
    ASSERT(loom_dag_add_edge(dag, 0, 1) == LOOMWORKS_OK, "first edge OK");
    ASSERT(loom_dag_add_edge(dag, 0, 1) == LOOMWORKS_ERR_INVALID, "duplicate edge rejected");
    loom_dag_destroy(dag);
    kill_pool(&pool);
}

/* Sparse ids are the regression guard for the Kahn index bug (defect e):
 * ids 0,2,5 must run, and a chain 0->2->5 must be honoured. */
static void test_errors_sparse_ids(void)
{
    loom_thread_pool_t *pool = make_pool(2, 0);
    loom_dag_t         *dag  = NULL;
    loom_dag_create(pool, 8, &dag);
    atomic_int       ran = 0;
    loom_dag_node_t *n0 = NULL, *n2 = NULL, *n5 = NULL;
    loom_dag_add_node(dag, 0, count_task, &ran, 5, &n0);
    loom_dag_add_node(dag, 2, count_task, &ran, 5, &n2);
    loom_dag_add_node(dag, 5, count_task, &ran, 5, &n5);
    loom_dag_add_edge(dag, 0, 2);
    loom_dag_add_edge(dag, 2, 5);
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_OK, "sparse submit OK");
    struct timespec dl = SAFE_DEADLINE();
    ASSERT(loom_dag_wait_timeout(dag, &dl) == LOOMWORKS_OK, "sparse wait OK");
    ASSERT(atomic_load(&ran) == 3, "sparse chain ran all three");
    loom_dag_destroy(dag);
    kill_pool(&pool);
}

static void test_errors_mutation_after_submit(void)
{
    loom_thread_pool_t *pool = make_pool(2, 0);
    loom_dag_t         *dag  = NULL;
    loom_dag_create(pool, 4, &dag);
    loom_dag_add_node(dag, 0, count_task, NULL, 5, NULL);
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_OK, "submit");
    struct timespec dl = SAFE_DEADLINE();
    loom_dag_wait_timeout(dag, &dl);
    ASSERT(loom_dag_add_node(dag, 1, count_task, NULL, 5, NULL) == LOOMWORKS_ERR_INVALID,
           "add_node after submit rejected");
    ASSERT(loom_dag_add_edge(dag, 0, 1) == LOOMWORKS_ERR_INVALID, "add_edge after submit rejected");
    ASSERT(loom_dag_submit(dag) == LOOMWORKS_ERR_INVALID, "resubmit rejected");
    loom_dag_destroy(dag);
    kill_pool(&pool);
}

/* ------------------------------------------------------------------ */

int main(void)
{
    test_lifecycle_happy();
    test_lifecycle_empty();
    test_lifecycle_never_submitted_destroy();
    test_lifecycle_null_args();
    test_topology_diamond();
    test_topology_deep_chain();
    test_topology_wide_fan();
    test_topology_wide_then_narrow();
    test_errors_cycles_and_self_edge();
    test_errors_unknown_and_duplicate();
    test_errors_sparse_ids();
    test_errors_mutation_after_submit();

    fprintf(stdout, "PASS %d  FAIL %d\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
