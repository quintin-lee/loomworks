#include "loomworks/loomworks.h"

#include <stdio.h>

#define CHECK(expr, msg)                                                                           \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "demo failed at %s (%s)\n", msg, #expr);                               \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)

static loom_result_t stage_a(void *ud)
{
    (void)ud;
    puts("[A] load inputs");
    return LOOMWORKS_OK;
}

static loom_result_t stage_b(void *ud)
{
    (void)ud;
    puts("[B] load reference data");
    return LOOMWORKS_OK;
}

static loom_result_t stage_c(void *ud)
{
    (void)ud;
    puts("[C] aggregate A + B");
    return LOOMWORKS_OK;
}

static loom_result_t stage_d(void *ud)
{
    (void)ud;
    puts("[D] publish");
    return LOOMWORKS_OK;
}

int main(void)
{
    loom_pool_config_t cfg = {.worker_count = 4, .queue_capacity = 0};
    loom_thread_pool_t *pool = NULL;
    CHECK(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "pool create");

    loom_dag_t *dag = NULL;
    CHECK(loom_dag_create(pool, 4, &dag) == LOOMWORKS_OK, "dag create");

    loom_dag_node_t *a = NULL;
    loom_dag_node_t *b = NULL;
    loom_dag_node_t *c = NULL;
    loom_dag_node_t *d = NULL;
    CHECK(loom_dag_add_node(dag, 0, stage_a, NULL, LOOMWORKS_PRIORITY_NORMAL, &a) == LOOMWORKS_OK,
          "add A");
    CHECK(loom_dag_add_node(dag, 1, stage_b, NULL, LOOMWORKS_PRIORITY_NORMAL, &b) == LOOMWORKS_OK,
          "add B");
    CHECK(loom_dag_add_node(dag, 2, stage_c, NULL, LOOMWORKS_PRIORITY_NORMAL, &c) == LOOMWORKS_OK,
          "add C");
    CHECK(loom_dag_add_node(dag, 3, stage_d, NULL, LOOMWORKS_PRIORITY_NORMAL, &d) == LOOMWORKS_OK,
          "add D");
    CHECK(loom_dag_add_edge(dag, 0, 2) == LOOMWORKS_OK, "edge A->C");
    CHECK(loom_dag_add_edge(dag, 1, 2) == LOOMWORKS_OK, "edge B->C");
    CHECK(loom_dag_add_edge(dag, 2, 3) == LOOMWORKS_OK, "edge C->D");

    puts("DAG: A->C, B->C, C->D (4 nodes, 3 edges)");
    CHECK(loom_dag_submit(dag) == LOOMWORKS_OK, "submit");

    loom_result_t rc = loom_dag_wait(dag);
    printf("dag_wait -> %d\n", (int)rc);
    printf("node status: A=%d B=%d C=%d D=%d\n", (int)loom_dag_node_status(a),
           (int)loom_dag_node_status(b), (int)loom_dag_node_status(c),
           (int)loom_dag_node_status(d));

    loom_dag_destroy(dag);
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    puts("demo OK");
    return 0;
}
