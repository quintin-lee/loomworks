/**
 * @example task_group_demo.c
 * @brief Demonstrates task group usage.
 */
#include "loomworks/task_group.h"
#include <stdio.h>

static void work(void *arg)
{
    int id = *(int *)arg;
    printf("Task %d executing\n", id);
}

int main(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_create(NULL, &pool);

    loom_task_group_t *group = NULL;
    loom_task_group_create(pool, &group);

    for (int i = 0; i < 10; i++) {
        loom_task_group_submit(group, work, &i, NULL);
    }

    printf("Pending: %u\n", loom_task_group_pending_count(group));
    loom_task_group_wait(group);
    printf("All tasks done.\n");

    loom_task_group_destroy(&group);
    loom_pool_destroy(&pool);
    return 0;
}
