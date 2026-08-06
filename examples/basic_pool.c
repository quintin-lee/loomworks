/**
 * @example basic_pool.c
 * @brief Demonstrates basic thread pool usage.
 */
#include <stdio.h>
#include "loomworks/thread_pool.h"

static void say_hello(void *arg)
{
    printf("Hello from worker! arg=%d\n", *(int *)arg);
}

static void *compute(void *arg)
{
    int n = *(int *)arg;
    int sum = 0;
    for (int i = 0; i < n; i++) {
        sum += i;
    }
    int *result = (int *)malloc(sizeof(int));
    if (result) {
        *result = sum;
    }
    return (void *)result;
}

int main(void)
{
    loom_thread_pool_t *pool = NULL;
    if (loom_pool_create(NULL, &pool) != LOOMWORKS_OK) {
        fprintf(stderr, "Failed to create pool\n");
        return 1;
    }

    /* Fire-and-forget tasks */
    for (int i = 0; i < 5; i++) {
        loom_pool_submit(pool, say_hello, &i);
    }

    /* Future-based task */
    int n = 100;
    loom_future_t *fut = NULL;
    if (loom_pool_submit_future(pool, compute, &n, &fut) == LOOMWORKS_OK) {
        void *result = NULL;
        loom_future_wait(fut, &result);
        if (result) {
            printf("Sum 0..99 = %d\n", *(int *)result);
            free(result);
        }
        loom_future_destroy(fut);
    }

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    printf("Pool destroyed.\n");
    return 0;
}
