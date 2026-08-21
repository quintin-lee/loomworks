#define _POSIX_C_SOURCE 200809L
#include "loomworks/runtime.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static void thread_task(void *arg)
{
    int *n = (int *)arg;
    printf("  [thread] task #%d running\n", *n);
}

static void coro_task(void *arg)
{
    int *n = (int *)arg;
    printf("  [coro]   task #%d started\n", *n);
    loom_coro_yield();
    printf("  [coro]   task #%d resumed\n", *n);
}

int main(void)
{
    loom_runtime_config_t cfg = {0};
    cfg.worker_count          = 2;
    cfg.queue_capacity        = 0; /* unbounded */

    loom_runtime_t *rt = NULL;
    loom_result_t   rc = loom_runtime_create(&cfg, &rt);
    if (rc != LOOMWORKS_OK) {
        fprintf(stderr, "create failed: %d\n", rc);
        return 1;
    }

    printf("workers: %u  pending: %u\n",
           loom_runtime_worker_count(rt),
           loom_runtime_pending_count(rt));

    int             nums[]    = {1, 2, 3, 4};
    loom_fn_union_t thread_fn = {.thread_fn = thread_task};
    loom_fn_union_t coro_fn   = {.coro_fn = coro_task};

    for (int i = 0; i < 4; i++) {
        loom_runtime_submit(rt,
                            i % 2 == 0 ? thread_fn : coro_fn,
                            &nums[i],
                            i % 2 == 0 ? LOOM_SUBMIT_THREAD : LOOM_SUBMIT_CORO,
                            5,
                            NULL);
    }

    printf("submitted 4 tasks\n");
    printf("pending: %u\n", loom_runtime_pending_count(rt));

    loom_runtime_shutdown(rt);
    loom_runtime_destroy(&rt);

    printf("done.\n");
    return 0;
}
