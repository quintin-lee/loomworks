/**
 * @file bench.c
 * @brief Performance benchmarks for loomworks thread pool.
 *
 * Benchmarks run by default:
 *   1. submit_latency   — per-submit call overhead (ns)
 *   2. throughput       — max tasks/sec with N workers
 *   3. worker_scaling   — throughput vs worker count (1,2,4,8,16,32,64)
 *   4. bounded_queue    — throughput with bounded vs unbounded queue
 *   5. future_overhead  — fire-and-forget vs future-based submission
 *   6. coro_create_destroy — coroutine create+destroy lifecycle cost (ns/cycle)
 *
 * Usage: ./bench [--iterations N] [--tasks M]
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/thread_pool.h"
#include "loomworks/coroutine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ---------- Config ---------- */
static int g_iterations  = 100;
static int g_task_count  = 10000;
static int g_json_output = 0;

/* JSON result capture (populated by each bench when g_json_output) */
static double g_submit_latency_avg_ns = 0.0;
static double g_throughput_tps        = 0.0;
static double g_queue_depth_tps[4]    = {0.0, 0.0, 0.0, 0.0};
static int    g_queue_depths[4]       = {1000, 10000, 50000, 100000};

/* ---------- Timer ---------- */
static double now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

/* ---------- Task helpers ---------- */
static void noop_task(void *arg)
{
    (void)arg;
}

static void *noop_result_task(void *arg)
{
    (void)arg;
    return NULL;
}

/* ---------- Benchmark 1: submit_latency ----------
 * Submit one task at a time from the main thread and
 * measure the wall-clock time per call.
 * ---------- */
static void bench_submit_latency(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 0, .queue_capacity = 0};
    loom_pool_create(&cfg, &pool);

    double total = 0.0;
    double min_t = 1e18;
    double max_t = 0.0;

    for (int i = 0; i < g_iterations; i++) {
        double t0 = now_ns();
        loom_pool_submit(pool, noop_task, NULL, NULL);
        double t1 = now_ns();
        double dt = t1 - t0;
        total += dt;
        if (dt < min_t) {
            min_t = dt;
        }
        if (dt > max_t) {
            max_t = dt;
        }
    }

    double avg = total / g_iterations;
    printf("  submit_latency:   avg=%.1f ns  min=%.1f ns  max=%.1f ns\n", avg, min_t, max_t);
    g_submit_latency_avg_ns = avg;

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}

/* ---------- Benchmark 2: throughput ----------
 * Submit N tasks from a single thread, time the full batch.
 * The workers run concurrently, so this measures end-to-end
 * throughput (submission + execution).
 * ---------- */
static void bench_throughput(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 0, .queue_capacity = 0};
    loom_pool_create(&cfg, &pool);

    double t0 = now_ns();
    for (int i = 0; i < g_task_count; i++) {
        loom_pool_submit(pool, noop_task, NULL, NULL);
    }
    loom_pool_shutdown(pool);
    double t1         = now_ns();
    double elapsed_ms = (t1 - t0) / 1e6;
    double tps        = (double)g_task_count / (elapsed_ms / 1000.0);

    printf("  throughput:       %d tasks in %.2f ms  → %.0f tasks/sec\n",
           g_task_count,
           elapsed_ms,
           tps);
    g_throughput_tps = tps;

    loom_pool_destroy(&pool);
}

/* ---------- Benchmark 3: worker_scaling ----------
 * Run the throughput benchmark with different worker counts.
 * ---------- */
static void bench_worker_scaling(void)
{
    int  workers[] = {1, 2, 4, 8, 16, 32, 64};
    int  n_workers = (int)(sizeof(workers) / sizeof(workers[0]));
    long max_cpus  = sysconf(_SC_NPROCESSORS_ONLN);
    if (max_cpus < 1) {
        max_cpus = 1;
    }

    printf("  %-8s  %s\n", "workers", "tasks/sec");
    printf("  -------  --------\n");
    for (int i = 0; i < n_workers; i++) {
        int w = workers[i];
        if (w > (int)max_cpus) {
            break;
        }

        loom_thread_pool_t *pool = NULL;
        loom_pool_config_t  cfg  = {.worker_count = (uint32_t)w, .queue_capacity = 0};
        loom_pool_create(&cfg, &pool);

        double t0 = now_ns();
        for (int j = 0; j < g_task_count; j++) {
            loom_pool_submit(pool, noop_task, NULL, NULL);
        }
        loom_pool_shutdown(pool);
        double t1         = now_ns();
        double elapsed_ms = (t1 - t0) / 1e6;
        double tps        = (double)g_task_count / (elapsed_ms / 1000.0);

        printf("  %8d  %.0f\n", w, tps);
        loom_pool_destroy(&pool);
    }
}

/* ---------- Benchmark 4: bounded_queue ----------
 * Compare unbounded (capacity=0) vs bounded queue.
 * With a bounded queue, submit should block when full, so
 * throughput may drop under heavy load.
 * ---------- */
static void bench_bounded_queue(void)
{
    uint32_t capacities[] = {0, 128, 1024, 8192};

    printf("  %-12s  %s\n", "capacity", "tasks/sec");
    printf("  -----------  --------\n");
    for (int i = 0; i < (int)(sizeof(capacities) / sizeof(capacities[0])); i++) {
        uint32_t            cap  = capacities[i];
        loom_thread_pool_t *pool = NULL;
        loom_pool_config_t  cfg  = {.worker_count = 0, .queue_capacity = cap};
        loom_pool_create(&cfg, &pool);

        double t0 = now_ns();
        for (int j = 0; j < g_task_count; j++) {
            loom_result_t rc = loom_pool_submit(pool, noop_task, NULL, NULL);
            if (rc != LOOMWORKS_OK) {
                /* Queue full — retry after brief spin */
                for (int retry = 0; retry < 1000; retry++) {
                    if (loom_pool_submit(pool, noop_task, NULL, NULL) == LOOMWORKS_OK) {
                        break;
                    }
                    struct timespec ts = {0, 100};
                    nanosleep(&ts, NULL);
                }
            }
        }
        loom_pool_shutdown(pool);
        double t1         = now_ns();
        double elapsed_ms = (t1 - t0) / 1e6;
        double tps        = (double)g_task_count / (elapsed_ms / 1000.0);

        printf("  %12u  %.0f\n", cap, tps);
        loom_pool_destroy(&pool);
    }
}

/* ---------- Benchmark 6: queue_depth ----------
 * Single worker + N noop tasks => the queue is effectively N deep.
 * An O(n) enqueue degrades throughput as N grows; the bucketized
 * queue stays flat.  This is the acceptance metric for the queue
 * refactor.  Runs only in JSON mode (used by the CI compare step).
 * ---------- */
static void bench_queue_depth(void)
{
    if (!g_json_output) {
        printf("  (run with --json to collect queue_depth data)\n");
        return;
    }
    for (int i = 0; i < 4; i++) {
        int                 d    = g_queue_depths[i];
        double              best = 1e18;
        for (int r = 0; r < 3; r++) {
            loom_thread_pool_t *pool = NULL;
            loom_pool_config_t  cfg  = {.worker_count = 1, .queue_capacity = 0};
            loom_pool_create(&cfg, &pool);

            double t0 = now_ns();
            for (int j = 0; j < d; j++) {
                loom_pool_submit(pool, noop_task, NULL, NULL);
            }
            loom_pool_shutdown(pool);
            double t1         = now_ns();
            double elapsed_ms = (t1 - t0) / 1e6;
            if (elapsed_ms < best) {
                best = elapsed_ms;
            }

            loom_pool_destroy(&pool);
        }
        g_queue_depth_tps[i] = (double)d / (best / 1000.0);
    }
}

/* ---------- Benchmark 5: future_overhead ----------
 * Compare fire-and-forget (submit) vs future-based (submit_future
 * + future_wait) submission latency.
 * ---------- */
static void bench_future_overhead(void)
{
    const int FUTURE_N = 100;

    /* --- Fire-and-forget --- */
    loom_thread_pool_t *pool_ff = NULL;
    loom_pool_config_t  cfg_ff  = {.worker_count = 0, .queue_capacity = 0};
    loom_pool_create(&cfg_ff, &pool_ff);
    double t0 = now_ns();
    for (int i = 0; i < FUTURE_N; i++) {
        loom_pool_submit(pool_ff, noop_task, NULL, NULL);
    }
    loom_pool_shutdown(pool_ff);
    double t1     = now_ns();
    double ff_avg = (t1 - t0) / FUTURE_N;

    /* --- Future-based --- */
    loom_thread_pool_t *pool_fut = NULL;
    loom_pool_config_t  cfg_fut  = {.worker_count = 0, .queue_capacity = 0};
    loom_pool_create(&cfg_fut, &pool_fut);
    t0 = now_ns();
    for (int i = 0; i < FUTURE_N; i++) {
        loom_future_t *fut = NULL;
        loom_pool_submit_future(pool_fut, noop_result_task, NULL, &fut, NULL);
        loom_future_wait(fut, NULL);
        loom_future_destroy(fut);
    }
    t1             = now_ns();
    double fut_avg = (t1 - t0) / FUTURE_N;

    printf("  future_overhead:  fire-and-forget=%.1f ns  future+wait=%.1f ns  (×%.1f)\n",
           ff_avg,
           fut_avg,
           fut_avg / ff_avg);

    loom_pool_destroy(&pool_ff);
    loom_pool_shutdown(pool_fut);
    loom_pool_destroy(&pool_fut);
}

/* ---------- Benchmark 6: coro_create_destroy ----------
 * Coroutine create + destroy lifecycle cost. With stack pooling,
 * the steady-state path drops to ~0 syscalls (pool hit) instead of
 * mmap + mprotect + munmap per cycle.
 * ---------- */
static void coro_trivial_fn(void *arg)
{
    (void)arg;
}

static void bench_coro_create_destroy(void)
{
    const int N = 100000;
    double    t0 = now_ns();
    for (int i = 0; i < N; i++) {
        loom_coroutine_t *coro = NULL;
        if (loom_coro_create(coro_trivial_fn, NULL, 0, &coro) != LOOMWORKS_CORO_OK) {
            fprintf(stderr, "coro create failed at %d\n", i);
            exit(1);
        }
        loom_coro_destroy(&coro);
    }
    double t1     = now_ns();
    double avg_ns = (t1 - t0) / (double)N;
    printf("  coro_create_destroy: %.1f ns/cycle (%d create+destroy cycles)\n", avg_ns, N);
}

/* ---------- Usage ---------- */
static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s [--iterations N] [--tasks M] [--json]\n"
            "  --iterations  Number of repeat runs per benchmark (default: %d)\n"
            "  --tasks       Number of tasks in throughput benchmark (default: %d)\n"
            "  --json        Output results in JSON format (adds queue_depth scenario)\n",
            prog,
            g_iterations,
            g_task_count);
}

/* ---------- Main ---------- */
int main(int argc, char *argv[])
{
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            char *end;
            long  v = strtol(argv[++i], &end, 10);
            if (end == argv[i] || v <= 0) {
                fprintf(stderr, "Invalid --iterations value: %s\n", argv[i]);
                return 1;
            }
            g_iterations = (int)v;
        } else if (strcmp(argv[i], "--tasks") == 0 && i + 1 < argc) {
            char *end;
            long  v = strtol(argv[++i], &end, 10);
            if (end == argv[i] || v <= 0) {
                fprintf(stderr, "Invalid --tasks value: %s\n", argv[i]);
                return 1;
            }
            g_task_count = (int)v;
        } else if (strcmp(argv[i], "--json") == 0) {
            g_json_output = 1;
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    printf("loomworks benchmark  (iterations=%d, tasks=%d)\n", g_iterations, g_task_count);
    printf("---------------------------------------------------\n");

    printf("\n[1/5] submit_latency\n");
    fflush(stdout);
    bench_submit_latency();

    printf("\n[2/5] throughput\n");
    fflush(stdout);
    bench_throughput();

    printf("\n[3/5] worker_scaling\n");
    fflush(stdout);
    bench_worker_scaling();

    printf("\n[4/5] bounded_queue\n");
    fflush(stdout);
    bench_bounded_queue();

    printf("\n[5/5] future_overhead\n");
    fflush(stdout);
    bench_future_overhead();

    printf("\n[6/7] coro_create_destroy\n");
    fflush(stdout);
    bench_coro_create_destroy();

    printf("\n[7/7] queue_depth\n");
    fflush(stdout);
    bench_queue_depth();

    printf("\nDone.\n");

    if (g_json_output) {
        printf("\n{\n");
        printf("  \"benchmark\": \"loomworks\",\n");
        printf("  \"iterations\": %d,\n", g_iterations);
        printf("  \"task_count\": %d,\n", g_task_count);
        printf("  \"submit_latency_avg_ns\": %.1f,\n", g_submit_latency_avg_ns);
        printf("  \"throughput_tps\": %.0f,\n", g_throughput_tps);
        printf("  \"queue_depths\": [%d, %d, %d, %d],\n",
               g_queue_depths[0], g_queue_depths[1], g_queue_depths[2], g_queue_depths[3]);
        printf("  \"queue_depth_tps\": [%.0f, %.0f, %.0f, %.0f]\n",
               g_queue_depth_tps[0], g_queue_depth_tps[1], g_queue_depth_tps[2],
               g_queue_depth_tps[3]);
        printf("}\n");
    }

    return 0;
}
