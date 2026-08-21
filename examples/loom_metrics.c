/**
 * @file loom_metrics.c
 * @brief CLI tool for reading loomworks shared-memory metrics.
 *
 * Usage:
 *   loom-metrics [--name <name>] [--watch <secs>] [--format text|json]
 *
 * Examples:
 *   loom-metrics my-pool              # single snapshot, text output
 *   loom-metrics --name my-pool       # same
 *   loom-metrics --name my-pool -w 1  # watch every 1 s
 *   loom-metrics --name my-pool -j    # JSON output
 */
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/*  Metrics struct — mirrors loom_metrics_shm_t                       */
/* ------------------------------------------------------------------ */
typedef struct {
    _Atomic uint64_t submitted;
    _Atomic uint64_t started;
    _Atomic uint64_t completed;
    _Atomic uint64_t cancelled;
    _Atomic uint64_t failed;
    _Atomic uint64_t latency_sum_ns;
    _Atomic uint64_t latency_max_ns;
} metrics_shm_t;

/* ------------------------------------------------------------------ */
/*  Globals                                                           */
/* ------------------------------------------------------------------ */
static const char           *g_name           = "default";
static double                g_watch_interval = 0.0; /* 0 = single-shot */
static int                   g_json           = 0;
static volatile sig_atomic_t g_running        = 1;

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */
static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s [OPTIONS] [NAME]\n"
            "\n"
            "Read metrics from a loomworks shared-memory region.\n"
            "\n"
            "Options:\n"
            "  -n, --name NAME     Shared-memory name (default: \"default\")\n"
            "  -w, --watch SEC     Watch mode: poll every SEC seconds (0 = single shot)\n"
            "  -j, --json          Output in JSON format\n"
            "  -h, --help          Show this help\n"
            "\n"
            "Examples:\n"
            "  %s my-pool                     # single snapshot\n"
            "  %s --name my-pool -w 1         # poll every second\n"
            "  %s -n my-pool -j               # JSON output\n",
            prog,
            prog,
            prog,
            prog);
}

static void handle_signal(int sig)
{
    (void)sig;
    g_running = 0;
}

/* Open (or create) the shm region.  Returns NULL on failure. */
static metrics_shm_t *open_shm(const char *name)
{
    char path[256];
    int  rc = snprintf(path, sizeof(path), "/loomworks_%s", name);
    if (rc < 0 || (size_t)rc >= sizeof(path)) {
        return NULL;
    }

    int fd = shm_open(path, O_RDWR, 0666);
    if (fd < 0) {
        fprintf(stderr, "shm_open(%s): %s\n", path, strerror(errno));
        return NULL;
    }

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size == 0) {
        /* Region not yet created by a runtime — nothing to read. */
        close(fd);
        return NULL;
    }

    metrics_shm_t *shm =
        (metrics_shm_t *)mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (shm == MAP_FAILED) {
        return NULL;
    }
    return shm;
}

static void close_shm(metrics_shm_t *shm, size_t sz)
{
    if (shm && shm != MAP_FAILED) {
        munmap(shm, sz);
    }
}

/* ------------------------------------------------------------------ */
/*  Print                                                             */
/* ------------------------------------------------------------------ */
static void print_text(const metrics_shm_t *shm)
{
    uint64_t submitted      = atomic_load_explicit(&shm->submitted, memory_order_relaxed);
    uint64_t started        = atomic_load_explicit(&shm->started, memory_order_relaxed);
    uint64_t completed      = atomic_load_explicit(&shm->completed, memory_order_relaxed);
    uint64_t cancelled      = atomic_load_explicit(&shm->cancelled, memory_order_relaxed);
    uint64_t failed         = atomic_load_explicit(&shm->failed, memory_order_relaxed);
    uint64_t latency_sum_ns = atomic_load_explicit(&shm->latency_sum_ns, memory_order_relaxed);
    uint64_t latency_max_ns = atomic_load_explicit(&shm->latency_max_ns, memory_order_relaxed);

    uint64_t avg_ns = (completed > 0) ? (latency_sum_ns / completed) : 0;
    double   avg_us = (double)avg_ns / 1e3;
    double   max_us = (double)latency_max_ns / 1e3;

    printf("name:           %s\n", g_name);
    printf("submitted:      %-12llu\n", (unsigned long long)submitted);
    printf("started:        %-12llu\n", (unsigned long long)started);
    printf("completed:      %-12llu\n", (unsigned long long)completed);
    printf("cancelled:      %-12llu\n", (unsigned long long)cancelled);
    printf("failed:         %-12llu\n", (unsigned long long)failed);
    printf("avg_latency:    %.2f us\n", avg_us);
    printf("max_latency:    %.2f us\n", max_us);
}

static void print_json(const metrics_shm_t *shm)
{
    uint64_t submitted      = atomic_load_explicit(&shm->submitted, memory_order_relaxed);
    uint64_t started        = atomic_load_explicit(&shm->started, memory_order_relaxed);
    uint64_t completed      = atomic_load_explicit(&shm->completed, memory_order_relaxed);
    uint64_t cancelled      = atomic_load_explicit(&shm->cancelled, memory_order_relaxed);
    uint64_t failed         = atomic_load_explicit(&shm->failed, memory_order_relaxed);
    uint64_t latency_sum_ns = atomic_load_explicit(&shm->latency_sum_ns, memory_order_relaxed);
    uint64_t latency_max_ns = atomic_load_explicit(&shm->latency_max_ns, memory_order_relaxed);

    uint64_t avg_ns = (completed > 0) ? (latency_sum_ns / completed) : 0;

    printf("{\n");
    printf("  \"name\": \"%s\",\n", g_name);
    printf("  \"submitted\":      %llu,\n", (unsigned long long)submitted);
    printf("  \"started\":        %llu,\n", (unsigned long long)started);
    printf("  \"completed\":      %llu,\n", (unsigned long long)completed);
    printf("  \"cancelled\":      %llu,\n", (unsigned long long)cancelled);
    printf("  \"failed\":         %llu,\n", (unsigned long long)failed);
    printf("  \"avg_latency_ns\": %llu,\n", (unsigned long long)avg_ns);
    printf("  \"max_latency_ns\": %llu\n", (unsigned long long)latency_max_ns);
    printf("}\n");
}

/* ------------------------------------------------------------------ */
/*  Main                                                              */
/* ------------------------------------------------------------------ */
int main(int argc, char **argv)
{
    int opt;
    while ((opt = getopt(argc, argv, "n:w:jh?")) != -1) {
        switch (opt) {
        case 'n':
            g_name = optarg;
            break;
        case 'w':
            g_watch_interval = strtod(optarg, NULL);
            if (g_watch_interval <= 0) {
                fprintf(stderr, "invalid interval: %s\n", optarg);
                return 1;
            }
            break;
        case 'j':
            g_json = 1;
            break;
        default:
            usage(argv[0]);
            return (opt == 'h' || opt == '?') ? 0 : 1;
        }
    }

    /* Positional name argument (overrides -n if given). */
    if (optind < argc) {
        g_name = argv[optind];
    }

    /* Install signal handlers for graceful watch-mode exit. */
    struct sigaction sa = {.sa_handler = handle_signal};
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    size_t shm_size = sizeof(metrics_shm_t);

    if (g_watch_interval > 0) {
        /* Watch mode: poll until signal. */
        printf("watching /loomworks_%s every %.1f s  (Ctrl+C to stop)\n", g_name, g_watch_interval);
        while (g_running) {
            metrics_shm_t *shm = open_shm(g_name);
            if (!shm) {
                fprintf(stderr, "no region found for \"%s\" (yet)\n", g_name);
                /* Don't die immediately — the runtime may still be starting up. */
                goto sleep_and_retry;
            }
            if (g_json) {
                print_json(shm);
            } else {
                print_text(shm);
            }
            close_shm(shm, shm_size);

        sleep_and_retry:
            if (g_running) {
                struct timespec ts = {
                    .tv_sec  = (time_t)g_watch_interval,
                    .tv_nsec = (long)((g_watch_interval - (double)(time_t)g_watch_interval) * 1e9)};
                nanosleep(&ts, NULL);
            }
        }
        printf("\nstopped.\n");
    } else {
        /* Single-shot mode. */
        metrics_shm_t *shm = open_shm(g_name);
        if (!shm) {
            fprintf(stderr,
                    "no shared-memory region found for \"%s\"\n"
                    "  (did you set shm_name in loom_runtime_config_t?)\n",
                    g_name);
            return 1;
        }
        if (g_json) {
            print_json(shm);
        } else {
            print_text(shm);
        }
        close_shm(shm, shm_size);
    }

    return 0;
}
