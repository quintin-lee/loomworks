#define _POSIX_C_SOURCE 200809L
#include "loomworks/metrics_shm.h"

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/* Maximum length of the user-supplied name (leaves room for prefix + NUL). */
#define LOOMWORKS_SHM_NAME_MAX 127u

/* Prefix for the /dev/shm pathname. */
#define LOOMWORKS_SHM_PREFIX "/loomworks_"

/* Size of the shared-memory metrics struct — stable ABI. */
#define LOOMWORKS_SHM_SIZE sizeof(loom_metrics_shm_t)

/* ------------------------------------------------------------------ */
/*  Internal helpers                                                   */
/* ------------------------------------------------------------------ */

static int validate_name(const char *name)
{
    if (!name || name[0] == '\0') {
        return -1;
    }
    size_t n = strlen(name);
    if (n == 0 || n > LOOMWORKS_SHM_NAME_MAX) {
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        char c = name[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
              c == '_')) {
            return -1;
        }
    }
    return 0;
}

static int build_path(char *buf, size_t bufsz, const char *name)
{
    int rc = snprintf(buf, bufsz, "%s%s", LOOMWORKS_SHM_PREFIX, name);
    if (rc < 0 || (size_t)rc >= bufsz) {
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------ */

int loom_metrics_shm_create(const char *name, loom_metrics_shm_t **out)
{
    if (!name || !out) {
        return -1;
    }
    if (validate_name(name) != 0) {
        return -1;
    }

    char path[256];
    if (build_path(path, sizeof(path), name) != 0) {
        return -1;
    }

    /* Open or create the POSIX shared-memory object.  O_EXCL ensures we
     * detect already-existing segments (caller should destroy first). */
    int fd = shm_open(path, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        return -1;
    }

    /* Size the object.  ftruncate is the only reliable way to ensure the
     * region is fully mapped even on platforms that do not zero-fill. */
    if (ftruncate(fd, LOOMWORKS_SHM_SIZE) != 0) {
        close(fd);
        return -1;
    }

    void *ptr = mmap(NULL, LOOMWORKS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        close(fd);
        return -1;
    }

    /* Zero-initialise all counters (the object may already contain data
     * from a prior run that was not properly destroyed). */
    loom_metrics_shm_t *shm = (loom_metrics_shm_t *)ptr;
    atomic_store(&shm->submitted, 0);
    atomic_store(&shm->started, 0);
    atomic_store(&shm->completed, 0);
    atomic_store(&shm->cancelled, 0);
    atomic_store(&shm->failed, 0);
    atomic_store(&shm->latency_sum_ns, 0);
    atomic_store(&shm->latency_max_ns, 0);

    close(fd); /* fd is no longer needed; the mapping holds the ref. */
    *out = shm;
    return 0;
}

void loom_metrics_shm_destroy(const char *name, loom_metrics_shm_t *ptr)
{
    if (!name || !ptr) {
        return;
    }
    if (munmap(ptr, LOOMWORKS_SHM_SIZE) != 0) {
        return;
    }
    char path[256];
    if (build_path(path, sizeof(path), name) == 0) {
        shm_unlink(path);
    }
}

int loom_metrics_shm_snapshot(const loom_metrics_shm_t *shm, loom_metrics_shm_t *out)
{
    if (!shm || !out) {
        return -1;
    }
    /* memcpy gives an eventually-consistent snapshot — each field is a
     * valid value but they may come from different points in time.
     * This is the documented trade-off and sufficient for monitoring. */
    memcpy(out, shm, LOOMWORKS_SHM_SIZE);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Event-driven shm counter updater — called from worker threads      */
/* ------------------------------------------------------------------ */

void loom_metrics_shm_write(loom_metric_event_t event, loom_metrics_shm_t *shm)
{
    if (!shm) {
        return;
    }
    switch (event) {
    case LOOMWORKS_METRIC_SUBMITTED:
        atomic_fetch_add(&shm->submitted, 1);
        break;
    case LOOMWORKS_METRIC_STARTED:
        atomic_fetch_add(&shm->started, 1);
        break;
    case LOOMWORKS_METRIC_COMPLETED:
        atomic_fetch_add(&shm->completed, 1);
        break;
    case LOOMWORKS_METRIC_CANCELLED:
        atomic_fetch_add(&shm->cancelled, 1);
        break;
    case LOOMWORKS_METRIC_FAILED:
        atomic_fetch_add(&shm->failed, 1);
        break;
    default:
        break;
    }
}
