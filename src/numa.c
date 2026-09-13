#if !defined(LOOMWORKS_POSIX_FALLBACK) && defined(__linux__)
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sched.h>
#endif

#include "portability.h"
#include "numa_internal.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static inline bool is_whitespace(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

static int parse_cpulist(const char *str, uint32_t **out_cpus, uint32_t *out_count)
{
    if (!str || !out_cpus || !out_count) {
        return -1;
    }
    size_t    cap   = 16;
    size_t    count = 0;
    uint32_t *arr   = (uint32_t *)malloc(cap * sizeof(uint32_t));
    if (!arr) {
        return -1;
    }

    const char *p = str;
    while (*p) {
        while (*p && (is_whitespace(*p) || *p == ',')) {
            p++;
        }
        if (!*p) {
            break;
        }
        char *endptr = NULL;
        long  start  = strtol(p, &endptr, 10);
        if (endptr == p || start < 0) {
            break;
        }
        long end = start;
        p        = endptr;
        if (*p == '-') {
            p++;
            long r_end = strtol(p, &endptr, 10);
            if (endptr != p && r_end >= start) {
                end = r_end;
                p   = endptr;
            }
        }
        for (long c = start; c <= end; c++) {
            if (count >= cap) {
                size_t    ncap = cap * 2;
                uint32_t *narr = (uint32_t *)realloc(arr, ncap * sizeof(uint32_t));
                if (!narr) {
                    free(arr);
                    return -1;
                }
                arr = narr;
                cap = ncap;
            }
            arr[count++] = (uint32_t)c;
        }
    }

    if (count == 0) {
        free(arr);
        return -1;
    }
    *out_cpus  = arr;
    *out_count = (uint32_t)count;
    return 0;
}

static loom_result_t create_single_domain_fallback(loom_numa_topology_t **out)
{
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) {
        n = 1;
    }
    loom_numa_topology_t *topo = (loom_numa_topology_t *)calloc(1, sizeof(*topo));
    if (!topo) {
        return LOOMWORKS_ERR_ALLOC;
    }
    topo->domains = (loom_numa_domain_info_t *)calloc(1, sizeof(loom_numa_domain_info_t));
    if (!topo->domains) {
        free(topo);
        return LOOMWORKS_ERR_ALLOC;
    }
    topo->domains[0].cpu_ids = (uint32_t *)calloc((size_t)n, sizeof(uint32_t));
    if (!topo->domains[0].cpu_ids) {
        free(topo->domains);
        free(topo);
        return LOOMWORKS_ERR_ALLOC;
    }
    for (long i = 0; i < n; i++) {
        topo->domains[0].cpu_ids[i] = (uint32_t)i;
    }
    topo->domains[0].domain_id = 0;
    topo->domains[0].node_id   = 0;
    topo->domains[0].cpu_count = (uint32_t)n;
    topo->domain_count         = 1;
    topo->total_cpus           = (uint32_t)n;
    topo->mode                 = LOOM_NUMA_AUTO;
    *out                       = topo;
    return LOOMWORKS_OK;
}

loom_result_t loom_numa_detect_topology(loom_numa_topology_t **out)
{
    if (!out) {
        return LOOMWORKS_ERR_INVALID;
    }

    DIR *dir = opendir("/sys/devices/system/node");
    if (!dir) {
        return create_single_domain_fallback(out);
    }

    struct dirent *ent;
    uint32_t       node_ids[256];
    uint32_t       node_count = 0;

    while ((ent = readdir(dir)) != NULL) {
        if (strncmp(ent->d_name, "node", 4) == 0 && isdigit((unsigned char)ent->d_name[4])) {
            char *endptr = NULL;
            long  nid    = strtol(ent->d_name + 4, &endptr, 10);
            if (endptr != ent->d_name + 4 && nid >= 0 && node_count < 256) {
                node_ids[node_count++] = (uint32_t)nid;
            }
        }
    }
    closedir(dir);

    if (node_count == 0) {
        return create_single_domain_fallback(out);
    }

    /* Sort node_ids ascending */
    for (uint32_t i = 0; i < node_count; i++) {
        for (uint32_t j = i + 1; j < node_count; j++) {
            if (node_ids[i] > node_ids[j]) {
                uint32_t tmp = node_ids[i];
                node_ids[i]  = node_ids[j];
                node_ids[j]  = tmp;
            }
        }
    }

    loom_numa_topology_t *topo = (loom_numa_topology_t *)calloc(1, sizeof(*topo));
    if (!topo) {
        return LOOMWORKS_ERR_ALLOC;
    }
    topo->domains = (loom_numa_domain_info_t *)calloc(node_count, sizeof(loom_numa_domain_info_t));
    if (!topo->domains) {
        free(topo);
        return LOOMWORKS_ERR_ALLOC;
    }

    uint32_t total = 0;
    for (uint32_t i = 0; i < node_count; i++) {
        char path[256];
        snprintf(path, sizeof(path), "/sys/devices/system/node/node%u/cpulist", node_ids[i]);
        FILE *f = fopen(path, "r");
        char  buf[512];
        buf[0] = '\0';
        if (f) {
            if (fgets(buf, sizeof(buf), f) == NULL) {
                buf[0] = '\0';
            }
            fclose(f);
        }

        uint32_t *cpus = NULL;
        uint32_t  cc   = 0;
        if (buf[0] != '\0' && parse_cpulist(buf, &cpus, &cc) == 0) {
            topo->domains[i].cpu_ids   = cpus;
            topo->domains[i].cpu_count = cc;
        } else {
            /* Failed to read cpulist for this node, fallback single domain */
            loom_numa_topology_free(topo);
            return create_single_domain_fallback(out);
        }

        topo->domains[i].domain_id = i;
        topo->domains[i].node_id   = node_ids[i];
        total += cc;
    }

    topo->domain_count = node_count;
    topo->total_cpus   = total;
    topo->mode         = LOOM_NUMA_AUTO;
    *out               = topo;
    return LOOMWORKS_OK;
}

loom_result_t loom_numa_virtual_topology(uint32_t virtual_domains, loom_numa_topology_t **out)
{
    if (!out) {
        return LOOMWORKS_ERR_INVALID;
    }

    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) {
        n = 1;
    }

    if (virtual_domains == 0) {
        virtual_domains = ((uint32_t)n >= 2) ? 2 : 1;
    }
    if (virtual_domains > (uint32_t)n) {
        virtual_domains = (uint32_t)n;
    }

    loom_numa_topology_t *topo = (loom_numa_topology_t *)calloc(1, sizeof(*topo));
    if (!topo) {
        return LOOMWORKS_ERR_ALLOC;
    }
    topo->domains =
        (loom_numa_domain_info_t *)calloc(virtual_domains, sizeof(loom_numa_domain_info_t));
    if (!topo->domains) {
        free(topo);
        return LOOMWORKS_ERR_ALLOC;
    }

    uint32_t base_cpus = (uint32_t)n / virtual_domains;
    uint32_t rem_cpus  = (uint32_t)n % virtual_domains;
    uint32_t curr_cpu  = 0;

    for (uint32_t i = 0; i < virtual_domains; i++) {
        uint32_t count           = base_cpus + (i < rem_cpus ? 1 : 0);
        size_t   alloc_count     = (count > 0) ? (size_t)count : 1;
        topo->domains[i].cpu_ids = (uint32_t *)calloc(alloc_count, sizeof(uint32_t));
        if (!topo->domains[i].cpu_ids) {
            loom_numa_topology_free(topo);
            return LOOMWORKS_ERR_ALLOC;
        }
        for (uint32_t j = 0; j < count; j++) {
            topo->domains[i].cpu_ids[j] = curr_cpu++;
        }
        topo->domains[i].domain_id = i;
        topo->domains[i].node_id   = i;
        topo->domains[i].cpu_count = count;
    }

    topo->domain_count = virtual_domains;
    topo->total_cpus   = (uint32_t)n;
    topo->mode         = LOOM_NUMA_VIRTUAL;
    *out               = topo;
    return LOOMWORKS_OK;
}

void loom_numa_topology_free(loom_numa_topology_t *topo)
{
    if (!topo) {
        return;
    }
    if (topo->domains) {
        for (uint32_t i = 0; i < topo->domain_count; i++) {
            free(topo->domains[i].cpu_ids);
        }
        free(topo->domains);
    }
    free(topo);
}

void loom_numa_map_worker(const loom_numa_topology_t *topo,
                          uint32_t                    worker_idx,
                          uint32_t                   *out_domain_idx,
                          uint32_t                   *out_cpu_id)
{
    if (!topo || topo->domain_count == 0) {
        if (out_domain_idx) {
            *out_domain_idx = 0;
        }
        if (out_cpu_id) {
            *out_cpu_id = (uint32_t)-1;
        }
        return;
    }

    uint32_t                       domain_idx = worker_idx % topo->domain_count;
    const loom_numa_domain_info_t *d          = &topo->domains[domain_idx];

    uint32_t cpu_id = (uint32_t)-1;
    if (d->cpu_count > 0 && d->cpu_ids != NULL) {
        uint32_t worker_in_domain = worker_idx / topo->domain_count;
        uint32_t cpu_idx          = worker_in_domain % d->cpu_count;
        cpu_id                    = d->cpu_ids[cpu_idx];
    }

    if (out_domain_idx) {
        *out_domain_idx = domain_idx;
    }
    if (out_cpu_id) {
        *out_cpu_id = cpu_id;
    }
}

bool loom_numa_bind_current_thread(uint32_t cpu_id)
{
#if defined(__linux__) && !defined(LOOMWORKS_POSIX_FALLBACK)
    /* CPU_SET() has no bounds checking: an id >= CPU_SETSIZE (or one that
     * goes negative through the (int) cast) writes out of the stack cpuset.
     * Reject instead of binding nothing — callers treat false as "no
     * affinity" and continue unpinned. */
    if (cpu_id == (uint32_t)-1 || cpu_id >= (uint32_t)CPU_SETSIZE) {
        return false;
    }
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET((int)cpu_id, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    return (rc == 0);
#else
    (void)cpu_id;
    return true;
#endif
}
