#define _POSIX_C_SOURCE 200809L
#include "thread_pool_internal.h"

#include <stdlib.h>

/* Array-backed binary min-heap on deadline_ns.  All functions require
 * pool->timer_lock held (the caller — timer thread or submit path).
 * The heap is embedded in loom_thread_pool_t (timer_heap/timer_len/
 * timer_cap); push grows it with realloc. */

static void sift_up(loom_timer_entry_t *h, size_t i)
{
    while (i > 0) {
        size_t p = (i - 1) / 2;
        if (h[p].deadline_ns <= h[i].deadline_ns) {
            break;
        }
        loom_timer_entry_t t = h[p];
        h[p]                 = h[i];
        h[i]                 = t;
        i                    = p;
    }
}

static void sift_down(loom_timer_entry_t *h, size_t n, size_t i)
{
    for (;;) {
        size_t l = 2 * i + 1;
        size_t r = 2 * i + 2;
        size_t m = i;
        if (l < n && h[l].deadline_ns < h[m].deadline_ns) {
            m = l;
        }
        if (r < n && h[r].deadline_ns < h[m].deadline_ns) {
            m = r;
        }
        if (m == i) {
            break;
        }
        loom_timer_entry_t t = h[i];
        h[i]                 = h[m];
        h[m]                 = t;
        i                    = m;
    }
}

void loom_timer_push(loom_thread_pool_t *pool, loom_timer_entry_t e)
{
    if (pool->timer_len == pool->timer_cap) {
        size_t              cap = pool->timer_cap == 0 ? 64 : pool->timer_cap * 2;
        loom_timer_entry_t *nh = (loom_timer_entry_t *)realloc(pool->timer_heap, cap * sizeof(*nh));
        if (nh == NULL) {
            /* Timer registration is best-effort: the owner's sem_timedwait
             * window (worker loop) is a fallback, but with no heap entry the
             * coroutine may sleep past its deadline until the next wake.
             * Reserve capacity up-front in pool_init for the common case. */
            return;
        }
        pool->timer_heap = nh;
        pool->timer_cap  = cap;
    }
    pool->timer_heap[pool->timer_len] = e;
    sift_up(pool->timer_heap, pool->timer_len);
    pool->timer_len++;
}

bool loom_timer_peek(const loom_thread_pool_t *pool, int64_t *deadline_ns)
{
    if (pool->timer_len == 0) {
        return false;
    }
    *deadline_ns = pool->timer_heap[0].deadline_ns;
    return true;
}

/* Pops the heap root unconditionally. The caller (timer thread) has already
 * peeked and confirmed the root is due (deadline <= now). */
bool loom_timer_pop_due(loom_thread_pool_t *pool, loom_timer_entry_t *out)
{
    if (pool->timer_len == 0) {
        return false;
    }
    *out                = pool->timer_heap[0];
    pool->timer_heap[0] = pool->timer_heap[pool->timer_len - 1];
    pool->timer_len--;
    if (pool->timer_len > 0) {
        sift_down(pool->timer_heap, pool->timer_len, 0);
    }
    return true;
}

/* Removes the entry for task_id (if any), returning it via *out when found. */
bool loom_timer_remove_by_task(loom_thread_pool_t *pool, uint64_t task_id, loom_timer_entry_t *out)
{
    for (size_t i = 0; i < pool->timer_len; i++) {
        if (pool->timer_heap[i].task_id == task_id) {
            loom_timer_entry_t e = pool->timer_heap[i];
            pool->timer_heap[i]  = pool->timer_heap[pool->timer_len - 1];
            pool->timer_len--;
            if (pool->timer_len > 0) {
                /* Restore heap invariant from position i in both directions. */
                sift_down(pool->timer_heap, pool->timer_len, i);
                sift_up(pool->timer_heap, i);
            }
            if (out) {
                *out = e;
            }
            return true;
        }
    }
    return false;
}