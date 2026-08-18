/**
 * @file portability.h
 * @brief Internal platform portability shims (header-only).
 *
 * Owning the feature-test macros here (instead of a bare `#define
 * _GNU_SOURCE` in each translation unit) lets every unit share the same
 * platform switch: with LOOMWORKS_POSIX_FALLBACK defined we drop the GNU
 * extensions and compile against the strictly-POSIX path, which is exactly
 * what a macOS/BSD build would hit.
 */
#ifndef LOOMWORKS_PORTABILITY_H
#define LOOMWORKS_PORTABILITY_H

/* _GNU_SOURCE MUST be defined before ANY system header is included.  Only
 * glibc/Linux needs pthread_tryjoin_np; on every other POSIX platform we
 * simulate its EBUSY semantics with a pthread_kill probe instead. */
#if defined(__linux__) && !defined(LOOMWORKS_POSIX_FALLBACK)
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <pthread.h>

/* Non-blocking thread join probe.
 *
 * GNU path: pthread_tryjoin_np — returns 0 once the thread exits, EBUSY while
 * it is still running.
 * POSIX-only path: pthread_kill(tid, 0) returns 0 while the thread EXISTS;
 * a terminated-but-not-joined thread still exists, so the probe may return
 * EBUSY for a short window after exit — the caller's re-post loop tolerates
 * that exactly as it tolerates GNU's EBUSY.  Once the thread has exited and
 * is reaped, pthread_kill fails (ESRCH) and we join for real.
 */
static inline int loom_tryjoin(pthread_t thread, void **retval)
{
#if defined(__linux__) && !defined(LOOMWORKS_POSIX_FALLBACK)
    return pthread_tryjoin_np(thread, retval);
#else
    if (pthread_kill(thread, 0) == 0) {
        return EBUSY;
    }
    return pthread_join(thread, retval);
#endif
}

/* macOS/older BSDs name the anonymous-mmap flag MAP_ANON. */
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#endif /* LOOMWORKS_PORTABILITY_H */