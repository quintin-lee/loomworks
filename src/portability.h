/**
 * @file portability.h
 * @brief Internal platform portability shims (header-only).
 *
 * Owning the feature-test macro here (instead of a bare `#define` in each
 * translation unit) lets every unit share the same platform switch: with
 * LOOMWORKS_POSIX_FALLBACK defined we drop the GNU extensions and compile
 * against the strictly-POSIX path, which is exactly what a macOS/BSD build
 * would hit.
 */
#ifndef LOOMWORKS_PORTABILITY_H
#define LOOMWORKS_PORTABILITY_H

/* POSIX.1-2008 must be visible BEFORE any system header: glibc's strict
 * -std=c11 hides clock_gettime / pthread_condattr_setclock / posix_memalign
 * without a feature-test macro, and non-GNU platforms expose them by default.
 * Guarded so a translation unit that defines it itself (e.g. coroutine.c)
 * does not trip a redefinition warning. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>

/* macOS/older BSDs name the anonymous-mmap flag MAP_ANON. */
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#endif /* LOOMWORKS_PORTABILITY_H */
