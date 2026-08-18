# R12 Closure — Cross-Platform Portability (tryjoin shim)

## 1. Background

`docs/risk-assessment.md` register row R12 (High/Med/Med) is the top maintenance
priority. The audit identified macOS as the long pole: the codebase relies on
GNU extensions that macOS/BSD do not provide.

Codebase-wide grep (2026-08-18) established the exact platform-dependency plane:

| Dependency | Location | Linux x86-64/aarch64 | macOS / BSD |
|---|---|---|---|
| `pthread_tryjoin_np` (GNU extension) | src/thread_pool.c:2071, 2096, 2132 | available | **absent** |
| `_GNU_SOURCE` (required by the above) | src/thread_pool.c:21 | fine | harmless but unnecessary |
| `pthread_condattr_setclock` + `CLOCK_MONOTONIC` | thread_pool.c:123, task_group.c:45 | available | available (macOS 10.12+) |
| `clock_gettime(CLOCK_MONOTONIC)` | multiple | available | available (macOS 10.12+) |
| `MAP_ANONYMOUS` | src/coroutine.c:208 | available | some BSD use `MAP_ANON` |
| `sem_t` / `sem_post` / `sem_wait` | thread_pool.c (work_sem) | available | available (modern macOS) |
| `sigaction` / `SIGBUS` / `sched_yield` | coroutine.c, thread_pool.c | POSIX | POSIX |

The only hard portability defect is `pthread_tryjoin_np` at three call sites
(two grow-rollback loops + the shrink join loop). The three call sites all use
the identical `while (rc != 0) { sem_post; sched_yield; }` re-post pattern.

`_GNU_SOURCE` is currently defined unconditionally in thread_pool.c only to
expose `pthread_tryjoin_np`.

## 2. Scope

**In scope:** a library-internal `portability.h` shim for `pthread_tryjoin_np`,
an optional CMake switch that forces the fallback path so the non-GNU code
path is actually executed and tested on Linux, and `MAP_ANONYMOUS` portability.
Docs updates (risk-assessment, CHANGELOG, design-decisions, api-reference
platform note).

**Out of scope (anti-scope-creep):**
- asm backend Mach-O/AArch64-MachO port (cannot be validated on Linux; the
  review already documents asm as the default for x86-64 and aarch64 Linux)
- macOS-era shims for `sem_*` / `clock_gettime` (modern macOS 10.12+ satisfies
  POSIX here; documented platform floor)
- Windows/FreeBSD full porting (no CI, no environment, nothing to verify)

## 3. Design

### 3.1 New internal header `src/portability.h`

Library-internal only (same residence as `thread_pool_internal.h`); NOT
installed, NOT exported.

```c
#ifndef LOOMWORKS_PORTABILITY_H
#define LOOMWORKS_PORTABILITY_H

/*
 * Cross-platform glue for non-POSIX APIs threads use.
 */

#include <pthread.h>

/* GNU-only pthread_tryjoin_np is not available on macOS/BSD.  Provide the
 * same contract via a POSIX-only probe: pthread_kill(thread, 0) == 0 means
 * the thread is still running (-> EBUSY); ESRCH means it has exited and is
 * waiting to be reaped, so pthread_join returns immediately.  There is no
 * race: joining a thread that exits right after a successful probe is the
 * caller's next loop iteration, and the shrink/spill loops already re-post
 * the semaphore token on EBUSY, so a thread that exits right after a probe
 * is picked up by that re-post. */
int loom_tryjoin(pthread_t thread, void **retval);

/* Some BSDs call the anonymous-mmap flag MAP_ANON. */
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#endif /* LOOMWORKS_PORTABILITY_H */
```

```c
/* portability.c — implementation */
#include "portability.h"

int loom_tryjoin(pthread_t thread, void **retval)
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
```

Contract mirrors `pthread_tryjoin_np`: returns 0 on successful reaping, EBUSY
if the thread is still running, other errno on failure. The three call sites'
existing `while (rc != 0) { sem_post; sched_yield; }` loops require no change.

### 3.2 Threads shim in library only

Where do the tryjoin calls live? `src/thread_pool.c` shrink + grow rollback.
The three call sites are replaced with `loom_tryjoin`:

- grow rollback loop A (worker_arg malloc failure)
- grow rollback loop B (pthread_create failure)
- shrink join loop (displaced workers)

`#define _GNU_SOURCE` moves from thread_pool.c into a single guarded location.
To keep the `pthread_tryjoin_np` path compiled only when it is actually
available, `_GNU_SOURCE` is defined in portability.c before including
`portability.h`, conditioned the same way (Linux && !fallback):

```c
#if defined(__linux__) && !defined(LOOMWORKS_POSIX_FALLBACK)
#define _GNU_SOURCE
#endif
```

(pre-warning: this is the same rule; the existing unconditional
`#define _GNU_SOURCE` at thread_pool.c:21 is removed; any other file
needing it keeps its own.)

### 3.3 CMake switch

`CMakeLists.txt`:

```cmake
option(LOOMWORKS_POSIX_FALLBACK
       "Force the portable pthread_tryjoin fallback (simulates non-GNU platforms)"
       OFF)
if(LOOMWORKS_POSIX_FALLBACK)
    target_compile_definitions(loomworks PRIVATE LOOMWORKS_POSIX_FALLBACK)
endif()
```

Switching it ON builds the exact code path macOS/BSD would compile, enabling
deterministic verification on Linux.

## 4. Testing

| Test | Command | Expect |
|---|---|---|
| Default GNU path | `cmake --build build --parallel 4` + `ctest --test-dir build --output-on-failure` | 4/4 pass, baselines 20744 / 5611 / 78746 (±40) / 200014 |
| Fallback simulation | `cmake -S . -B build-posix -DLOOMWORKS_POSIX_FALLBACK=ON` + build + ctest | 4/4 pass with identical assertions |
| tryjoin exercised | both runs include resize grow/shrink tests that hit the 3 call sites | pass |

Existing baselines must stay green or only increase.

## 5. Docs

- `docs/risk-assessment.md`: R12 register row → `✅ resolved (2026-08-18): portable tryjoin shim (portability.h) with POSIX-only fallback; CMake LOOMWORKS_POSIX_FALLBACK simulates non-GNU platforms in CI; platform floor macOS 10.12+/Linux`. Detailed section + maintenance-priority rewrite (R12 removed from open list).
- `CHANGELOG.md` [Unreleased] Added/Fixed bullet.
- `docs/design-decisions.md`: decision 20 — portable tryjoin shim (why shim instead of restructuring; why EBUSY probe is race-free; why fallback force-switch exists).
- `docs/api-reference.md`: platform note (Linux x86-64/aarch64 primary; macOS 10.12+ BSD via fallback; no Windows).

## 6. Commit Plan

- `refactor(pool): add portable tryjoin shim (R12)`
- `docs: record portability shim (R12 resolved)`

## 7. Acceptance Criteria

AC1. `loom_tryjoin` compiles on both paths; default path delegates to
     `pthread_tryjoin_np`, fallback uses kill-probe + join.
AC2. All three tryjoin call sites delegate through `loom_tryjoin`; call-site
     loop logic unchanged.
AC3. `MAP_ANONYMOUS` defined as `MAP_ANON` fallback where missing.
AC4. `_GNU_SOURCE` no longer unconditional in thread_pool.c.
AC5. Default build: ctest 4/4, baselines not regressed.
AC6. build-posix (LOOMWORKS_POSIX_FALLBACK=ON): ctest 4/4, same assertion counts.
AC7. Docs updated (risk-assessment row, CHANGELOG, design-decisions, platform note).