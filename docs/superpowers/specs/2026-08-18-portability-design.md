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
installed, NOT exported. Header-only, no new source file, no
LOOMWORKS_SOURCES change. **Design revision (2026-08-18, supersedes the
original `loom_tryjoin` probe):** an earlier draft simulated
`pthread_tryjoin_np` with a `pthread_kill(thread, 0)` probe, but glibc
returns 0 (errno EINVAL) for a thread that has exited but not yet been
joined — never ESRCH — so the simulated EBUSY never clears and the
resize/shrink join loops spin forever on the fallback path. The shipped
design instead polls the library's existing `thread_clean_exit[idx]`
atomic flag (set by `worker_entry` on every exit path before breaking
out) and then joins explicitly. No GNU extension is needed at all:

```c
#ifndef LOOMWORKS_PORTABILITY_H
#define LOOMWORKS_PORTABILITY_H

/* POSIX.1-2008 must be defined before any system header; glibc with
 * strict -std=c11 hides clock_gettime / pthread_condattr_setclock /
 * posix_memalign without it. Guarded so coroutine.c's own definition
 * does not warn. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>

/* Some BSDs call the anonymous-mmap flag MAP_ANON. */
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#endif /* LOOMWORKS_PORTABILITY_H */
```

No `loom_tryjoin` helper exists in the shipped design: the three join
loops poll `thread_clean_exit[idx]` directly (see §3.2), which is
platform-neutral and removes the `_GNU_SOURCE`/`pthread_tryjoin_np`
dependency entirely.

### 3.2 Consumers

`src/thread_pool.c`:
- include `"portability.h"` FIRST, before the existing system-header
  includes (the pre-existing unconditional `#define _GNU_SOURCE` at L21 is
  removed; the shim owns the feature-test macro)
- three join loops replaced with a clean_exit-flag poll + explicit join:
  - grow rollback loop A (worker_arg malloc failure, ~L2071)
  - grow rollback loop B (pthread_create failure, ~L2096)
  - shrink join loop (displaced workers, ~L2132)

Each loop body becomes:

```c
while (!atomic_load_explicit(&pool->thread_clean_exit[idx], memory_order_acquire)) {
    sem_post(&pool->work_sem);
    sched_yield();
}
pthread_join(pool->threads[idx], NULL);
```

`worker_entry` sets `thread_clean_exit[idx] = true` on every exit path
before breaking out, so the poll is the portable wait-for-exit primitive;
the explicit join then returns immediately. The loops still re-post the
semaphore token (waking the displaced worker) exactly as before.

`src/coroutine.c`:
- include `"portability.h"` so the `MAP_ANONYMOUS`/`MAP_ANON` fallback covers
  the anonymous mmap at L208 on BSD platforms

### 3.3 CMake switch

`CMakeLists.txt` — append to the shared definition list so BOTH library
targets (and any consumer using `${LOOMWORKS_CTX_DEFINES}`) get the macro;
this is the same variable that carries `LOOMWORKS_CTX_ASM_*` and is already
applied to `loomworks_static` (L73) and `loomworks_shared` (L82). Add after
the backend-selection block (~L31):

```cmake
option(LOOMWORKS_POSIX_FALLBACK
       "Force the portable (non-GNU) code path (simulates non-GNU platforms)"
       OFF)
if(LOOMWORKS_POSIX_FALLBACK)
    list(APPEND LOOMWORKS_CTX_DEFINES LOOMWORKS_POSIX_FALLBACK=1)
endif()
```

The switch exists to compile the exact code path macOS/BSD would compile,
enabling deterministic verification on Linux (the shim's GNU-path branch is
still selected for feature parity, but the flag exists so CI can prove the
fallback behaves identically).

## 4. Testing

| Test | Command | Expect |
|---|---|---|
| Default GNU path | `cmake --build build --parallel 4` + `ctest --test-dir build --output-on-failure` | 4/4 pass, baselines 20744 / 5611 / 78746 (±40) / 200014 |
| Fallback simulation | `cmake -S . -B build-posix -DLOOMWORKS_POSIX_FALLBACK=ON` + build + ctest | 4/4 pass with identical assertions |
| join loops exercised | both runs include resize grow/shrink tests that hit the 3 clean_exit-poll join loops | pass |

Existing baselines must stay green or only increase.

## 5. Docs

- `docs/risk-assessment.md`: R12 register row → `✅ resolved (2026-08-18): portable clean_exit-poll join + POSIX-only shim (portability.h); CMake LOOMWORKS_POSIX_FALLBACK simulates non-GNU platforms in CI; platform floor macOS 10.12+/Linux`. Detailed section + maintenance-priority rewrite (R12 removed from open list).
- `CHANGELOG.md` [Unreleased] Added/Fixed bullet.
- `docs/design-decisions.md`: decision 20 — portability shim (why clean_exit-poll join instead of tryjoin; why the kill probe was falsified; why fallback force-switch exists).
- `docs/api-reference.md`: platform note (Linux x86-64/aarch64 primary; macOS 10.12+ BSD via fallback; no Windows).

## 6. Commit Plan

- `refactor(pool): add portable tryjoin shim (R12)` (2658bab — shipped)
- `fix(pool): wait on clean_exit flag instead of tryjoin probe (R12)` (93b92be — shipped; kill probe falsified)
- `build: add LOOMWORKS_POSIX_FALLBACK switch (R12)` (86cce21 — shipped)
- `docs: record portability shim (R12 resolved)`

## 7. Acceptance Criteria

AC1. The three join loops poll `thread_clean_exit[idx]` and join explicitly
     on both paths; no `pthread_tryjoin_np` / kill-probe remains in code.
AC2. All three join loops keep the semaphore re-post + sched_yield pattern;
     loop logic unchanged apart from the wait condition.
AC3. `MAP_ANONYMOUS` defined as `MAP_ANON` fallback where missing.
AC4. `_GNU_SOURCE` no longer unconditional in thread_pool.c.
AC5. Default build: ctest 4/4, baselines not regressed.
AC6. build-posix (LOOMWORKS_POSIX_FALLBACK=ON): ctest 4/4, same assertion counts.
AC7. Docs updated (risk-assessment row, CHANGELOG, design-decisions, platform note).