# loomworks

Industrial-grade C11 concurrency library featuring a **thread pool**, a **stackful coroutine** subsystem, and higher-level **pipeline**, **task group**, and **metrics** layers.

```
Tests: ~20771 pool + ~5611 coroutine + ~78759 integration + ~200014 ctx_smoke — all passing
Build: gcc -Wall -Wextra -Werror -pedantic -std=c11 -pthread — zero warnings
```

## Features

### Thread Pool (`loom_thread_pool_t`)

| Feature | Description |
|---------|-------------|
| Opaque pointer API | Internal struct definitions are not exposed in headers |
| Configurable workers | `0` auto-detected as `min(n, 64) * 2` where `n = sysconf(_SC_NPROCESSORS_ONLN)` (clamped to 64 before doubling: 2–128 workers) |
| Priority scheduling | 256 priority buckets (REALTIME=0 … LOW=10) with O(1) occupancy bitmap scan |
| Lock-free fast path | Vyukov bounded ring for NORMAL-priority tasks, with spill to the lanes |
| Work stealing | Per-worker Chase-Lev deques: LIFO local pops, FIFO cross-worker steal, bulk ring→deque batches of 8 |
| Future return values | `loom_pool_submit_future()` + `loom_future_wait()` for async results |
| Cancellation | `loom_pool_cancel()` / `cancel_by_id()` / `cancel_all()` via an open-addressing cancel index |
| Task node pooling | Lock-free ABA-tagged Treiber stack recycles `loom_task_t` nodes |
| Bounded queue with back-pressure | `queue_capacity > 0` waits up to 60 s on submit, then `ERR_TIMEOUT` |
| Graceful shutdown | `loom_pool_shutdown()` drains all pending tasks before exiting; resize-safe |
| Cache-line alignment | Locks and queue pointers aligned to 64B to prevent false sharing |

### Pipeline (`loom_pc_t`)

| Feature | Description |
|---------|-------------|
| Bounded / unbounded FIFO | `capacity > 0` caps pending items; `0` means unbounded |
| Internal consumers | Optional internal pool (`worker_count > 0`) drains the queue; `take()` for callers |
| Explicit close | `shutdown()` wakes consumers; submit returns `ERR_SHUTDOWN` after close |
| Counters | Atomic `submitted` / `taken` plus locked `pending_count` |

### Task Groups (`loom_task_group_t`)

| Feature | Description |
|---------|-------------|
| Grouped lifecycle | `group_cancel()` cancels every pending tracked task; `group_destroy()` cancels then frees |
| Task IDs | `group_submit()` / `group_submit_future()` return per-task `task_id` |
| `group_wait()` | Waits for every tracked task to finish; the pool stays alive and usable afterwards (no shutdown) |

### Metrics (`loom_metrics_t`)

| Feature | Description |
|---------|-------------|
| Event counters | SUBMITTED / STARTED / COMPLETED / CANCELLED / FAILED |
| Latency tracking | Sum / max / average latency; CAS-guarded max update |
| Snapshot API | Consistent read of all counters under one lock |
| Callback wiring | `loom_pool_set_metrics_callback()` fires events to an application callback |

### Coroutines (`loom_coroutine_t`)

| Feature | Description |
|---------|-------------|
| Opaque pointer API | Internal struct definitions are not exposed in headers |
| mmap stack allocation | Stacks allocated via `mmap` with `PROT_NONE` guard pages on both ends |
| Signal handling | SIGSEGV/SIGBUS caught for stack overflow; `longjmp` returns error state |
| Per-thread scheduler | Scheduler context uses `_Thread_local` for cross-thread safety |
| Full lifecycle | `create → resume → yield/resume → terminate → destroy` |
| 64-bit safe | asm backends (x86-64/aarch64) pass arguments in registers; ucontext fallback passes them via `uintptr_t` |

## Project Structure

```
loomworks/
├── CMakeLists.txt
├── include/loomworks/
│   ├── loomworks.h            # Convenience single-header include
│   ├── thread_pool.h          # Thread pool public API
│   ├── coroutine.h            # Coroutine public API
│   ├── pipeline.h             # Pipeline public API
│   ├── task_group.h           # Task group public API
│   └── metrics.h              # Metrics public API
├── src/
│   ├── thread_pool.c          # Thread pool implementation
│   ├── thread_pool_internal.h
│   ├── coroutine.c            # Coroutine implementation
│   ├── coroutine_internal.h
│   ├── coro_ctx.h             # Context-switch backend abstraction
│   ├── ctx_x86_64.S           # asm backend (x86-64, SysV)
│   ├── ctx_aarch64.S          # asm backend (aarch64, AAPCS64)
│   ├── pipeline.c             # Pipeline implementation
│   ├── task_group.c           # Task group implementation
│   └── metrics.c              # Metrics implementation
├── tests/
│   ├── test_thread_pool.c     # ~20771 assertions
│   ├── test_coroutine.c       # ~5611 assertions
│   ├── test_integration.c     # ~78759 assertions
│   └── ctx_smoke.c            # ~200014 context-switch smoke checks
├── examples/
│   ├── basic_pool.c           # Minimal pool usage
│   ├── bench.c                # Benchmark harness
│   ├── backpressure_demo.c    # Bounded-queue backpressure demo
│   ├── cancel_demo.c          # Cancellation demo
│   ├── coroutine_demo.c       # Coroutine usage demo
│   ├── monitor_demo.c         # Metrics monitoring demo
│   ├── pipeline_demo.c        # Pipeline usage demo
│   ├── priority_demo.c        # Priority scheduling demo
│   ├── resize_demo.c          # Resize demo
│   └── task_group_demo.c      # Task group usage demo
├── tools/
│   └── bench_compare.py       # A/B benchmark comparator
└── docs/
    ├── architecture.md
    ├── architecture-review.md # Third-party design review (2026-08-17)
    ├── api-reference.md
    ├── contributing.md
    ├── design-decisions.md
    ├── faq.md
    ├── migration.md
    └── risk-assessment.md     # Risk register + resolution history
```

## Build and Test

### Using CMake

```bash
cmake -S . -B build_cmake
cmake --build build_cmake
cmake --build build_cmake --target test
# or directly
cd build_cmake && ctest --output-on-failure
```

### Using GCC Directly

```bash
# Each test needs all five implementation translation units:
SRCS="src/thread_pool.c src/coroutine.c src/pipeline.c src/task_group.c src/metrics.c"
gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include $SRCS \
    tests/test_thread_pool.c -o test_thread_pool && ./test_thread_pool

gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include $SRCS \
    tests/test_coroutine.c -o test_coroutine && ./test_coroutine

gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include $SRCS \
    tests/test_integration.c -o test_integration && ./test_integration
```

## Quick Start

### Thread Pool

```c
#include "loomworks/thread_pool.h"

// Create a thread pool (defaults: worker_count=0, queue_capacity=0)
loom_thread_pool_t *pool = NULL;
loom_pool_create(NULL, &pool);

// Submit a fire-and-forget task (task_id out-param may be NULL)
int sum = 0;
uint64_t tid = 0;
loom_pool_submit(pool, simple_task, &sum, &tid);

// Submit a task with a future result
loom_future_t *fut = NULL;
uint64_t fut_id = 0;
loom_pool_submit_future(pool, result_fn, NULL, &fut_id, &fut);
void *result = NULL;
loom_future_wait(fut, &result);

// Shutdown and destroy
loom_pool_shutdown(pool);
loom_pool_destroy(&pool);

// ---- Task functions ----

void simple_task(void *arg) {
    int *s = arg;
    __sync_fetch_and_add(s, 1);
}

void *result_fn(void *arg) {
    (void)arg;
    return malloc(42);  /* caller must free */
}
```

### Coroutine

```c
#include "loomworks/coroutine.h"

// Create a coroutine
loom_coroutine_t *coro = NULL;
loom_coro_create(my_coro_fn, user_data, 0, &coro);  // 0 = default 64 KiB stack

// Start or resume
loom_coro_result_t rc = loom_coro_resume(coro);
if (rc != LOOMWORKS_CORO_OK) { /* handle error */ }

// Yield inside the coroutine
void my_coro_fn(void *arg) {
    // ... execute ...
    loom_coro_yield();   // yield control, resume here on next resume()
    // ... execute ...
}

// Force-terminate
loom_coro_terminate(coro);

// Destroy
loom_coro_destroy(&coro);
```

## API Reference

### Thread Pool Result Codes

```c
typedef enum {
    LOOMWORKS_OK,          // Success
    LOOMWORKS_ERR_ALLOC,   // Memory allocation failed
    LOOMWORKS_ERR_THREAD,  // Thread creation failed
    LOOMWORKS_ERR_INVALID, // Invalid argument
    LOOMWORKS_ERR_SHUTDOWN,// Pool is shutting down or shut down
    LOOMWORKS_ERR_TIMEOUT, // Timeout expired (blocking submit / future_wait_timeout)
} loom_result_t;
```

### Coroutine States

```c
typedef enum {
    LOOMWORKS_CORO_NEW,       // Created, not yet started
    LOOMWORKS_CORO_RUNNING,   // Currently executing
    LOOMWORKS_CORO_SUSPENDED, // Paused via yield
    LOOMWORKS_CORO_DONE,      // Execution completed
    LOOMWORKS_CORO_ERROR,     // Error state (e.g., guard page hit)
} loom_coro_state_t;
```


## Design Constraints

| Requirement | Implementation |
|-------------|----------------|
| Pure C11 | Uses only `stdatomic.h`, `_Thread_local`, `_Alignas` |
| Memory safety | `mmap` + `mprotect` PROT_NONE guard pages, NULL checks |
| False-sharing prevention | Locks and queues use `__attribute__((aligned(64)))` separation |
| System call robustness | All `pthread_*`/`malloc`/`mmap`/`mprotect` return values checked |
| 64-bit compatible | asm backends pass arguments in registers; ucontext fallback casts via `uintptr_t` |

## Contributing

See [docs/contributing.md](docs/contributing.md) for coding standards and the submission process.

## FAQ

See [docs/faq.md](docs/faq.md) for frequently asked questions.

## Migration

Migrating from ctpool? See [docs/migration.md](docs/migration.md).

## Notes

- **Shared library supported.** CMake builds both `libloomworks.a` (static) and `libloomworks.so` (shared, SOVERSION 1). The shared library works at runtime, including the coroutine subsystem, on modern toolchains (default `-fPIC` handles `_Thread_local` correctly).
- **Signal handler safety:** The coroutine guard-page handler uses `longjmp`, which means `loom_coro_resume()` may return from a non-deterministic point. Do not rely on state after a guard-page error beyond calling `loom_coro_destroy()`.
- **Scheduler stack residency:** Each thread allocates a 128 KiB scheduler stack on first coroutine use. It is freed at thread exit by `loom_coro_exit()`, and any stragglers are freed at process exit.
- **POSIX dependency:** Requires a POSIX-compliant platform with `mmap(2)` and `pthread(3)`. The context backend is hand-written assembly on x86-64 and aarch64; POSIX `ucontext(3)` is used only as a compile-time fallback on other platforms. Tested on Linux/x86_64 and aarch64 (QEMU in CI).
