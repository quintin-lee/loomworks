# loomworks

Industrial-grade C11 concurrency library featuring a **thread pool** and a **stackful coroutine** subsystem.

```
Tests: 1025 pool + 36 coroutine + 50511 integration — all passing
Build: gcc -Wall -Wextra -Werror -pedantic -std=c11 -pthread — zero warnings
```

## Features

### Thread Pool (`loom_thread_pool_t`)

| Feature | Description |
|---------|-------------|
| Opaque pointer API | Internal struct definitions are not exposed in headers |
| Configurable workers | `0` auto-detected as `hardware_concurrency * 2`, clamped to 64 |
| Bounded / unbounded queue | `queue_capacity > 0` blocks on submit; `0` means unbounded |
| Future return values | `loom_pool_submit_future()` + `loom_future_wait()` for async results |
| Graceful shutdown | `loom_pool_shutdown()` drains all pending tasks before exiting |
| Cache-line alignment | Locks and queue pointers aligned to 64B to prevent false sharing |

### Coroutines (`loom_coroutine_t`)

| Feature | Description |
|---------|-------------|
| Opaque pointer API | Internal struct definitions are not exposed in headers |
| mmap stack allocation | Stacks allocated via `mmap` with `PROT_NONE` guard pages on both ends |
| Signal handling | SIGSEGV/SIGBUS caught for stack overflow; `longjmp` returns error state |
| Per-thread scheduler | Scheduler context uses `_Thread_local` for cross-thread safety |
| Full lifecycle | `create → resume → yield/resume → terminate → destroy` |
| 64-bit safe | `makecontext` args passed via `uintptr_t → unsigned long` cast |

## Project Structure

```
loomworks/
├── CMakeLists.txt
├── include/loomworks/
│   ├── loomworks.h            # Convenience single-header include
│   ├── thread_pool.h       # Thread pool public API
│   └── coroutine.h         # Coroutine public API
├── src/
│   ├── thread_pool.c       # Thread pool implementation
│   ├── thread_pool_internal.h
│   ├── coroutine.c         # Coroutine implementation
│   └── coroutine_internal.h
├── tests/
│   ├── test_thread_pool.c   # 1025 assertions
│   ├── test_coroutine.c     # 36 assertions
│   └── test_integration.c   # 50511 assertions
└── docs/
    ├── architecture.md
    ├── api-reference.md
    ├── contributing.md
    ├── design-decisions.md
    ├── faq.md
    └── migration.md
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
gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include \
    src/thread_pool.c src/coroutine.c \
    tests/test_thread_pool.c -o test_thread_pool && ./test_thread_pool

gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include \
    src/thread_pool.c src/coroutine.c \
    tests/test_coroutine.c -o test_coroutine && ./test_coroutine

gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include \
    src/thread_pool.c src/coroutine.c \
    tests/test_integration.c -o test_integration && ./test_integration
```

## Quick Start

### Thread Pool

```c
#include "loomworks/thread_pool.h"

// Create a thread pool (defaults: worker_count=0, queue_capacity=0)
loom_thread_pool_t *pool = NULL;
loom_pool_create(NULL, &pool);

// Submit a fire-and-forget task
int sum = 0;
loom_pool_submit(pool, simple_task, &sum);

// Submit a task with a future result
loom_future_t *fut = NULL;
loom_pool_submit_future(pool, result_fn, NULL, &fut);
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
    LOOMWORKS_ERR_INVALID, // Invalid argument or queue full
    LOOMWORKS_ERR_SHUTDOWN,// Pool is shutting down or shut down
    LOOMWORKS_ERR_TIMEOUT, // Timeout (reserved)
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


## FAQ

**Q: Can I use loomworks as a shared library (.so)?**

A: No. Coroutines use `_Thread_local` storage, which is incompatible with shared library linking on x86_64 Linux. Use static linking only (`libloomworks.a`). The CMake build still produces a `.so` for completeness, but linking it will fail at runtime.

**Q: Can I resume a coroutine from a different thread than the one that created it?**

A: No. Coroutines are bound to the thread that created them. `ucontext_t` is not thread-safe — calling `loom_coro_resume()` on a coroutine from a different thread than it was created on results in undefined behavior (typically SIGSEGV).

**Q: What happens if a coroutine overflows its stack?**

A: The library installs a SIGSEGV/SIGBUS handler that detects access to guard pages. When triggered, the coroutine is moved to the `ERROR` state and `loom_coro_resume()` returns `LOOMWORKS_CORO_ERR_GUARD`. The process does not crash.

**Q: Is `loom_pool_shutdown()` idempotent?**

A: Yes. Calling `loom_pool_shutdown()` multiple times on the same pool is safe — only the first call performs the drain and join. Subsequent calls are no-ops.

**Q: What is the maximum number of worker threads?**

A: When `worker_count` is set to 0 (auto), the pool creates `min(hardware_concurrency * 2, 64)` threads. The hard upper bound is 64.

**Q: Does `loom_pool_submit()` block when the queue is full?**

A: No. When `queue_capacity > 0` and the queue is full, `loom_pool_submit()` returns `LOOMWORKS_ERR_INVALID` immediately. The caller must retry or handle the error. Use `queue_capacity = 0` for an unbounded (potentially large) queue.

**Q: How do I compile with AddressSanitizer?**

A: Add `-fsanitize=address -fno-omit-frame-pointer` to your compiler flags. The library is fully compatible with ASan. Run `LD_PRELOAD=/usr/lib/libasan.so.8 ./build/test_integration` for leak-checked integration tests.


## FAQ

**Q: Can I use loomworks as a shared library (.so)?**

A: No. Coroutines use `_Thread_local` storage, which is incompatible with shared library linking on x86_64 Linux. Use static linking only (`libloomworks.a`). The CMake build still produces a `.so` for completeness, but linking it will fail at runtime.

**Q: Can I resume a coroutine from a different thread than the one that created it?**

A: No. Coroutines are bound to the thread that created them. `ucontext_t` is not thread-safe — calling `loom_coro_resume()` on a coroutine from a different thread than it was created on results in undefined behavior (typically SIGSEGV).

**Q: What happens if a coroutine overflows its stack?**

A: The library installs a SIGSEGV/SIGBUS handler that detects access to guard pages. When triggered, the coroutine is moved to the `ERROR` state and `loom_coro_resume()` returns `LOOMWORKS_CORO_ERR_GUARD`. The process does not crash.

**Q: Is `loom_pool_shutdown()` idempotent?**

A: Yes. Calling `loom_pool_shutdown()` multiple times on the same pool is safe — only the first call performs the drain and join. Subsequent calls are no-ops.

**Q: What is the maximum number of worker threads?**

A: When `worker_count` is set to 0 (auto), the pool creates `min(hardware_concurrency * 2, 64)` threads. The hard upper bound is 64.

**Q: Does `loom_pool_submit()` block when the queue is full?**

A: No. When `queue_capacity > 0` and the queue is full, `loom_pool_submit()` returns `LOOMWORKS_ERR_INVALID` immediately. The caller must retry or handle the error. Use `queue_capacity = 0` for an unbounded (potentially large) queue.

**Q: How do I compile with AddressSanitizer?**

A: Add `-fsanitize=address -fno-omit-frame-pointer` to your compiler flags. The library is fully compatible with ASan. Run `LD_PRELOAD=/usr/lib/libasan.so.8 ./build/test_integration` for leak-checked integration tests.

## Design Constraints

| Requirement | Implementation |
|-------------|----------------|
| Pure C11 | Uses only `stdatomic.h`, `_Thread_local`, `_Alignas` |
| Memory safety | `mmap` + `mprotect` PROT_NONE guard pages, NULL checks |
| False-sharing prevention | Locks and queues use `__attribute__((aligned(64)))` separation |
| System call robustness | All `pthread_*`/`malloc`/`mmap`/`mprotect` return values checked |
| 64-bit compatible | `makecontext` args cast via `uintptr_t → unsigned long` |

## Contributing

See [docs/contributing.md](docs/contributing.md) for coding standards and the submission process.

## FAQ

See [docs/faq.md](docs/faq.md) for frequently asked questions.

## Migration

Migrating from ctpool? See [docs/migration.md](docs/migration.md).

## Notes

- **Dynamic library (`.so`) is not supported.** Coroutines use `_Thread_local` storage; linking as a shared object produces TPOFF relocation errors. Static linking only (`libloomworks.a`).
- **Signal handler safety:** The coroutine guard-page handler uses `longjmp`, which means `loom_coro_resume()` may return from a non-deterministic point. Do not rely on state after a guard-page error beyond calling `loom_coro_destroy()`.
- **Scheduler stack residency:** Each thread allocates a 128 KiB scheduler stack on first coroutine use. This memory is intentionally never freed (reclaimed by the OS at thread exit) to avoid reallocation overhead.
- **POSIX dependency:** Requires a POSIX-compliant platform with `ucontext(3)`, `mmap(2)`, and `pthread(3)`. Tested on Linux/x86_64. macOS and other POSIX platforms may work with minor adjustments.
