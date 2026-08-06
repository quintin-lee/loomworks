# ctpool

Industrial-grade C11 concurrency library featuring a **thread pool** and a **stackful coroutine** subsystem.

```
Tests: 1025 pool + 36 coroutine + 50511 integration — all passing
Build: gcc -Wall -Wextra -Werror -pedantic -std=c11 -pthread — zero warnings
```

## Features

### Thread Pool (`ctpool_thread_pool_t`)

| Feature | Description |
|---------|-------------|
| Opaque pointer API | Internal struct definitions are not exposed in headers |
| Configurable workers | `0` auto-detected as `hardware_concurrency * 2`, clamped to 64 |
| Bounded / unbounded queue | `queue_capacity > 0` blocks on submit; `0` means unbounded |
| Future return values | `ctpool_pool_submit_future()` + `ctpool_future_wait()` for async results |
| Graceful shutdown | `ctpool_pool_shutdown()` drains all pending tasks before exiting |
| Cache-line alignment | Locks and queue pointers aligned to 64B to prevent false sharing |

### Coroutines (`ctpool_coroutine_t`)

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
ctpool/
├── CMakeLists.txt
├── include/ctpool/
│   ├── ctpool.h            # Convenience single-header include
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
    ├── design-decisions.md
    └── contributing.md
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
#include "ctpool/thread_pool.h"

// Create a thread pool (defaults: worker_count=0, queue_capacity=0)
ctpool_thread_pool_t *pool = NULL;
ctpool_pool_create(NULL, &pool);

// Submit a fire-and-forget task
int sum = 0;
ctpool_pool_submit(pool, ^(void *arg) {
    int *s = arg;
    __sync_fetch_and_add(s, 1);
}, &sum);

// Submit a task with a future result
ctpool_future_t *fut = NULL;
ctpool_pool_submit_future(pool, result_fn, NULL, &fut);
void *result = NULL;
ctpool_future_wait(fut, &result);

// Shutdown and destroy
ctpool_pool_shutdown(pool);
ctpool_pool_destroy(&pool);
```

### Coroutine

```c
#include "ctpool/coroutine.h"

// Create a coroutine
ctpool_coroutine_t *coro = NULL;
ctpool_coro_create(my_coro_fn, user_data, 0, &coro);  // 0 = default 64 KiB stack

// Start or resume
ctpool_coro_result_t rc = ctpool_coro_resume(coro);
if (rc != CTPPOOL_CORO_OK) { /* handle error */ }

// Yield inside the coroutine
void my_coro_fn(void *arg) {
    // ... execute ...
    ctpool_coro_yield();   // yield control, resume here on next resume()
    // ... execute ...
}

// Force-terminate
ctpool_coro_terminate(coro);

// Destroy
ctpool_coro_destroy(&coro);
```

## API Reference

### Thread Pool Result Codes

```c
typedef enum {
    CTPPOOL_OK,          // Success
    CTPPOOL_ERR_ALLOC,   // Memory allocation failed
    CTPPOOL_ERR_THREAD,  // Thread creation failed
    CTPPOOL_ERR_INVALID, // Invalid argument or queue full
    CTPPOOL_ERR_SHUTDOWN,// Pool is shutting down or shut down
    CTPPOOL_ERR_TIMEOUT, // Timeout (reserved)
} ctpool_result_t;
```

### Coroutine States

```c
typedef enum {
    CTPPOOL_CORO_NEW,       // Created, not yet started
    CTPPOOL_CORO_RUNNING,   // Currently executing
    CTPPOOL_CORO_SUSPENDED, // Paused via yield
    CTPPOOL_CORO_DONE,      // Execution completed
    CTPPOOL_CORO_ERROR,     // Error state (e.g., guard page hit)
} ctpool_coro_state_t;
```

## Design Constraints

| Requirement | Implementation |
|-------------|----------------|
| Pure C11 | Uses only `stdatomic.h`, `_Thread_local`, `_Alignas` |
| Memory safety | `mmap` + `mprotect` PROT_NONE guard pages, NULL checks |
| False-sharing prevention | Locks and queues use `__attribute__((aligned(64)))` separation |
| System call robustness | All `pthread_*`/`malloc`/`mmap`/`mprotect` return values checked |
| 64-bit compatible | `makecontext` args cast via `uintptr_t → unsigned long` |

## Notes

- **Dynamic library (`.so`) is not supported.** Coroutines use `_Thread_local` storage; linking as a shared object produces TPOFF relocation errors. Static linking only (`libctpool.a`).
