# loomworks API Reference

> This document describes all public APIs of the loomworks library. All handles are opaque pointers; struct definitions are not exposed in headers.

---

## Table of Contents

1. [Thread Pool API](#1-thread-pool-api)
2. [Coroutine API](#2-coroutine-api)
3. [Result Codes Quick Reference](#3-result-codes-quick-reference)
4. [Thread Safety](#4-thread-safety)

---

## 1. Thread Pool API

### 1.1 Create and Destroy

```c
loom_result_t loom_pool_create(const loom_pool_config_t *config,
                                   loom_thread_pool_t **pool);

void loom_pool_destroy(loom_thread_pool_t **pool);
```

| Parameter | Description |
|-----------|-------------|
| `config` | Configuration struct; pass `NULL` for defaults (auto worker count, unbounded queue) |
| `pool` | Output parameter; set to the new pool handle on success |

**Default configuration:**
- `worker_count`: `min(hardware_concurrency * 2, 64)`
- `stack_size`: 128 KiB
- `queue_capacity`: 0 (unbounded)

**Note:** Always call `loom_pool_shutdown()` to drain pending tasks before calling `loom_pool_destroy()`.

### 1.2 Submit Tasks

```c
loom_result_t loom_pool_submit(loom_thread_pool_t *pool,
                                   loom_task_fn fn,
                                   void *data);

loom_result_t loom_pool_submit_future(loom_thread_pool_t *pool,
                                          loom_task_fn_result fn,
                                          void *data,
                                          loom_future_t **future);
```

| Parameter | Description |
|-----------|-------------|
| `fn` | Task function, signature `void (*)(void*)` or `void* (*)(void*)` |
| `data` | Opaque pointer passed to the task function |
| `future` | Output parameter, used only with `submit_future` |

**Difference:**
- `submit`: fire-and-forget, no return value
- `submit_future`: returns a `future` handle; retrieve the result with `loom_future_wait()`

### 1.3 Retrieve Future Results

```c
loom_result_t loom_future_wait(loom_future_t *future, void **result);
void            loom_future_destroy(loom_future_t *future);
```

| Parameter | Description |
|-----------|-------------|
| `future` | Handle returned by `submit_future` |
| `result` | Output parameter pointing to the pointer returned by the task function (caller is responsible for freeing) |

**Note:** Memory pointed to by `result` is allocated by the task function via `malloc`. The caller must free it before calling `loom_future_destroy()`.

### 1.4 Shutdown and Query

```c
void loom_pool_shutdown(loom_thread_pool_t *pool);
uint32_t loom_pool_worker_count(const loom_thread_pool_t *pool);
uint32_t loom_pool_pending_count(const loom_thread_pool_t *pool);
```

- `shutdown()`: Blocks until all submitted tasks complete, then joins all worker threads
- `worker_count()`: Returns the actual number of worker threads created (including auto-computed value)
- `pending_count()`: Returns the current number of tasks waiting in the queue (may be inaccurate due to concurrent operations)

### 1.5 Configuration Structure

```c
typedef struct {
    uint32_t worker_count;     /* 0 = auto, max 64 */
    size_t   stack_size;       /* 0 = 128 KiB */
    uint32_t queue_capacity;   /* 0 = unbounded, max 1M */
} loom_pool_config_t;
```

---

## 2. Coroutine API

### 2.1 Create and Destroy

```c
loom_coro_result_t loom_coro_create(loom_coro_fn fn,
                                        void *data,
                                        size_t stack_size,
                                        loom_coroutine_t **coro);

void loom_coro_destroy(loom_coroutine_t **coro);
```

| Parameter | Description |
|-----------|-------------|
| `fn` | Coroutine entry function, signature `void (*)(void*)` |
| `data` | Opaque pointer passed to the entry function |
| `stack_size` | Stack size in bytes; 0 uses default 64 KiB |
| `coro` | Output parameter |

**Destroy note:** Must only be called when the coroutine is in `DONE` or `ERROR` state.

### 2.2 Start and Resume

```c
loom_coro_result_t loom_coro_resume(loom_coroutine_t *coro);
```

| State | Behavior |
|-------|----------|
| `NEW` | First start: allocate and configure ucontext, execute entry function |
| `SUSPENDED` | Resume from the last yield point |
| `DONE` / `ERROR` | Return `LOOMWORKS_CORO_ERR_RUNNING` |

### 2.3 Yield and Terminate

```c
void loom_coro_yield(void);
void loom_coro_suspend(void);

loom_coro_result_t loom_coro_terminate(loom_coroutine_t *coro);
```

- `yield()` / `suspend()`: Yield control back to the caller. `suspend()` is an alias for `yield()`.
- `terminate()`: Forcefully terminate the coroutine, set `state=DONE`, and resume execution in the scheduler. If the coroutine is currently running in the calling thread, the switch happens immediately.

### 2.4 State and Debugging

```c
loom_coro_state_t loom_coro_state(const loom_coroutine_t *coro);

loom_coro_result_t loom_coro_stack_info(const loom_coroutine_t *coro,
                                             void **start,
                                             void **end);

const char *loom_coro_result_str(loom_coro_result_t result);
```

- `state()`: Returns the current state enum value
- `stack_info()`: Returns the stack address range (for debugging / memory checking)
- `result_str()`: Converts a result code to a human-readable string

### 2.5 Configuration Constants

```c
#define LOOMWORKS_CORO_DEFAULT_STACK_SIZE    (64 * 1024)   /* 64 KiB */
#define LOOMWORKS_CORO_GUARD_PAGES_EACH      1              /* Guard pages per side */
```

---

## 3. Result Codes Quick Reference

### Thread Pool (`loom_result_t`)

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `LOOMWORKS_OK` | Success |
| 1 | `LOOMWORKS_ERR_ALLOC` | Memory allocation failed |
| 2 | `LOOMWORKS_ERR_THREAD` | Thread creation failed |
| 3 | `LOOMWORKS_ERR_INVALID` | Invalid argument or queue full |
| 4 | `LOOMWORKS_ERR_SHUTDOWN` | Pool is shutting down or shut down |
| 5 | `LOOMWORKS_ERR_TIMEOUT` | Timeout (reserved) |

### Coroutine (`loom_coro_result_t`)

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `LOOMWORKS_CORO_OK` | Success |
| 1 | `LOOMWORKS_CORO_ERR_ALLOC` | mmap allocation failed |
| 2 | `LOOMWORKS_CORO_ERR_CONTEXT` | ucontext operation failed |
| 3 | `LOOMWORKS_CORO_ERR_MPROTECT` | mprotect failed |
| 4 | `LOOMWORKS_CORO_ERR_INVALID` | Invalid argument |
| 5 | `LOOMWORKS_CORO_ERR_GUARD` | Guard page triggered (stack overflow) |
| 6 | `LOOMWORKS_CORO_ERR_RUNNING` | Invalid state for operation |

### Coroutine State (`loom_coro_state_t`)

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `LOOMWORKS_CORO_NEW` | Created, not started |
| 1 | `LOOMWORKS_CORO_RUNNING` | Currently executing |
| 2 | `LOOMWORKS_CORO_SUSPENDED` | Yielded, waiting for resume |
| 3 | `LOOMWORKS_CORO_DONE` | Completed normally |
| 4 | `LOOMWORKS_CORO_ERROR` | Error occurred |

---

## 4. Complete Usage Example

```c
#include <stdio.h>
#include <stdlib.h>
#include <loomworks/loomworks.h>

// Example: computing sum of squares in parallel
void sum_squares_task(void *arg) {
    int *total = (int *)arg;
    __sync_fetch_and_add(total, 100);  // each task adds 100
}

int *compute_result(void *arg) {
    (void)arg;
    int *ptr = (int *)malloc(sizeof(int));
    *ptr = 42;
    return ptr;
}

int main(void) {
    // Create pool
    loom_pool_config_t cfg = { .worker_count = 4, .queue_capacity = 1000 };
    loom_thread_pool_t *pool = NULL;
    LOOMWORKS_CHECK(loom_pool_create(&cfg, &pool));

    // Submit fire-and-forget tasks
    int sum = 0;
    for (int i = 0; i < 10; i++) {
        LOOMWORKS_CHECK(loom_pool_submit(pool, sum_squares_task, &sum));
    }

    // Submit a future task
    loom_future_t *fut = NULL;
    LOOMWORKS_CHECK(loom_pool_submit_future(pool, compute_result, NULL, &fut));

    // Wait for result
    void *result = NULL;
    LOOMWORKS_CHECK(loom_future_wait(fut, &result));
    printf("future result: %d\n", *(int *)result);
    free(result);

    // Print stats
    printf("pending tasks: %u\n", loom_pool_pending_count(pool));
    printf("worker count: %u\n", loom_pool_worker_count(pool));

    // Cleanup
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    return 0;
}

#define LOOMWORKS_CHECK(rc) do {     if ((rc) != LOOMWORKS_OK) {         fprintf(stderr, "loomworks error: %d\n", (int)(rc));         exit(1);     } } while (0)
```

---

## 5. Thread Safety


| API | Thread-safe | Notes |
|-----|-------------|-------|
| `loom_pool_create` | ✅ Yes | Call within a single thread |
| `loom_pool_submit` | ✅ Yes | Safe for concurrent calls; internal locking |
| `loom_pool_submit_future` | ✅ Yes | Safe for concurrent calls; internal locking |
| `loom_pool_shutdown` | ⚠️ Call once only | Must be called after all submits are complete |
| `loom_pool_destroy` | ✅ Yes (NULL-safe) | Must be called after shutdown |
| `loom_future_wait` | ✅ Yes | Internal spin + condition variable wait |
| `loom_future_destroy` | ✅ Yes | Must be called after `wait()` returns |
| `loom_coro_create` | ✅ Yes | Call within a single thread |
| `loom_coro_resume` | ✅ Yes | Call within a single thread |
| `loom_coro_yield` | ✅ Yes | Call from within the coroutine |
| `loom_coro_destroy` | ✅ Yes | Must be called after coroutine reaches DONE |
| `loom_coro_stack_info` | ✅ Yes | Read-only operation, no lock needed |

**Prohibited operations:**
- Calling `submit()` / `submit_future()` after `shutdown()`
- Calling `resume()` / `yield()` / `terminate()` on the same coroutine from different threads
- Calling `destroy()` on a coroutine that is not in DONE/ERROR state
