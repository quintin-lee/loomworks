# ctpool API Reference

> This document describes all public APIs of the ctpool library. All handles are opaque pointers; struct definitions are not exposed in headers.

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
ctpool_result_t ctpool_pool_create(const ctpool_pool_config_t *config,
                                   ctpool_thread_pool_t **pool);

void ctpool_pool_destroy(ctpool_thread_pool_t **pool);
```

| Parameter | Description |
|-----------|-------------|
| `config` | Configuration struct; pass `NULL` for defaults (auto worker count, unbounded queue) |
| `pool` | Output parameter; set to the new pool handle on success |

**Default configuration:**
- `worker_count`: `min(hardware_concurrency * 2, 64)`
- `stack_size`: 128 KiB
- `queue_capacity`: 0 (unbounded)

**Note:** Always call `ctpool_pool_shutdown()` to drain pending tasks before calling `ctpool_pool_destroy()`.

### 1.2 Submit Tasks

```c
ctpool_result_t ctpool_pool_submit(ctpool_thread_pool_t *pool,
                                   ctpool_task_fn fn,
                                   void *data);

ctpool_result_t ctpool_pool_submit_future(ctpool_thread_pool_t *pool,
                                          ctpool_task_fn_result fn,
                                          void *data,
                                          ctpool_future_t **future);
```

| Parameter | Description |
|-----------|-------------|
| `fn` | Task function, signature `void (*)(void*)` or `void* (*)(void*)` |
| `data` | Opaque pointer passed to the task function |
| `future` | Output parameter, used only with `submit_future` |

**Difference:**
- `submit`: fire-and-forget, no return value
- `submit_future`: returns a `future` handle; retrieve the result with `ctpool_future_wait()`

### 1.3 Retrieve Future Results

```c
ctpool_result_t ctpool_future_wait(ctpool_future_t *future, void **result);
void            ctpool_future_destroy(ctpool_future_t *future);
```

| Parameter | Description |
|-----------|-------------|
| `future` | Handle returned by `submit_future` |
| `result` | Output parameter pointing to the pointer returned by the task function (caller is responsible for freeing) |

**Note:** Memory pointed to by `result` is allocated by the task function via `malloc`. The caller must free it before calling `ctpool_future_destroy()`.

### 1.4 Shutdown and Query

```c
void ctpool_pool_shutdown(ctpool_thread_pool_t *pool);
uint32_t ctpool_pool_worker_count(const ctpool_thread_pool_t *pool);
uint32_t ctpool_pool_pending_count(const ctpool_thread_pool_t *pool);
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
} ctpool_pool_config_t;
```

---

## 2. Coroutine API

### 2.1 Create and Destroy

```c
ctpool_coro_result_t ctpool_coro_create(ctpool_coro_fn fn,
                                        void *data,
                                        size_t stack_size,
                                        ctpool_coroutine_t **coro);

void ctpool_coro_destroy(ctpool_coroutine_t **coro);
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
ctpool_coro_result_t ctpool_coro_resume(ctpool_coroutine_t *coro);
```

| State | Behavior |
|-------|----------|
| `NEW` | First start: allocate and configure ucontext, execute entry function |
| `SUSPENDED` | Resume from the last yield point |
| `DONE` / `ERROR` | Return `CTPPOOL_CORO_ERR_RUNNING` |

### 2.3 Yield and Terminate

```c
void ctpool_coro_yield(void);
void ctpool_coro_suspend(void);

ctpool_coro_result_t ctpool_coro_terminate(ctpool_coroutine_t *coro);
```

- `yield()` / `suspend()`: Yield control back to the caller. `suspend()` is an alias for `yield()`.
- `terminate()`: Forcefully terminate the coroutine, set `state=DONE`, and resume execution in the scheduler. If the coroutine is currently running in the calling thread, the switch happens immediately.

### 2.4 State and Debugging

```c
ctpool_coro_state_t ctpool_coro_state(const ctpool_coroutine_t *coro);

ctpool_coro_result_t ctpool_coro_stack_info(const ctpool_coroutine_t *coro,
                                             void **start,
                                             void **end);

const char *ctpool_coro_result_str(ctpool_coro_result_t result);
```

- `state()`: Returns the current state enum value
- `stack_info()`: Returns the stack address range (for debugging / memory checking)
- `result_str()`: Converts a result code to a human-readable string

### 2.5 Configuration Constants

```c
#define CTPPOOL_CORO_DEFAULT_STACK_SIZE    (64 * 1024)   /* 64 KiB */
#define CTPPOOL_CORO_GUARD_PAGES_EACH      1              /* Guard pages per side */
```

---

## 3. Result Codes Quick Reference

### Thread Pool (`ctpool_result_t`)

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `CTPPOOL_OK` | Success |
| 1 | `CTPPOOL_ERR_ALLOC` | Memory allocation failed |
| 2 | `CTPPOOL_ERR_THREAD` | Thread creation failed |
| 3 | `CTPPOOL_ERR_INVALID` | Invalid argument or queue full |
| 4 | `CTPPOOL_ERR_SHUTDOWN` | Pool is shutting down or shut down |
| 5 | `CTPPOOL_ERR_TIMEOUT` | Timeout (reserved) |

### Coroutine (`ctpool_coro_result_t`)

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `CTPPOOL_CORO_OK` | Success |
| 1 | `CTPPOOL_CORO_ERR_ALLOC` | mmap allocation failed |
| 2 | `CTPPOOL_CORO_ERR_CONTEXT` | ucontext operation failed |
| 3 | `CTPPOOL_CORO_ERR_MPROTECT` | mprotect failed |
| 4 | `CTPPOOL_CORO_ERR_INVALID` | Invalid argument |
| 5 | `CTPPOOL_CORO_ERR_GUARD` | Guard page triggered (stack overflow) |
| 6 | `CTPPOOL_CORO_ERR_RUNNING` | Invalid state for operation |

### Coroutine State (`ctpool_coro_state_t`)

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `CTPPOOL_CORO_NEW` | Created, not started |
| 1 | `CTPPOOL_CORO_RUNNING` | Currently executing |
| 2 | `CTPPOOL_CORO_SUSPENDED` | Yielded, waiting for resume |
| 3 | `CTPPOOL_CORO_DONE` | Completed normally |
| 4 | `CTPPOOL_CORO_ERROR` | Error occurred |

---

## 4. Thread Safety

| API | Thread-safe | Notes |
|-----|-------------|-------|
| `ctpool_pool_create` | ✅ Yes | Call within a single thread |
| `ctpool_pool_submit` | ✅ Yes | Safe for concurrent calls; internal locking |
| `ctpool_pool_submit_future` | ✅ Yes | Safe for concurrent calls; internal locking |
| `ctpool_pool_shutdown` | ⚠️ Call once only | Must be called after all submits are complete |
| `ctpool_pool_destroy` | ✅ Yes (NULL-safe) | Must be called after shutdown |
| `ctpool_future_wait` | ✅ Yes | Internal spin + condition variable wait |
| `ctpool_future_destroy` | ✅ Yes | Must be called after `wait()` returns |
| `ctpool_coro_create` | ✅ Yes | Call within a single thread |
| `ctpool_coro_resume` | ✅ Yes | Call within a single thread |
| `ctpool_coro_yield` | ✅ Yes | Call from within the coroutine |
| `ctpool_coro_destroy` | ✅ Yes | Must be called after coroutine reaches DONE |
| `ctpool_coro_stack_info` | ✅ Yes | Read-only operation, no lock needed |

**Prohibited operations:**
- Calling `submit()` / `submit_future()` after `shutdown()`
- Calling `resume()` / `yield()` / `terminate()` on the same coroutine from different threads
- Calling `destroy()` on a coroutine that is not in DONE/ERROR state
