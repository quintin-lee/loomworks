# loomworks API Reference

> This document describes all public APIs of the loomworks library. All handles are opaque pointers; struct definitions are not exposed in headers.
>
> **Platform support**: Linux x86-64 / aarch64 is the primary target (hand-written
> context-switch assembly, QEMU cross-compile CI). macOS 10.12+ and BSD are
> supported through POSIX fallbacks (ucontext context backend, portable
> clean_exit-poll joins, `MAP_ANON` mmap). Windows is not supported.

---

## Table of Contents

1. [Thread Pool API](#1-thread-pool-api)
2. [Coroutine API](#2-coroutine-api)
3. [Pipeline API](#3-pipeline-api)
4. [Task Group API](#4-task-group-api)
5. [Metrics API](#5-metrics-api)
6. [Unified Runtime API](#6-unified-runtime-api)
7. [Result Codes Quick Reference](#7-result-codes-quick-reference)
8. [Complete Usage Example](#8-complete-usage-example)
9. [Thread Safety](#9-thread-safety)

---

## 1. Thread Pool API

### 1.1 Create and Destroy

```c
loom_result_t loom_pool_create(const loom_pool_config_t *config,
                               loom_thread_pool_t **pool);

loom_result_t loom_pool_destroy(loom_thread_pool_t **pool);
```

| Parameter | Description |
|-----------|-------------|
| `config` | Configuration struct; pass `NULL` for defaults (auto worker count, unbounded queue) |
| `pool` | Output parameter; set to the new pool handle on success |

**Note:** Always call `loom_pool_shutdown()` to drain pending tasks before calling `loom_pool_destroy()`. Destroying a pool whose workers may still be running is rejected with `LOOMWORKS_ERR_INVALID` and the handle is left untouched (NULL-safe: a NULL handle or already-NULL target returns `LOOMWORKS_OK`).

### 1.2 Configuration Structure

```c
typedef struct {
    uint32_t worker_count;   /* 0 = auto: sysconf(_SC_NPROCESSORS_ONLN) clamped to [1,64], then x2 -> up to 128 */
    size_t   stack_size;     /* 0 = 128 KiB (LOOMWORKS_DEFAULT_STACK_SIZE) */
    uint32_t queue_capacity; /* 0 = unbounded, max 1M */
} loom_pool_config_t;
```

**Worker count auto rule:** `worker_count = min(online_cpus, 64) * 2`, so the maximum is **128** workers (not 64).

### 1.3 Submit Tasks

All submit variants take a trailing `uint64_t *task_id` out-parameter (nullable — pass `NULL` to ignore). The assigned ID is usable with `loom_pool_cancel_by_id()`.

```c
loom_result_t loom_pool_submit(loom_thread_pool_t *pool,
                               loom_task_fn fn, void *data,
                               uint64_t *task_id);

loom_result_t loom_pool_submit_blocking(loom_thread_pool_t *pool,
                                        loom_task_fn fn, void *data,
                                        uint64_t *task_id);

loom_result_t loom_pool_submit_priority(loom_thread_pool_t *pool,
                                        loom_task_fn fn, void *data,
                                        uint8_t priority, uint64_t *task_id);

loom_result_t loom_pool_submit_future(loom_thread_pool_t *pool,
                                      loom_task_fn_result fn, void *data,
                                      loom_future_t **future, uint64_t *task_id);

loom_result_t loom_pool_submit_future_priority(loom_thread_pool_t *pool,
                                               loom_task_fn_result fn, void *data,
                                               uint8_t priority,
                                               loom_future_t **future, uint64_t *task_id);
```

| Parameter | Description |
|-----------|-------------|
| `fn` | Task function, signature `void (*)(void*)` or `void* (*)(void*)` |
| `data` | Opaque pointer passed to the task function |
| `priority` | Task priority (see below); lower number runs first |
| `future` | Output parameter, used only with `submit_future` variants |
| `task_id` | Output parameter receiving the assigned task ID (may be `NULL`) |

**Task priorities** (`loom_task_priority_t`, lower value = higher priority, runs first):

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `LOOMWORKS_PRIORITY_REALTIME` | Realtime / critical priority |
| 1 | `LOOMWORKS_PRIORITY_HIGH` | High priority |
| 5 | `LOOMWORKS_PRIORITY_NORMAL` | Normal priority (default) |
| 10 | `LOOMWORKS_PRIORITY_LOW` | Default low priority |

**Behavior notes:**
- `submit`: fire-and-forget. Blocks only when `queue_capacity > 0` and the queue is full; after 60 s it returns `LOOMWORKS_ERR_TIMEOUT` (or `LOOMWORKS_ERR_SHUTDOWN` if the pool shuts down while waiting).
- `submit_blocking`: same as `submit` but always blocks (with the same 60 s timeout) when the queue is full instead of relying on queue-capacity interpretation.
- NORMAL-priority tasks take a lock-free ring fast path; all other priorities go to the priority lane buckets.
- After `loom_pool_shutdown()` all submit variants return `LOOMWORKS_ERR_SHUTDOWN`.
- `submit_future` variants: the future handle must be released with `loom_future_destroy()`.
- Result memory from `loom_task_fn_result` is **owned by the caller**; the pool never frees it.

### 1.4 Retrieving Future Results

```c
loom_result_t loom_future_wait(loom_future_t *future, void **result);

loom_result_t loom_future_wait_timeout(loom_future_t *future, void **result,
                                       const struct timespec *deadline);

loom_result_t loom_future_destroy(loom_future_t *future);
```

| Parameter | Description |
|-----------|-------------|
| `future` | Handle returned by a `submit_future` variant |
| `result` | Output pointer set to the pointer returned by the task function (caller owns it; may be `NULL`) |
| `deadline` | Absolute `timespec` (`CLOCK_MONOTONIC`) after which to give up waiting |

- `wait()`: blocks until the task completes. Reentrant — returns immediately if the future is already ready. If the task was cancelled before it ran, returns `LOOMWORKS_ERR_CANCELLED`.
- `wait_timeout()`: returns `LOOMWORKS_ERR_TIMEOUT` if the deadline passes before the task completes; `LOOMWORKS_ERR_CANCELLED` if the task was cancelled before it ran.
- `destroy()`: releases the future handle; call after `wait()`/`wait_timeout()` has returned. NULL-safe. Destroying a future whose task has not yet completed is rejected with `LOOMWORKS_ERR_INVALID` — the worker would otherwise write into freed memory when the task finishes.

### 1.5 Cancellation

```c
loom_result_t loom_pool_cancel(loom_thread_pool_t *pool, void *data);

loom_result_t loom_pool_cancel_by_id(loom_thread_pool_t *pool, uint64_t task_id);

void loom_pool_cancel_all(loom_thread_pool_t *pool, uint32_t *count);
```

- `cancel(data)`: cancels a pending (not yet started) task whose `user_data` pointer matches. Returns `LOOMWORKS_OK` if cancelled, `LOOMWORKS_ERR_INVALID` if not found or already running, `LOOMWORKS_ERR_SHUTDOWN` after shutdown.
- `cancel_by_id(id)`: same, but matches the unique task ID returned at submission — safer when several tasks share one `user_data` pointer.
- `cancel_all(&count)`: cancels every task still waiting in the queue; `count` (optional) receives the number cancelled. Removed tasks are never executed.

Canceled tasks are freed by the pool, including their `user_data` if the task was flagged to free it (see task function ownership note in the header docs).

### 1.6 Resize

```c
loom_result_t loom_pool_resize(loom_thread_pool_t *pool, uint32_t count);
```

- Grows by spawning new workers; shrinks by letting excess idle workers exit at their next loop iteration.
- Workers currently executing a task are **not** interrupted.
- Returns `LOOMWORKS_ERR_SHUTDOWN` after shutdown, `LOOMWORKS_ERR_INVALID` on bad arguments.

### 1.7 Shutdown and Wake

```c
void loom_pool_shutdown(loom_thread_pool_t *pool);
void loom_pool_broadcast(loom_thread_pool_t *pool);
```

- `shutdown()`: blocks until all submitted tasks complete (drains), then joins all worker threads. Idempotent; safe to call multiple times.
- `broadcast()`: wakes all workers currently blocked waiting for work. Used when external state changes require workers to re-check conditions (e.g. pipeline shutdown signaling its backing pool).

### 1.8 Queries

```c
uint32_t loom_pool_worker_count(const loom_thread_pool_t *pool);
uint32_t loom_pool_pending_count(const loom_thread_pool_t *pool);
uint32_t loom_pool_active_count(const loom_thread_pool_t *pool);
uint32_t loom_pool_idle_count(const loom_thread_pool_t *pool);
double   loom_pool_utilization(const loom_thread_pool_t *pool);
```

- `worker_count()`: actual number of worker threads (including auto-computed value).
- `pending_count()`: tasks currently waiting in the queue (locked read; approximate under concurrent load).
- `active_count()`: workers currently executing a task (atomic; may lag briefly).
- `idle_count()`: `worker_count - active_count`, clamped at 0.
- `utilization()`: `active_count / worker_count` in `[0.0, 1.0]` (`0.0` when `worker_count == 0`).

---

## 2. Coroutine API

Stackful coroutines on `ucontext`, with mmap'd stacks, guard pages, and a small exact-size stack pool. All lifecycle calls must stay on the **same thread**.

### 2.1 Create and Destroy

```c
loom_coro_result_t loom_coro_create(loom_coro_fn fn,
                                    void *data,
                                    size_t stack_size,
                                    loom_coroutine_t **coro);

loom_coro_result_t loom_coro_destroy(loom_coroutine_t **coro);
```

| Parameter | Description |
|-----------|-------------|
| `fn` | Coroutine entry function, signature `void (*)(void*)` |
| `data` | Opaque pointer passed to the entry function |
| `stack_size` | Stack size in bytes; 0 uses `LOOMWORKS_CORO_DEFAULT_STACK_SIZE` (64 KiB) |
| `coro` | Output parameter |

**Destroy note:** Returns `LOOMWORKS_CORO_ERR_INVALID` unless the coroutine is in `NEW`, `DONE`, or `ERROR` state — destroying a running/suspended coroutine would free its stack mid-switch. Cross-thread destroy of a quiescent coroutine remains allowed (the stack pool is process-global).

### 2.2 Start and Resume

```c
loom_coro_result_t loom_coro_resume(loom_coroutine_t *coro);
```

| State | Behavior |
|-------|----------|
| `NEW` | First start: allocate and configure the ucontext, execute the entry function |
| `SUSPENDED` | Resume from the last yield point |
| `DONE` / `ERROR` | Return `LOOMWORKS_CORO_ERR_RUNNING` |

The scheduler context and stack are created lazily on first resume per thread and tracked for cleanup.

### 2.3 Yield and Terminate

```c
void loom_coro_yield(void);
void loom_coro_suspend(void);

loom_coro_result_t loom_coro_terminate(loom_coroutine_t *coro);
```

- `yield()` / `suspend()`: yield control back to the caller. `suspend()` is an alias for `yield()`.
- `terminate()`: force-stop the coroutine, set `state = DONE`, and switch back to the scheduler. If the coroutine is running in the calling thread, the switch happens immediately.

### 2.4 Per-Thread Cleanup

```c
void loom_coro_exit(void);
```

- Frees this thread's scheduler stack (if any) and removes it from the scheduler-stack registry.
- The thread pool calls this at the top of every worker loop iteration, so pool workers that ran coroutines release their scheduler stacks automatically when they exit.
- Stray stacks are reclaimed by a process-exit destructor.

### 2.5 Guard Handler

```c
void loom_coro_install_guard_handler(void);
void loom_coro_uninstall_guard_handler(void);
```

- Installs the SIGSEGV/SIGBUS handler that detects writes to the guard pages and converts them into `LOOMWORKS_CORO_ERR_GUARD` from `loom_coro_resume()`. Idempotent (installs once).
- The library installs the handler lazily; call `install_guard_handler()` explicitly if you need it before the first resume.

### 2.6 State and Debugging

```c
loom_coro_state_t loom_coro_state(const loom_coroutine_t *coro);

loom_coro_result_t loom_coro_stack_info(const loom_coroutine_t *coro,
                                        void **start,
                                        void **end);

const char *loom_coro_result_str(loom_coro_result_t result);
```

- `state()`: returns the current state enum value.
- `stack_info()`: returns the usable stack address range (for debugging / memory checking).
- `result_str()`: converts a result code to a human-readable string.

### 2.7 Configuration Constants

```c
#define LOOMWORKS_CORO_DEFAULT_STACK_SIZE (64 * 1024) /* 64 KiB */
#define LOOMWORKS_CORO_GUARD_PAGES_EACH   1            /* guard pages per side */
```

Stack layout per coroutine mapping: `[guard][usable][guard][guard]` (one guard at the top edge, two at the bottom edge).

---

## 3. Pipeline API

A FIFO item queue with optional internal worker pool. Capacity-bounded (back-pressure) or unbounded.

```c
#define LOOM_PC_OWN_PAYLOADS (1u << 0)

loom_result_t loom_pc_create(uint32_t worker_count, uint32_t capacity, loom_pc_t **pc);
loom_result_t loom_pc_create_ex(uint32_t worker_count, uint32_t capacity, uint32_t flags,
                                void (*discard)(void *data, void *ctx), void *discard_ctx,
                                loom_pc_t **pc);
void          loom_pc_destroy(loom_pc_t **pc);
loom_result_t loom_pc_submit(loom_pc_t *pc, void *item);
loom_result_t loom_pc_take(loom_pc_t *pc, void **item);
void          loom_pc_shutdown(loom_pc_t *pc);
uint32_t      loom_pc_pending_count(const loom_pc_t *pc);
uint64_t      loom_pc_submitted_count(const loom_pc_t *pc);
uint64_t      loom_pc_taken_count(const loom_pc_t *pc);
```

| Parameter | Description |
|-----------|-------------|
| `worker_count` | 0 = no internal pool (caller consumes via `take()`); >0 creates an internal pool with that many workers which drain the queue via `take()` |
| `capacity` | 0 = unbounded; >0 = max pending items; `submit` waits up to 60 s at capacity then `ERR_TIMEOUT` |
| `flags` | `loom_pc_create_ex` only. Bitmask; unknown bits → `ERR_INVALID`. `LOOM_PC_OWN_PAYLOADS`: the library never frees a payload; discard handlers (or the caller) own cleanup |
| `discard` / `discard_ctx` | `loom_pc_create_ex` only. Optional discard handler installed atomically at creation; the `set_discard_handler` form remains for post-create install |
| `item` | Opaque user pointer enqueued / dequeued |

**Behavior notes:**
- `loom_pc_create(...)` is identical to `create_ex(worker_count, capacity, 0, NULL, NULL, pc)`.
- `submit()` after `shutdown()` returns `LOOMWORKS_ERR_SHUTDOWN`.
- `take()` after shutdown returns `LOOMWORKS_ERR_SHUTDOWN` with `*item = NULL` (queue drained first).
- **Payload ownership (three-way contract).** The library frees a payload only
  when: internal consumers are used (`worker_count > 0`) **and** no discard
  handler is installed **and** `LOOM_PC_OWN_PAYLOADS` is not set. With a
  discard handler installed, the handler (not the library) owns cleanup and is
  invoked on the internal worker thread during consumption and on the calling
  thread during the `destroy()` drain. With `LOOM_PC_OWN_PAYLOADS`, the
  library never calls `free()`: handlers still run when present, and items are
  otherwise dropped without freeing. `create_ex` rejects
  `OWN_PAYLOADS` + internal pool + no handler at creation (it can only leak).
  With `worker_count == 0` the flag is a no-op: the library never touches
  payloads — `take()` hands them to the caller and `destroy()` drains
  leftovers without freeing.
- `shutdown()`: closes the queue for submits, wakes blocked takers, and shuts down the internal pool if present.
- `destroy()`: shuts down + joins the internal pool, then drains any leftover items (via the discard handler if set, else dropped without freeing).

---

## 4. Task Group API

Tracks submitted tasks so the whole group can be cancelled or waited on at once.

```c
loom_result_t loom_task_group_create(loom_thread_pool_t *pool, loom_task_group_t **group);
loom_result_t loom_task_group_destroy(loom_task_group_t **group);
loom_result_t loom_task_group_submit(loom_task_group_t *group, loom_task_fn fn,
                                     void *data, uint64_t *task_id);
loom_result_t loom_task_group_submit_future(loom_task_group_t *group, loom_task_fn_result fn,
                                            void *data, loom_future_t **future,
                                            uint64_t *task_id);
void          loom_task_group_cancel(loom_task_group_t *group);
loom_result_t loom_task_group_wait(loom_task_group_t *group);
loom_result_t loom_task_group_wait_timeout(loom_task_group_t *group,
                                           const struct timespec *deadline);
uint32_t      loom_task_group_pending_count(const loom_task_group_t *group);
```

| Parameter | Description |
|-----------|-------------|
| `pool` | Backing pool; the group tracks task `user_data` pointers on this pool |
| `group` | Output handle |
| `future` | Output future handle (only `submit_future`); caller must free it |
| `task_id` | Output for the underlying pool-submit task ID (may be `NULL`) |

**Behavior notes:**
- `destroy()`: cancels all pending tracked tasks (`loom_pool_cancel` on each), waits for in-flight tasks to finish, then frees the group. Returns `LOOMWORKS_ERR_INVALID` if called **from a worker of the group's own pool** (the call would block forever waiting on work only that worker could run) or with a NULL handle.
- `cancel()`: same cancellation sweep; the group can be reused afterwards.
- `wait()`: blocks until every tracked task finishes; the backing pool stays fully usable afterwards. Returns `LOOMWORKS_ERR_INVALID` when called from a worker of the group's own pool, or with a NULL handle.
- `wait_timeout()`: same contract as `wait()` but gives up once the absolute `CLOCK_MONOTONIC` deadline passes, returning `LOOMWORKS_ERR_TIMEOUT`. The group is left fully usable (pending accounting + tracking list untouched), so a later `wait()`/`wait_timeout()`/`destroy()` resumes where the timed-out call left off. A `NULL` deadline means "wait forever" (identical to `wait()` — which is implemented as `wait_timeout(group, NULL)`). Same `ERR_INVALID` conditions as `wait()`.
- **Fragility:** cancellation matches tasks by `user_data` pointer equality. If data is freed or reused before cancel, matching can be wrong. Prefer tracking `task_id` and cancelling via `loom_pool_cancel_by_id()` when many tasks share one pointer.

---

## 5. Metrics API

Event counters + latency histogram over the pool's life, either polled or pushed via callback.

```c
typedef enum {
    LOOMWORKS_METRIC_SUBMITTED = 0,
    LOOMWORKS_METRIC_STARTED,
    LOOMWORKS_METRIC_COMPLETED,
    LOOMWORKS_METRIC_CANCELLED,
    LOOMWORKS_METRIC_FAILED,
} loom_metric_event_t;

typedef struct {
    uint64_t submitted; uint64_t started; uint64_t completed;
    uint64_t cancelled; uint64_t failed;
    uint64_t latency_sum_ns; uint64_t latency_max_ns;
} loom_metrics_snapshot_t;

loom_result_t loom_metrics_create(loom_thread_pool_t *pool, loom_metric_fn cb,
                                  void *data, loom_metrics_t **out);
void          loom_metrics_destroy(loom_metrics_t **metrics);

void          loom_metrics_fire(loom_metrics_t *metrics, loom_metric_event_t event);
void          loom_metrics_record_latency(loom_metrics_t *metrics, uint64_t ns);

uint64_t loom_metrics_submitted(const loom_metrics_t *metrics);
uint64_t loom_metrics_started(const loom_metrics_t *metrics);
uint64_t loom_metrics_completed(const loom_metrics_t *metrics);
uint64_t loom_metrics_cancelled(const loom_metrics_t *metrics);
uint64_t loom_metrics_failed(const loom_metrics_t *metrics);
uint64_t loom_metrics_latency_sum_ns(const loom_metrics_t *metrics);
uint64_t loom_metrics_latency_max_ns(const loom_metrics_t *metrics);
uint64_t loom_metrics_avg_latency_ns(const loom_metrics_t *metrics);

loom_result_t loom_metrics_snapshot(const loom_metrics_t *metrics,
                                    loom_metrics_snapshot_t *out);

void loom_pool_set_metrics_callback(loom_thread_pool_t *pool,
                                    loom_metric_fn cb, void *user_data);
void loom_pool_set_metrics(loom_thread_pool_t *pool, loom_metrics_t *metrics);
```

- `create()`: creates the collector and attaches it to the pool (sets pool metrics + callback). `cb` is optional per-event callback, `data` is its user pointer.
- `started()`: tasks that began execution. `failed()`: counts workers that exited abnormally (a task calling `pthread_exit`, or any other crash inside a worker) — the worker is lost and the pool reports the loss at shutdown.
- `avg_latency_ns()`: `latency_sum / completed`, or 0 when nothing completed.
- `snapshot()`: reads all counters under one lock acquisition into `loom_metrics_snapshot_t` — mutually consistent across fields.
- `pool_set_metrics_callback()`: attach/detach a callback directly on the pool (independent of a collector object).
- `pool_set_metrics()`: attach a collector object to a pool so the worker loop updates it.

### Callback contract (regression-locked)

The callback fires **synchronously on the thread that produces the event**:
worker threads for `STARTED`/`COMPLETED`, the submitting thread for
`SUBMITTED`, the shutting-down thread for `FAILED`. It is **always invoked
outside the pool lock**, so the callback may safely call back into the pool
(e.g. `pending_count()`); conversely it **must be cheap and non-blocking** —
a slow callback throttles its producing thread, and a blocking one can stall
the whole pool. The counters themselves are lock-free relaxed atomics. This
contract is enforced by regression tests
(`test_metrics_callback_on_worker_thread`,
`test_metrics_callback_outside_lock`, `test_metrics_callback_lifecycle_counts`).

---

## 6. Result Codes Quick Reference

### Thread Pool / Pipeline / Task Group (`loom_result_t`)

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `LOOMWORKS_OK` | Success |
| 1 | `LOOMWORKS_ERR_ALLOC` | Memory allocation failed |
| 2 | `LOOMWORKS_ERR_THREAD` | Thread creation failed |
| 3 | `LOOMWORKS_ERR_INVALID` | Invalid argument, queue full, or cancellation target not found |
| 4 | `LOOMWORKS_ERR_SHUTDOWN` | Pool/pipeline is shutting down or shut down |
| 5 | `LOOMWORKS_ERR_TIMEOUT` | Operation timed out (blocking submit / `future_wait_timeout` / pipeline capacity wait) |
| 6 | `LOOMWORKS_ERR_CANCELLED` | A pending future's task was cancelled; the future completes with this code instead of hanging |

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
| 4 | `LOOMWORKS_CORO_ERROR` | Error occurred (e.g. guard-page fault) |

---

## 7. Complete Usage Example

```c
#include <stdio.h>
#include <stdlib.h>
#include <loomworks/loomworks.h>

#define LOOMWORKS_CHECK(rc)                                                    \
    do {                                                                        \
        if ((rc) != LOOMWORKS_OK) {                                             \
            fprintf(stderr, "loomworks error: %d\n", (int)(rc));                \
            exit(1);                                                            \
        }                                                                       \
    } while (0)

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
    // Create pool (4 workers, bounded queue of 1000)
    loom_pool_config_t cfg = { .worker_count = 4, .queue_capacity = 1000 };
    loom_thread_pool_t *pool = NULL;
    LOOMWORKS_CHECK(loom_pool_create(&cfg, &pool));

    // Submit fire-and-forget tasks (task IDs can be ignored)
    uint64_t tid = 0;
    int sum = 0;
    for (int i = 0; i < 10; i++) {
        LOOMWORKS_CHECK(loom_pool_submit(pool, sum_squares_task, &sum, &tid));
    }

    // Submit a future task and wait for its result
    loom_future_t *fut = NULL;
    LOOMWORKS_CHECK(loom_pool_submit_future(pool, compute_result, NULL, &fut, NULL));
    void *result = NULL;
    LOOMWORKS_CHECK(loom_future_wait(fut, &result));
    printf("future result: %d\n", *(int *)result);
    free(result);
    loom_future_destroy(fut);

    // Print stats
    printf("pending tasks: %u\n", loom_pool_pending_count(pool));
    printf("worker count: %u\n", loom_pool_worker_count(pool));
    printf("utilization: %.2f\n", loom_pool_utilization(pool));

    // Cleanup
    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
    return 0;
}
```

Build: `cc -std=c11 -Iinclude example.c -Lbuild -lloomworks -lpthread` (or `-lloomworks_static` for the static archive). Static: `libloomworks.a`; shared: `libloomworks.so` (SOVERSION 1) — both work, including coroutines on modern toolchains.

---

## 8. Thread Safety

| API | Thread-safe | Notes |
|-----|-------------|-------|
| `loom_pool_create` | ✅ Yes | Call within a single thread |
| `loom_pool_submit` / `submit_blocking` / `submit_priority` | ✅ Yes | Safe for concurrent calls; lock-free ring + locked lanes + per-worker deques |
| `loom_pool_submit_future` / `submit_future_priority` | ✅ Yes | Safe for concurrent calls |
| `loom_pool_cancel` / `cancel_by_id` / `cancel_all` | ✅ Yes | Safe concurrently with submits (cancel-index CAS); tasks resident in worker deques are found until the worker pops them |
| `loom_pool_broadcast` | ✅ Yes | Safe on any valid pool |
| `loom_pool_resize` | ⚠️ Usually | Must not race with `shutdown`; other operations safe. Displaced workers spill their deques back to the shared queue before exiting |
| `loom_pool_shutdown` | ⚠️ Call once only | Must be called after all submits are complete; idempotent |
| `loom_pool_destroy` | ✅ Yes (NULL-safe) | Must be called after shutdown; rejected with `ERR_INVALID` on a running pool |
| `loom_pool_worker/pending/active/idle_count` / `utilization` | ✅ Yes | Locked or relaxed atomic reads |
| `loom_future_wait` / `wait_timeout` | ✅ Yes | Multiple waiters OK; result safe after return |
| `loom_future_destroy` | ⚠️ One owner | Must not race with the task completing; destroy of a pending future returns `ERR_INVALID` |
| `loom_coro_create` | ✅ Yes | Per-thread lifecycle |
| `loom_coro_resume` | ✅ Yes | Per-thread lifecycle |
| `loom_coro_yield` / `suspend` | ✅ Yes | Call from within the coroutine |
| `loom_coro_exit` | ✅ Yes | Per-thread; registry mutation serialized internally |
| `loom_coro_destroy` | ✅ Yes | Returns `ERR_INVALID` unless state is NEW/DONE/ERROR; cross-thread destroy of a quiescent coroutine allowed |
| `loom_coro_state` / `stack_info` | ✅ Yes | Read-only operations |
| `loom_pc_submit` / `pc_take` / `pc_*_count` | ✅ Yes | Single shared lock + atomics |
| `loom_task_group_*` | ✅ Yes | Group lock; backing pool handles its own concurrency |
| `loom_metrics_*` | ✅ Yes | Atomic counters; snapshot under one lock |

**Prohibited operations:**
- Calling any submit variant after `shutdown()`.
- Calling `resume()` / `yield()` / `terminate()` on the same coroutine from different threads (ucontext is not thread-safe; confine the whole lifecycle to one thread).
- Calling `destroy()` on a coroutine that is not in NEW/DONE/ERROR state.
- Calling `loom_task_group_wait()` / `loom_task_group_destroy()` from a worker of the group's own pool — the call would block forever waiting on work only that worker could run; both reject it with `ERR_INVALID` (self-wait guard).
---

## 6. Unified Runtime API

The unified runtime (`loom_runtime_t`) provides a single entry point for
submitting both thread tasks and coroutine tasks through the same API.
It wraps an internal `loom_thread_pool_t` and routes submissions based on
a `loom_submit_flag_t` flag.

### 6.1 Create and Destroy

```c
typedef struct {
    uint32_t worker_count;    /**< 0 = auto-detect */
    size_t   stack_size;      /**< Per-worker stack size (0 = default) */
    uint32_t queue_capacity;  /**< 0 = unbounded */
    uint32_t coro_stack_size; /**< 0 = default 64 KiB */
} loom_runtime_config_t;

loom_result_t loom_runtime_create(const loom_runtime_config_t *cfg,
                                  loom_runtime_t **out);
void          loom_runtime_destroy(loom_runtime_t **rt);
```

`destroy` calls `shutdown()` internally then frees resources. Idempotent.

### 6.2 Submit

```c
typedef enum {
    LOOM_SUBMIT_THREAD = 0, /**< Route to thread-pool worker */
    LOOM_SUBMIT_CORO   = 1, /**< Route to per-worker coroutine scheduler */
} loom_submit_flag_t;

typedef union {
    loom_task_fn thread_fn;
    loom_coro_fn coro_fn;
    void        *ptr;
} loom_fn_union_t;

loom_result_t loom_runtime_submit(loom_runtime_t    *rt,
                                  loom_fn_union_t    fn,
                                  void              *data,
                                  loom_submit_flag_t flag,
                                  uint8_t            priority,
                                  uint64_t          *task_id);

loom_result_t loom_runtime_submit_future(loom_runtime_t       *rt,
                                         loom_task_fn_result   fn,
                                         void                 *data,
                                         loom_future_t       **future,
                                         uint64_t             *task_id);
```

`LOOM_SUBMIT_CORO` routes to the per-worker coroutine scheduler where
workers multiplex coroutines via a ready FIFO (M:N scheduling).
`LOOM_SUBMIT_THREAD` routes to the standard thread pool with priority.
`submit_future` is only valid for `LOOM_SUBMIT_THREAD`.

### 6.3 Cancellation

```c
loom_result_t loom_runtime_cancel(loom_runtime_t *rt, uint64_t task_id);
void          loom_runtime_cancel_all(loom_runtime_t *rt, uint32_t *count);
```

Semantic cancel — pending tasks are flagged and skipped on dequeue.
Running tasks are not interrupted.

### 6.4 Resize and Metrics

```c
loom_result_t loom_runtime_resize(loom_runtime_t *rt, uint32_t count);
void          loom_runtime_set_metrics_callback(loom_runtime_t *rt,
                                              loom_metric_fn cb,
                                              void *user_data);
```

`resize` is forwarded to the backing pool. `set_metrics_callback`
registers a callback that fires synchronously on worker threads.

### 6.5 Queries

```c
uint32_t loom_runtime_worker_count (const loom_runtime_t *rt);
uint32_t loom_runtime_pending_count(const loom_runtime_t *rt);
uint32_t loom_runtime_active_count (const loom_runtime_t *rt);
uint32_t loom_runtime_idle_count   (const loom_runtime_t *rt);
double   loom_runtime_utilization  (const loom_runtime_t *rt);
void     loom_runtime_shutdown(loom_runtime_t *rt);
```

All queries delegate to the backing pool.

### 6.6 Key Design Decisions

- **Thin wrapper**: zero duplicated logic; all work is delegated to the
  backing `loom_thread_pool_t`.
- **Layered priority**: thread tasks use priorities 0–7; coroutine tasks
  run in their own per-worker FIFO (no priority sub-classification).
- **Cancellation is semantic**: tasks in the lock-free ring or per-worker
  deques are flagged, not physically removed; memory is reclaimed when
  the worker eventually dequeues them.
- **M:N coroutine multiplexing**: when a coroutine yields or sleeps, the
  owner worker immediately resumes the next coroutine in its ready FIFO.

