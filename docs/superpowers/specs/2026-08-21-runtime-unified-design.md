# Unified Runtime: Thread + Coroutine Single Entry Point

> **Status**: Draft (2026-08-21)
>
> **Goal**: Introduce `loom_runtime_t` — a single-entry-point abstraction that
> presents a uniform `submit()` API to users while internally routing to either
> the existing thread-pool path or the coroutine multi-path scheduling path,
> with layered priority and cooperative M:N multiplexing.

---

## 1. Motivation

loomworks currently exposes two independent subsystems:

- **Thread pool** (`loom_thread_pool_t`) — worker-managed, lock-free ring, work-stealing,
  priority buckets, cancellation index, futures.
- **Coroutine subsystem** (`loom_coroutine_t`) — stackful, mmap-guarded, per-thread scheduler,
  yield/sleep primitives.

They interoperate today via `loom_pool_submit_coroutine()`, which requires the caller to
explicitly know which path to use. This creates friction:

1. Users must import both headers and understand the distinction.
2. There is no single lifecycle object — a pool and a coroutine runtime are separate.
3. The M:N coroutine multiplexing already exists internally (`coro_ready` FIFO + timer heap)
   but is only reachable through the pool's coroutine task path; there is no clean
   single-entry API.

This spec introduces `loom_runtime_t` as the **single public entry point** for all
concurrency work, hiding the internal distinction behind a flag-driven submit.

---

## 2. Design Decisions

### 2.1 Single-Singleton Runtime

The runtime is a **process-global singleton**. Users call:

```c
loom_runtime_create(&config);   // one-shot init
loom_runtime_submit(r, fn, data, flag);
loom_runtime_cancel(task_id);
loom_runtime_destroy();         // waits for drain, then frees
```

No per-application instance management. This matches the existing pool's operational
model and avoids the complexity of multi-runtime isolation that most users do not need.

### 2.2 Single Submit Entry with Flag Routing

```c
typedef enum {
    LOOM_SUBMIT_THREAD = 0,   /* route to thread-pool worker */
    LOOM_SUBMIT_CORO   = 1,   /* route to coroutine scheduler    */
} loom_submit_flag_t;

loom_result_t
loom_runtime_submit(loom_runtime_t       *rt,
                    void                 *fn,   /* raw pointer; cast at runtime */
                    void                 *data,
                    loom_submit_flag_t    flag,
                    uint8_t               priority,
                    uint64_t             *task_id);
```

A single function handles both paths. The `flag` disambiguates; `priority` applies
only to the thread path (coroutine tasks always use their own yield-based scheduler).

Rationale over two-entry API (`loom_submit_thread` / `loom_submit_coro`):
a single entry reduces the surface area, makes the API feel like a unified product,
and keeps the caller from having to remember two function names.

### 2.3 Layered Priority Queues

| Priority range | Tasks served | Behaviour |
|----------------|-------------|-----------|
| 0 – 7          | Thread tasks only | REALTIME → LOW (existing) |
| 8 – 15         | Coroutine tasks only | Coroutine ready FIFO (FIFO, no preemption) |

Thread tasks and coroutine tasks **never compete** in the same queue. A flood of
coroutine tasks cannot starve a REALTIME thread task, and vice versa.

The existing 256-bucket priority lane mechanism is preserved for thread tasks.
Coroutine tasks have their own per-worker FIFO (`coro_ready[idx]`) with no
priority sub-classification — the layered range is the only abstraction needed.

### 2.4 Cancellation Semantics

- **Pending (not yet started)**: removed from queue, freed, returns `LOOMWORKS_OK`.
- **Running** (thread executing, or coroutine mid-yield-loop): **not interruptible**.
  The task runs to completion. No `loom_coro_terminate()` is called on running
  coroutines by the cancel path.

This matches the existing thread-pool cancellation contract and is the safe choice:
force-terminating a running coroutine destroys its stack frame and leaves the
scheduler in an undefined state.

### 2.5 Coroutine M:N Multiplexing (Already Exists)

The infrastructure already exists in `src/thread_pool.c`:

- `pool->coro_ready[idx]` — per-worker linked list of ready coroutines.
- `pool->timer_heap` — min-heap of `(deadline, task_id, worker_idx)` entries.
- `pool->timer_thread` — wakes sleeping coroutines at deadline and pushes them
  onto the owner worker's ready FIFO.
- Worker loop already checks `coro_ready[idx]` before falling back to task dequeue.

This spec **does not add** any new multiplexing mechanism. It only exposes the
existing one through the runtime API and ensures the ready-FIFO path is the
default when `LOOM_SUBMIT_CORO` is used.

---

## 3. Public API

### 3.1 Configuration

```c
typedef struct {
    uint32_t worker_count;   /* 0 = auto (same formula as loom_pool_config) */
    size_t   stack_size;     /* per-worker stack (0 = default 128 KiB) */
    uint32_t queue_capacity; /* 0 = unbounded */
    uint32_t coro_stack_size;/* 0 = default 64 KiB per coroutine */
} loom_runtime_config_t;
```

### 3.2 Lifecycle

```c
loom_result_t loom_runtime_create(const loom_runtime_config_t *cfg,
                                  loom_runtime_t **out);
void          loom_runtime_destroy(loom_runtime_t **rt);
```

`destroy` blocks until all pending tasks (both thread and coroutine) complete,
then joins all workers and frees resources. Idempotent: a second call is a no-op.

### 3.3 Submit

```c
loom_result_t loom_runtime_submit(loom_runtime_t       *rt,
                                  void                 *fn,
                                  void                 *data,
                                  loom_submit_flag_t    flag,
                                  uint8_t               priority,
                                  uint64_t             *task_id);
```

- `flag == LOOM_SUBMIT_THREAD`: routes to the existing thread-pool submit path.
  `priority` is used; `fn` is cast to `loom_task_fn`.
- `flag == LOOM_SUBMIT_CORO`: routes to the coroutine submit path. `priority`
  is ignored (maps internally to the coroutine bucket, priority 8+). `fn` is
  cast to `loom_coro_fn`.

Both paths return the same `task_id` via the pool's monotonically-allocated
counter, enabling a single `loom_runtime_cancel(task_id)` call to work for both.

### 3.4 Cancellation

```c
loom_result_t loom_runtime_cancel(loom_runtime_t *rt, uint64_t task_id);
void          loom_runtime_cancel_all(loom_runtime_t *rt, uint32_t *count);
```

Same semantics as the pool equivalents: only pending (not yet started) tasks
are removed. Running tasks are left to finish.

### 3.5 Query

```c
uint32_t loom_runtime_worker_count (const loom_runtime_t *rt);
uint32_t loom_runtime_pending_count(const loom_runtime_t *rt);
uint32_t loom_runtime_active_count (const loom_runtime_t *rt);
uint32_t loom_runtime_idle_count   (const loom_runtime_t *rt);
double   loom_runtime_utilization  (const loom_runtime_t *rt);
```

Delegated to the backing pool. No new internals needed.

### 3.6 Shutdown Control

```c
void loom_runtime_shutdown(loom_runtime_t *rt);   /* initiate drain, non-blocking */
```

Called before `destroy` if the application wants to stop accepting new submissions
while letting in-flight tasks finish. `destroy` calls this implicitly if not
already called.

---

## 4. Internal Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    loom_runtime_t                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  backing pool: loom_thread_pool_t (existing, unchanged)  │    │
│  │  — 256-priority lanes                                    │    │
│  │  — lock-free Vyukov ring (NORMAL fast path)              │    │
│  │  — per-worker Chase-Lev deques                           │    │
│  │  — cancel index                                          │    │
│  │  — coro_ready FIFO + timer heap (already wired in)       │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  loom_runtime_submit():                                          │
│    flag == THREAD  → loom_pool_submit_*()                       │
│    flag == CORO    → loom_pool_submit_coroutine()               │
│                                                                  │
│  loom_runtime_cancel():                                          │
│    → loom_pool_cancel_by_id()  (works for both paths)           │
│                                                                  │
│  loom_runtime_destroy():                                         │
│    → loom_pool_shutdown() + loom_pool_destroy()                 │
│                                                                  │
│  All query/metrics functions delegate to backing pool.           │
└──────────────────────────────────────────────────────────────────┘
```

The runtime wrapper is a **thin shim** — it does not duplicate any logic.
All worker loop, ring, deque, cancel-index, timer-heap code remains in
`src/thread_pool.c` and `src/coroutine.c`. The runtime simply forwards calls.

---

## 5. File Changes

| File | Change |
|------|--------|
| `include/loomworks/runtime.h` | **New** — public runtime API |
| `src/runtime.c` | **New** — thin wrapper implementation |
| `include/loomworks/loomworks.h` | Add `#include "runtime.h"` |
| `CMakeLists.txt` | Add `src/runtime.c` to library sources; add `example_runtime` target |
| `tests/test_runtime.c` | **New** — submit both paths, cancel, destroy, mixed workload |
| `examples/runtime_demo.c` | **New** — simple demo showing both paths |

No changes to `thread_pool.c`, `coroutine.c`, `pipeline.c`, `task_group.c`, or
`metrics.c` are required. The existing coroutine-ready infrastructure is used
as-is.

---

## 6. What Is Out of Scope

- Cross-thread coroutine resume (preserved restriction from existing design).
- Changing the coroutine context backend (asm / ucontext).
- Adding a new scheduling algorithm (work-stealing, priority, ring remain unchanged).
- Exposing internal metrics beyond what the pool already provides.
- Multiple concurrent `loom_runtime_t` instances (singleton only).

---

## 7. Open Questions

1. **Should `loom_runtime_t` also expose `loom_future_t` retrieval?**
   Currently futures are returned by `loom_pool_submit_future()`. The runtime
   could expose `loom_runtime_submit_future(rt, fn, data, flag, ...)` returning
   a future. *Decision: include in V1 — futures are a natural extension of the
   single-entry model.*

2. **Should the runtime hold a reference to the backing pool for future
   extensibility (e.g. pluggable backends)?**
   *Decision: keep it simple — the runtime owns the pool directly, no indirection
   needed for now.*
