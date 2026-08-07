# Thread Pool Monitoring Improvements — Design

**Date:** 2026-08-07
**Status:** Approved (Approach C)
**Library:** loomworks (C11 thread pool + pipeline library)

## 1. Motivation

The current monitoring surface of loomworks is incomplete in three ways:

1. **Metrics enum lies.** `LOOMWORKS_METRIC_STARTED` and `LOOMWORKS_METRIC_FAILED`
   are declared in the public enum but `loom_metrics_fire()` falls through
   to `default: break` for both — no `started` or `failed` counter exists.
2. **Runtime health is invisible.** There is no way to know how many workers
   are currently *busy* vs *idle*, so pool saturation/utilization cannot be
   observed (relevant for load monitoring and autoscaling decisions).
3. **No consistent snapshot.** Reading multiple counters requires several
   separate `atomic_load` calls, producing a racy, inconsistent cross-section
   across fields.

Goal: complete a coherent monitoring story — counters are queryable,
runtime state is visible, a unicall snapshot reads consistently, and a demo
shows live output.

## 2. Architecture

```
                     Monitoring API layer (new)
   loom_pool_active_count / idle_count / utilization
   loom_metrics_started / failed / avg_latency_ns
   loom_metrics_snapshot()   <- consistent snapshot
   ------------------------------------------------
   Data sources
   pool:     _Atomic active_workers   (updated in worker_entry)
   metrics:  started/failed counters   (wired in loom_metrics_fire)
   ------------------------------------------------
   Existing infra
   worker_entry loop | metrics_fire | atomic counters
```

Three independent, individually-testable components: **pool runtime health**,
**metrics counter completion**, **snapshot reading**. They do not depend on
one another and each is exposed through existing public API conventions.

## 3. Components and API surface

### Component A — Pool runtime health

Files: `src/thread_pool.c`, `src/thread_pool_internal.h`, `include/loomworks/thread_pool.h`.

- Add `_Atomic uint32_t active_workers;` to `struct loom_thread_pool`
  (lock-free atomic increments/decrements).
- In `worker_entry()`: increment `active_workers` after dequeuing a task,
  decrement after `fn(data)` completes and `COMPLETED` is fired.
- New public getters:

```c
uint32_t loom_pool_active_count(const loom_thread_pool_t *pool);   /* workers currently executing */
uint32_t loom_pool_idle_count(const loom_thread_pool_t *pool);     /* worker_count - active */
double   loom_pool_utilization(const loom_thread_pool_t *pool);    /* active / worker_count */
```

- Also fire `LOOMWORKS_METRIC_STARTED` just before invoking `fn(data)`
  (currently only `COMPLETED` is fired in the worker loop).

### Component B — Metrics counter completion

Files: `src/metrics.c`, `include/loomworks/metrics.h`.

- Add `_Atomic uint64_t started;` and `_Atomic uint64_t failed;` to `struct loom_metrics`.
- In `loom_metrics_fire()`: `STARTED → ++started`, `FAILED → ++failed`
  (replacing the current fall-through to `default: break`).
- New getters:

```c
uint64_t loom_metrics_started(const loom_metrics_t *m);
uint64_t loom_metrics_failed(const loom_metrics_t *m);
uint64_t loom_metrics_avg_latency_ns(const loom_metrics_t *m);   /* sum / completed; 0 if completed == 0 */
```

### Component C — Consistent snapshot

Files: `src/metrics.c`, `include/loomworks/metrics.h`.

```c
typedef struct {
    uint64_t submitted; uint64_t started; uint64_t completed;
    uint64_t cancelled; uint64_t failed;
    uint64_t latency_sum_ns; uint64_t latency_max_ns;
} loom_metrics_snapshot_t;

loom_result_t loom_metrics_snapshot(const loom_metrics_t *m, loom_metrics_snapshot_t *out);
```

Reads all counters once while holding the existing `m->lock`, eliminating
the racy multi-`atomic_load` cross-section. No new lock required.

## 4. Data flow and thread safety

- `active_workers` uses relaxed atomics for increment/decrement and
  `atomic_load` for reads; no contented lock on the hot path.
- Metrics counters remain relaxed `fetch_add`; the snapshot is the only
  read path that takes `m->lock`, mutually exclusive with destroy cleanup.
- Invariant: `active <= worker_count`. When `loom_pool_resize` shrinks, a
  worker may exit mid-task; `active` is task-scoped and only decremented
  after the task finishes, so a transient `active == worker_count` is a
  legal saturation state and never overflows.
- Ordering invariant: `STARTED` fires before `fn`, `COMPLETED` after `fn` ⇒
  `started >= completed` always holds.

## 5. Error handling and edge cases

- All getters keep the existing style: return `0` (or `0.0`) on NULL; no errno.
- `avg_latency_ns` guards against division by zero when `completed == 0`.
- `snapshot` returns `LOOMWORKS_ERR_INVALID` when `m == NULL || out == NULL`.
  Destroy during snapshot is prevented by `m->lock` (destroy already NULLs
  `m->pool` before cleanup).
- `FAILED`: no C trigger exists yet (reserved for future exception/callback
  mechanisms). The counter and getter are landed first; docs mark it "reserved".

## 6. Testing and example

- `tests/test_thread_pool.c`:
  - `test_metrics_monitoring`: submit N tasks → `started == completed == N`,
    `failed == 0`, `avg_latency_ns > 0`, snapshot's 7 fields match the
    individual getters.
  - `test_pool_health`: during concurrent submission, poll that
    `active_count ∈ [0, worker_count]`, `idle == worker_count - active`,
    `utilization ∈ [0, 1]`; after shutdown, `active == 0`.
- New `examples/monitor_demo.c`: create a pool, submit a batch of short
  tasks, poll and print one line every 50 ms
  (`active/idle/queue/submitted/completed`), then print the final snapshot
  once drained — mirroring the `[monitor]` output style of the pipeline demo.
- `CMakeLists.txt`: register the `monitor_demo` target.
- Update `docs/api-reference.md` for the new API surface.

## 7. Files touched

- `src/thread_pool_internal.h` — struct field
- `src/thread_pool.c` — worker_entry counting + STARTED fire + getters
- `include/loomworks/thread_pool.h` — declarations
- `src/metrics.c` — counters, fire wiring, getters, snapshot
- `include/loomworks/metrics.h` — declarations + snapshot struct
- `tests/test_thread_pool.c` — two new tests
- `examples/monitor_demo.c` — new example
- `CMakeLists.txt` — register target
- `docs/api-reference.md` — update

## 8. Out of scope (YAGNI)

- No `loom_pool*` snapshot struct combining pool + metrics (keeps components
  decoupled; demo composes the reads instead).
- No failure triggering mechanism (no exception model in C).
- No automatic periodic logging inside the library.
- No perf/scheduler hot-path changes beyond the two atomic increments.