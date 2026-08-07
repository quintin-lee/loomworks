# Thread Pool Queue Performance — Design

**Date:** 2026-08-07
**Status:** Approved (Approach A+C: bucketized priority queue + baseline acceptance)
**Library:** loomworks (C11 thread pool + pipeline library)

## 1. Motivation

Profiling the submit/execute hot path in `src/thread_pool.c` surfaced four
quantified bottlenecks (all verified by code inspection on 2026-08-07):

1. **O(n) enqueue.** `loom_enqueue_unlocked()` walks the whole queue from the
   head to find the priority insertion point
   (`while (cur && cur->priority <= task->priority)`). Even when every task
   shares the same priority, each submit scans the entire queue. Throughput
   degrades linearly with queue depth.
2. **Per-task malloc churn.** `task_create()` mallocs one node per submit;
   the worker frees it after execution. Under high concurrency this becomes
   allocator contention; for micro-tasks malloc dominates the cost.
3. **Unconditional clock_gettime.** `worker_entry()` takes two
   `clock_gettime(CLOCK_MONOTONIC)` calls and computes latency for every task,
   even when no metrics collector is attached (the default) — a fixed
   ~40–80 ns tax per task.
4. **Single global mutex.** Every submit/take serializes on one lock. The
   existing 64-byte cache-line alignment mitigates false sharing but the
   contention itself remains (out of scope for this iteration — see §8).

Goal: eliminate #1–#3 with internal-only changes (public headers untouched,
API semantics unchanged), and add a benchmark baseline + CI regression gate so
the improvement is measured and protected.

## 2. Architecture

```
 Component A: bucketized priority queue      src/thread_pool.c, thread_pool_internal.h
 Component B: task node pool                 src/thread_pool.c, thread_pool_internal.h
 Component C: lazy clock_gettime             src/thread_pool.c
 Component D: benchmark baseline + CI gate   examples/bench.c, .github/workflows/perf.yml
 ------------------------------------------------
 Existing invariants (must be preserved)
 priority ordering | FIFO within priority | graceful shutdown | cancel semantics
```

Four independent, individually-testable components. A/B/C change internal
data structures only; D is tooling.

## 3. Components

### Component A — Bucketized priority queue

Replace the single linked list (`queue_head`/`queue_tail`/`queue_len`) with:

```c
loom_task_t *buckets_head[256];  /* one FIFO per uint8 priority value */
loom_task_t *buckets_tail[256];
uint64_t     nonempty_bits[4];   /* 256-bit occupancy bitmap */
uint32_t     queue_len;
```

- **Enqueue (O(1))**: `b = task->priority`; append to `buckets_tail[b]`;
  set `nonempty_bits[b/64] |= 1u << (b%64)`.
- **Dequeue (O(1))**: scan the four bitmap words, find the lowest set bit
  (`__builtin_ctzll`, ~4 instructions), pop that bucket's head; clear the bit
  when the bucket empties.
- **Semantics preserved exactly**: lower numeric priority runs first; FIFO
  within a bucket (tail append). The existing `test_priority_ordering`,
  `test_priority_future`, `test_submit_priority_returns_task_id` tests are the
  regression gate.
- **Cold paths unchanged in complexity**: `loom_pool_cancel` /
  `cancel_by_id` / `cancel_all` iterate the 256 buckets (O(n) worst case, same
  as today).
- **Memory cost**: 512 pointers + 32 bytes ≈ 4.2 KB per pool. Negligible.

**Decision (approved): 256 buckets**, because the public API accepts any
`uint8_t` priority; a 4-bucket variant would require clamping/mapping and
break that contract.

### Component B — Task node pool

- Add `loom_task_t *free_list` to `struct loom_thread_pool`, protected by the
  same global lock (no ABA concern under the lock).
- `task_create()`: pop from `free_list` first; `malloc` only when empty.
- Worker completion / cancel paths: push the node back to `free_list` instead
  of `free()`.
- **Cap at 4096** retained nodes to prevent unbounded residency after bursts;
  `loom_pool_destroy()` walks and frees all remaining nodes (free list +
  buckets).
- The in-progress `free_data` work (freeing the future context on cancel) is
  orthogonal: `user_data` is freed before the node returns to the pool.

**Decision (approved): pool-wide free list with a 4096 cap** — ~20 lines,
correct under the existing lock. Per-worker lock-free free lists are deferred
(see §8).

### Component C — Lazy clock_gettime

Move the two `clock_gettime` calls and the latency computation inside
`if (pool->metrics)`. With no metrics attached (the default) the per-task
cost drops to zero; with metrics attached behavior is unchanged. Two-line
change.

### Component D — Benchmark baseline and CI regression gate

- `examples/bench.c` (already has throughput / scaling / bounded-queue /
  `--json`) gains a `--queue-depth D` scenario: `worker_count = 1`, prefill
  the queue to depth D, measure submit+execute throughput as D grows. Today
  the curve degrades linearly (O(n)); after the fix it is flat.
- New `tools/bench_compare.py`: compares two `--json` outputs and prints a
  delta table.
- `.github/workflows/perf.yml`: add a compare step — compare against the
  parent commit's artifact; **fail the CI run on throughput regression > 15 %**;
  otherwise record the run as the new baseline.

**Decision (approved): 15 % regression threshold** — conservative, given
micro-task throughput jitter.

## 4. Data flow and thread safety

- All queue/node-pool operations remain under `pool->lock`; only the internal
  representation changes. No new locks, no lock-free code.
- Bitmap words are only touched under the lock; reads of `queue_len` /
  pending counts keep the existing relaxed access pattern.
- `worker_entry()` still checks `queue_len == 0 && !shutdown` before waiting
  on `cond`; the dequeue helper now consults the bitmap.
- Ordering invariant unchanged: `STARTED` fires before `fn`, `COMPLETED`
  after `fn`.

## 5. Error handling and edge cases

- `task_create()` still returns NULL on malloc failure → submit paths keep
  their existing error returns. Node pool is a pure optimization; OOM paths
  unchanged.
- `loom_pool_cancel_all` with nodes spanning multiple buckets: each bucket is
  walked and drained; the counter aggregates across buckets.
- Pool destroy: free all bucket nodes (any priority), then all free-list
  nodes, then worker arrays. No double-free (nodes are in exactly one list).
- `loom_coro_exit()` in the worker loop is legitimate cleanup for resized-out
  workers — untouched.
- Priority 255 (legal but undocumented): bucket 255 works exactly like any
  other; worst-case dequeue scans all 4 bitmap words (~4 loads) — bounded,
  constant.

## 6. Testing and example

- `tests/test_thread_pool.c` additions:
  - Bucket boundary: submit priorities {0, 1, 254, 255} interleaved; assert
    strict numeric order with single worker.
  - FIFO within bucket: N same-priority tasks execute in submission order.
  - Node-pool reuse: functional test that 2N submissions on a drained pool
    still complete correctly (allocation count is an internal detail, not
    asserted directly).
  - Cross-bucket `cancel_all` / `cancel_by_id`.
- All existing suites (50111+ assertions) must pass unchanged — especially
  the priority-ordering tests.
- `examples/bench.c`: `--queue-depth` scenario for the acceptance numbers.

## 7. Acceptance criteria

| Metric | Today | Target |
|--------|-------|--------|
| submit+execute throughput at queue depth 10k | linear degradation (O(n)) | flat vs empty queue (±5 %) |
| per-task overhead with no metrics attached | ~40–80 ns clock tax | ~0 |
| all existing assertions (50111+) | pass | pass (incl. priority tests) |
| new tests (buckets / FIFO / pool / cross-bucket cancel) | — | pass |

## 8. Files touched

- `src/thread_pool_internal.h` — struct fields: buckets, bitmap, free_list
- `src/thread_pool.c` — enqueue/dequeue/cancel walkers, task_create/destroy,
  worker_entry clock laziness
- `tests/test_thread_pool.c` — four new test groups
- `examples/bench.c` — `--queue-depth` scenario
- `tools/bench_compare.py` — new comparison script
- `.github/workflows/perf.yml` — compare step + regression gate

## 9. Out of scope (YAGNI)

- No lock-free / MPSC queue (Approach B). The single mutex remains; a future
  design may revisit contention as a separate effort with its own baseline.
- No per-worker free lists or work-stealing.
- No public API changes (headers untouched).
- No changes to coroutine, pipeline, task_group, or metrics modules.
