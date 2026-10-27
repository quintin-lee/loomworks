# Hot-Path Task-Node Allocation (Per-Thread Batching Cache) — Design

Status: approved 2026-10-28. Scope: one independently revertable commit slice of the
"hot-path optimization" effort — the submit/enqueue side only. Worker-loop lock
granularity, wake mechanics, and bulk-steal parameters are out of scope (future
slices).

## Problem

Profiling (`perf record -g`, w=16/t=100k bench) attributes the library-side
submit cost to:

| Hotspot | Self% | Note |
|---|---|---|
| `worker_entry` | 6.6 | worker loop (next slice) |
| `enqueue_task` | 2.8 | submit-side, this slice |
| `task_create` | 2.7 | per-submit node allocation |
| `deque_pop` | 1.9 | worker-side |
| `malloc` + `cfree` | ~0.9 | node-pool fallback paths |
| `pthread_mutex_lock` / `sem_post` / `sem_wait` | ~2.0 | wake mechanics (next slice) |

Today every `task_create` does one ABA-tagged Treiber `node_stack_pop` CAS loop
on the **global** pool node stack, and every `task_destroy` one `node_stack_push`
CAS loop. Under multi-producer load the global stack is a shared CAS hot spot;
each submit pays a lock-free atomic round-trip that a local allocation could
amortize.

Measured reference (same host, `bench --json`): submit_latency avg ~4.0 µs,
w1 tps 496k, w8 tps 866k (bench `worker_scaling`), parallel_scaling
w4 135k / w8 190k tps.

## Key mechanical fact (calibrates expectations)

In this library, node **creation** happens on the submitting thread (user/
test/pipeline caller) and node **destruction** happens at the owning worker's
run boundary (`worker_entry`). No main path round-trips a node through one
thread. Therefore a plain per-thread free-list cache would never hit in the
throughput scenarios. The mechanism that actually pays is **batching**:

- create side: when a thread's local cache is empty, **pull up to K nodes in
  one batch** (single CAS lowering the global top by N + N relaxed index reads);
- destroy side: when a worker's local cache is full, **flush up to K nodes in
  one batch** (N relaxed frame writes + release fence + single CAS raising the
  global top by N).

Global CAS volume on the node stack drops by ~K× (K = 16) on both sides; the
per-submit cost becomes an array subscript.

Expected effect (honest): modest — 1–5% tps on multi-producer scenarios,
small submit-latency reduction. This slice is taken only if the A/B gate
below passes; otherwise it is reverted (the whole slice is one commit).

## Mechanism

All changes in `src/thread_pool.c` (+ one constant in
`src/thread_pool_internal.h`). No API changes, no header changes, no lock-order
changes.

### TLS cache (new)

```c
/* Per-thread batched cache of pooled task nodes.  One shared cache per
 * thread (nodes carry no pool identity; a slot records which pool's node
 * array it points into so cross-pool stale entries are never reused). */
#define LOOMWORKS_TLS_NODE_SLOTS 16 /* 4 cache lines */

typedef struct tls_node_slot {
    loom_task_t        *node;  /* NULL = empty slot */
    loom_thread_pool_t *pool;  /* pool owning node (compare-only, never
                                  dereferenced when pool != current) */
} tls_node_slot_t;

static _Thread_local tls_node_slot_t g_tls_nodes[LOOMWORKS_TLS_NODE_SLOTS];
static _Thread_local unsigned        g_tls_next = 0;
```

Invariants:

- **Slot selection is round-robin** (`g_tls_next++`). A slot is reusable
  only when `slot.node == NULL` or `slot.pool == pool` (a compare-only
  pointer test against the current pool — never a dereference).
- **Cross-pool stale entries**: a slot holding pool A's node is skipped by a
  pool B submission (it is never *dereferenced* — the decision is
  `slot.pool == pool`). Stale pointer values may outlive their pool; only the
  pointer values themselves are read, which is memory-safe.
- **Lock-free by construction**: cache operations touch only TLS storage and
  the global node-stack top; they never take `pool->lock`, `coro_lock`, or
  `timer_lock`. `task_destroy` at the run boundary is called with
  `pool->lock` held on some paths — the cache push must stay lock-free, which
  it does. No lock-ordering impact.
- **Only pooled nodes are cached.** `node_from_pool(pool, t)` false
  (malloc'd node, or the pool's `node_pool` was OOM-null) → the node goes
  straight to `free()` on destroy and straight from `malloc()` on create,
  exactly as today. An OOM-degraded pool (cap 0) simply bypasses the cache
  entirely.

### Batch primitives (new, replace per-node stack use)

```c
/* Pop up to `want` indices in one batch: relaxed reads of the top `n`
 * frames, then one CAS of the global top from T to T-n (weak, retried).
 * Returns n in [0, want]. Concurrent pushes/steals are safe: only frames
 * below the published top are ever read; a failed CAS re-reads. */
size_t node_stack_batch_pop(loom_thread_pool_t *pool, loom_task_t **out, size_t want);

/* Push `n` nodes in one batch: relaxed writes of the next `n` frames at
 * [top, top+n), an atomic_thread_fence(release), then one CAS of the top
 * from T to T+n (weak, retried).  Returns true on success. */
bool node_stack_batch_push(loom_thread_pool_t *pool, loom_task_t *const *in, size_t n);
```

ABA tags: the tag half-word increments by 1 per successful batch (same as one
increment per successful single op today — the tag counts operations, not
frames; the ABA window is bounded by the stack cap as before).

The existing single-node `node_stack_pop`/`node_stack_push` remain for the
odd-count residue paths (they are cheap and few); or are retired in favor of
batch ops with `want = 1` — implementation detail, spec does not fix it.

### `task_create` / `task_destroy` (modified)

```
task_create(pool, ...):
  t = NULL
  scan TLS cache round-robin from g_tls_next for a slot with
      slot.node != NULL && slot.pool == pool          (<= K reads, K = 16)
  if found:  t = slot.node; slot.node = NULL
  else:      refill = node_stack_batch_pop(pool, buf, K)
             if refill > 0: store popped nodes into free TLS slots (owner = pool)
             t = one of the just-cached nodes, or malloc fallback
             (cache refill and the first hand-off combined: pull K, keep K-1,
              return 1)

task_destroy(pool, t):
  if pool && node_from_pool(pool, t):
      find a free TLS slot (round-robin from g_tls_next) with
          slot.node == NULL || slot.pool == pool;
      if found: cache t there (slot.node = t, slot.pool = pool)
      else:     /* all K slots full */
          flush all K cached nodes + t in ONE batch push
          (K+1 nodes, one global CAS), leaving the slots empty
  else:
      free(t)
```

At most 1 of every K+1 destroys touches the global node stack (the flush);
the other K are pure TLS storage. Global CAS count per K+1 destroys
drops to ~1 instead of K+1.

### `pool_init` / destroy paths (unchanged, verified)

- Node pool capacity (`LOOMWORKS_NODE_POOL_CAP`, default) unchanged; the batch
  ops index the same fixed array.
- `pool_destroy_internal` already joins all workers (destroy sites on worker
  threads are drained) before freeing `node_pool`; TLS stale pointers that
  outlive the pool are compare-only and never dereferenced, so no use-after-
  free is reachable.

## Out of scope (explicit)

- Worker-loop lock granularity, `sem_post`/`sem_wait` wake mechanics,
  Step-C0 `coro_lock` nesting (next slice: worker side).
- `metrics_fire` fast path, backpressure quick-out (C2, separate slice).
- Bulk-steal parameters (`LOOMWORKS_BULK_DEQUEUE`, `STEAL_TRIES`).
- Coroutine node allocation (`loom_coroutine_t` is a separate class, untouched).

## Testing & acceptance

### New deterministic test (`tests/test_thread_pool.c`)

`test_node_cache_cross_pool` — consumer-visible invariants, not performance:

1. Single thread: submit N1 tasks to pool A (counter asserts each ran exactly
   once), shutdown + destroy. Use N1 = 4·K so each worker's cache actually
   flushes at least once through a worker-side batch push (≥ K+2 tasks).
2. Same thread: submit N1 tasks to pool B, assert same; destroy B.
   Exercises the stale-slot-skip path on the submitting thread.
3. One thread alternates small submit/shutdown/destroy cycles on two pools
   created concurrently (each < K between recycles) so TLS slots hold
   mixed-owner entries; assert all counts exact.
4. Under ASan: run clean (`detect_leaks=1`, 0 errors) — catches the
   dereference-a-stale-node regression class.

Existing suites (steal/drain/cancel "ran exactly once" counters) already
regression-net batched recycling; no new perf-assertion tests (perf claims
belong to the A/B gate, not the test suite).

### A/B performance gate (same host, median of 3 `bench --json` runs)

| Gate | Keep condition |
|---|---|
| No regression anywhere | All scenarios within the CI perf-gate limits vs the **pre-change baseline** (median-of-3 `bench --json` recorded on the untouched tree first): throughput ≥60% and p99 ≤2× of that baseline |
| Positive gain | ≥ 2% tps improvement in **at least one** of: parallel_scaling (w4/w8), worker_scaling (w4/w8), submit_latency |
| Otherwise | Revert the slice — no net-neutral merges of perf work |

Scenarios: `--iterations 20 --tasks 10000` (worker_scaling, parallel_scaling,
submit_latency) and `--tasks 100000` (queue_depth, tail_latency) with
`--workers 4` / `--workers 8`.

### Regression gates (hard, before commit)

- ctest 8/8 (Release).
- ASan build: ctest 8/8 with `ASAN_OPTIONS=detect_leaks=1`.
- UBSan build: ctest 8/8 (`halt_on_error` semantics via the suite).
- CI: TSan + valgrind jobs green.
- clang-format 18 **and** 22 dry-run clean on touched files (repo-wide dual-
  version gate rule).

## Docs

- `CHANGELOG.md` `[Unreleased]` → `### Changed`: one-line entry (submit/node
  allocation batches node recycling per thread; measurable multi-producer
  throughput improvement).
- `docs/architecture.md`: node-pool paragraph — mention the per-thread
  batch cache (2–3 sentences, internal detail).
- No new risk row: mechanism is internal, all invariants above are covered by
  the regression gates; R-register unchanged.

## Commit (single, revertable)

```
perf(pool): ⚡ batch task-node recycling per thread on the submit path

... (body: mechanism + measured A/B table, one row per scenario, with the
median-of-3 numbers and the ≥2% gate result)
```

## Rollback

One `git revert` — no API surface, no on-disk state, no config knob. If a
later worker-side slice lands, re-baseline from the merged tree (baseline
document convention: new `docs/perf/baseline-*.md`).
