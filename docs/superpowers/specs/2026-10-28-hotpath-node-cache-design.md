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
| `worker_entry` | 6.6 | worker loop (future slice) |
| `enqueue_task` | 2.8 | submit-side |
| `task_create` | 2.7 | per-submit node allocation |
| `deque_pop` | 1.9 | worker-side |
| `malloc` + `cfree` | ~0.9 | task-node allocation (see stage 1) |
| `pthread_mutex_lock` / `sem_post` / `sem_wait` | ~2.0 | wake mechanics (future slice) |

Measured reference (same host, `bench --json`): submit_latency avg ~4.0 µs,
w1 tps 496k, w8 tps 866k (bench `worker_scaling`), parallel_scaling
w4 135k / w8 190k tps.

**Precondition discovered in plan review (2026-10-28):** the node pool
documented in `docs/architecture.md` is currently **dormant**.
`pool_init` seeds `node_stack` to 0, and the only code path that ever
returns a pool-array address is `node_stack_pop` (which needs `top > 0`) — a
bootstrap circularity. Empirically verified: after `loom_pool_create` and
after 5000 submits + full drain, `node_stack`'s top is still 0 (cap 4096).
So today **every** `task_create` is a `malloc` and every `task_destroy` a
`free`; the 4096-slot array and its ABA stack are allocated but never used,
and there is no global-CAS cost to reduce yet.

## Key mechanical fact (calibrates expectations)

In this library, node **creation** happens on the submitting thread (user/
test/pipeline caller) and node **destruction** happens at the owning worker's
run boundary (`worker_entry`). No main path round-trips a node through one
thread. Therefore a plain per-thread free-list cache would never hit in the
throughput scenarios. The mechanism that actually pays is **batching**
(applies to Stage 2, *after* Stage 1 has made the pool active — today the
pool is dormant and every node is a malloc/free, so there is no global CAS
to reduce yet):

- create side: when a thread's local cache is empty, **pull up to K nodes in
  one batch** (single CAS lowering the global top by N + N relaxed index reads);
- destroy side: when a worker's local cache is full, **flush up to K nodes in
  one batch** (N relaxed frame writes + release fence + single CAS raising the
  global top by N).

Global CAS volume on the node stack drops by ~K× (K = 16) on both sides; the
per-submit cost becomes an array subscript.

Expected effect (honest): Stage 1 modest-to-large (removes steady-state
malloc/free from every submit); Stage 2 modest — 1–5% tps on multi-producer
scenarios, small submit-latency reduction. Each stage is gated independently
(§Testing, Gates 1 and 2); a failed gate reverts that stage's commit only.

## Stage 1: activate the dormant node pool (prerequisite, own commit)

**Corrected during implementation (2026-10-29).** The originally-planned
"seed the counter full" change is **unsound and was abandoned**: seeding the
old counter made `test_thread_pool` hang past the 900 s CTest limit
(deterministic), while the unmodified tree passes in ~40 s. Root cause: the
old protocol was a *counter* (`node_stack` low32 = free-slot count), which is
sound only under strict **LIFO** destroy order — it never recorded *which*
slot was freed. With ≥2 workers destroying out of order (worker A pops slot
50, worker B pops slot 49, B finishes first), the counter marks a
still-in-flight slot free and hands it to a third task: two live nodes alias
one `loom_task_t`, `task_create` stomps the fields, a completion is lost, and
`loom_pool_shutdown` waits forever. The bug was latent only because the pool
was dormant (`top = 0` → always malloc).

The fix is a **Treiber free-list of slot indices** — order-independent by
construction:

- New pool member `_Atomic uint32_t *node_free_next` (calloc'd alongside
  `node_pool`, freed in `pool_destroy_internal`); `node_free_next[i]` links
  slot i to the next free slot, or the `LOOMWORKS_NODE_FREE_END` sentinel
  (`UINT32_MAX`, distinct from every valid index).
- `node_stack` is re-interpreted: low32 = **head slot index** (or the
  sentinel = empty), high32 = ABA tag, exactly as before.
- `pool_init` seeds the chain `0 → 1 → … → (cap-1) → END`, head = 0: the
  pool starts full. OOM-degraded pools store the sentinel and behave as today.
- `node_stack_pop`: acquire-load `node_free_next[head]`, CAS
  `(tag,head) → (tag+1,next)`, hand out `&node_pool[head]`.
- `node_stack_push`: recover the slot index from the pointer, release-store
  the link cell, CAS `(tag,old_head) → (tag+1,slot)`.
- **Both link-cell accesses are atomic (release-store / acquire-load).**
  The slot being pushed is not the one being popped, so ordering only
  `node_stack` leaves the cell's store/load unpaired — a real data race,
  confirmed by TSan (`node_stack_push` vs `node_stack_pop`) and fixed by
  this pairing.
- `loom_task_t` is unchanged: its `next` member is in-flight queue linkage
  (priority buckets), never a free-list link.
- `task_create` additionally resets `t->completion = NULL`, so a recycled
  node cannot carry a stale future pointer into the cancel paths.

Expected effect (honest): removes `malloc`/`free` from every steady-state
submit (the ~0.9% `malloc + cfree` line and part of `task_create`'s 2.7%);
adds one ABA CAS to `task_create` and one to `task_destroy` — cheap while
the pool is active, but the net win must clear the gate, not be assumed.

**Gate 1 (vs the unmodified tree):** same A/B rules as §Testing below,
median-of-3, no scenario regresses beyond the CI limits, ≥2% tps gain in at
least one scenario. If Gate 1 fails → revert Stage 1, stop; Stage 2 is
meaningless without an active pool.

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
  if found:  t = slot.node; slot.node = NULL; slot.pool = NULL
  else:      /* no cached node for THIS pool */
      room = number of empty slots (slot.node == NULL)
      if room >= 1:
          refill = node_stack_batch_pop(pool, buf, min(K, room))
          cache buf[0 .. refill-2] into empty slots (owner = pool)
          t = buf[refill-1]  /* hand off the last popped node */
          if refill == 0: t = NULL
      t = t ?: malloc fallback
  ... field init as today, PLUS t->completion = NULL (Stage 1) ...

task_destroy(pool, t):
  if pool && node_from_pool(pool, t):
      find an empty TLS slot (round-robin from g_tls_next) with
          slot.node == NULL
      if found: cache t there (slot.node = t, slot.pool = pool)
      else:     /* all K slots occupied — some by other pools */
          flush only slots owned by THIS pool, together with t, in ONE
          batch push (<= K+1 nodes, one CAS); clear those slots only;
          leave other-pool slots untouched — their owning pool's worker
          will flush them (slots are never shared across pools)
  else:
      free(t)
```

At most 1 of every K+1 destroys for a given (thread, pool) pair touches the
global node stack; the other K are pure TLS storage. Two subtleties the
seeded pool makes mandatory (they were invisible while the pool was
dormant, when no node ever came back through it):

- **Cross-pool flush hygiene**: the flush collects only slots whose
  `slot.pool == pool`. Flushing another pool's cached node would write it
  onto this pool's stack; a later `node_stack_pop` on the *other* pool
  would hand it out from the wrong array — an out-of-bounds hazard.
- **Refill capacity**: the cache is bounded by K slots, so the batch pop
  is capped at `min(K, room)`; popping more than `room` would leave a node
  with no slot to hold it.

### `pool_init` / destroy paths

- **Stage 1**: `pool_init` seeds the Treiber chain `0 → 1 → … → (cap-1) → END`
  full (head = slot 0, tag = 0), and allocates `node_free_next` alongside
  `node_pool`. An OOM-degraded pool stores the `LOOMWORKS_NODE_FREE_END`
  sentinel in `node_stack` and keeps cap 0, bypassing pooling exactly as
  today.
  `pool_destroy_internal` is unchanged apart from also freeing
  `node_free_next`; it joins all workers before either array is freed, so
  recycling costs nothing at teardown.
- **Stage 2**: the batch ops index the same fixed array; capacity
  (`LOOMWORKS_NODE_POOL_CAP`) unchanged. TLS stale pointers that outlive a
  pool are compare-only and never dereferenced, so no use-after-free is
  reachable.

## Out of scope (explicit)

- Worker-loop lock granularity, `sem_post`/`sem_wait` wake mechanics,
  Step-C0 `coro_lock` nesting (next slice: worker side).
- `metrics_fire` fast path, backpressure quick-out (C2, separate slice).
- Bulk-steal parameters (`LOOMWORKS_BULK_DEQUEUE`, `STEAL_TRIES`).
- Coroutine node allocation (`loom_coroutine_t` is a separate class, untouched).

## Testing & acceptance

Both stages run the **hard regression gates** first, then their own A/B
gate. No stage commits until its gates are green.

### Hard regression gates (each stage, before commit)

- ctest 8/8 (Release).
- ASan build: ctest 8/8 with `ASAN_OPTIONS=detect_leaks=1`.
- UBSan build: ctest 8/8 (`halt_on_error` semantics via the suite).
- CI: TSan + valgrind jobs green.
- clang-format 18 **and** 22 dry-run clean on touched files (repo-wide dual-
  version gate rule).

### Stage 1 (pool activation)

- New deterministic test `test_node_pool_free_list_full_after_drain`
  (`tests/test_thread_pool.c`): submits `2 · LOOMWORKS_NODE_POOL_CAP` tasks
  to an 8-worker pool (forcing the free list empty → malloc fallback → refill
  under contention), asserts every task ran exactly once, then walks
  `node_free_next` from `node_stack`'s head and asserts the chain reaches
  every one of the `cap` slots **exactly once** and terminates at
  `LOOMWORKS_NODE_FREE_END`. Guards (each independently checked): out-of-range
  link, cycle (`seen > cap`), duplicate link (bitmap), lost slot (short count).
  This is the regression net for the order-independence property — the
  property whose violation made the seeded pool hang.
- Probe-detection was validated out-of-band against a synthetic chain with a
  cycle, an out-of-range link, and a dead-end slot (each detected, no crash).
- A/B **Gate 1** — baseline = untouched tree, median of 3 `bench --json`:
  - no scenario regresses beyond CI perf-gate limits (throughput ≥60%,
    p99 ≤2× of baseline);
  - ≥ 2% tps gain in at least one of: parallel_scaling (w4/w8),
    worker_scaling (w4/w8), submit_latency.
  - **Gate 1 fails → revert Stage 1; Stage 2 is not attempted** (it builds
    on an active pool).

### Stage 2 (per-thread batching cache)

- New deterministic test `test_node_cache_cross_pool`
  (`tests/test_thread_pool.c`) — consumer-visible invariants, not
  performance:
  1. Single thread: submit N1 tasks to pool A (counter asserts each ran
     exactly once), shutdown + destroy. N1 = 4·K so each worker's cache
     actually flushes through a worker-side batch push (≥ K+2 tasks).
  2. Same thread: submit N1 tasks to pool B, assert same; destroy B.
     Exercises the stale-slot-skip path on the submitting thread.
  3. One thread alternates small submit/shutdown/destroy cycles on two
     concurrently-lived pools (each < K between recycles) so TLS slots
     hold mixed-owner entries; assert all counts exact.
  4. Under ASan: run clean (`detect_leaks=1`, 0 errors) — catches the
     dereference-a-stale-node and cross-pool-flush regression classes.
- Existing suites (steal/drain/cancel "ran exactly once" counters)
  regression-net batched recycling; no perf-assertion tests (perf claims
  belong to the gate, not the suite).
- A/B **Gate 2** — baseline = **Stage 1's tree** (seeded pool, single-node
  CAS ops), same host, median of 3 `bench --json`:
  - no scenario regresses beyond CI perf-gate limits;
  - ≥ 2% tps gain in at least one of: parallel_scaling (w4/w8),
    worker_scaling (w4/w8), submit_latency.
  - **Gate 2 fails → revert Stage 2 only** (Stage 1's commit stands;
    no net-neutral perf merges).
- Scenarios for both gates: `--iterations 20 --tasks 10000`
  (worker_scaling, parallel_scaling, submit_latency) and `--tasks 100000`
  (queue_depth, tail_latency) with `--workers 4` / `--workers 8`.

## Docs

- `CHANGELOG.md` `[Unreleased]` → `### Changed`: Stage 1 entry (node pool
  now actually recycles: steady-state submits no longer malloc/free) and
  Stage 2 entry (submit/node allocation batches node recycling per
  thread; measurable multi-producer throughput improvement).
- `docs/architecture.md`: node-pool paragraph — correct the dormant-pool
  description (seeded full at init) and mention the per-thread batch cache
  (2–3 sentences, internal detail).
- No new risk row: mechanisms are internal; invariants are covered by the
  hard gates; R-register unchanged.

## Commits (two, independently revertable)

```
1. fix(pool): 🐛 seed node pool full at init so task nodes actually recycle
   (pool was dormant: top=0 at init means no node was ever pool-owned;
   every submit malloc'd, every destroy free'd. Seed top=cap, reset
   completion=NULL in task_create. A/B table, gate 1 result.)

2. perf(pool): ⚡ batch task-node recycling per thread on the submit path
   (TLS batch cache, K=16, batch pop/push on the ABA stack; A/B table vs
   Stage 1 tree, gate 2 result.)
```

## Rollback

One `git revert` per commit — no API surface, no on-disk state, no config
knob. Stage 2 reverts independently of Stage 1. If a later worker-side
slice lands, re-baseline from the merged tree (baseline document
convention: new `docs/perf/baseline-*.md`).
