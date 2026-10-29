# Per-Thread Task-Node Batching Cache (Stage 2) — Design

Status: approved 2026-10-30. Scope: one independently revertable commit slice of
the "hot-path optimization" effort, submit/enqueue side.

**Spec being extended:** `docs/superpowers/specs/2026-10-28-hotpath-node-cache-design.md`
(Stage 1 delivered 2026-10-29 as commit `ed0cbcb`).

## Relationship to Stage 1

Stage 1 activated the dormant node pool by replacing its **LIFO-only counter**
with an order-independent **Treiber free-list of slot indices**. That changed the
protocol this design depends on, so three things the original Stage 2 text
asserted are now **false** and are superseded here:

| Superseded (old spec text) | Reality after `ed0cbcb` |
|---|---|
| `top + n ≤ cap` — batch ops can never overflow the array | Counter invariant; **does not exist**. New invariant: every free slot appears in exactly one chain cell |
| batch pop = "lower the top by N + N relaxed reads" | It is a **chain unwind**: walk N links, one CAS repoints the head at the Nth successor |
| LIFO order is preserved | The free-list is **order-independent**; any LIFO assertion is invalid |

The old spec's `test_node_stack_batch_roundtrip` asserted
`out[0] == &node_pool[1] && out[1] == &node_pool[0]` — that assertion is **wrong**
under the Treiber list and is removed by this design.

## Problem

After Stage 1, `malloc`/`free` are gone from the steady-state submit path, but
every node creation still pays one global CAS on `node_stack` and every destroy
one more. Under multi-producer load the shared head is a contention point.

**Measured submit modes** (`examples/bench.c`, verified by reading the source):
`submit_latency` and `fairness`/`tail_latency` submit **one task at a time**;
`throughput` and `parallel_scaling` submit **thousands per thread**. This
asymmetry is the reason the refill width must be adaptive rather than fixed:
a fixed K is optimal in one mode and wasted work in the other.

## Decisions locked (this session)

1. **Gate standard = "no regression" (1B).** The ≥2% tps gain is a *recorded*
   item, never a commit condition. Reason: the measurement host is currently
   saturated (load average 15.29 on 8 cores from unrelated processes), so any
   perf number taken today is not trustworthy. A gate that cannot be executed
   is worse than no gate — it produces false-negative reverts.
2. **Cross-pool model = single owner (2B).** One `owner_id` per thread, not a
   per-slot `{node, pool}` pair. `loom_runtime` owns exactly one pool
   (`src/runtime.c:15`), so single-owner is the common case by construction.
3. **Dangling protection = monotonic pool ID (B').** Pool addresses are reused by
   the allocator, so an address-based tombstone is unsound: after
   `free(pool)` a new `posix_memalign` can return the same address and stale
   TLS nodes would be treated as valid. Monotonic IDs never repeat, so the
   identity test is reliable.

## Architecture — four components

| Component | Responsibility | Depends on |
|---|---|---|
| 1. Batch primitives (`node_stack_batch_pop/push`) | Unwind N links / link N nodes, **one CAS** | `node_stack` protocol only |
| 2. TLS cache (`g_tls_nodes[K]`, `g_tls_owner_id`) | Turns one CAS per op into one per K ops | components 1 + 3 |
| 3. Pool-ID registry (`g_pool_reg[]` + one mutex) | Answers "is this pool ID still alive?" (positive hint only) | new global mutex |
| 4. `task_create`/`task_destroy` rewiring | The only callers; semantics identical to today | components 1 + 2 |

**The registry is a positive hint, not an authority.** Hit → push stale nodes back
to the old pool (preserves capacity). Miss → discard pointers (never
dereferenced, never pushed back, so no UAF; nodes die with the old pool's
`node_pool` block, so no leak). This decouples registry accuracy from pool
lifetime: a missed registration only costs capacity, never safety.

This design **preserves** the `docs/architecture.md` promise that the node pool
is the sole task-node allocator — that promise is exactly why the
push-back/discard pair exists rather than a bare `free()`.

## Data contracts

```c
#define LOOMWORKS_TLS_NODE_SLOTS 16        /* = 4 cache lines; see below */

typedef struct {
    loom_task_t *node;                     /* NULL = empty slot; NO per-slot pool pointer */
} tls_node_slot_t;

_Thread_local tls_node_slot_t g_tls_nodes[LOOMWORKS_TLS_NODE_SLOTS];
_Thread_local uint64_t        g_tls_owner_id = 0;   /* 0 = never refilled */
_Thread_local unsigned        g_tls_next      = 0;  /* round-robin cursor */
```

**Why K = 16.** It is a starting point, not a derived constant. It keeps the
cache at exactly 4 cache lines (16 × 8 B), and amortizes the global CAS over a
window large enough that the throughput-mode submit loop wins clearly while a
single-shot submit still costs no more than today (an empty cache pays one CAS
it was already paying). Because the gain is a *recorded*, not a *gating*, number
this session (see §Decisions), K is deliberately **not** tuned by measurement
here; tuning requires a quiet host and is a separate follow-up.

`loom_thread_pool_t` gains `uint64_t id` (+8 B). The registry is
`g_pool_reg[256]` of `{id, pool}` guarded by a `g_pool_reg_lock`, written only in
`pool_init`, `pool_destroy_internal`, and on TLS owner switch.

**Registry overflow.** 256 live pools is far beyond any use in this library
(`loom_runtime` is single-pool; `loom_pc_*` creates one internal pool per
pipeline). On overflow registration **fails silently**: the new pool still works
normally, but a thread switching away from it takes state III (discard) instead
of state II (push back) — losing ≤ K slots of that pool, never memory safety.
An unregistered pool therefore degrades to Stage-1 behavior for its stale nodes,
which is exactly the intended failure mode: capacity loss, never corruption.

### Batch primitives

- `size_t node_stack_batch_pop(pool, loom_task_t *out[], size_t want)` —
  returns **actual** count `n` with `1 ≤ n ≤ min(want, chain length)`; returns
  **0** when the chain is empty (caller falls back to `malloc`). `out[0..n-1]`
  are valid nodes, in no particular order.
- `bool node_stack_batch_push(pool, loom_task_t *in[], size_t n)` — **all or
  nothing**: on success all `n` are linked; on failure all `n` remain the
  caller's. Never partially succeeds, because partial success creates ambiguous
  ownership.

### Ownership rules (binding)

- **R1.** Nodes returned by `batch_pop` carry **unwind-residue** values in
  `free_next`; they are the caller's. The chain is not rewritten for popped
  nodes, so these values must be re-linked before any future push.
- **R2.** The retry path re-fills **only the last** link cell. Rewriting an
  already-fixed local chain segment can create a cycle → a later `pop` spins
  forever.
- **R3.** Flushing to a **non-owner pool** is forbidden even when the addresses
  happen to be in range: another pool's chain does not know those slots.

### Dangling protection — three states (evaluated on owner switch)

| State | Condition | Action |
|---|---|---|
| I | `g_tls_owner_id == pool->id` | Reuse directly. Pure integer compare. |
| II | differs, old ID **hits** registry | Push stale nodes back to the old pool in one batch; clear TLS; adopt new owner. Preserves capacity. |
| III | differs, old ID **misses** (pool dead) | Discard pointers only — no dereference, no push-back. No UAF, no leak. That pool permanently loses ≤ K slots. |

There is deliberately no fourth state: when `g_tls_owner_id != 0` it necessarily
came from a pool that once existed.

## Rewired flows

**`task_create` (submitting thread)**

```
t = NULL
if (g_tls_owner_id != pool->id) resolve_owner_switch(pool)   /* states I/II/III */
scan TLS for a slot with node != NULL
    hit   -> t = slot.node; slot.node = NULL
    miss  -> room = empty slot count
             n = node_stack_batch_pop(pool, buf, min(K, room))   /* adaptive */
             if n > 0: cache buf[0..n-2] into empty slots; t = buf[n-1]
             else    : t = malloc(sizeof *t)
if (!t) return NULL
... field init, identical to today, incl. completion = NULL ...
```

The `min(K, room)` bound is what makes the design adaptive: a short chain
naturally yields fewer nodes instead of burning a CAS to discover it is short.

**`task_destroy` (worker thread / cancel paths)**

```
if (pool && node_from_pool(pool, t)) {
    if (g_tls_owner_id != pool->id) resolve_owner_switch(pool)
    find an empty TLS slot (round-robin)
        found -> cache and return
        full  -> push this pool's occupied slots + t in ONE batch_push;
                 clear those slots
} else {
    free(t)      /* malloc'd node or OOM-degraded pool */
}
```

At most 1 of every K+1 destroys touches the global head.

## Error handling

| Case | Handling |
|---|---|
| refill returns 0 (empty chain) | `malloc` fallback; on destroy `node_from_pool` is false → `free()`. Pre-existing behavior, zero change |
| `batch_push` fails (returns false) | Caller **retains all n nodes** and `free()`s them. No retry-via-single-push branch: `node_stack_batch_push` already retries internally against `node_stack`, so an external failure means the single CAS failed under contention — a retry path here would be untestable dead code. Note that `task_destroy` freeing rather than re-queueing these nodes trades capacity for simplicity on a path that contention only |
| TLS stale + old pool dead | State III above: discard only |
| OOM-degraded pool (cap 0) | `node_from_pool` always false → never cached, always malloc/free |

Including the unreachable-fallback row is deliberate: it stops an implementer
from adding an untestable retry branch.

## Testing

1. `test_node_batch_roundtrip` — primitive invariants: `n ≤ want`, empty chain
   returns 0, and the ownership contract (nodes handed to `batch_push` are
   either all linked or all returned to the caller). The existing test-only
   fault-injection hooks (`loom_test_arm_alloc_failure` / `g_test_alloc_fail_at`,
   `src/thread_pool.c:3008`) deliberately guard **`loom_pool_resize` call sites
   only** and do not inject into `task_create`'s `malloc`; they are therefore
   **not** used here. Since `node_stack_batch_push` retries internally against
   `node_stack` and its only failure mode is CAS contention (which a
   single-threaded test cannot force), the failure branch is covered by
   asserting the documented contract on the success path plus the "all or
   nothing" signature — not by a synthetic injection. No LIFO-order assertion
   is included, because the list is order-independent.
2. `test_tls_cache_cross_pool` — **the most important new test.** Two pools
   alternated on one thread, exercising all three owner-switch states,
   including destroy-A-then-submit-to-B (the exact use-after-free regression
   path). Run under ASan with `detect_leaks=1`.
3. `test_node_batch_concurrent` — concurrent create/destroy mixing batch and
   single-node paths.

### Hard gates (any failure → revert this slice)

- `ctest` 8/8 (Release)
- ASan `ctest` 8/8 with `ASAN_OPTIONS=detect_leaks=1` — catches dangling/UAF/leak
- TSan: **zero reports on code added by this slice**, compared **per site**, not
  per count (HEAD already has ~16 fluctuating pre-existing reports, so a
  count-based "no new warnings" gate would be unexecutable)
- `clang-format` 18.1.8 **and** 22.1.8 clean on all gated files
- CI Performance job: no p99 / throughput regression vs baseline

### Recorded, not gating

- ≥2% tps gain in any scenario. Today's host is saturated, so this may not be
  measurable; it is written to `docs/perf/baseline-*.md` for the record only.

## Out of scope

- Worker-loop lock granularity, `sem_post`/`sem_wait` wake mechanics
- `metrics_fire` fast path, backpressure quick-out
- Bulk-steal parameters (`LOOMWORKS_BULK_DEQUEUE`, `STEAL_TRIES`)
- Coroutine node allocation (`loom_coroutine_t` is a separate class)
- Any change to `loom_task_t`, the public API, or `worker_entry`
- Single-node `node_stack_pop`/`push` become dead code after rewiring and are
  **deleted**, not retained
