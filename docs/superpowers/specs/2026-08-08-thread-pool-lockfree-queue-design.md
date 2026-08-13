# Thread Pool Lock-Free Queue (Hybrid Two-Tier) — Design

**Date:** 2026-08-08
**Status:** Approved (Approach A: hybrid two-tier — Vyukov ring for NORMAL + bucketized priority lanes)
**Library:** loomworks (C11 thread pool + coroutine library)

> **Superseded (partially) by the work-stealing scheduler (2026-08-12).** The ring + lanes
> design described here remains the submission/overflow layer, but the worker drain path
> changed: workers now bulk-claim batches of 8 from the ring into per-worker Chase-Lev
> deques (LIFO local pops, FIFO cross-worker steal) instead of popping the ring directly.
> See [2026-08-12-work-stealing-design.md](2026-08-12-work-stealing-design.md) for the
> current worker loop order.

## 1. Motivation

The benchmark worker-scaling scenario exposes a hard scaling ceiling:

| workers | tasks/sec |
|---------|-----------|
| 1       | 2,110,772 |
| 2       | 1,197,972 |
| 4       | 524,551  |
| 8       | 505,599  |

Throughput *falls* as workers are added — a classic single-mutex + condvar
contention signature. Every task cycle takes two lock acquisitions (submit +
dequeue) plus a `pthread_cond_signal` futex wakeup (thundering herd with N
waiters). The bench uses trivial atomic-increment tasks, so lock + wakeup
latency dominates and more workers only add contention.

The previous queue-performance iteration (2026-08-07) explicitly deferred this
in its spec §9: *"No lock-free / MPSC queue (Approach B). The single mutex
remains; a future design may revisit contention as a separate effort with its
own baseline."* This design is that separate effort.

Goal: make the steady-state submit/dequeue hot path lock-free for the common
case (NORMAL priority) while preserving every existing public behavior —
strict priority ordering, bounded-capacity semantics, cancel-by-id/data,
node pooling, shutdown/drain, metrics.

## 2. Architecture

```
                    ┌──────────────────────────────────────────┐
 submit(NORMAL) ───►│ Vyukov bounded MPMC ring (lock-free)     │──┐
 (p == 5)           │ 4096 slots (unbounded mode) /            │  │ worker pops ring
                    │ next_pow2(capacity) (bounded mode)       │  │ (lock-free, CAS)
                    └──────────────────────────────────────────┘  ▼
                                                                 worker
                    ┌──────────────────────────────────────────┐  ▲
 submit(≠NORMAL)───►│ 256-bucket priority lanes (mutex)        │──┘
 (p < 5 or p > 5)   │ rare path — contention negligible        │  worker drains
                    └──────────────────────────────────────────┘  (strict order)
```

**Core split:** NORMAL (= `LOOMWORKS_PRIORITY_NORMAL`, the value used by the
entire benchmark suite and the overwhelming majority of real workloads) goes
to a lock-free Vyukov MPMC ring. REALTIME/HIGH/LOW/custom priorities stay on
the existing 256-bucket mutex-protected queue, where their rarity makes lock
contention negligible.

The ring's slots are **never freed during pool lifetime** — sequence-number
CAS (Vyukov) prevents ABA, so no memory-reclamation scheme (hazard pointers /
epochs) is needed. This deliberately avoids reopening the epoch-reclamation
topic closed in design-decisions.md §11.

**Dequeue ordering (strict, unchanged semantics):** each worker round
1. atomically peek the 256-bit bitmap; if any bucket with `p < 5` is non-empty
   → take the mutex, drain that bucket first (higher priority preempts);
2. otherwise pop the ring (NORMAL, lock-free);
3. only when the ring is empty, drain buckets with `p >= 5` (the NORMAL lane
   spill target at p == 5 first via lowest-set-bit, then LOW at p > 5).

Step 3 must be `p >= 5`, NOT `p > 5`: NORMAL tasks that spilled to the NORMAL
lane (unbounded ring full) live at p == 5 and would otherwise never be
dequeued. Bitmap lowest-set-bit naturally orders 5 before 10, so the spill
lane drains before LOW — consistent with today's strict ordering.

Priority ordering is byte-for-byte identical to today: numerically smaller
priority runs first, FIFO within a priority.

## 3. Components

### Component A — Vyukov bounded MPMC ring (NORMAL fast path)

New in `src/thread_pool_internal.h`:

```c
typedef struct ring_cell {
    _Atomic uint64_t seq;   /* 2n empty, 2n+1 full (Vyukov sequence) */
    void            *task;  /* loom_task_t* — stable while in ring */
} ring_cell_t;

struct loom_thread_pool {
    /* ... existing fields unchanged ... */
    _Atomic uint64_t ring_head;    /* consumer index (monotonic) */
    _Atomic uint64_t ring_tail;    /* producer index (monotonic) */
    ring_cell_t     *ring;         /* NULL when bounded-mode ring not used */
    uint64_t         ring_mask;    /* ring_size - 1 (power of two) */
    _Atomic uint32_t ring_count;   /* tasks currently in ring */
    /* lock-free id→task index for cancel-by-id (see Component C) */
    _Atomic uint64_t *cancel_slots;
    uint64_t          cancel_cap;
};
```

**Ring sizing & capacity semantics (exact, non-negotiable):**
- **Logical capacity is gated by an atomic `queue_len`** (single counter
  covering ring + lanes), NOT by ring physical fullness. `queue_capacity` must
  remain exact for *any* value — tests use 5, 100, 1000 (non-powers of two).
  Enqueue on any path first checks `queue_len >= queue_capacity` and fails with
  `LOOMWORKS_ERR_INVALID` (or blocks, for `submit_blocking`). Since
  `next_pow2(capacity) >= capacity` always holds, the logical gate trips before
  the ring ever physically fills in bounded mode → **bounded mode has no spill
  path** and no capacity drift.
- **Bounded mode** (`queue_capacity > 0`): `ring_size = next_pow2(queue_capacity)`
  as the physical upper bound; logical rejection stays exactly at
  `queue_capacity` via the atomic gate. `queue_len` counts ring + lanes.
- **Unbounded mode** (`queue_capacity == 0`, the bench's mode): fixed 4096-slot
  ring (user-confirmed). Steady state never approaches fullness under the bench
  workload; if it ever physically fills, NORMAL tasks **spill to the NORMAL
  bucket lane** (mutex path) — graceful degradation, correctness preserved,
  zero reclamation. `queue_len` is still the single counter.

**Enqueue (lock-free):** Vyukov producer protocol — `pos = fetch_add(tail)`;
`cell = &ring[pos & mask]`; spin on `cell->seq == pos*2` (wait for consumer);
store `task`; `release` store `seq = pos*2+1`. On NORMAL path: atomic
`queue_len` gate first (reject at capacity, else `fetch_add`), then ring CAS
enqueue; if the ring physically filled in unbounded mode (seq mismatch), fall
back to the NORMAL lane (which re-checks the gate under the mutex) and
`fetch_sub` the speculative increment. Non-NORMAL path: mutex → lane → same
gate → same `LOOMWORKS_ERR_INVALID` semantics as today.

**Dequeue (lock-free):** Vyukov consumer protocol — `pos = fetch_add(head)`;
spin on `cell->seq == pos*2+1`; acquire-load `task`; `seq = pos*2+2`. Skip
tombstoned tasks (Component C): count as CANCELLED, decrement ring_count.

### Component B — 256-bucket priority lanes (non-NORMAL path)

Unchanged structure (`buckets_head/tail`, `nonempty_bits`, mutex-protected).
All 256 buckets remain; only NORMAL (=5) is a *fast-lane escape*: NORMAL tasks
go to the ring, and the NORMAL bucket serves as the spill/overflow target.
All other priorities use the lanes exactly as today (including exact-delete
cancel). This preserves every existing priority test verbatim.

The occupancy bitmap gains an atomic fast-peek path: `nonempty_bits` reads
become `atomic_load` so workers can check "any p<5 work?" without taking the
mutex. Mutated under the lock as today.

### Component C — Cancel: tombstone + lock-free id index

Ring tasks cannot be removed mid-ring → **tombstone** (`task->cancelled` is
already atomic-capable; mark + skip at dequeue, count CANCELLED). But
`loom_pool_cancel_by_id` must return `LOOMWORKS_OK` for a queued task (hard
test constraint), so the ring needs a find mechanism.

Exploit a structural invariant: `task_id` comes from a monotonic atomic
counter and **never repeats within a pool lifetime** → a small open-addressing
hash set keyed by task_id is ABA-free by construction:

```c
/* cancel_slots[i].task_id: 0 = EMPTY, id+1 = occupied, 1 = TOMBSTONE */
/* cancel_slots[i].data:    task data pointer (for loom_pool_cancel) */
/* hash(task_id) → linear probe */
```

- submit (ring path): CAS-insert `{task_id+1, data}` at the probe slot.
- worker pop: CAS-remove `task_id+1` (set TOMBSTONE, lazily reclaimed).
- `cancel_by_id`: hash-probe; found → CAS `task->cancelled = true` → OK;
  else INVALID.
- `cancel(data)`: the index is small (≤ `next_pow2(ring_size*2)` = 8192 slots
  for a 4096 ring), so a rare **linear scan of occupied slots** finds the first
  matching `data`, then tombstones via task_id — no second index needed. (The
  existing API contract is "first task whose data matches"; a bounded scan of
  the ring's live set preserves that contract. Lane tasks keep exact-delete.)
- Lane tasks (non-NORMAL): exact-delete as today (unchanged code path).

**Semantics preserved:** cancel returns OK/INVALID exactly as today, cancelled
tasks never execute, CANCELLED metric fires, `cancel_all` still drains both
ring and lanes. The id-index is sized `next_pow2(ring_size * 2)` and only
exists when the ring does.

### Component D — Wakeup: POSIX semaphore replaces condvar

`condvar` requires the mutex → incompatible with lock-free enqueue. Replace
`pool->cond` with `sem_t work_sem`:

- submit (any path): `sem_post(&work_sem)` after enqueue (lock-free call, no
  lost wakeup — semaphore counts).
- worker: `sem_wait` when queue empty (ring empty + bitmap zero); re-check
  under the brief mutex only for lane work.
- shutdown: set flag + `sem_post × worker_count` to wake all, then join.
- `drain_cond` retained (external waiters on pending_count).

Semaphore is the standard lock-free-queue pairing (same pattern as
Moodycamel/Seastar). No spurious-wakeup handling needed; workers re-check
shutdown/queue state after each wait.

### Component E — Node pool & lifecycle

- **Ring path:** the ring owns node lifetimes; a popped node is either
  tombstoned (freed after CANCELLED accounting) or executed and returned to
  the free-list. No per-task malloc on the hot path beyond the existing pool.
- **Lane path:** existing `task_create`/`task_destroy` free-list, unchanged.
- **Shutdown:** drain ring + lanes with existing semantics (pending tasks
  freed; running tasks complete); `free_all` releases ring cells and index.
- **Metrics:** SUBMITTED fires on enqueue (any path); STARTED/COMPLETED/
  CANCELLED unchanged; latency timing unchanged.

## 4. Data flow (submit → execute)

```
submit(fn, data, NORMAL)
  ├─ task = task_create(pool, fn, data, NORMAL)          (free-list)
  ├─ gate: if queue_len >= queue_capacity (bounded) → ERR_INVALID (or block)
  │        else queue_len++ (atomic fetch_add, speculative)
  ├─ insert id into cancel index
  ├─ Vyukov enqueue (lock-free, no mutex) → sem_post
  └─ if ring physically full (unbounded mode, seq mismatch):
       queue_len-- (undo speculative), mutex → NORMAL lane (re-gates under
       lock, p==5) → sem_post → unlock

submit(≠NORMAL) — mutex → lane (unchanged) → sem_post → unlock

worker loop
  ├─ if bitmap[any p<5]: mutex → drain lowest such lane → unlock → run
  ├─ else if ring non-empty: Vyukov pop (lock-free) → remove id → run
  │    (tombstone → CANCELLED, skip)
  ├─ else if bitmap[any p>=5]: mutex → drain lowest such lane (NORMAL spill
  │    lane at p==5 first, then LOW) → unlock → run
  └─ else: sem_wait (sleep)
```

## 5. Public API & behavior contract (must be preserved)

| Behavior | Today | After |
|----------|-------|-------|
| Priority ordering (lower first, FIFO within) | strict | strict (identical rule) |
| `queue_capacity>0` reject when full | ERR_INVALID | ERR_INVALID (ring exact capacity) |
| `queue_capacity==0` unbounded | never rejects | never rejects (4096 ring + spill) |
| cancel_by_id / cancel(data) | OK/INVALID | OK/INVALID (tombstone + id index) |
| cancel_all | drains queue | drains ring + lanes |
| shutdown/drain | graceful | graceful (sem wake + join) |
| pending_count / active / idle / utilization | atomic reads | unchanged |
| resize | add/remove workers | unchanged (workers see ring) |
| metrics events | submitted/started/completed/cancelled | unchanged |
| thread safety | any-thread submit | any-thread submit (lock-free ring) |

No public header changes. `thread_pool_internal.h` grows the ring + index
fields only.

## 6. Performance expectations (acceptance criteria)

Measured via `./build_plan/examples/bench` (baseline captured 2026-08-08):

1. **worker_scaling** (unbounded, trivial tasks): throughput at 8 workers must
   be ≥ throughput at 1 worker (currently 505K vs 2,110K — invert the curve).
   Stretch: monotonic increase with workers.
2. **submit_latency**: per-submit latency (avg) must drop measurably (lock-free
   enqueue removes mutex + signal from the submit path).
3. **No regressions**: throughput/bounded_queue/future_overhead within ±10% of
   baseline; coro_create_destroy untouched.

## 7. Testing

**Regression (must stay green, unchanged):** all existing 341 thread-pool
assertions — priority_ordering, priority_future, cancel_by_id (found / not
found / after shutdown / same-data / running), cancel, cancel_all, bounded
capacity, shutdown, resize, task_group, metrics, health.

**New tests (tests/test_thread_pool.c):**
1. `test_ring_basic` — N submissions with NORMAL on unbounded pool, all run,
   count exact, no lost tasks.
2. `test_ring_priority_preempt` — REALTIME/HIGH tasks submitted after a burst
   of NORMAL still run before them (bitmap peek ordering).
3. `test_ring_cancel_by_id` — submit NORMAL ×3, cancel middle, exactly one
   task does not run; CANCELLED metric fired.
4. `test_ring_cancel_not_found` — cancel_by_id of never-submitted id → INVALID.
5. `test_ring_tombstone_skip` — cancel then shutdown; cancelled task never
   executes even though it was already in the ring.
6. `test_ring_bounded_full` — bounded pool, submit capacity+1 → ERR_INVALID.
7. `test_ring_unbounded_spill` — unbounded pool; submit 4096 gate-blocked
   NORMAL tasks to fill the ring, then a 4097th NORMAL task (spills to the
   NORMAL lane) → release gate → all 4097 execute exactly once, no loss, no
   reorder across the spill boundary beyond the documented approximation.
8. `test_ring_shutdown_drains` — tasks queued in ring complete before shutdown
   returns (existing drain semantics).
9. `test_ring_multithread_stress` — 4 producer threads × 25k NORMAL tasks,
   8 workers, exact total count.
10. `test_ring_cancel_data` — `loom_pool_cancel()` (data-keyed scan) on a
    NORMAL/ring task returns OK and the task never runs (mirrors existing
    test_cancel semantics on the new path).

**Gates (every task):** `cmake --build build_plan` (-Werror + clang-tidy +
clang-format) → run all three test binaries, zero failures → ASan/UBSan gate
(`build_asan`) → bench delta check.

## 8. Out of scope (YAGNI)

- **Full lock-free for ALL priorities** (per-priority MS-queues + hazard
  pointers): would reopen the CLOSED epoch-reclamation topic; non-NORMAL
  traffic is rare so the mutex cost is unmeasurable. Documented in §10 as
  future work.
- **Unbounded lock-free ring growth** (segmented rings): reclamation of empty
  segments re-introduces ABA; the 4096-ring + spill design avoids it.
- **Work-stealing / per-worker queues**: changes priority semantics.
- **128-bit CAS / cmpxchg16b**: not portable, not needed (Vyukov uses 64-bit
  seq CAS).
- No public API changes, no coroutine/metrics changes.

## 9. Risks & mitigations

| Risk | Mitigation |
|------|------------|
| Vyukov memory-ordering bugs | Release/acquire discipline exactly per Vyukov's algorithm; ASan+UBSan + valgrind on new tests; stress test with many producers/consumers |
| Ring full → spill ordering nuance (NORMAL split across ring+lane) | Within-priority FIFO becomes approximate only at the spill boundary; priority *classes* unaffected; document in header comment |
| cancel index ABA | task_id monotonic & unique per pool → structurally impossible |
| Semaphore portability | POSIX `sem_t` already required (project is POSIX-only per README) |
| `queue_len` atomicity | All mutators use `atomic_fetch_add/sub`; readers relaxed |

## 10. Future work (explicitly deferred)

- Full per-priority lock-free lanes (needs hazard-pointer/epoch reclamation —
  reopen design-decisions §11 CLOSED item deliberately).
- Ring growth for unbounded mode without spill.
- Lock-free cancel (current: index + tombstone, atomic but cancel itself may

---

## 11. Implementation notes (reconciled with shipped code, 2026-08-10)

The shipped implementation (`src/thread_pool.c`) follows this design with one
encoding variance worth recording:

- **Sequence-number encoding.** The design sketches the classic Vyukov
  formulation (`2n` empty / `2n+1` full). The implementation instead uses the
  **position-relative encoding** `seq == pos` (empty), `seq == pos + 1` (full,
  release store after `cancel_index_insert` between the plain task store and the
  publish), and the consumer releases with `seq = pos + ring_size`
  (`ring_try_dequeue`). Both encodings are correct Vyukov variants; the shipped
  one keys the sequence off the slot position rather than the ticket parity.
- **Cancel index placement.** `cancel_index_insert` runs **between** the task
  store and the release store of `seq` (thread_pool.c `ring_try_enqueue`), so a
  canceller racing the publish can still find and tombstone the task before it
  becomes visible to consumers. `ring_count` is incremented with the release
  store (relaxed `fetch_add` after publish).
- **Spill semantics.** On a full ring the enqueue returns `false` with **no slot
  claimed**; the submit path falls back to the NORMAL lane bucket
  (`spill_to_normal_lane`), preserving the capacity pre-check and the 60 s
  back-pressure behaviour for bounded pools.
  briefly spin).
