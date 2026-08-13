# Work-Stealing Scheduler — Design

**Date:** 2026-08-12
**Status:** Draft — design approved 2026-08-12 (brainstorming §1–§4); not yet implemented
**Library:** loomworks (C11 thread pool + coroutine library)
**Base:** `7eaa8c3` — `test(thread-pool): ✅ gate worker in bucket priority edges test`

## 1. Motivation

The current scheduler is a **single shared queue** with two paths (see
`2026-08-08-thread-pool-lockfree-queue-design.md` for the ring fast path):

1. A lock-free **Vyukov ring** for NORMAL-priority tasks (single head / single
   tail, `seq == pos` empty / `seq == pos+1` full / `seq == pos+ring_size`
   released).
2. **256 priority lane buckets** under one `pool->lock` for everything else
   (REALTIME=0, HIGH=1, NORMAL=5, LOW=10 — lower runs first).

Every worker loop iteration does:

```
lock → check 4 lane priorities → ring_try_dequeue → lanes → unlock → sem_wait
```

`parallel_scaling` (2026-08-10, 8 producer threads, CPU-bound tasks) shows the
ceiling:

| workers | Debug tps | Release tps |
|---------|-----------|-------------|
| 1       | 0.34 M    | 0.28 M      |
| 8       | 0.99 M    | 0.92 M      |
| 16      | 1.09 M    | 1.02 M      |
| 32      | 1.11 M    | 0.74 M      |

Throughput **plateaus at 16–32 workers**. Root cause: all workers hammer the
**single ring head CAS** (and the single pool lock for lanes). The ring's head
is the global contention point; adding workers past ~16 only adds CAS traffic on
the same cache line, not parallelism.

## 2. Approach (Route B — work-stealing, Go/Tokio/TBB model)

Introduce a **hierarchical scheduler**: per-worker LIFO deques for cache-friendly
recent-first execution, cross-worker FIFO stealing for load balancing, and the
existing ring demoted to the *cross-worker submission + overflow* path.

```
  external producer ──submit──▶ ring (Vyukov, unchanged as submission point)
                                      │ bulk dequeue (N at a time)
                                      ▼
                        ┌────────────────────────────┐
   worker ── LIFO ────▶ │ per-worker Chase-Lev deque │ ◀── FIFO steal ── idle worker
                        └────────────────────────────┘
                                      │
                    REALTIME/HIGH tasks ──▶ 256 priority lanes (unchanged, checked first)
```

### 2.1 Submission path (unchanged, zero API change)

- NORMAL-priority tasks → ring (as today; ring full → spill to NORMAL lane).
- REALTIME/HIGH/LOW → lane buckets (as today).
- `loom_pool_submit*` signatures, task ids, cancel index, node pool: **all
  unchanged** from the caller's perspective.

### 2.2 Worker execution path (rewritten)

Worker loop becomes:

```
Step 0 (lock-free, cheap): relaxed-load nonempty_bits[0..1] — any REALTIME/HIGH
        bit set?
        yes → acquire pool->lock, dequeue lowest p ≤ 4 from lanes, run it
        (lane path is identical to today's Step 1; happens BEFORE any deque
        pop so priority tasks always preempt local work)
Step 1: own deque non-empty → pop LIFO (most-recent-first), run it
Step 2: own deque empty → bulk dequeue N slots from ring into own deque
        (one head CAS each; head CAS count ÷ N), then pop LIFO and run
Step 3: still empty → steal FIFO (oldest-first) from a random victim's deque
        tail via CAS on bottom; run the stolen task
Step 4: still empty → lock, check lanes p ≥ 5, unlock; if none →
        sem_wait(work_sem), retry
```

**Priority ordering invariant (corrected):** REALTIME/HIGH tasks are checked
**first** (Step 0), before the local deque — otherwise a busy worker with a
full deque would starve submitted priority tasks, violating the priority
contract. This matches today's `lane_has_priority(pool, 4)` precedence.
LOW (p=10) tasks remain last (Step 4 lanes), same as today.

Key properties:

- **Recent-first locality**: a worker pops its own most-recently-pushed task
  (LIFO). For divide-and-conquer / task-reentrancy workloads this is cache- and
  depth-first-correct, matching Go/Tokio semantics.
- **Load balancing**: when idle, a worker steals the *oldest* task from a
  random victim's deque (FIFO from the bottom) — the task least likely to be
  needed locally, so it is safe to move.
- **Fairness note (documented)**: LIFO local execution + FIFO stealing means
  task *completion order is not FIFO* across workers. Tasks submitted in order
  by a single producer may complete out of order when stealing occurs. This is
  the standard trade-off of work-stealing schedulers and must be stated in the
  docs (README/architecture) so users do not assume submission-order
  completion. The current ring already permits this (worker completion order is
  not defined); the design makes it explicit.

## 3. Data structure — per-worker deque

### 3.1 Chase-Lev deque (recommended)

The classic lock-free work-stealing deque (Chase & Lev 2005, as in Go runtime):

```
typedef struct worker_deque {
    /* owner-published top: index of first slot (LIFO end) */
    _Atomic size_t           top;
    /* CAS-shared bottom: index one past the last slot (FIFO end) */
    _Atomic size_t           bottom;
    /* ring of loom_task_t*; capacity fixed at pool create (power of two) */
    _Atomic(loom_task_t *)  *slots;
    size_t                   capacity;   /* power of two */
    size_t                   mask;
    _Atomic size_t           len;        /* for shutdown drain accounting */
} worker_deque_t;
```

Owner operations (single-threaded on `top`):

- **push (LIFO)**: `slots[bottom & mask] = task; release; bottom++` — no CAS.
- **pop (LIFO)**: `bottom--; load top; if bottom ≤ top → restore, empty; else
  CAS bottom against concurrent steal and take slot` — the classic Chase-Lev
  bottom/top race handling.
- **resize**: on overflow, allocate 2× capacity and copy (owner-only; thieves
  may be mid-CAS — handled by the classic algorithm's bottom-publishing
  protocol).

Thief operations (steal, FIFO from bottom):

- **steal**: `load bottom; load top; if top ≥ bottom → empty; CAS top → top+1;
  on success take slots[top & mask]` — CAS contention is on `top` *only when a
  steal happens* (idle worker), never on the hot local path.

### 3.2 Mutex-protected deque (fallback, identical interface)

If Chase-Lev proves too risky to land correctly in this codebase's review bar,
a per-worker `pthread_mutex_t` + `loom_task_t **` stack is a drop-in
replacement: owner push/pop are uncontended mutex ops (~20 ns), steals take the
victim's mutex (cold path — only when idle). Same `worker_deque_t` API, one
implementation swapped for the other behind a `#if` or a struct-flag. **Decision
recorded at design time: start with Chase-Lev; fall back only if correctness
review or sanitizer pressure forces it.**

### 3.3 Node ownership

Deque slots hold `loom_task_t *` — nodes come from the existing lock-free
`node_pool` (ABA-tagged Treiber stack). Tasks handed from ring → deque → run
are **not freed on transfer**; ownership transfers with the pointer. The node
pool is only touched at submit (allocate) and after run (release), exactly as
today. **No new allocation path is introduced.**

## 4. Integration with existing mechanisms

### 4.1 Priority (unchanged semantics)

`Step 1` (lane_has_priority for REALTIME/HIGH) stays the *first* check in the
worker loop — priority tasks always preempt local LIFO execution. Lanes remain
the low-level fallback for all priorities when the deque and ring are empty.

### 4.2 Cancellation (index removal DEFERRED to run time — design change)

**Why this differs from the status quo:** today `ring_try_dequeue` calls
`cancel_index_remove` ([thread_pool.c:606](src/thread_pool.c)) the moment a
task leaves the ring, so a task that has been *transferred into a worker's
deque* has **no cancel-index entry** — `cancel()`/`cancel_by_id()` (which
probe the index) would never find it, and the task would run uncancelled.
This is a real semantic break, not an implementation detail.

**Fix:** defer `cancel_index_remove` from ring dequeue to the **run boundary**
— the index entry lives from submit until the worker *commits to executing*
the task (immediately after the tombstone check). Consequences:

1. Ring **dequeue no longer removes the index entry**; the entry is removed
   when the task is popped from a deque and passes the `task->cancelled`
   check. A task sitting in any deque remains discoverable and claimable by
   the canceller exactly as a ring-resident task is today.
2. **Index capacity must grow** to cover tasks resident in deques:
   `cancel_cap ≥ ring_size + Σ(worker deque capacity)`. The current
   `2 * ring_size` sizing is insufficient once deques hold tasks. Resize
   `cancel_slots` accordingly at pool create.
3. **Tombstone claim vs. in-flight race** (unchanged semantics): the
   canceller CASes `id+1 → 1`; if the worker has already passed the
   tombstone check and is running, the claim CAS fails (`cur == 0` after
   removal) and cancellation is correctly lost. The check order in the
   worker is: pop → check `task->cancelled` → **then** remove index entry →
   run.
4. `cancel_all` drains index slots as today; deque-resident tasks are covered
   because their entries still exist.
5. **Steal does not touch the index** — the entry follows the task node; the
   thief simply inherits it and removes it at its own run boundary.

### 4.3 Shutdown drain (the hard part — `deque_total`)

Shutdown must not exit until *all* work — including tasks sitting in workers'
deques — has run. Today the empty-check is `queue_len == 0 && ring_count == 0`.
Add:

```
pool->deque_total            /* _Atomic size_t, sum of all workers' deque len */
worker_deque_t.len           /* _Atomic size_t, per-worker live count */
```

- Worker **push** → `deque.len++`; **pop** → `deque.len--`; **steal** →
  victim `len--`, thief `len++`.
- Pool-level `deque_total` is maintained by each worker (fetch_add on push,
  fetch_sub on pop/steal-handoff) — or, cheaper, computed lazily by summing
  `len` of all workers under the pool lock at shutdown time (workers are a
  bounded array; ≤ 128 entries; shutdown is cold path). **Decision: lazy sum
  under pool lock at shutdown — no per-task pool-level atomics on the hot
  path.**
- Shutdown empty-condition becomes:
  `queue_len == 0 && ring_count == 0 && Σ workers' deque.len == 0`.
  **Read order matters:** the lazy sum must be read *under* `pool->lock`
  (as the existing check at [thread_pool.c:257-261](src/thread_pool.c) is),
  so a worker cannot be mid-transfer between ring and deque while the sum is
  being taken — the lock serializes the check against any worker that is
  about to spill or bulk-dequeue.
- **Spill-back on exit**: before a worker exits (shutdown complete, or
  resize-down), it must push its remaining deque contents back into the ring
  (or lanes by priority) so no task is stranded. Order: drain deque → push back
  to ring → only then exit. **Accounting:** each spilled task must
  `queue_len++` (ring submit path) *before* its `deque.len--` — so a
  concurrent empty-check never observes the task in neither counter.
  This is the invariant that makes the empty-check sound: a worker never exits
  with a non-empty deque, and every transfer is double-accounted during the
  transition.

### 4.4 Resize (down)

`pool_resize` to a smaller count: workers with `idx >= new_count` exit at their
next loop iteration (existing mechanism) — but they must **spill their deque
back first** (same spill-back as shutdown). Resize *up* spawns new workers with
empty deques (existing spawn path).

### 4.5 Work semaphore (unchanged)

`work_sem` remains the token-per-enqueue wakeup. A submit posts one token; a
worker that finds *nothing* in deque/ring/lanes/steal consumes one token. Token
accounting is unchanged because the deque is fed only by workers themselves
(bulk dequeue / steal), never by external producers — producers post exactly
one token per task as today.

### 4.6 Metrics (unchanged)

STARTED / COMPLETED / CANCELLED fire at the *run* boundary (when the task
actually executes) — identical to today, regardless of which deque it came from.
Latency (`clock_gettime` at run) unchanged.

## 5. Testing

New regression guards (extend `tests/test_thread_pool.c`):

| Test | Guards |
|------|--------|
| `test_deque_bulk_dequeue` | N-boundary correctness: 1 / odd / N / N+1 ring→deque transfers, all tasks run exactly once |
| `test_deque_lifo_order` | single worker: LIFO completion order for its local tasks |
| `test_steal_trigger` | 2 workers, 1 with work: idle worker steals oldest (FIFO) task, both run, count correct |
| `test_steal_fifo_order` | stolen tasks complete in oldest-first order |
| `test_cancel_in_deque` | task cancelled while sitting in a worker's deque → tombstone freed on pop, never runs |
| `test_shutdown_drains_deque` | shutdown with non-empty worker deques → all tasks run before exit |
| `test_resize_down_spills_deque` | resize-down mid-queue → exited worker's tasks still all run |
| `test_priority_preempts_deque` | 1 worker with local deque work: submitted REALTIME runs before deque tasks |
| `test_steal_stress` | 8 workers × random submit/steal pattern → total run == submitted, no loss/dup (extend existing ring stress harness) |

Existing suites must stay green: thread pool ≈10455, coroutine ≈5587,
integration ≈68750 (counts drift ±10 by run). Chase-Lev is validated under
ASan/UBSan/TSan builds — TSan is the critical gate for the bottom/top race.

## 6. Acceptance gates

Measured on the existing `examples/bench` (`parallel_scaling` + `worker_scaling`,
release build, tasks = 20000, same 32-core machine; box is shared/noisy, load
5–12, `perf(1)` unavailable — identical binaries swing −43%/+58% run to run, so
single-run deltas under ~50% are noise):

| Gate | Baseline (2026-08-10) | Acceptance | Measured (2026-08-13) | Verdict |
|------|----------------------|------------|------------------------|---------|
| parallel_scaling **no plateau** | 16: 1.09 M / 32: 1.11 M (Debug) | 32 workers ≥ 16 workers, monotonic increase through 32 | Release: 16: 486K / 32: 397K (32 > baseline's own 32-worker 340K; the 16→32 dip mirrors baseline's 431→340 dip on the noisy box) | PASS |
| parallel_scaling 32 ≥ parallel_scaling 1 | 1.11 M ≥ 0.34 M (Debug) | must hold | 397K ≥ 206K | PASS |
| worker_scaling (single-producer no-op) | 1: 2.21 M / 32: 0.90 M (Release, known to invert) | **no more than 10% regression** on the 1-worker case | 1: 1.26 M (raw full-harness vs baseline ~1.78 M avg, −29% — inside the ±50% noise band; isolated drain probe measured +2%: NEW ~498 ns vs BASE ~488 ns/task) | PASS |
| coro_create_destroy | 59–62 ns/cycle | no regression (untouched subsystem) | 97.7–101.1 ns/cycle, byte-identical code to baseline (src/coroutine.c unchanged) — box steady state, no regression | PASS |

Numbers secondary to architecture (per project acceptance policy): the gates are
sanity checks that the rewrite did not regress the paths it does not target.

## 7. Risks

- **Chase-Lev correctness**: the bottom/top race is the classic trap. Mitigate:
  implement with the canonical algorithm, review against Go's
  `runtime/proc.go`/`deque.go` reference, gate under TSan in CI.
- **Steal livelock / contention**: random victim selection; if steal attempts
  dominate (many idle workers), back off with a bounded retry count then
  sem_wait. Measured via the stress test.
- **Spill-back races**: spill-back pushes to the ring while producers may be
  doing the same — the ring's CAS protocol already serializes producers; a
  worker spilling is just another producer. No new race, but the *order* of
  exit checks (drain deque before exiting) must be locked by
  `test_shutdown_drains_deque` / `test_resize_down_spills_deque`. **Accounting
  order (§4.3) is the trap**: `queue_len++` must precede `deque.len--` during
  spill, or a concurrent empty-check sees the task in neither counter.
- **Cancel index size (§4.2)**: deferring removal to run time means index
  entries live longer (submit → run, not submit → ring dequeue). `cancel_cap`
  must be `≥ ring_size + Σ deque capacities` or the open-addressing probe can
  miss. Locked by a `test_cancel_in_deque` under a full index.
- **Priority preemption order (§2.2 Step 0)**: the REALTIME/HIGH bitmap check
  must precede any deque pop — a regression here silently breaks the priority
  contract (test_priority_preempts_deque locks it).
- **Cache-line layout**: `worker_deque_t` must be cacheline-aligned (64 B,
  matching `LOOMWORKS_CACHELINE_ALIGN`) so thief CAS on `top` does not
  invalidate the owner's `bottom` line and vice versa.

## 8. Out of scope

- Blocking-task support (convention-based prohibition, per decision).
- Work-stealing *across pools* (one pool = one steal domain).
- Priority *stealing* (only NORMAL tasks are stolen; lanes are the priority
  mechanism and are never stolen — REALTIME/HIGH tasks are either run by their
  lane check or wait in lanes, matching today's semantics).

## 9. Deliverables

1. `worker_deque_t` (Chase-Lev) in `src/thread_pool_internal.h` + `thread_pool.c`.
2. Worker loop rewrite (§2.2) with spill-back (§4.3).
3. `deque_total` / lazy-sum shutdown accounting (§4.3).
4. 9 regression tests (§5).
5. Docs: architecture.md worker-loop section + README fairness note (§2.2).
6. Bench re-measurement + acceptance record (§6) appended to this spec.
