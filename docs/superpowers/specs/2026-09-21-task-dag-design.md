# Task DAG / Join-Graph API — Design

Date: 2026-09-21 (rev 2: 2026-10-18)
Status: approved for implementation
Baseline: master @ e3260fa (tree green: Debug + ASan + UBSan full suites, format gate)

Rev 2 settles semantics found ambiguous during implementation review and
makes the release-bookkeeping invariants explicit:

- **D6 — release-failure futures**: a node whose release fails *after* its
  embedded future was initialized keeps that future — the release path
  completes it with the release error, so `loom_dag_node_future()` returns
  it and `loom_dag_node_status()` reads the error after drain (§4.2, §4.4).
- **D7 — destroy return**: `loom_dag_destroy()` returns `LOOMWORKS_OK` on a
  successful teardown (NULL no-op included); the run outcome is observed via
  `wait`/`wait_timeout`, never through `destroy` (§3, §4.4).
- **Release bookkeeping (§4.2)**: every release increments `in_flight`
  under `done_mu` before its pool submit; every release outcome (success or
  failure) bumps `released_total` and check-signals `drained` under
  `done_mu`.  Missing either side is a hang or a counter underflow.

## 1. Goal

A user-facing dependency-aware task layer on top of the existing thread pool:
users build a directed acyclic graph (DAG) of task nodes; a node runs exactly
once, when all its predecessors have completed. The layer is a client of
`loom_thread_pool_t` — it introduces no new threads and no polling.

## 2. Scope (v1, agreed)

| Decision | Choice |
|---|---|
| Feature depth | **Minimal join-DAG**: no cancel propagation, no cascade skip, no priority inheritance, no stage barriers |
| Failure policy | **Report-only**: a node's task returns `loom_result_t`; dependents run regardless; users inspect per-node futures / status |
| Task functions | **Thread tasks only** (`loom_result_t (*)(void *)`); coroutine variants deferred to a later spec |
| Execution model | **Engine-driven release**: a node's task is submitted only when all predecessors are done; no thread ever blocks waiting on a predecessor |
| Release mechanism | **Inline release in the task wrapper** (chosen over per-DAG scheduler thread and per-completion advance tasks) |
| Node surface | **Per-node futures** plus a status accessor |
| DAG lifetime | **Single-use**: build → `submit` → `wait` → `destroy`. No resubmit. |

Non-goals: cyclic graphs, dynamic edge addition after submit, mixed
thread/coroutine nodes, distributed / multi-pool graphs.

## 3. Public API

New header `include/loomworks/dag.h` (pulled into `loomworks.h`):

```c
typedef loom_result_t (*loom_dag_task_fn)(void *user_data);
typedef struct loom_dag_node loom_dag_node_t; /* opaque */
typedef struct loom_dag      loom_dag_t;      /* opaque */

loom_result_t loom_dag_create(loom_thread_pool_t *pool,
                              uint32_t node_capacity, loom_dag_t **out);
loom_result_t loom_dag_destroy(loom_dag_t *dag); /* joins in-flight; NULL-safe; OK on teardown (D7) */

loom_result_t loom_dag_add_node(loom_dag_t *dag, uint32_t node_id,
                                loom_dag_task_fn fn, void *user_data,
                                uint8_t priority, loom_dag_node_t **out_node);
loom_result_t loom_dag_add_edge(loom_dag_t *dag, uint32_t from, uint32_t to);

loom_result_t loom_dag_submit(loom_dag_t *dag);
loom_result_t loom_dag_wait(loom_dag_t *dag);
loom_result_t loom_dag_wait_timeout(loom_dag_t *dag, const struct timespec *deadline); /* monotonic */
loom_future_t *loom_dag_node_future(loom_dag_node_t *node);
loom_result_t  loom_dag_node_status(loom_dag_node_t *node);

/* `priority` is a `loom_task_priority_t` value cast to uint8_t:
 * LOOMWORKS_PRIORITY_REALTIME=0 / HIGH=1 / NORMAL=5 / LOW=10. */

Contracts:

- `node_id` is a user-chosen index in `[0, node_capacity)` fixed at create;
  edges reference ids. `add_node` on a duplicate id → `LOOMWORKS_ERR_INVALID`.
  `priority` is a `loom_task_priority_t` value (uint8_t, lower = runs first).
- `add_edge(from, to)` means "`to` runs after `from`". Unknown ids,
  self-edges, and duplicate edges → `LOOMWORKS_ERR_INVALID` at add time.
  **Acyclicity is checked lazily at `submit`** (Kahn topological pass);
  a cycle makes `submit` return `LOOMWORKS_ERR_INVALID` and the DAG remains
  usable for further inspection but never re-submittable.
- `add_node` / `add_edge` after `submit` → `LOOMWORKS_ERR_INVALID`.
- `submit` releases every zero-in-degree node (roots) to the pool.
  Empty DAG (0 nodes): `submit` succeeds immediately (no nodes to
  release), so `wait` returns `LOOMWORKS_OK` right after `submit`.
- Invariant: `wait` succeeds only after a **successful** `submit`.
  `wait` on a DAG whose `submit` was never successful (never called,
  or cycle-rejected) returns `LOOMWORKS_ERR_INVALID` immediately —
  it never blocks on unreleased work.
- `wait` blocks until the graph has fully released: when it returns
  `LOOMWORKS_OK`, every node's future is ready and
  `loom_dag_node_status()` is readable.  A DAG whose release path
  recorded a failure (cycle-rejected, or a pool submit failure
  mid-graph — §4.4) returns the recorded `fail_error` after the
  started runners drain.  `loom_dag_wait_timeout(dag, deadline)`
  returns `LOOMWORKS_ERR_TIMEOUT` when the monotonic deadline expires
  with `drained` unset; the DAG remains usable.
- Node futures are owned by the DAG. `loom_future_wait` on a node's
  future is always safe once that node has completed; before completion
  it blocks until the DAG's `wait`/`destroy` drain finishes. The DAG
  alone calls `loom_future_destroy` on its node futures (inside
  `loom_dag_destroy`) — callers must not destroy them themselves.
  A node whose release failed after its embedded future was created has
  that future completed with the release error (D6), so waiting on it
  returns once the drain finishes; only a node whose release attempt
  failed *before* future creation has `loom_dag_node_future() == NULL`.
- `loom_dag_node_status` returns `LOOMWORKS_ERR_INVALID` while the
  node's future is not yet ready; otherwise it returns the node's task
  return value.
- `destroy` blocks until every started runner has drained (the same join
  as `wait`), frees all resources, and returns `LOOMWORKS_OK`; a NULL
  handle is a no-op returning `LOOMWORKS_OK` (D7).  The run outcome is
  **not** propagated through `destroy` — observe it via
  `wait`/`wait_timeout` beforehand.  Calling `destroy` twice, or using
  the handle after it, is user error (the handle is freed).
- The pool must outlive the DAG: it must not be shut down or destroyed
  before `loom_dag_destroy`. If it is, `loom_dag_wait` may block
  indefinitely (no wrapper signals the final in-flight release) and
  node futures may never become ready. Passing NULLs anywhere →
  `LOOMWORKS_ERR_INVALID`.

## 4. Internal Architecture

New TU `src/dag.c` (+ `src/dag_internal.h`). Pool core changes: the
§4.3 hooks only (submit hook + embedded-future helpers).

### 4.1 Structures

```c
struct loom_dag_node {
    uint32_t       node_id;
    loom_dag_t      *dag;        /* back-pointer for the runner (R2) */
    loom_dag_task_fn fn;
    void          *user_data;
    int            priority;
    loom_future_t  future;
    loom_result_t  status;         /* fn's return value; set before future complete */
    uint32_t       in_degree;   /* build-phase in-degree (under d->lock); stamped to pending_preds at submit */
    atomic_uint32_t pending_preds; /* in-degree remaining; 0 => releasable */
    atomic_bool     future_owner; /* embedded future initialised; node_future() exposes it (D6) */
    loom_dag_node_t **succs;     /* dynamic array of successor node ptrs, grown on add_edge */
    uint32_t       succs_cap, succs_len;
};

struct loom_dag {
    loom_thread_pool_t *pool;
    loom_dag_node_t    *nodes;       /* [node_capacity]; NULL until add_node */
    uint32_t           node_capacity;
    pthread_mutex_t    lock;         /* build phase + one-shot submit only */
    atomic_bool        submitted;    /* frozen after a successful submit OR a cycle (R6) */
    bool               destroyed;
    atomic_bool        failed;       /* D3: a release failed (or cycle at submit) */
    loom_result_t      fail_error;   /* written before failed=true (release order) */
    atomic_uint32_t    in_flight;    /* started runners not yet drained (D4) */
    atomic_uint32_t    released_total; /* successful releases (D5) */
    atomic_uint32_t    added_count;  /* added nodes; stamped at submit (D5) */
    pthread_cond_t    done_cv;       /* wait() sleep; CLOCK_MONOTONIC attr; guarded by done_mu */
    pthread_mutex_t   done_mu;
};

**Completion predicate (D5).** `in_flight == 0` alone is *not* completion:
with independent roots, `in_flight` can transiently hit 0 while `submit`
is still releasing later roots (or a runner finished ahead of the
increment), so a "stamp `all_done` on decrement-to-0" design returns
`wait` early.  Instead, drainedness is a predicate evaluated under
`done_mu`:

    drained(d) ⇔ d->in_flight == 0 &&
                  (d->failed || d->released_total == d->added_count)

Monotone: `in_flight` never rises again once `released_total ==
added_count` (no further releases are possible), and `failed` is
sticky.  Every event that can flip `drained` from false to true runs
its `in_flight` mutation **inside** `done_mu` and signals `done_cv`
when the predicate is true, so `wait`/`destroy` can never miss the
transition, and a joiner observing `drained` under `done_mu` knows no
runner or releaser will touch the DAG again (safe to free).
```

### 4.2 Release path (hot path, lock-free)

1. `submit` (under `lock`, one-shot): Kahn pass over the built graph.
   Stamps `pending_preds`, detects cycles, stamps `added_count`.
   On cycle: set `failed` + `fail_error = LOOMWORKS_ERR_INVALID` +
   signal-if-drained under `done_mu` (freeze; §4.4).  On success,
   for each root: `loom_dag_release_node(dag, node)` → submit the
   node's wrapper task to the pool at the node's priority; stop the
   loop when `failed` is set.  Empty added-node set: `drained` is
   statically true, so `wait` returns `LOOMWORKS_OK` without
   blocking.
2. **Wrapper** (runs on a pool worker):
   ```c
   status = node->fn(node->user_data);
   node->status = status;
   loom_future_complete(node->future, status);   /* D1: new internal helper */
   for each succ in node->succs:
       if (atomic_load(&dag->failed)) break;     /* D3: stop releasing */
       if (atomic_fetch_sub(&succ->pending_preds) == 1)
           loom_dag_release_node(dag, succ);   /* submits succ's wrapper */
   lock(dag->done_mu);
   atomic_fetch_sub(&dag->in_flight);           /* D4: started runners only */
   if (drained(dag)) pthread_cond_signal(&dag->done_cv);   /* D5 */
   unlock(dag->done_mu);
   ```
   Exactly one queue round-trip per edge, one `atomic_fetch_sub` per edge.
   No new threads, no polling, no recursion (release depth is unbounded
   because it is carried by the pool's queues, not the call stack — a
   1000-deep chain costs O(depth) *queued* wrappers, not stack frames).

   **Release bookkeeping (D4/D5).** `loom_dag_release_node` performs its
   `in_flight` increment **under `done_mu`, before the pool submit**, on
   every release (roots from `submit` included — the root loop must not
   double-count).  Success path: after the submit returns, bump
   `released_total`, then — still under `done_mu` — evaluate `drained`
   and signal `done_cv` when it holds (`released_total` reaching
   `added_count` can be the event that completes a drained DAG when the
   runner finished first).  Failure path: undo the `in_flight`
   increment, record the failure (below), bump `released_total`, then
   check and signal.  Both paths therefore signal; a missing signal is a
   hang, and a missing increment is a counter underflow that strands
   `wait` — these two invariants are the primary regression targets of
   §5.6.

   **Release-failure semantics (D3/D4/D5).**
   `loom_pool_submit_internal`
   is non-blocking (§4.3): a bounded-full queue returns
   `LOOMWORKS_ERR_INVALID` immediately.  `dag_release_node`'s failure
   path frees the ctx, records `fail_error`, sets `failed`, then runs
   the undo-decrement of `in_flight` (or, on a malloc failure, no
   decrement is owed) inside `done_mu` and signals when `drained`
   holds; already-started runners keep running and keep releasing
   *their* successors until `failed` short-circuits them —
   `in_flight` counts started runners
   only, so it still drains to 0 on a failed DAG.  A task that merely
   *returns* an error does not stop successors (§2 report-only);    only a
   *release* failure (pool hook error) or a cycle at `submit` sets
   `failed`.

   **Failed-release future (D6) and error recording.** The node's
   `loom_future_init` runs before the pool submit; when the submit
   itself fails, the embedded future already exists, so the failure
   path completes it with the submit error —
   `loom_future_complete(&node->future, rc)` — before recording
   `failed`; `loom_dag_node_future()` returns it and
   `loom_dag_node_status()` reports `rc` after drain.  When the release
   fails *before* future initialization (runner-ctx malloc failure, or
   a failing `loom_future_init`), no future exists for the node and
   `loom_dag_node_future()` stays NULL.  Either way the node's task
   function never runs.  The **first** release failure — recorded under
   `done_mu`, like the bookkeeping around it — wins `fail_error`; later
   failures keep the sticky `failed` flag as-is.
3. `dag_wait`: if `!submitted`, return `LOOMWORKS_ERR_INVALID`
   immediately (no block — §4.4 invariant).  Else lock `done_mu`;
   block on `done_cv` until `drained(dag)`; unlock; return
   `fail_error` if `failed`, else `LOOMWORKS_OK`.  `drained` is
   statically true for empty DAGs and cycle-rejected DAGs (no
   runner ever started), so `wait` never blocks on them.
   `loom_dag_wait_timeout` is the same loop with
   `pthread_cond_timedwait` on the monotonic `done_cv`; on expiry it
   returns `LOOMWORKS_ERR_TIMEOUT` without touching the DAG (which
   remains usable).
   **Pool shutdown mid-graph**: queued runners complete during the
   pool's shutdown drain; *new* releases fail with
   `LOOMWORKS_ERR_SHUTDOWN`, which sets `failed`/`fail_error`, so
   `wait` returns the pool error once `in_flight` drains via the
   running runners — no hang.  A mid-runner *worker crash* is the
   documented pool-outlives-DAG hazard (§3): the crashed runner's
   drain is lost, `in_flight` can strand, and `wait`/`destroy` may
   block indefinitely (mitigation: `loom_dag_wait_timeout`).

### 4.3 Pool hooks

`thread_pool_internal.h` gains four exported internal functions (not
public API):

```c
/* Submit without backpressure blocking: DAG capacity was bounded at
 * build time, so a blocking submit would let a user graph stall the
 * graph itself.  `free_data=false` — the DAG owns the runner ctx and
 * frees it in the runner (D2).  A bounded-full queue returns
 * LOOMWORKS_ERR_INVALID immediately (the enum has no queue-full code;
 * the pool rejects non-blocking oversubmits with ERR_INVALID). */
loom_result_t loom_pool_submit_internal(loom_thread_pool_t *pool, loom_task_fn fn,
                                        void *user, uint8_t priority);

/* Embedded-future lifecycle (R1): loom_dag_node_t embeds loom_future_t by
 * value; init needs the pool's static CLOCK_MONOTONIC condattr, so the
 * pool exports these.  complete() packs a loom_result_t into
 * future.result for loom_dag_node_status / loom_future_wait. */
loom_result_t loom_future_init(loom_future_t *fut);   /* ERR_ALLOC on mutex/cond init failure */
void loom_future_complete(loom_future_t *fut, loom_result_t status);
void loom_future_destroy_embedded(loom_future_t *fut);
```

Implementation: `loom_pool_submit_internal` is the existing `enqueue_task`
path with `free_data=false, block=false` — one code path, no duplicated
ring/lane logic.  The three future helpers factor out of the init blocks
duplicated in `loom_pool_submit_future` / `loom_pool_submit_future_priority`
(thread_pool.c:2045–2058, 2117–2130); those public entries keep their
inline init (zero behaviour change).

### 4.4 Error handling

| Situation | Result |
|---|---|
| NULL arg anywhere | `LOOMWORKS_ERR_INVALID` |
| id out of range / duplicate node / duplicate edge / self-edge at build | `LOOMWORKS_ERR_INVALID` |
| mutation after submit | `LOOMWORKS_ERR_INVALID` |
| cycle at submit | `LOOMWORKS_ERR_INVALID`; DAG frozen — `submitted` + `failed` set, `drained` statically true: still usable for inspection, never re-submittable |
| release submit failure mid-graph (bounded queue full → `LOOMWORKS_ERR_INVALID`; pool shutdown → `LOOMWORKS_ERR_SHUTDOWN`) | `failed` + first `fail_error` recorded; the failed node's initialized future is completed with the error (D6); successors of the failed release are not started; started runners keep draining `in_flight`; `wait` returns `fail_error` |
| `wait` / `wait_timeout` on a never-submitted DAG | `LOOMWORKS_ERR_INVALID` immediately (no block) |
| `destroy` (never submitted, drained, or failed-drained) | joins any started runners, plain free, NULL-safe; returns `LOOMWORKS_OK` (D7) |

Every syscall checked; every allocation path has a free path; NULL-safe
destroy — per repo conventions.

## 5. Testing

New `tests/test_dag.c` (hand-rolled `ASSERT` framework, suite conventions,
`#ifdef __VALGRIND__` scaling, `WAIT_UNTIL` for state polling). Registered
as `DagTests` in `ctest` (timeout 900s, like the other suites).

1. **Lifecycle**: create/add/submit/wait/destroy happy path; empty DAG
   no-op; destroy of never-submitted DAG; NULL-safety matrix.
2. **Topology**: diamond, deep chain (1000 nodes — regression for
   stack-depth independence), wide fan-in/fan-out, and a
   wide-then-narrow 50k-node stress (valgrind-scaled).
3. **Errors**: 2-cycle, 5-cycle, self-edge, unknown-id edge, duplicate
   edge, mutation-after-submit, cycle DAG stays frozen.
4. **Semantics**: report-only failure (failed parent → child still runs;
   `loom_dag_node_status` reports parent's error), per-node future status,
   priority respected (assert lane occupancy via
   `thread_pool_internal.h`, matching existing pool test patterns).
5. **Integration**: pool shutdown mid-graph (gate/park pattern),
   destroy with in-flight work, future completion order under ASan.
6. **Release failure**: bounded pool (small `queue_capacity`) + wide
   fan-out forces `dag_release_node` to hit `LOOMWORKS_ERR_INVALID`
   mid-graph; assert `wait` returns the recorded error; the failed
   node's `node_future` is non-NULL, its future completes, and
   `node_status` reads the release error (D6); never-attempted
   successors' `node_future` is NULL, started ones complete, `destroy`
   OK (regression guard for D3/D4/D6).
7. **`loom_dag_wait_timeout`**: gate-held DAG → `ERR_TIMEOUT` on
   expiry; then un-gate → `loom_dag_wait` → `LOOMWORKS_OK`.

## 6. Performance & Benchmark

- **Character**: push-only release. Idle graph (edges pending, no ready
  work) adds zero threads and zero polling. Hot-path cost per edge:
  one `atomic_fetch_sub` + a conditional submit. DAG mutex touches
  nothing on the hot path.
- New `bench_dag_throughput` in `examples/bench.c`: wide-diamond lattice
  of N nodes; measures submit→complete p50/p99 and throughput, plus the
  flat-N-independent-tasks baseline (upper bound).
- `perf.yml` regression gate: existing rules apply (fail on >60%
  throughput drop or p99 > 2×).

## 7. Deliverables

| File | Change |
|---|---|
| `include/loomworks/dag.h` | new public header |
| `include/loomworks/loomworks.h` | include `dag.h` |
| `src/dag.c`, `src/dag_internal.h` | engine + release path |
| `src/thread_pool.c`, `src/thread_pool_internal.h` | `loom_pool_submit_internal` + embedded-future helpers (§4.3) |
| `CMakeLists.txt` | `dag.c` into both library targets; `DagTests` ctest entry; bench target |
| `tests/test_dag.c` | new suite |
| `examples/bench.c` | `bench_dag_throughput` |
| `examples/task_dag_demo.c` | new demo: 5-node DAG on a 4-worker pool |
| `CHANGELOG.md` | Unreleased → Added: task DAG API |
| `docs/architecture.md` | DAG layer in the component diagram + data-flow section |
| `docs/design-decisions.md` | ADR-15: DAG release mechanism (inline wrapper vs scheduler thread vs advance task) |

Version stays **1.0.x** — purely additive, no breaking change.

## 8. Risks

- **R24 (new)**: in-flight DAG destroy vs pool shutdown racing on node
  futures — mitigated by the existing future lifetime contract
  (`loom_future_destroy` after pool drain) and the reclaim slot
  atomics shipped in d846a07; verified under ASan full suite.
- **R25 (new)**: unbounded graph sizes — `node_capacity` is explicit at
  create; edge arrays grow per node; the submit hook short-circuits
  backpressure by design (documented in §4.3). Stress test bounds it.
- **R26 (new)**: DAG runner release on a bounded pool — a blocking
  release would occupy the calling worker on `space_cond` until the
  whole pool stalls; with every worker running a DAG runner that is a
  self-deadlock.  Mitigated by D3 (non-blocking submit + `failed`
  short-circuit) and D4 (started-only `in_flight` still drains);
  regression test §5.6.
- **R27 (new)**: `in_flight` under-count after a partial release failure
  would let `loom_dag_destroy` free nodes whose runners still execute.
  Mitigated by the D4 invariant (`destroy` joins `all_done` before
  freeing) and §5.6.
