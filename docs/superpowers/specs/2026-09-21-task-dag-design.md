# Task DAG / Join-Graph API — Design

Date: 2026-09-21
Status: approved for implementation
Baseline: master @ e3260fa (tree green: Debug + ASan + UBSan full suites, format gate)

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
loom_result_t loom_dag_destroy(loom_dag_t *dag); /* joins in-flight nodes; NULL-safe */

loom_result_t loom_dag_add_node(loom_dag_t *dag, uint32_t node_id,
                                loom_dag_task_fn fn, void *user_data,
                                int priority, loom_dag_node_t **out_node);
loom_result_t loom_dag_add_edge(loom_dag_t *dag, uint32_t from, uint32_t to);

loom_result_t loom_dag_submit(loom_dag_t *dag);
loom_result_t loom_dag_wait(loom_dag_t *dag);
loom_future_t *loom_dag_node_future(loom_dag_node_t *node);
loom_result_t  loom_dag_node_status(loom_dag_node_t *node);
```

Contracts:

- `node_id` is a user-chosen index in `[0, node_capacity)` fixed at create;
  edges reference ids. `add_node` on an id used twice → `LOOMWORKS_ERR_INVALID`.
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
- `wait` blocks until every node's task has completed (or the pool is
  shut down / reclaimed, in which case `wait` returns the pool's error).
- `destroy` before `submit` is a plain free; `destroy` with in-flight work
  joins all pending node tasks first, then frees.
- Node futures are valid from `add_node` until `destroy` of the DAG.
  A node's future completes with its task's `loom_result_t`; a node that
  never ran because of a cycle (impossible after submit) or pool
  shutdown completes with `LOOMWORKS_ERR_SHUTDOWN` / `LOOMWORKS_ERR_CANCELLED`
  per existing pool semantics.
- The pool must outlive the DAG. Passing NULLs → `LOOMWORKS_ERR_INVALID`.
  A DAG created on a pool that is later shut down mid-graph is drained
  by the existing shutdown-drain / crash-reclaim machinery; `wait`
  reports the pool error.

## 4. Internal Architecture

New TU `src/dag.c` (+ `src/dag_internal.h`). Zero changes to pool core
except one internal hook.

### 4.1 Structures

```c
struct loom_dag_node {
    uint32_t       node_id;
    loom_dag_task_fn fn;
    void          *user_data;
    int            priority;
    loom_future_t  future;
    loom_result_t  status;         /* fn's return value; set before future complete */
    atomic_uint32_t pending_preds; /* in-degree remaining; 0 => releasable */
    atomic_bool     released;       /* wrapper submitted exactly once */
    loom_dag_node_t *succs;        /* dynamic array, grown on add_edge */
    uint32_t       succs_cap, succs_len;
};

struct loom_dag {
    loom_thread_pool_t *pool;
    loom_dag_node_t    *nodes;       /* [node_capacity]; NULL until add_node */
    uint32_t           node_capacity;
    pthread_mutex_t    lock;         /* build phase + one-shot submit only */
    bool               submitted, destroyed;
    atomic_uint32_t    in_flight;    /* nodes released, not yet complete */
    atomic_bool        all_done;
    pthread_cond_t    done_cv;       /* wait() sleep; guarded by done_mu */
    pthread_mutex_t   done_mu;
};
```

### 4.2 Release path (hot path, lock-free)

1. `submit` (under `lock`, one-shot): Kahn pass over the built graph.
   Stamps `pending_preds`, detects cycles. On success, for each root:
   `loom_dag_release_node(dag, node)` → submit the node's wrapper task
   to the pool at the node's priority, incrementing `in_flight`.
   A DAG whose release count is zero (empty, or all roots already
   accounted — only the empty case in v1) sets `all_done` immediately
   so `wait` returns without blocking.
2. **Wrapper** (runs on a pool worker):
   ```c
   status = node->fn(node->user_data);
   node->status = status;
   loom_future_complete(node->future, status);
   for each succ in node->succs:
       if (atomic_fetch_sub(&succ->pending_preds) == 1)
           loom_dag_release_node(dag, succ);   /* submits succ's wrapper */
   if (atomic_fetch_sub(&dag->in_flight) == 0)
       signal done_cv;                        /* last node: graph finished */
   ```
   Exactly one queue round-trip per edge, one `atomic_fetch_sub` per edge.
   No new threads, no polling, no recursion (release depth is unbounded
   because it is carried by the pool's queues, not the call stack — a
   1000-deep chain costs O(depth) *queued* wrappers, not stack frames).
3. `dag_wait`: lock `done_mu`; if `all_done` is already set, return
   `LOOMWORKS_OK` immediately; else block on `done_cv` until the last
   wrapper sets it. `all_done` starts **false** at create; `in_flight`
   is incremented as `submit` releases each root.
   **Before** a successful `submit` (never submitted, or cycle-rejected),
   `wait` returns `LOOMWORKS_ERR_INVALID` without touching the condvar —
   the §4.4 invariant. On pool shutdown mid-graph, the reclaim path
   signals `done_cv` so `wait` unblocks with the pool error rather than
   hanging.

### 4.3 Pool hook

`thread_pool_internal.h` gains one exported internal function (not public
API):

```c
/* Submit without backpressure blocking: DAG capacity was bounded at build
 * time, so a blocking submit would let a user graph stall the graph itself. */
loom_result_t loom_pool_submit_internal(loom_thread_pool_t *pool, loom_task_fn fn,
                                        void *user, int priority);
```
Implementation: existing submit path with the queue-full/block branch
short-circuited (bounded queue returns `LOOMWORKS_ERR_QUEUE_FULL`
immediately; unbounded always succeeds). This keeps one code path — no
duplicated ring/lane logic.

### 4.4 Error handling

| Situation | Result |
|---|---|
| NULL arg anywhere | `LOOMWORKS_ERR_INVALID` |
| id out of range / duplicate node / duplicate edge / self-edge at build | `LOOMWORKS_ERR_INVALID` |
| mutation after submit | `LOOMWORKS_ERR_INVALID` |
| cycle at submit | `LOOMWORKS_ERR_INVALID`; DAG marked unusable |
| `wait` before a successful `submit` | returns `LOOMWORKS_ERR_INVALID` immediately (no block): a cycle-rejected or never-submitted DAG has unreleased nodes; `all_done` is pre-set at create and only meaningful after `submit` |
| pool shut down mid-graph | futures complete `ERR_SHUTDOWN`; `wait` returns that; `destroy` joins whatever the pool drained |
| `destroy` never submitted | plain free, NULL-safe |
| `wait` on never-submitted / cycle-rejected DAG | returns `LOOMWORKS_ERR_INVALID` immediately (no block): `all_done` is pre-set at create, so the condvar branch sees it |

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
   edge, mutation-after-submit, cycle DAG stays unusable.
4. **Semantics**: report-only failure (failed parent → child still runs;
   `loom_dag_node_status` reports parent's error), per-node future status,
   priority respected (assert lane occupancy via
   `thread_pool_internal.h`, matching existing pool test patterns).
5. **Integration**: pool shutdown mid-graph (gate/park pattern),
   destroy with in-flight work, future completion order under ASan.

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
| `src/thread_pool.c`, `src/thread_pool_internal.h` | `loom_pool_submit_internal` hook |
| `CMakeLists.txt` | `dag.c` into both library targets; `DagTests` ctest entry; bench target |
| `tests/test_dag.c` | new suite |
| `examples/bench.c` | `bench_dag_throughput` |
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
