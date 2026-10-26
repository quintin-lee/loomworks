# R22 — Coroutine Completion Handle (`submit_coroutine_ex`) — Design

Date: 2026-10-27
Status: approved for implementation
Baseline: master @ `2b34ecb` (v1.0.2 released, CI 11/11 green, valgrind hard gate)

## 1. Objective

Close risk R22: `loom_pool_submit_coroutine` is fire-and-forget — there is
no completion handle or result code per task; failures surface only through
`LOOMWORKS_METRIC_FAILED`, and cancellation only through the metric. Add an
`_ex` variant that optionally produces a `loom_future_t` whose wait yields
the task's final outcome, at both the pool and runtime layers.

User-confirmed decisions:
- **API shape**: `_ex` variant with an optional future out-param; the
  existing `loom_pool_submit_coroutine` becomes a thin wrapper (NULL
  future) and keeps its exact current behavior.
- **Completion semantics**: three-state outcome — `LOOMWORKS_OK` (coroutine
  ran to DONE), `LOOMWORKS_ERR_FAILED` (coroutine errored), and
  `LOOMWORKS_ERR_CANCELLED` (cancelled before/during execution).
- **Surface**: pool layer + runtime layer (the runtime layer's existing
  coroutine path already routes through the pool and uses
  `rt->coro_stack_size`).

Non-goals: no change to the coroutine's `void` return contract (a "result
value" would require a coroutine API change; this design delivers the
*completion status* only); no retroactive changes to v1.0.2; no DAG
changes (the embedded-future pattern is reused, not modified); no new
metric values.

## 2. API

```c
/* thread_pool.h — added after the existing submit_coroutine declaration */
loom_result_t loom_pool_submit_coroutine_ex(
    loom_thread_pool_t *pool, loom_coro_fn fn, void *arg, size_t stack_size,
    loom_future_t **future,   /* optional; NULL = legacy fire-and-forget */
    uint64_t *task_id);

/* runtime.h — stack_size comes from rt->coro_stack_size, same as the
 * existing loom_runtime coroutine path */
loom_result_t loom_runtime_submit_coroutine_ex(
    loom_runtime_t *rt, loom_coro_fn fn, void *arg,
    loom_future_t **future, uint64_t *task_id);
```

`loom_result_t` gains `LOOMWORKS_ERR_FAILED`, appended **after**
`LOOMWORKS_ERR_CANCELLED` (append-only; no existing value changes, so the
ABI-safe numeric values are preserved).

Contract (documented on the new declarations):
- `future == NULL` → identical to `loom_pool_submit_coroutine` (metrics
  only, no future is allocated).
- `future != NULL` → `*future` is set to a pool-allocated future; the
  caller must `loom_future_wait()` then `loom_future_destroy()` it.
  `loom_future_wait` returns `LOOMWORKS_OK`, `LOOMWORKS_ERR_FAILED`, or
  `LOOMWORKS_ERR_CANCELLED`; `*result` is always `NULL` (coroutines return
  `void`).
- On submit failure the future out-param is set to `NULL` and no future
  is allocated (validation happens before allocation).

## 3. Lifecycle matrix (every exit path completes or marks the future)

| Path | Current behavior | `_ex` addition |
|---|---|---|
| Submitted; coroutine runs to DONE | `COMPLETED` metric | `loom_future_complete(fut, LOOMWORKS_OK)` |
| Coroutine errors (resume failure, state ERROR incl. guard-page recovery) — every branch that fires `FAILED` | `FAILED` metric | `complete(fut, LOOMWORKS_ERR_FAILED)` |
| Cancelled while queued (cancel index / cancel_by_id) | `CANCELLED` metric + task freed | `future_mark_cancelled(ctx->future)` — wait already returns `ERR_CANCELLED` |
| Cancelled while sleeping (timer-heap removal in `cancel_by_id`) | `mark_cancelled` + ready node | same mark path |
| Terminated during shutdown drain (resize-down `coro_terminate`, Step C0 tombstone) | `CANCELLED` metric | `mark_cancelled` |
| Submit fails (ALLOC / SHUTDOWN / INVALID) | error code returned | `*future = NULL`; nothing allocated |

Key mechanisms:
- The coroutine task node gains an internal `void *completion` field
  (`thread_pool_internal.h`, valid only while `is_coro`). It holds the
  future pointer from submit until the completing path runs, then is set
  to `NULL` to prevent double-completion.
- `loom_future_t` gains an internal `bool failed` field;
  `loom_future_complete` sets it when `status != LOOMWORKS_OK`;
  `loom_future_wait` checks `cancelled` first (returns
  `ERR_CANCELLED`), then `failed` (returns `ERR_FAILED`), then returns
  `OK` with `*result == NULL`.
- `loom_future_complete` gains a cancelled guard: under the future mutex,
  if `cancelled` is already set, it does not overwrite (return without
  touching fields). This closes the race between a concurrent cancel
  (which calls `mark_cancelled`) and the worker's completion call.
- Ownership: the pool allocates the future at submit; the task node holds
  it until a terminal path runs; the caller frees it via
  `loom_future_destroy` after `loom_future_wait` returns. Same ownership
  shape as `loom_pool_submit_future`.
- `loom_pool_submit_coroutine(pool, fn, arg, stack_size, task_id)`
  becomes `{ return loom_pool_submit_coroutine_ex(pool, fn, arg,
  stack_size, NULL, task_id); }` — zero behavior change (the wrapper
  skips all future code on `future == NULL`).
- Runtime layer: `loom_runtime_submit_coroutine_ex` forwards to the pool
  `_ex` with `rt->coro_stack_size`, mirroring the existing
  `loom_runtime_submit` coroutine routing.

## 4. Test plan (tests/test_thread_pool.c + tests/test_runtime.c)

| # | Case | Assertions |
|---|---|---|
| 1 | Normal completion | `_ex` with future → wait OK, result NULL, destroy OK |
| 2 | `future == NULL` legacy parity | no future allocated (out-param untouched/NULL), task_id matches, metrics fire |
| 3 | Coroutine error → FAILED | injected coroutine error → wait `ERR_FAILED` |
| 4 | Cancel before run | gate the sole worker → cancel → **release before wait** → wait `ERR_CANCELLED` |
| 5 | Cancel during sleep | coroutine `sleep_until` → `cancel_by_id` (timer-heap path) → release → wait `ERR_CANCELLED` |
| 6 | Shutdown drain cancellation | gate the worker → shutdown → wait `ERR_CANCELLED` |
| 7 | Cancel/complete race stress | N-iteration loop, cancel racing completion: wait ∈ {OK, CANCELLED}, no hang |
| 8 | Runtime layer | `loom_runtime_submit_coroutine_ex` mirrors #1 and #4 |
| 9 | Legacy regression | all existing `submit_coroutine` tests unchanged and green |

All new loops wrapped in `VGI()`; gate/park convention; single-CPU ×10
stability run before push (the 2026-10-22 lesson: assert final outcomes,
not intermediate scheduling states).

## 5. Risks / Mitigations

| Risk | Mitigation |
|---|---|
| `complete` overwrites a concurrently `mark_cancelled` future | cancelled guard inside `complete` under the future mutex |
| Enum append breaks exhaustive switches | no exhaustive switches over `loom_result_t` exist (verified by grep); clang-tidy covers new code |
| Missed completion on an error path hangs `wait` | every coroutine-destroy exit is paired with complete/mark; cases 3+6 would hang the suite deterministically if missed |
| ABI: enum append + internal struct fields | enum append-only; `loom_future_t` is opaque and the new fields live in internal headers; soversion unchanged |
| Runtime `stack_size` surprise | `_ex` uses `rt->coro_stack_size` exactly like the existing runtime coroutine path; documented |

## 6. Release placement

New public API → ships in **1.1.0** (minor bump, CHANGELOG Added). Not
backfilled into 1.0.2.

## 7. Spec self-review

- No placeholders: every lifecycle row names the mechanism
  (complete vs mark_cancelled); the enum position is fixed.
- Consistency: §3's cancel rows use mark_cancelled (wait returns
  ERR_CANCELLED) and §2's contract lists ERR_CANCELLED — same mechanism;
  the DONE/FAILED rows use complete, matching the three-state contract.
- Scope: two headers, one internal header, two source files, two test
  files — single-plan size.
- Ambiguity: "every branch that fires FAILED" is grounded in the two
  code sites (worker loop post-resume and Step C0); the plan will grep
  `LOOMWORKS_METRIC_FAILED` in the coroutine paths and pair each with a
  completion call.
