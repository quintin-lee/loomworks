# Concurrency Semantics Hardening — Design

**Date:** 2026-08-15
**Status:** Implemented — merged as `1cb941d..d6dcc63` (6 commits), verified 2026-08-15; this document records the design retroactively
**Library:** loomworks (C11 thread pool + coroutine library)
**Base:** `c7ab21f` — `chore: 🏷️ rework -n/--dry-run into a pure action-list preview (no execution)`
**Scope:** full-project review → 7 prioritized recommendations → 5 implementation stages + final validation

## 1. Motivation

A full analysis of `loomworks` (thread pool, coroutine runtime, pipeline, task
groups, metrics — all green: zero-warning build, 3 test suites passing)
surfaced a risk list. Five items were behavioral defects or latent UB; two were
build/documentation drift. Each was scoped to a single commit, one per stage.

| ID | Severity | Issue | Stage |
|----|----------|-------|-------|
| P0-1 | P0 | `task_group_wait()` calls `loom_pool_shutdown()` — drains the **entire** pool, not just the group; pool unusable afterwards; `data == NULL` tasks untracked and uncancellable | 1 |
| P0-2 | P0 | Pipeline `take()` polls on a 100 ms sleep; `destroy()` **leaks queued payloads** | 2 |
| P1-3 | P1 | Coroutine cross-thread `resume()`/`terminate()` is undefined (ucontext state is thread-bound) | 3 |
| P1-4 | P1 | Context switching hard-coded to ucontext(3); no seam for alternate backends | 4 |
| P2-5 | P2 | CMakeLists duplicates the clang-format block (lines 29 & 115 in the pre-merge file) | 5 |
| P2-6 | P2 | README's `worker_count == 0` wording (`hardware_concurrency * 2, clamped to 128`) is ambiguous versus `min(n, 64) * 2` in code | 5 |
| P2-7 | P2 | Perf regression gate in CI | 5 (no change — already present) |

## 2. Approach and guiding constraints

- **Treat the pool as read-only.** `thread_pool.c` is the performance core
  (256-priority lanes, lock-free Vyukov ring, ABA-tagged node pool,
  open-addressing cancel index, counting `work_sem`). None of its internals
  change; all fixes live in the wrapper layers (`task_group`, `pipeline`,
  `coroutine`) and build/docs.
- **Minimal hot-path impact.** Wrapper runs (completion signalling, thread
  affinity checks, discard dispatch) happen on the worker after the pool lock
  is released, so the pool's own throughput and invariants are untouched.
- **Every stage lands green.** Each commit must build zero-warning and pass all
  three suites before the next stage starts.

## 3. Stage 1 — task_group: wait without shutdown; cancel by task_id

### 3.1 Problem

`loom_task_group_wait()` was a thin alias for `loom_pool_shutdown(pool)`:

- it drains **every** task in the pool, not just the group's;
- afterwards the pool is shut down and cannot be reused — the typical
  caller pattern (wait → reuse pool for the next batch) was impossible;
- tracking keyed on the `user_data` pointer, so `data == NULL` tasks were
  never tracked and could not be cancelled through the group.

### 3.2 Design

Replace the tracking/cancellation/completion pipeline:

```
submit(fn, data)
   │
   ├─ ctx = { group, fn, data }
   ├─ node = { task_id, ctx, next }
   ├─ loom_pool_submit(pool, wrapper, ctx, &task_id)   // free_data = false
   ├─ OK: append node; node_count++; pending++    (success ⇒ always tracked)
   └─ fail: free ctx + node, return error

wrapper(ctx)   [runs on a worker, pool lock already released]
   ├─ ctx->fn(ctx->data)
   ├─ lock(group); pending--; pending == 0 ⇒ broadcast(done_cond); unlock
   └─ free(ctx)                                   (wrapper self-frees)
```

- **New group fields:** `pthread_cond_t done_cond`, `uint32_t pending`.
- **Node now keys on `task_id`** (from `loom_pool_submit`), so `data == NULL`
  tasks are tracked like any other and are cancellable.
- **`cancel()`** iterates nodes and calls `loom_pool_cancel_by_id(pool, id)`.
  The pool's `cancel_index_claim` runs under `pool->lock` — `OK` is a hard
  guarantee the task will never run:
  - `OK` → wrapper will not execute → cancel frees `ctx`, decrements `pending`,
    unlinks the node, frees it;
  - `ERR_INVALID` → task is running or already done → wrapper is the sole owner
    of `ctx` (it self-frees); cancel only unlinks+frees the node.
  These are mutually exclusive by construction: no double-free, no leak.
- **`wait()`** → `while (pending > 0) pthread_cond_wait(&done_cond, &lock)`.
  Does **not** touch `pool_shutdown`; the pool stays fully usable afterwards.
  The waiting wrapper always signals (completion or cancellation), so the wait
  is lossless even when tasks are cancelled mid-flight.
- **`destroy()`** → cancel everything tracked, then wait for `pending == 0`
  (in-flight wrappers finish before the group is freed), then destroy the
  condvar/mutex and free. This closes the UAF window where a wrapper could
  reference a freed group; `destroy` semantics become *"cancel queued, wait for
  running, then release"*.
- **`pending_count()`** now returns `pending` (submitted-but-unfinished), the
  count users actually asserted.

### 3.3 Behavior changes (documented in `task_group.h`)

- `wait()` no longer shuts down the backing pool — README + header updated.
- `data == NULL` tasks are tracked and cancellable.
- Test updates: five tests called `pool_destroy` right after `wait` where the
  old `wait` had shut the pool down implicitly; each gained an explicit
  `loom_pool_shutdown(pool)` before `pool_destroy`. `destroy_cancels` already
  had one.

## 4. Stage 2 — pipeline: blocking take(); no payload leak on destroy

### 4.1 Problem

- `loom_pc_take()` on an empty queue slept in 100 ms slices
  (`pthread_cond_timedwait`, CLOCK_MONOTONIC) — added latency, CPU wakeups,
  and a comment admitting a leaked producer could block consumers forever
  after shutdown races.
- `loom_pc_destroy()` drained residual nodes but only `free`d the node —
  the queued **payload** pointer was leaked (source comment: *"leaked here —
  acceptable for a demo"*).

### 4.2 Design

- **`take()`** switches to an unbounded `pthread_cond_wait`:
  `while (head == NULL) { if (shutdown) return ERR_SHUTDOWN; wait; }`.
  The producer's `submit()` already broadcasts on append and `shutdown()`
  broadcasts once; so a blocked consumer is always woken, and the only exit
  paths are a successful take or a shutdown. The 100 ms polling comment and
  the `loom_pool_broadcast(pc->pool)` in `shutdown()` (whose only purpose was
  to cut the poll latency) are removed.
- **Discard callback:** `loom_pc_set_discard_handler(pc, fn, ctx)` — new API.
  A non-`NULL` handler is invoked with the payload wherever a payload would
  otherwise be dropped without user code seeing it:
  - internal consumer worker: `on_discard(item, discard_ctx)` instead of
    `free(item)`;
  - `destroy()` drain loop: `on_discard(node->data, discard_ctx)` per residual
    node, then free the node.
  NULL handler ⇒ previous behavior (plain `free`) — fully backward compatible,
  all existing pipeline tests unchanged.
- Sequencing note: the handler is not synchronized against concurrent
  `take()`/`submit()`; docs require installing it immediately after
  `create()`, before the pipeline is shared.

## 5. Stage 3 — coroutine: reject cross-thread resume/terminate

### 5.1 Problem

ucontext state is threaded through each coroutine's own stack; swapping it
from a foreign thread is undefined. `_Thread_local` scheduler state makes the
failure mode a silent corruption.

### 5.2 Design

- `struct loom_coroutine` gains `pthread_t owner`, captured in `create()` as
  `pthread_self()`.
- `resume()` and `terminate()` check
  `pthread_equal(c->owner, pthread_self())` right after the NULL check and
  return `LOOMWORKS_CORO_ERR_INVALID` on mismatch — the existing error code is
  reused; no new API.
- **Not guarded:** `yield()`/`suspend()` (only reachable inside the owner
  thread's execution, `g_current` is thread-local) and `destroy()` (the global
  stack pool has its own lock; cross-thread destroy is allowed **only** for
  DONE/ERROR coroutines, documented in the header).
- Docs thread-affinity contract added to `coroutine.h`; tests added for both
  the resume and terminate guard paths (+16 assertions).

## 6. Stage 4 — coroutine: pluggable context backend

### 6.1 Problem

`coroutine.c` called `getcontext`/`makecontext`/`swapcontext` directly —
fine for POSIX (glibc retains ucontext despite long-standing deprecation) but
a hard coupling if a future backend (e.g. Windows fibers, AssemblyScript-style
stack via `ValgrindStack`, or a from-scratch `asm` switcher) is wanted.

### 6.2 Design

- New header `src/coro_ctx.h` defines an inline-only backend seam, defaulting
  to ucontext:

  ```c
  typedef ucontext_t loom_coro_ctx_t;
  static inline int loom_coro_ctx_get(loom_coro_ctx_t *ctx);
  static inline void loom_coro_ctx_set_stack(loom_coro_ctx_t *, void *sp, size_t size);
  static inline void loom_coro_ctx_set_link(loom_coro_ctx_t *, loom_coro_ctx_t *target);
  static inline int loom_coro_ctx_has_link(const loom_coro_ctx_t *);
  static inline void loom_coro_ctx_make(loom_coro_ctx_t *, void (*fn)(void *), void *arg);
  static inline int loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to);
  static inline int loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx);
  ```

- `coroutine.c` semantic replacements (all 7 ucontext call sites):
  `g_scheduler` → `loom_coro_ctx_t`; scheduler init → `get`/`set_stack`/
  `set_link`; coro entry → `has_link`/`swap_to_link`; resume NEW branch →
  `get`/`set_stack`/`set_link`/`make`; resume/yield/terminate swaps →
  `loom_coro_ctx_swap`.
- `coroutine_internal.h`: `ucontext_t ctx` → `loom_coro_ctx_t ctx`, includes
  `coro_ctx.h`. **Zero behavior change** — a different backend is a
  drop-in replacement of the inline implementations (plus a stack allocator
  if the backend needs different stack layout).

## 7. Stage 5 — build & docs polish

- **CMakeLists:** the clang-format block existed twice (option + GLOB + lint
  target at lines ~28–48 and a second GLOB + `format-check` + five
  `add_dependencies` at lines ~110–138). Merged into one block, placed after
  the test targets, guarded by `ENABLE_CLANG_FORMAT AND CLANG_FORMAT_BIN`
  (the duplicate block re-ran the GLOB and used `${CLANG_FORMAT_BIN}` without
  a found-check — a latent configure hazard if the binary were missing).
- **README:** `worker_count == 0` described as
  `hardware_concurrency * 2, clamped to 128`. Code is
  `n = sysconf(_SC_NPROCESSORS_ONLN)`; clamp `n` to `[1, 64]`; then `n * 2` —
  i.e. `min(n, 64) * 2` (2–128 workers). README now states the formula
  exactly. The task-group row (`group_wait()` drains the backing pool) was
  corrected to match the new Stage-1 semantics.
- **CI:** `.github/workflows/perf.yml` already exists (worktree baseline +
  `tools/bench_compare.py` 15 % regression gate + artifact upload) — item
  P2-7 needed no change and was marked resolved.

## 8. Data flow and thread safety

| Stage | Shared state | Synchronization | Invariants |
|-------|--------------|-----------------|------------|
| 1 | group nodes, `pending`, `done_cond` | `g->lock` + condvar | wrapper self-free vs cancel-free: mutually exclusive via `cancel_by_id` result; wait is lossless (broadcast on `pending == 0`); destroy blocks until zero in-flight wrappers |
| 2 | pc queue, `shutdown` | `pc->lock` + condvar + atomics (`submitted`, `taken`) | take: unbounded wait, only exit = data or `ERR_SHUTDOWN`; handler installed pre-share; no free/use-after-free on drain |
| 3 | `owner` (immutable after create) | none needed (read-only check) | resume/terminate only from owner thread; yield/suspend implicitly owner-bound |
| 4 | inline backend wrappers | none (compile-time seam) | byte-for-byte behavior parity with direct ucontext calls |

Lock order across layers never forms a cycle: workers call wrappers *after*
releasing `pool->lock`, so `group->lock` / `pc->lock` are leaf locks with
respect to the pool.

## 9. Error handling and edge cases

- **Stage 1 cancel races:** `OK` vs `ERR_INVALID` from `cancel_by_id` is the
  single source of truth for `ctx` ownership; the wrapper path is the only
  other owner. Node still present after `ERR_INVALID` ⇒ wrapper decrements
  `pending` on completion; node already unlinked ⇒ nothing to do. No
  double-free, no lost wakeup.
- **Stage 1 destroy during flight:** destroy cancels tracked tasks, then waits
  for `pending == 0`; a running wrapper fully finishes (under `group->lock`)
  before the group is freed.
- **Stage 2 timing edge:** a consumer blocked in `take()` when `destroy()`
  runs is woken by the shutdown broadcast and returns `ERR_SHUTDOWN`.
- **Stage 3 rule completeness:** cross-thread `destroy` is only safe for
  DONE/ERROR coroutines — documented; guarding it was deliberately out of
  scope (pool lock already serializes, and a hard guard would break the
  documented pattern).

## 10. Testing and verification (run 2026-08-15)

Build, every stage, zero warnings under
`gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread
-I include src/{thread_pool,coroutine,pipeline,task_group,metrics}.c`.

| Suite | Before | After | Notes |
|-------|--------|-------|-------|
| test_thread_pool | 10442 passed | 10451 passed, 0 failed | count varies 10442–10462 with concurrent-test timing; pass criterion is 0 failed |
| test_coroutine   | 5587 passed  | 5603 passed (+16), 0 failed | new cross-thread guard assertions |
| test_integration | 68761 passed| 68761 passed, 0 failed | |

CMake Release configure + full build (clang-tidy and clang-format gates on)
plus `ctest`: **3/3 suites pass** (ThreadPool 16.05 s, Coroutine 0.02 s,
Integration 0.58 s).

## 11. Files touched

| File | Stage |
|------|-------|
| `src/task_group.c`, `include/loomworks/task_group.h`, `tests/test_thread_pool.c` | 1 |
| `src/pipeline.c`, `include/loomworks/pipeline.h` | 2 |
| `src/coroutine.c`, `src/coroutine_internal.h`, `include/loomworks/coroutine.h`, `tests/test_coroutine.c` | 3 |
| `src/coro_ctx.h` (new), `src/coroutine.c`, `src/coroutine_internal.h` | 4 |
| `CMakeLists.txt`, `README.md` | 5 |

## 12. Out of scope (YAGNI)

- Pool internals: no changes to `thread_pool.c` (cancel-publish/future
  resolution semantics on cancelled tasks remain as-is).
- `pthread` → C11 threads / Windows port — separate effort, backend seam
  (Stage 4) is the enabling first step.
- Guarding cross-thread `destroy()` with a hard error (documented as
  caller responsibility for DONE/ERROR states).
- `docs/architecture.md` / `docs/design-decisions.md` editorial passes
  (unchanged by this work).
- Item P2-7 (CI perf gate): resolved as *already present*, no commit needed.