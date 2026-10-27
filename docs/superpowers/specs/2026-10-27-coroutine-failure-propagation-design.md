# Coroutine Failure Propagation (`loom_coro_fail`) — Design

Date: 2026-10-27
Status: approved for implementation
Baseline: master @ `e30d69d` (v1.1.0 + perf-gate stability in `[Unreleased]`;
CI/Performance green)

## 1. Objective

R22 closed the *handle* gap (`submit_coroutine_ex` returns a three-state
future: OK / ERR_FAILED / ERR_CANCELLED) but left `ERR_FAILED`
**unreachable from user code**: `loom_coro_fn` is `void (*)(void*)`,
`loom_coro_exit()` has no status outlet, and the only paths that complete a
future with `ERR_FAILED` are infrastructure errors (context/alloc/guard
page). The pool test suite therefore cannot deterministically exercise the
FAILED branch (documented in commit `6bac094`).

Add a deterministic in-coroutine failure primitive so user logic can report
failure and it propagates to:
- standalone callers: `loom_coro_resume` return + new
  `loom_coro_exit_status`
- pool callers: the existing future → `LOOMWORKS_ERR_FAILED`
  (no pool-side mapping change; the two worker failure sites already fire
  on `crc != OK && != RUNNING`)

User-confirmed decisions:
- **Status-only** (conservative): user's `loom_result_t err` payload is
  readable via `loom_coro_exit_status` on the handle; the future's
  `*result` stays NULL and the R22 three-state contract is untouched.
- `loom_coro_fail` carries a `loom_result_t err` payload (not a bool).

## 2. API (coroutine.h)

```c
/* appended to loom_coro_result_t (ABI: enum append) */
LOOMWORKS_CORO_ERR_FAILED,  /**< loom_coro_fail() was invoked. */

/**
 * @brief Signal failure from within a coroutine; does NOT return.
 *
 * Records @p err on the current coroutine and longjmps back to the
 * resumer (same mechanism as the guard-page longjmp).  After return,
 * the coroutine state is LOOMWORKS_CORO_ERROR.  No-op when called
 * outside a running coroutine (documented user error).
 *
 * @param err  Failure code to record (any LOOMWORKS_ERR_* value; the
 *             LOOMWORKS_* constants are plain ints that widen to uint32_t).
 */
void loom_coro_fail(uint32_t err);

/**
 * @brief Read a coroutine's exit status after it stops running.
 *
 * @return state DONE            -> LOOMWORKS_OK
 *         state ERROR + fail()  -> the recorded err
 *         state ERROR + infra   -> LOOMWORKS_ERR_FAILED (guard/alloc/context)
 *         otherwise            -> LOOMWORKS_ERR_INVALID
 */
uint32_t loom_coro_exit_status(loom_coroutine_t *coro);
```

`loom_coroutine_t` is already the public opaque type from `coroutine.h`;
both functions take/return it, no new public types.

**Type constraint (why `uint32_t`, not `loom_result_t`):** `coroutine.h`
is the *leaf* header — `thread_pool.h` includes it (thread_pool.h:22) for
`loom_coro_fn`, so it cannot in turn include `thread_pool.h` to pull in
`loom_result_t` (include cycle). Sibling headers that use `loom_result_t`
(pipeline/metrics/dag/task_group) all sit *above* thread_pool.h. Hence the
public signatures use `uint32_t` codes; call sites passing
`LOOMWORKS_ERR_*` constants convert implicitly, and `coroutine.c` (which
includes `thread_pool.h`) still names the enum in its implementation.

## 3. Mechanism

1. **Struct fields** (`coroutine_internal.h`): replace
   `uint64_t padding[2]` with
   `uint32_t exit_code; uint8_t exit_failed; uint64_t padding[1];`
   (one `uint64_t` of padding consumed; struct stays 64-byte-padded, size
   unchanged, ASan fiber field untouched).
2. **`loom_coro_fail(err)`** (`coroutine.c`, async-signal-safe like the
   guard handler): `c = g_current; if (!c) return; c->state =
   LOOMWORKS_CORO_ERROR; c->exit_code = (uint32_t)err; c->exit_failed =
   true; siglongjmp(g_guard_jmp, 1);`
3. **`loom_coro_resume`** longjmp arm: today returns
   `LOOMWORKS_CORO_ERR_GUARD` unconditionally; becomes
   `coro->exit_failed ? LOOMWORKS_CORO_ERR_FAILED : LOOMWORKS_CORO_ERR_GUARD`.
   The guard handler (coroutine.c:207-210) sets `state=ERROR` but not
   `exit_failed`, so infra errors keep the guard semantics.
4. **Pool** (`thread_pool.c:823` and `:1152`): `LOOMWORKS_CORO_ERR_FAILED`
   is `!= OK && != RUNNING` → falls into the existing
   `metrics_fire(METRIC_FAILED)` + `loom_future_complete(completion,
   LOOMWORKS_ERR_FAILED)` branches. **No pool edits.** Cancellation wins:
   the future-cancel guard (R22 task 1) prevents a late fail from
   overwriting `ERR_CANCELLED`.
5. **`loom_coro_exit_status`** (`coroutine.c`): per §2 table; reads
   `exit_failed`/`exit_code` (owner-thread convention same as resume).

## 4. Test plan (tests/test_coroutine.c + tests/test_thread_pool.c)

- standalone fail: `fail(LOOMWORKS_ERR_INVALID)` → resume returns
  `ERR_FAILED`, `exit_status` returns `LOOMWORKS_ERR_INVALID`.
- standalone DONE: `exit_status` returns `LOOMWORKS_OK`.
- guard-path infra ERROR: `exit_status` returns `LOOMWORKS_ERR_FAILED`
  (generic).
- `loom_coro_fail` outside a coroutine → no-op (resume unaffected).
- **pool FAILED branch (completes the R22 matrix)**:
  `submit_coroutine_ex` of a coroutine that calls `fail(LOOMWORKS_ERR_*)`
  → `loom_future_wait` returns `LOOMWORKS_ERR_FAILED`; replaces the
  "ERR_FAILED not deterministically testable" comment from `6bac094`.
- cancel-during-sleep then fail-after-wake → future stays `ERR_CANCELLED`
  (cancel guard precedence).

## 5. Risks / Mitigations

- **longjmp out of user code**: same class of operation as the guard
  handler and `loom_coro_exit` (natural return hops via uc_link); ASan
  fiber save/restore macros are on the same jmp path, so no new fake-stack
  handling. Under TSan coroutine tests are suppressed at infra level (R19,
  accepted) — mitigation is ASan/UBSan/valgrind, same as guard.
- **Enum/ABI**: append-only enum value; struct grows inside reserved
  padding; `loom_coroutine_t` opaque — no consumer break.
- **Calling `fail` mid-`sleep`**: legal; the timer thread does NOT resume —
  it publishes a ready-FIFO node for the owning worker (thread_pool.c:662-681);
  the worker's `loom_coro_resume` (thread_pool.c:799) is where the failing
  coroutine runs to `fail` and longjmps into that worker's `g_guard_jmp`
  setjmp, returning `ERR_FAILED` to the worker's existing failure branch
  (no re-push, task destroyed). Covered by tests 5/6 above.

## 6. Release placement

New public API → **1.2.0** (minor bump, same rule as R22 → 1.1.0).
CHANGELOG `[Unreleased]` gains `### Added` (failure propagation); risk
register R22 row gains a follow-up note; new R24 row: "coroutine failure
propagation via `loom_coro_fail` — longjmp out of user code; guard/infra
paths retain guard semantics" (Medium/Low, mitigated by §5).

## 7. Spec self-review

- No placeholders; every API symbol defined in §2 and used consistently in
  §3–§4.
- Pool mapping claimed as "no edits" — verified against
  `thread_pool.c:823/1152` (branch fires on any `crc` not OK/RUNNING).
- Scope: 4 source files (`coroutine.h`, `coroutine_internal.h`,
  `coroutine.c`, + 2 test files) + docs — single implementation plan.
- Ambiguity resolved: status-only (no future `*result` payload);
  `fail()` outside a coroutine is a no-op, not abort (repo rule: never
  `abort()`).
