# LoomWorks — Group Wait Timeout (R4 residual closure)

- **Date:** 2026-08-17
- **Scope:** Thread pool task groups (`loom_task_group_t`)
- **Driver:** "聚焦为线程池与协程功能, 如何完善该项目. 不要扩散, 使其达到生产级要求"
- **Status:** Approved (design reviewed section-by-section with the user)

## 1. Background

The 2026-08-17 hardening pass closed the group **self-wait** deadlock (a
worker of the group's own pool can no longer block on `group_wait()` /
`group_destroy()` — `loom_pool_current()` TLS guard, commit `e0377ae`).
The residual R4 risk documented in `docs/risk-assessment.md` is the
**unbounded external wait**: a caller thread that is *not* a pool worker
can still block on `loom_task_group_wait()` forever if the group's tasks
never drain (e.g. a worker crashed before a task ran, or a long-lived task
never completes). Maintenance priority 4 in the risk register names a wait
timeout parameter as the fix.

## 2. Scope

- **In scope:** a timed variant of `loom_task_group_wait()`.
- **Out of scope (user-confirmed):** `loom_task_group_destroy()` stays
  blocking — destroy is a *finish-and-release* operation with no
  partial-destruction semantics. Callers that need to bound the total
  lifecycle use the documented "wait-timeout-then-destroy" pattern:
  `wait_timeout(...)` returns `ERR_TIMEOUT` → the caller inspects /
  cancels the group → then destroys. No new polling/busy-wait API.

## 3. API design

```c
/* include/loomworks/task_group.h — new declaration next to wait() */
loom_result_t loom_task_group_wait_timeout(loom_task_group_t *group, const struct timespec *deadline);
```

Semantics:

- `deadline` is an **absolute CLOCK_MONOTONIC** upper bound — identical to
  `loom_future_wait_timeout()` (already documented as such in
  `thread_pool.h`). This keeps one consistent time base library-wide
  (decision 16, design-decisions.md).
- `deadline == NULL` means "wait forever" — the exact behaviour of the
  existing `loom_task_group_wait()`.
- Returns:
  - `LOOMWORKS_OK` — all tracked tasks finished before the deadline.
  - `LOOMWORKS_ERR_TIMEOUT` — the deadline passed while `pending > 0`.
  - `LOOMWORKS_ERR_INVALID` — `group == NULL`, or the self-wait guard hits
    (`loom_pool_current() == group->pool`).
- **Timeout leaves the group fully usable:** the pending counter and node
  list are untouched; the caller may re-wait, cancel, submit, or destroy.
- An already-expired deadline returns `ERR_TIMEOUT` immediately;
  `pthread_cond_timedwait` yields `ETIMEDOUT` without blocking when the
  absolute time is in the past, so no special-casing is needed.
- `loom_task_group_wait()` keeps its signature and behaviour; its body
  becomes a one-line delegate: `return loom_task_group_wait_timeout(group, NULL);`.

## 4. Implementation

File: `src/task_group.c`.

1. **Monotonic condattr (required, not optional).** `pthread_cond_timedwait`
   interprets its absolute deadline against the condvar's clock attribute.
   `done_cond` is currently created with `NULL` attr (REALTIME); passing a
   CLOCK_MONOTONIC deadline to a REALTIME condvar is a mixed-clock misuse
   (undefined behaviour). Follow the Task 7 pattern:
   ```c
   static pthread_once_t      g_done_condattr_once  = PTHREAD_ONCE_INIT;
   static pthread_condattr_t  g_done_condattr_mono;
   static void init_monotonic_done_condattr(void)
   {
       pthread_condattr_init(&g_done_condattr_mono);
       pthread_condattr_setclock(&g_done_condattr_mono, CLOCK_MONOTONIC);
   }
   ```
   and in group creation replace the `pthread_cond_init(&g->done_cond, NULL)`
   call with the `pthread_once` + monotonic attr variant.
2. **Wait loop** with a NULL/infinite vs timed branch:
   ```c
   pthread_mutex_lock(&group->lock);
   while (group->pending > 0) {
       if (deadline == NULL) {
           pthread_cond_wait(&group->done_cond, &group->lock);
       } else if (pthread_cond_timedwait(&group->done_cond, &group->lock, deadline)
                  == ETIMEDOUT) {
           pthread_mutex_unlock(&group->lock);
           return LOOMWORKS_ERR_TIMEOUT;
       }
   }
   pthread_mutex_unlock(&group->lock);
   return LOOMWORKS_OK;
   ```
   Spurious wakeups are absorbed by the `while` loop (unchanged).
3. **Guards** run before locking, same order as the Task 6 guard: NULL check,
   then `loom_pool_current() == group->pool` → `ERR_INVALID`.
4. Add `#include <errno.h>` (for `ETIMEDOUT`) — not currently included.
5. `loom_task_group_wait()` becomes a delegate to `wait_timeout(group, NULL)`.

## 5. Tests

All in `tests/test_thread_pool.c` (task-group section, after the
`#include "loomworks/task_group.h"` line, using the file's
`/* ---------- Test: ... ---------- */` separators):

1. `test_task_group_wait_timeout_expired_and_reusable` — submit a gated
   slow task; `wait_timeout(group, past_deadline)` →
   `LOOMWORKS_ERR_TIMEOUT`; then release the gate and `wait(group)` →
   `LOOMWORKS_OK` (proves timeout left the group usable).
2. `test_task_group_wait_timeout_ok` — submit a fast task;
   `wait_timeout(group, monotonic_now + 5s)` → `LOOMWORKS_OK`.
3. `test_task_group_wait_timeout_null_deadline` — `wait_timeout(group, NULL)`
   is equivalent to `wait(group)` (fast task, returns OK).
4. Self-wait guard: a worker of the group's own pool calling
   `wait_timeout` → `LOOMWORKS_ERR_INVALID` (reuse the
   `g_group_wait_rc` / `group_wait_from_worker_fn` pattern from Task 6).
5. `wait_timeout(NULL, deadline)` → `LOOMWORKS_ERR_INVALID`.

Baseline assertion counts must stay green with increases only
(ThreadPoolTests 12604 → expected +N).

## 6. Documentation

- `docs/api-reference.md` — §4 task group: document `loom_task_group_wait_timeout`
  (monotonic absolute deadline, `NULL` = infinite, timeout leaves the group
  usable), note `wait()` ≡ `wait_timeout(group, NULL)`.
- `docs/risk-assessment.md` — R4 row:
  `⚠️ partial (2026-08-17): self-wait ... unbounded external waits remain documented`
  → `✅ resolved (2026-08-17): timed group wait (wait_timeout, CLOCK_MONOTONIC);
  destroy remains blocking by contract`. Remove the maintenance-priority
  "wait timeout parameter" item (was item 4).
- `CHANGELOG.md` — Unreleased: brief entry for the timed group wait.
- `docs/design-decisions.md` — no new decision required; the monotonic
  clock rationale is decision 16, extended to the group condvar.

## 7. Acceptance criteria

- AC1. `loom_task_group_wait_timeout` exists with the section-3 signature;
  `wait()` delegates with identical observable behaviour (existing group
  tests unchanged and green).
- AC2. Expired deadline → immediate `LOOMWORKS_ERR_TIMEOUT`; group fully
  usable afterwards (re-wait succeeds).
- AC3. Future-far deadline and `NULL` deadline both return `LOOMWORKS_OK`
  when tasks complete.
- AC4. Self-wait guard and NULL-handle guard return
  `LOOMWORKS_ERR_INVALID` for the timed variant.
- AC5. `done_cond` uses a CLOCK_MONOTONIC condattr (no mixed-clock
  `pthread_cond_timedwait`).
- AC6. All four test suites green: ThreadPoolTests, CoroutineTests,
  IntegrationTests, ctx_smoke; counts only increase.
- AC7. Docs updated per §6; R4 marked resolved in the risk register.

## 8. Non-goals (explicit)

- No timeout on `loom_task_group_destroy()` (contract: finish-and-release).
- No polling / busy-wait helpers.
- No changes to group submit/cancel semantics.
- No new public symbols beyond `loom_task_group_wait_timeout`.