# CI Stability — Valgrind Timing & Cancel-Callback Flake — Design

Date: 2026-10-21
Status: approved for implementation
Baseline: master @ `9facee5` (all CI + Performance green; valgrind job
continue-on-error)
Diagnostic evidence: 2026-10-19/21 session — valgrind failure class
reproduced deterministically in a Debian bookworm container
(valgrind 3.19, CI-equivalent build), timeline captured with an
instrumented spy binary.

## 1. Objective

Eliminate the two remaining CI failure classes so the valgrind job can
become a hard gate, making every CI job's result trustworthy:

1. **Valgrind deque-visibility family** — `shutdown-drain: observed
   deque-resident tasks` (:3942), `resize-spill: observed
   deque-resident tasks` (:3987), `steal: ring drained into worker
   deque` (:4057), `steal: parked worker stole from free worker`
   (:4065). Root cause: the tests' valgrind loop scaling `VGI()` never
   activates because `#ifdef __VALGRIND__` is never defined (the test
   never includes `valgrind.h`; plain `-O0` gcc never defines it). CI
   valgrind runs execute the full-size loops: `STEAL_MAX_TASKS=512`
   ~1ms-dwell tasks take ~70–80s to drain under valgrind (~100ms+/task)
   against a 60s observation deadline.
2. **Cancel-callback race** — `parity-ucontext` (and once
   `build (gcc, Debug)`, 2026-09-14): `test_metrics_callback_all_events`
   submits then immediately cancels with no gate; if the sole worker
   reaches the run boundary (`cancel_index_remove`,
   `src/thread_pool.c:1022`) before `loom_pool_cancel` probes the
   cancel index (`src/thread_pool.c:2495-2510`), the cancel finds
   nothing and no CANCELLED event fires → `ctx.cancelled == 0`.

Scope constraint (user-confirmed): **tests/ and ci.yml only**; `src/`
and `include/` must have a zero-line diff. Gate policy (user-confirmed):
valgrind job becomes a hard gate after the timing fixes. Fix style
(user-confirmed): gate/park per existing repo convention. Verification
strength (user-confirmed): local multi-round before every push.

Non-goals: no library behavior change; no new test framework or shared
test header (gate/park helpers stay file-local as today); no change to
`WAIT_UNTIL` semantics or its 11 call sites; no perf-gate tooling
changes (the 2026-10-21 THRESHOLD 0.65 raise is already landed,
`57b8af0`).

## 2. Change 1 — Runtime valgrind detection for VGI scaling

**File:** `tests/test_thread_pool.c` (header block ~lines 14–24, plus
`STEAL_MAX_TASKS` at :137 and `main()` initialization).

Current broken code:

```c
#ifdef __VALGRIND__          /* never defined: dead branch */
#define VGI(n) ((n) / 4)
#else
#define VGI(n) (n)
#endif
```

Replacement:

```c
/* Runtime valgrind detection: __VALGRIND__ is only defined by
 * valgrind.h under specific configurations; CI runs the plain -O0
 * binary under valgrind, so scaling must be decided at run time via
 * RUNNING_ON_VALGRIND (client request; 0 when not under valgrind). */
#if defined(__has_include) && __has_include(<valgrind/valgrind.h>)
#include <valgrind/valgrind.h>
#endif
#ifndef RUNNING_ON_VALGRIND
#define RUNNING_ON_VALGRIND 0
#endif

static int g_vgi_scale = 0;  /* set in main(): RUNNING_ON_VALGRIND ? 1 : 0 */

/* Scale loop counts to 1/4 under valgrind (same ratio as the original
 * compile-time intent) to keep total runtime within the CI timeout. */
#define VGI(n) ((g_vgi_scale) ? ((n) / 4) : (n))
```

`main()` gains one line before any test runs:
`g_vgi_scale = RUNNING_ON_VALGRIND;`

`STEAL_MAX_TASKS` at :137 stays a compile-time 512 (it sizes the
`g_steal_owner` array, which must not shrink); the steal-test submit
loop changes from `for (int i = 0; i < STEAL_MAX_TASKS; i++)` to
`for (int i = 0; i < VGI(STEAL_MAX_TASKS); i++)` (→128 submissions under
valgrind). The follow-up assertion
`ring_count == STEAL_MAX_TASKS` becomes
`ring_count == (size_t)VGI(STEAL_MAX_TASKS)` — the exact submitted
count, still exact (not weakened), and
`g_steal_owner_count == STEAL_MAX_TASKS` at :4064 gets the same pairing
(see the audit below).

Effect on the 10 other existing `VGI()` call sites (:529, :1254,
:1756, :1782, :2265, :2448, :2520, :3931, :3976): under valgrind they
now actually scale to 1/4 (previously dead); natively `g_vgi_scale == 0`
so behavior is bit-identical to today.

**Paired-constant audit (done during spec self-review):** three
assertions compare counts against non-scaled constants and MUST be
paired with the same commit, else valgrind runs fail on the shrunk
counts:

- `tests/test_thread_pool.c:3947` — `ASSERT(counter == 300, …)` →
  `counter == VGI(300)` (shutdown-drain)
- `tests/test_thread_pool.c:3993` — `ASSERT(counter == 300, …)` →
  `counter == VGI(300)` (resize-spill)
- `tests/test_thread_pool.c:4064` —
  `ASSERT(g_steal_owner_count == STEAL_MAX_TASKS, …)` →
  `g_steal_owner_count == (int)VGI(STEAL_MAX_TASKS)` (steal)

All other `VGI()` sites pair their counts correctly already (verified:
:263, :424 use `N`/`W` compile-time vals in test_dag.c which keeps its
own `#ifdef` blocks — out of scope here, still dead under CI valgrind
but no failure signature; test_dag.c is not touched in this round).

Timeline math for the steal test under valgrind after the fix:
128 tasks × ~100ms dwell ≈ 13s drain, well inside the 60s deadline
(4.6× margin); with the drain now actually observed, both steal
assertions become deterministic. `resize-spill`/`shutdown-drain` keep
their `VGI(300)` → 75 tasks (~7.5s), inside `WAIT_UNTIL(30, …)`.

`slow_count_task` dwell is unchanged (its 65536-iteration spin is the
observation-window maker; scaling it would re-open the original
F1 flake documented at :96-103).

## 3. Change 2 — gate/park the cancel-callback test

**File:** `tests/test_thread_pool.c`, `test_metrics_callback_all_events`
(~:2186–2223). Reuses the file's existing `gate_task` /
`g_gate_started` / `g_gate_parked` / `g_gate_release` globals, exactly
like the adjacent `test_metrics_callback_lifecycle_counts` (:2178–2204).

New flow:

1. Reset gate globals (same three lines as :2179-2181).
2. Submit `gate_task` → spin `while (g_gate_parked < 1) sched_yield();`
   — the sole worker is now frozen inside the gate.
3. Submit two `increment_task` + one `increment_task(&cancel_data)`.
   All three are queued; the cancel target is guaranteed resident in
   the cancel index (workers dequeue only from the front; the gate
   occupant cannot reach it).
4. `ASSERT(loom_pool_cancel(pool, &cancel_data) == LOOMWORKS_OK,
   "cancel queued task")` — new deterministic anchor; the original test
   never checked the return value.
5. `g_gate_release = 1;` → shutdown → destroy → metrics destroy
   (unchanged tail).
6. Assertions strengthened from `>=` to exact: `cancelled == 1`;
   `submitted` / `completed` exact values with the gate task counted
   (gate_task is itself one submit + one completion: expected
   `submitted == 4`, `completed == 3`). The exact submitted/completed
   numbers are flagged in the plan as an implementation-time
   verification point (assert against the observed metric semantics,
   matching how `test_metrics_callback_lifecycle_counts` counts its
   gate task: 3 submits → submitted==3, completed==3).

Both `parity-ucontext` and `build (gcc, Debug)` failed on this race;
the fix removes the race window entirely instead of widening the test's
tolerance.

## 4. Change 3 — valgrind job becomes a hard gate

**File:** `.github/workflows/ci.yml:90` — delete
`continue-on-error: true` from the `valgrind:` job. Everything else
in the job (10-minute timeout, retry step, suppression file) stays.

Margin math after changes 1–2: `test_thread_pool` under valgrind drops
from ~8 min (full loops) to an estimated ~4–5 min (1/4 loops), within
the 10-minute job timeout at ~2× headroom — healthier than the perf
gate's ~1.0× noise margin that caused the 2026-10-20 red streak.

Rollback: `git revert` of the one-line commit restores
continue-on-error; no other artifact depends on the gate policy.

## 5. Verification protocol (before each push)

| Layer | Command | Pass criterion |
|---|---|---|
| Single-CPU contention (CI congestion proxy) | `cd build_uc && for i in $(seq 1 10); do taskset -c 0 ./test_thread_pool; done` | 10/10 exit 0 |
| Container valgrind (CI-equivalent) | debian:bookworm-slim, valgrind 3.19, Debug build, `valgrind --leak-check=full --error-exitcode=1 --suppressions=.github/valgrind.supp ./test_thread_pool` ×3 rounds | 3/3: zero `FAIL:` lines, zero memcheck errors |
| New-timing evidence | container run log: steal submit count = 128, drain < 30s | log excerpt recorded in commit body |
| Full local suite | `cmake --build build && (cd build && ctest --output-on-failure)` | 8/8 |
| Native non-regression | native `./test_thread_pool` green with steal-loop still submitting 512 | suite passes (proves `VGI(n)==n` natively) |

Container build recipe (from the 2026-10-19 diagnosis): mount repo
read-only, copy to `/tmp/src`, add `-D_DEFAULT_SOURCE` to the Debug
`CMAKE_C_FLAGS` line in `cmake/BuildTypes.cmake` (bookworm hides
`MAP_ANON` under strict `_POSIX_C_SOURCE`; `src/portability.h` only
maps `MAP_ANONYMOUS`→`MAP_ANON`, not the reverse), then
`cmake -S /tmp/src -B /tmp/b -DCMAKE_BUILD_TYPE=Debug
-DENABLE_CLANG_FORMAT=OFF`.

## 6. Risks / Mitigations

- **Residual valgrind flake after scaling** (runner congestion):
  verification protocol runs 3 container rounds; if a red slip through
  post-landing, `git revert` the ci.yml commit first (restores
  continue-on-error) and keep the test fixes.
- **`VGI()` runtime branch cost**: one predictable branch per loop
  entry; loops run ≥100 iterations — unmeasurable, and native path is
  unchanged (`g_vgi_scale == 0` constant-folds after inlining).
- **Exact-count assertions too strict** (change 2): the numbers are
  verified in the verification protocol's container/native rounds; the
  plan marks the submitted/completed exact values as an explicit
  check-or-adjust point before commit.
- **Other VGI sites now shrinking under valgrind**: the paired-constant
  audit (§2) found exactly three mismatched assertions; they are fixed
  in the same commit. Remaining sites pair their counts via the same
  `VGI()` expression — verified in the container rounds (any missed
  pairing fails deterministically there, before push).
- **Test-only changes cannot corrupt the library**: `src/`/`include/`
  zero-diff is enforced by the plan's final `git diff` check.

## 7. Deliverables

1. Three commits, each independently revertable:
   - `test(pool): ✅ activate valgrind loop scaling via runtime detection`
   - `test(pool): ✅ gate worker before cancel in metrics all-events test`
   - `ci: 👷 make valgrind job a hard gate`
2. Post-push evidence: green CI run with valgrind as a hard gate
   (`gh run watch --exit-status`).

## 8. Spec Self-Review

- No TBDs: the only open number (exact submitted/completed counts in
  change 2) is explicitly routed through the verification protocol
  rather than guessed.
- Consistency: §2 timeline math (128 × ~100ms ≈ 13s < 60s) matches §4
  margin claim; scope constraint (tests/ci only) is honored by all
  three changes; §2's paired-constant audit resolved the ambiguity the
  risk register originally flagged as "per-site plan check" — the
  three sites are now named with line numbers.
- Scope: three point changes in two files (plus the three paired
  assertion edits inside the first commit) — single-plan size.
- Ambiguity: test_dag.c's own `#ifdef __VALGRIND__` blocks are dead the
  same way but produce no failure signature; explicitly out of scope
  this round (recorded in §2).
