# CI Stability — Valgrind Timing & Cancel-Callback Flake — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the valgrind CI job a trustworthy hard gate by (a) activating the test suite's never-working valgrind loop scaling at runtime, (b) removing the submit→cancel race in `test_metrics_callback_all_events` via the repo's gate/park pattern.

**Architecture:** Three independently revertable commits: runtime `VGI()` scaling in `tests/test_thread_pool.c` (+ three paired count-assertion fixes), gate/park rewrite of the cancel-callback test, and deletion of `continue-on-error` in `.github/workflows/ci.yml`. Spec: `docs/superpowers/specs/2026-10-21-ci-stability-valgrind-design.md`.

**Tech Stack:** C11 test code, GitHub Actions YAML, valgrind client requests (`RUNNING_ON_VALGRIND`), Debian bookworm container for CI-equivalent valgrind verification.

**Hard constraints (from spec §1):**
- `src/` and `include/` must have a **zero-line diff** in every commit.
- Natively (`RUNNING_ON_VALGRIND == 0`) test behavior must be bit-identical to today.
- Conventional Commits with gitmoji; one commit per change; reverts only via `git revert <sha>`.

---

### Task 1: Runtime VGI scaling + paired-constant fixes

**Files:**
- Modify: `tests/test_thread_pool.c` (header block :14–25, `main()` :4419, steal test :4026–4064, drain tests :3947/:3993)

- [ ] **Step 1: Replace the dead compile-time VGI block with runtime detection**

Replace lines 21–25 (the `#ifdef __VALGRIND__ … #endif` block) with:

```c
/* Runtime valgrind detection: __VALGRIND__ is only defined by valgrind.h
 * under specific configurations, and CI runs the plain -O0 binary under
 * valgrind — so loop scaling must be decided at run time via the
 * RUNNING_ON_VALGRIND client request (0 when not under valgrind). */
#if defined(__has_include) && __has_include(<valgrind/valgrind.h>)
#include <valgrind/valgrind.h>
#endif
#ifndef RUNNING_ON_VALGRIND
#define RUNNING_ON_VALGRIND 0
#endif

static int g_vgi_scale = 0; /* set in main(): 1 when running under valgrind */

/* Scale loop iterations to 1/4 under valgrind (the original compile-time
 * intent) to keep total runtime within the CI timeout.  Native behavior
 * is unchanged: g_vgi_scale == 0 makes VGI(n) == n. */
#define VGI(n) ((g_vgi_scale) ? ((n) / 4) : (n))
```

- [ ] **Step 2: Initialize the scale flag in main()**

In `main()` (line 4419), insert immediately after the opening `{`, before
the `printf("=== Thread Pool Tests ===\n");`:

```c
    g_vgi_scale = RUNNING_ON_VALGRIND;
```

- [ ] **Step 3: Scale the steal-test submit loop and its two count assertions**

In `test_steal_trigger`:

3a. Change the submit loop (:4026):

```c
    /* Flood the ring while both workers are frozen. */
    for (int i = 0; i < VGI(STEAL_MAX_TASKS); i++) {
```

3b. Change the ring_count assertion (:4030):

```c
    ASSERT(atomic_load_explicit(&pool->ring_count, memory_order_relaxed) ==
               (size_t)VGI(STEAL_MAX_TASKS),
           "steal: all submitted tasks resident in ring");
```

3c. Change the owner-count assertion (:4064):

```c
    ASSERT(g_steal_owner_count == (int)VGI(STEAL_MAX_TASKS),
           "steal: all submitted owner tasks ran exactly once");
```

- [ ] **Step 4: Pair the drain-test count constants**

4a. In `test_shutdown_drains_deque` (:3947):

```c
    ASSERT(counter == VGI(300), "shutdown-drain: all submitted tasks ran");
```

4b. In `test_resize_down_spills_deque` (:3993):

```c
    ASSERT(counter == VGI(300), "resize-spill: all submitted tasks ran exactly once");
```

- [ ] **Step 5: Build and run native suite (non-regression)**

Run:
```bash
cmake --build build -j$(nproc) && (cd build && ./test_thread_pool >/tmp/tp_native.log 2>&1; echo "EXIT=$?")
```
Expected: `EXIT=0`, and the log shows `Results:` with 0 failed. Native
steal test still submits 512 tasks (VGI is a no-op when
`g_vgi_scale == 0`) — grep `/tmp/tp_native.log` for zero `FAIL` lines.

Also build+run the ucontext Debug tree (used by Task 4's contention loop):

```bash
cmake --build build_uc -j$(nproc) && (cd build_uc && ./test_thread_pool >/dev/null 2>&1; echo "UC_EXIT=$?")
```
Expected: `UC_EXIT=0`.

- [ ] **Step 6: Verify actual valgrind scaling in the CI-equivalent container**

Reuse the container recipe from the spec (§5). One-time setup:

```bash
docker run -d --name loom-vg -v "$(pwd)":/work:ro debian:bookworm-slim sleep infinity
docker exec loom-vg bash -c "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq valgrind libc6-dbg gcc cmake"
docker exec loom-vg bash -c "cp -r /work /tmp/src && sed -i 's/-O0 -g\")/-O0 -g -D_DEFAULT_SOURCE\")/' /tmp/src/cmake/BuildTypes.cmake && cmake -S /tmp/src -B /tmp/b -DCMAKE_BUILD_TYPE=Debug -DENABLE_CLANG_FORMAT=OFF >/dev/null && cmake --build /tmp/b -j\$(nproc)"
```

Run valgrind round 1:

```bash
docker exec loom-vg bash -c "cd /tmp/b && valgrind --leak-check=full --error-exitcode=1 --suppressions=/tmp/src/.github/valgrind.supp ./test_thread_pool > /tmp/vg1.log 2>&1; echo VG_EXIT=\$?; grep -cE 'FAIL:' /tmp/vg1.log; grep 'ERROR SUMMARY' /tmp/vg1.log"
```

Expected: `VG_EXIT=0`, FAIL count `0`, `ERROR SUMMARY: 0 errors from 0
contexts`. If `VG_EXIT=124` (timeout), the scaling did not take effect —
check that `RUNNING_ON_VALGRIND` resolved (add a temporary
`fprintf(stderr, "vgi=%d\n", g_vgi_scale);` in main, re-run, remove it).

Total valgrind wall time must drop vs the ~8-minute pre-fix baseline
(expect ~4–5 min; measure with `time`).

- [ ] **Step 7: Two more container valgrind rounds (stability)**

```bash
for r in 2 3; do docker exec loom-vg bash -c "cd /tmp/b && valgrind --error-exitcode=1 ./test_thread_pool >/tmp/vg$r.log 2>&1"; echo "round$r exit=$?"; done
grep -l 'FAIL' /tmp/vg2.log /tmp/vg3.log || echo "no FAIL lines"
```

Expected: both rounds exit 0, no FAIL lines anywhere.

- [ ] **Step 8: Tear down container, verify zero src/ diff, commit**

```bash
docker rm -f loom-vg
git diff --stat   # must list ONLY tests/test_thread_pool.c
git add tests/test_thread_pool.c
git commit -m "test(pool): ✅ activate valgrind loop scaling via runtime detection"
```

---

### Task 2: Gate/park the cancel-callback test

**Files:**
- Modify: `tests/test_thread_pool.c` (`test_metrics_callback_all_events`, :2186–2223)

- [ ] **Step 1: Rewrite the test with the gate before cancel**

Replace the body between `loom_metrics_create(...)` success assert and
`loom_pool_shutdown(pool);` with the gated sequence (keep the existing
declarations `metrics_event_ctx_t ctx`, `loom_metrics_t *metrics`,
`int counter`):

```c
    g_gate_started = 0;
    g_gate_release = 0;
    g_gate_parked  = 0;

    /* Freeze the sole worker inside the gate: everything submitted below
     * stays queued (cancel index resident) until we release, so the
     * cancel below is deterministic — the old submit→cancel race let the
     * worker reach its run boundary first and drop the CANCELLED event. */
    ASSERT(loom_pool_submit(pool, gate_task, NULL, NULL) == LOOMWORKS_OK, "gate submitted");
    while (g_gate_parked < 1) {
        sched_yield();
    }

    int counter = 0;
    ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK, "increment 1");
    ASSERT(loom_pool_submit(pool, increment_task, &counter, NULL) == LOOMWORKS_OK, "increment 2");

    int cancel_data = 999;
    ASSERT(loom_pool_submit(pool, increment_task, &cancel_data, NULL) == LOOMWORKS_OK,
           "cancel target submitted");

    /* Worker is parked: the task is still in the cancel index. */
    ASSERT(loom_pool_cancel(pool, &cancel_data) == LOOMWORKS_OK, "cancel queued task");

    g_gate_release = 1;
```

The tail (`loom_pool_shutdown(pool);` … `loom_pool_destroy(&pool);`) is
unchanged.

- [ ] **Step 2: Strengthen the count assertions to exact values**

Replace the three trailing assertions:

```c
    ASSERT(ctx.submitted == 4, "all 4 submissions counted (gate + 3)");
    ASSERT(ctx.completed == 3, "gate + 2 increments completed, cancelled did not");
    ASSERT(ctx.cancelled == 1, "callback received cancelled event");
    ASSERT(ctx.failed == 0, "no failures");
    ASSERT(counter == 2, "both increments executed");
```

NOTE (spec §3 verification point): the submitted/completed split assumes
`gate_task` itself fires one SUBMITTED and one COMPLETED event and the
cancelled task fires neither COMPLETED nor FAILED. Run Step 3 first; if
the observed counts differ by exactly the gate task's accounting, adjust
the two numbers to the observed values and record the finding in the
commit body — `cancelled == 1` and `counter == 2` are NOT adjustable.

- [ ] **Step 3: Run native, then single-CPU contention loop**

```bash
cmake --build build -j$(nproc) && (cd build && ./test_thread_pool >/tmp/tp2.log 2>&1; echo "EXIT=$?"); grep -E 'FAIL|Results' /tmp/tp2.log | tail -3
cmake --build build_uc -j$(nproc) && for i in $(seq 1 10); do (cd build_uc && taskset -c 0 ./test_thread_pool >/dev/null 2>&1); echo "run$i exit=$?"; done
```

Expected: native `EXIT=0` (assertion counts confirmed or adjusted per
Step 2's note), then 10/10 contention runs exit 0.

- [ ] **Step 4: One container valgrind round on the updated test**

```bash
docker run -d --name loom-vg -v "$(pwd)":/work:ro debian:bookworm-slim sleep infinity
# reuse Task 1 Step 6 setup commands (apt install, copy, patched build)
docker exec loom-vg bash -c "cd /tmp/b && cmake --build /tmp/b -j\$(nproc) >/dev/null 2>&1 && valgrind --error-exitcode=1 ./test_thread_pool >/tmp/vg4.log 2>&1"; echo "exit=$?"
docker exec loom-vg grep -cE 'FAIL:' /tmp/vg4.log
docker rm -f loom-vg
```

Expected: exit 0, FAIL count 0.

- [ ] **Step 5: Commit**

```bash
git diff --stat   # must list ONLY tests/test_thread_pool.c
git add tests/test_thread_pool.c
git commit -m "test(pool): ✅ gate worker before cancel in metrics all-events test"
```

---

### Task 3: Valgrind job becomes a hard gate

**Files:**
- Modify: `.github/workflows/ci.yml` (valgrind job, line ~90)

- [ ] **Step 1: Delete the continue-on-error line**

In the `valgrind:` job (after `runs-on: ubuntu-latest`), remove:

```yaml
    continue-on-error: true
```

The 10-minute timeout, retry step, and suppression path stay untouched.

- [ ] **Step 2: Sanity-check YAML parses**

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"
```

Expected: `yaml ok`.

- [ ] **Step 3: Commit**

```bash
git diff --stat   # must list ONLY .github/workflows/ci.yml
git add .github/workflows/ci.yml
git commit -m "ci: 👷 make valgrind job a hard gate"
```

---

### Task 4: Push and end-to-end CI evidence

**Files:** none (verification only)

- [ ] **Step 1: Full local suite before push**

```bash
cmake --build build -j$(nproc) && (cd build && ctest --output-on-failure 2>&1 | tail -3)
```

Expected: `100% tests passed out of 8`.

- [ ] **Step 2: Push and watch both workflows**

```bash
git push && sleep 15 && gh run list --limit 2
gh run watch <CI_RUN_ID> --exit-status --interval 30; echo "CI=$?"
gh run watch <PERF_RUN_ID> --exit-status --interval 30; echo "PERF=$?"
```

Expected: CI run **success** — 11/11 jobs green, valgrind now counted as
a blocking job (check with `gh run view <id> --json jobs --jq '.jobs[] |
select(.name=="valgrind") | .conclusion'` → `"success"`); Performance
success. If the valgrind job goes red on the first push, do NOT fix
forward blindly: pull the failing signature, compare against the
container evidence, `git revert` the ci.yml commit if the failure is a
residual flake, and re-land only after the container rounds reproduce
the fix's stability.

- [ ] **Step 3: Record the outcome**

Add one line each to the commit bodies' context (no new files): valgrind
job green as hard gate + the container timing (rounds + wall time).
If the valgrind job failed and was reverted, record the signature in
`docs/superpowers/specs/2026-10-21-ci-stability-valgrind-design.md` §6
as a follow-up note instead.
