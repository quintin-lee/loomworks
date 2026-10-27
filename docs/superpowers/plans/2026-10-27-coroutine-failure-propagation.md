# Coroutine Failure Propagation (`loom_coro_fail`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let user coroutine code report a deterministic failure — `loom_coro_fail(err)` — that propagates to standalone callers (`loom_coro_resume` returns a new `LOOMWORKS_CORO_ERR_FAILED`; `loom_coro_exit_status` reads the code) and to pool callers (existing `submit_coroutine_ex` future completes `LOOMWORKS_ERR_FAILED`, closing the R22 FAILED branch that was unreachable from user code).

**Architecture:** Reuse the guard-page longjmp machinery (`g_guard_jmp`, installed in `loom_coro_resume`). `loom_coro_fail` records state+code on the current coroutine and `siglongjmp`s back into the resumer's setjmp; the resumer's jmp arm distinguishes user-fail (`exit_failed`) from infra guard (no `exit_failed`) and returns `ERR_FAILED` vs `ERR_GUARD`. Two new struct fields replace one `uint64_t` of `padding[2]` (struct size invariant). Pool needs **zero** changes: its failure sites already fire on any `crc` that is neither OK nor RUNNING.

**Tech Stack:** C11, POSIX pthreads/mmap, `sigaction` longjmp. Spec: `docs/superpowers/specs/2026-10-27-coroutine-failure-propagation-design.md`.

**Repo context the worker does not know:**
- Build: `cmake --build build -j$(nproc)` (a clang-format gate runs on every build when clang-format is present; keep new code clang-format-22 clean or the build fails).
- Test executables: `build/test_coroutine`, `build/test_thread_pool` (hand-rolled ASSERT; exit 0 = pass). Full: `cd build && ctest`.
- `loom_coroutine` is allocated with `calloc` (coroutine.c:487) → new fields start zeroed; a `calloc`-reclaimed stack-pool mapping is NOT rezeroed, but the fields are reset by the fail/resume path, so no init needed.
- `g_guard_jmp` is `_Thread_local sigjmp_buf` (coroutine.c:60); the guard SIGSEGV handler (coroutine.c:207-210) does `siglongjmp(g_guard_jmp, 1)` after setting `state = LOOMWORKS_CORO_ERROR` but does NOT set `exit_failed` — that's the discriminator.
- `loom_result_t` values are `int` enums 0..7; store as `uint32_t exit_code`.
- Test registration: each `tests/*.c` has a `main()` that calls `test_*()` functions; follow the local pattern (`grep "test_" tests/test_coroutine.c | tail`).
- Commit style: Conventional Commits with emoji.
- R22's `loom_future_complete` has a cancel guard: once a future is marked cancelled it cannot be overwritten — so a late `fail` after cancel keeps `ERR_CANCELLED` (the correct precedence).

---

## Task 1: public enum + struct fields + header declarations

**Files:**
- Modify: `include/loomworks/coroutine.h` (enum append ~line 71; two new decls after `loom_coro_exit` ~line 223)
- Modify: `src/coroutine_internal.h` (replace `padding[2]` at line 68)

- [ ] **Step 1: Append the enum value**

In `include/loomworks/coroutine.h`, change

```c
    LOOMWORKS_CORO_ERR_TIMER,    /**< Timer registration failed (sleep not tracked). */
    LOOMWORKS_CORO_ERR_TIMEOUT,  /**< Execution timeout exceeded. */
} loom_coro_result_t;
```

to

```c
    LOOMWORKS_CORO_ERR_TIMER,    /**< Timer registration failed (sleep not tracked). */
    LOOMWORKS_CORO_ERR_TIMEOUT,  /**< Execution timeout exceeded. */
    LOOMWORKS_CORO_ERR_FAILED,   /**< loom_coro_fail() was called inside the coroutine. */
} loom_coro_result_t;
```

- [ ] **Step 2: Add the two function declarations after `loom_coro_exit`**

In `include/loomworks/coroutine.h`, immediately after the `void loom_coro_exit(void);` declaration (line 223), insert:

```c
/**
 * @brief Signal failure from within a coroutine; does NOT return.
 *
 * Records @p err on the current coroutine and unwinds back to the thread
 * that last resumed it (the same longjmp path the guard-page handler uses).
 * After the resumer returns, the coroutine state is LOOMWORKS_CORO_ERROR.
 *
 * Calling outside a running coroutine (no current coroutine on this
 * thread) is a no-op.
 *
 * @param err  Failure code to record (any LOOMWORKS_ERR_* value).
 */
void loom_coro_fail(loom_result_t err);

/**
 * @brief Read a coroutine's exit status after it stops running.
 *
 * Valid from the owning thread once the coroutine is DONE or ERROR.
 *
 * @return   DONE              -> LOOMWORKS_OK
 *           ERROR + fail()   -> the err recorded by loom_coro_fail()
 *           ERROR + infra    -> LOOMWORKS_ERR_FAILED (guard/alloc/context)
 *           otherwise       -> LOOMWORKS_ERR_INVALID
 *
 * @param coro  Coroutine handle (NULL -> LOOMWORKS_ERR_INVALID).
 */
loom_result_t loom_coro_exit_status(loom_coroutine_t *coro);
```

Note: `loom_result_t` comes from `thread_pool.h`; `coroutine.h` already includes it (verify with `grep '#include' include/loomworks/coroutine.h`; if not present, add `#include "loomworks/thread_pool.h"` at the top — it is a sibling public header, no cycle).

- [ ] **Step 3: Add the struct fields**

In `src/coroutine_internal.h`, replace line 68:

```c
    uint64_t padding[2]; /**< Pad to 64-byte cache-line boundary. */
```

with

```c
    uint32_t exit_code;    /**< loom_coro_fail() payload (valid when exit_failed). */
    uint8_t  exit_failed;  /**< 1 if terminated via loom_coro_fail(). */
    uint64_t padding[1];   /**< Pad to 64-byte cache-line boundary. */
```

Struct size is unchanged (was 16B of padding, now 4+1+11B → 4B consumed by fields, 12B pad; total 64-byte alignment preserved — verify with the compile in Step 4).

- [ ] **Step 4: Build**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3`
Expected: build succeeds (clang-format gate passes; fix any format diff it flags before proceeding).

- [ ] **Step 5: Commit**

```bash
git add include/loomworks/coroutine.h src/coroutine_internal.h
git commit -m "feat(coroutine): ✨ add ERR_FAILED result and exit-status fields"
```

---

## Task 2: `loom_coro_fail` + resume jmp arm + `loom_coro_exit_status`

**Files:**
- Modify: `src/coroutine.c` (jmp arm at line 533-535; new functions near `loom_coro_exit` ~line 830)

- [ ] **Step 1: Write the failing standalone test**

In `tests/test_coroutine.c`, add (and register in `main()`):

```c
static void fail_probe_coro(void *arg)
{
    int *flag = (int *)arg;
    *flag = 1;
    loom_coro_fail(LOOMWORKS_ERR_INVALID);
    *flag = 2; /* must not run */
}

static void test_coro_fail_propagates(void)
{
    loom_coroutine_t  *coro = NULL;
    int               flag  = 0;
    loom_coro_result_t rc   = loom_coro_create(fail_probe_coro, &flag, 0, &coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "fail: create");
    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_ERR_FAILED, "fail: resume reports ERR_FAILED");
    ASSERT(flag == 1, "fail: code after loom_coro_fail did not run");
    ASSERT(loom_coro_exit_status(coro) == LOOMWORKS_ERR_INVALID, "fail: exit_status");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_ERROR, "fail: state ERROR");

    loom_coroutine_t *okcoro = NULL;
    loom_coro_result_t orc   = loom_coro_create(simple_coro_fn, &flag, 0, &okcoro);
    ASSERT(orc == LOOMWORKS_CORO_OK, "fail: create ok coro");
    loom_coro_resume(okcoro);
    ASSERT(loom_coro_exit_status(okcoro) == LOOMWORKS_OK, "done: exit_status OK");
    loom_coro_destroy(&okcoro);
    loom_coro_destroy(&coro);
}
```

(If `simple_coro_fn` doesn't exist in this file, use the file's existing simple runnable coroutine function — check with `grep -n "static void.*_coro\|_fn" tests/test_coroutine.c`.)

Register the test in `main()` next to the other `test_*` calls.

- [ ] **Step 2: Run to verify it fails to compile**

Run: `cmake --build build --target test_coroutine -j$(nproc) 2>&1 | grep -E "error: implicit|undefined" | head -3`
Expected: `error: implicit declaration of function 'loom_coro_fail'` (the symbol doesn't exist yet → compile error).

- [ ] **Step 3: Implement `loom_coro_fail` and the resume jmp arm**

In `src/coroutine.c`, change the resume jmp arm (lines 533-535):

```c
    if (sigsetjmp(g_guard_jmp, 1) != 0) {
        return LOOMWORKS_CORO_ERR_GUARD;
    }
```

to

```c
    if (sigsetjmp(g_guard_jmp, 1) != 0) {
        /* Two distinct longjmp sources share this target: a guard-page
         * fault (state=ERROR, exit_failed stays 0) and a user-initiated
         * loom_coro_fail() (exit_failed set).  Distinguish by the flag so
         * callers can report the specific failure. */
        return coro->exit_failed ? LOOMWORKS_CORO_ERR_FAILED : LOOMWORKS_CORO_ERR_GUARD;
    }
```

Then, near `loom_coro_exit` (find it with `grep -n "void loom_coro_exit" src/coroutine.c`), add:

```c
void loom_coro_fail(loom_result_t err)
{
    loom_coroutine_t *c = g_current;
    if (c == NULL) {
        return; /* not running inside a coroutine: documented no-op */
    }
    c->exit_code   = (uint32_t)err;
    c->exit_failed = 1;
    c->state       = LOOMWORKS_CORO_ERROR;
    g_current       = NULL;
    /* Same async-signal-safe longjmp the guard handler uses.  The resumer
     * re-acquires g_guard_jmp in its sigsetjmp arm and reports
     * LOOMWORKS_CORO_ERR_FAILED (exit_failed discriminator). */
    siglongjmp(g_guard_jmp, 1);
}

loom_result_t loom_coro_exit_status(loom_coroutine_t *coro)
{
    if (coro == NULL) {
        return LOOMWORKS_ERR_INVALID;
    }
    switch (coro->state) {
    case LOOMWORKS_CORO_DONE:
        return LOOMWORKS_OK;
    case LOOMWORKS_CORO_ERROR:
        if (coro->exit_failed) {
            return (loom_result_t)coro->exit_code;
        }
        return LOOMWORKS_ERR_FAILED; /* infra error: guard/alloc/context */
    default:
        return LOOMWORKS_ERR_INVALID; /* still NEW/RUNNING/SUSPENDED/SLEEPING */
    }
}
```

If `loom_coro_exit` is declared `no_noreturn` style or the compiler warns about a path after `siglongjmp`, annotate `loom_coro_fail` with `__attribute__((noreturn))` only if the compiler requires it — otherwise leave it returning `void` (the `siglongjmp` never returns in practice but the compiler doesn't know that). Prefer NOT adding noreturn to keep the guard-handler convention (which also `siglongjmp`s and returns `void`).

- [ ] **Step 4: Build and run the standalone test**

Run: `cmake --build build --target test_coroutine -j$(nproc) && ./build/test_coroutine 2>&1 | grep -E "test_coro_fail|Results" | tail -2`
Expected: `Results: N passed, 0 failed` (N larger than before).

- [ ] **Step 5: Commit**

```bash
git add src/coroutine.c tests/test_coroutine.c
git commit -m "feat(coroutine): ✨ implement loom_coro_fail and loom_coro_exit_status

Reuses the guard-page longjmp target with an exit_failed discriminator so
a user-initiated failure reports ERR_FAILED instead of ERR_GUARD.  No
pool changes: the two worker failure sites already map any crc that is
neither OK nor RUNNING to METRIC_FAILED + future ERR_FAILED."
```

---

## Task 3: pool-layer FAILED branch test (completes R22 matrix)

**Files:**
- Modify: `tests/test_thread_pool.c` (extend the R22 block; replace the "ERR_FAILED not deterministically testable" comment)

- [ ] **Step 1: Find the R22 test block and the failing comment**

Run: `grep -n "submit_coroutine_ex\|ERR_FAILED\|not deterministically\|R22" tests/test_thread_pool.c | head -12`

- [ ] **Step 2: Add a failing-coroutine helper and the pool test**

In `tests/test_thread_pool.c`, add a coroutine entry that fails (place near the existing R22 block):

```c
static void coro_fail_task(void *arg)
{
    (void)arg;
    loom_coro_fail(LOOMWORKS_ERR_INVALID);
}
```

Then add a test (and register in `main()` next to the R22 tests):

```c
static void test_submit_coroutine_ex_failed(void)
{
    loom_thread_pool_t *pool = NULL;
    loom_pool_config_t  cfg  = {.worker_count = 4, .queue_capacity = 0};
    ASSERT(loom_pool_create(&cfg, &pool) == LOOMWORKS_OK, "fail: create pool");

    loom_future_t *fut = NULL;
    uint64_t       tid = 0;
    ASSERT(loom_pool_submit_coroutine_ex(pool, coro_fail_task, NULL, 0, &fut, &tid) ==
               LOOMWORKS_OK,
           "fail: submit");
    ASSERT(fut != NULL, "fail: future allocated");

    void *result = NULL;
    ASSERT(loom_future_wait(fut, &result) == LOOMWORKS_ERR_FAILED,
           "fail: wait reports ERR_FAILED");
    ASSERT(result == NULL, "fail: *result stays NULL (R22 contract)");
    ASSERT(loom_future_destroy(fut) == LOOMWORKS_OK, "fail: destroy");

    loom_pool_shutdown(pool);
    loom_pool_destroy(&pool);
}
```

- [ ] **Step 3: Run the pool test**

Run: `cmake --build build --target test_thread_pool -j$(nproc) && ./build/test_thread_pool 2>&1 | grep -E "test_submit_coroutine_ex_failed|Results" | tail -2`
Expected: `Results: N passed, 0 failed`.

- [ ] **Step 4: Remove the stale "not deterministically testable" comment**

If Step 1 found a comment asserting the FAILED branch is untestable, delete that comment (the new test is the proof). Only edit the comment — no code.

- [ ] **Step 5: Run the full suite**

Run: `cd build && ctest --output-on-failure 2>&1 | grep -E "Tests\|% " | tail -4`
Expected: `100% tests passed out of 8`.

- [ ] **Step 6: Commit**

```bash
git add tests/test_thread_pool.c
git commit -m "test(pool): ✅ cover submit_coroutine_ex FAILED branch via loom_coro_fail

Completes the R22 three-state matrix: the ERR_FAILED future state is now
deterministically reachable from user coroutine code.  Replaces the
'deterministic FAILED primitive does not exist' note from 6bac094."
```

---

## Task 4: sanitizer + valgrind gates

**Files:** none (verification only).

- [ ] **Step 1: ASan (detect_leaks) — longjmp out of user code is the key risk**

Run:

```bash
cmake --build build_asan -j$(nproc) >/dev/null 2>&1
cd build_asan && ASAN_OPTIONS=detect_leaks=1 timeout 180 ./test_coroutine >/dev/null 2>&1; echo "CORO_ASAN=$?"
ASAN_OPTIONS=detect_leaks=1 timeout 180 ./test_thread_pool >/dev/null 2>&1; echo "TP_ASAN=$?"
```

Expected: `CORO_ASAN=0`, `TP_ASAN=0`.

- [ ] **Step 2: UBSan**

Run:

```bash
cd /home/quintin/Data/source/c_cpp/loomworks/build_ubsan && cmake --build . -j$(nproc) >/dev/null 2>&1
timeout 180 ./test_coroutine >/dev/null 2>&1; echo "CORO_UBSAN=$?"
timeout 180 ./test_thread_pool >/dev/null 2>&1; echo "TP_UBSAN=$?"
```

Expected: both `0`.

- [ ] **Step 3: valgrind memcheck on the new test (coroutine longjmp under valgrind)**

Run: `cd /home/quintin/Data/source/c_cpp/loomworks && valgrind --error-exitcode=99 --leak-check=full ./build/test_coroutine >/tmp/vg_fail.log 2>&1; echo "VG=$?"; grep -E "ERROR SUMMARY|Invalid" /tmp/vg_fail.log | tail -3`
Expected: `VG=0`, `ERROR SUMMARY: 0 errors`.

- [ ] **Step 4: If any gate fails, fix the root cause (likely ASan fake-stack save across the longjmp), re-run, and commit the fix** — do NOT widen scope to suppress warnings.

---

## Task 5: docs + push

**Files:**
- Modify: `CHANGELOG.md` (`[Unreleased]` gains `### Added`), `docs/risk-assessment.md` (R22 follow-up + new R24 row)

- [ ] **Step 1: CHANGELOG entry**

In `CHANGELOG.md`, under `## [Unreleased]` (currently has `### Changed` from the perf-gate work), add an `### Added` section ABOVE the `### Changed` (Keep-a-Changelog ordering):

```markdown
### Added
- **Coroutine failure propagation**: `loom_coro_fail(err)` lets a
  coroutine report a deterministic failure; `loom_coro_exit_status()`
  reads the recorded code on the handle.  A pool task that calls
  `loom_coro_fail` now completes its `submit_coroutine_ex` future with
  `LOOMWORKS_ERR_FAILED` — closing the R22 FAILED branch that previously
  could only be reached by infrastructure errors.
```

- [ ] **Step 2: Risk register**

In `docs/risk-assessment.md`, add a new row (R24) after R23, and append a follow-up to the R22 row's resolution cell:

R24 row (append to the table):

```
| R24 | Coroutine | `loom_coro_fail()` longjmps out of user code back to the resumer (guard-page machinery); ASan fake-stack state and the `g_guard_jmp` target must survive the switch | Medium | Low | **Medium** — ✅ mitigated (2026-10-27): reuses the guard handler's async-signal-safe longjmp path; ASan/UBSan/valgrind hard gates cover the new test paths |
```

R22 follow-up: append to its resolution cell: `; follow-up (2026-10-27): ERR_FAILED now reachable from user code via `loom_coro_fail` (R24)`.

- [ ] **Step 3: Build the whole suite one more time (Release) and run all tests**

Run: `cmake --build build -j$(nproc) >/dev/null 2>&1 && (cd build && ctest 2>&1 | grep "tests passed")`
Expected: `100% tests passed out of 8`.

- [ ] **Step 4: Commit docs**

```bash
git add CHANGELOG.md docs/risk-assessment.md
git commit -m "docs(changelog): 📝 record coroutine failure propagation"
```

- [ ] **Step 5: Push and watch CI + Performance to success**

Run: `git push && sleep 15 && gh run list --limit 2`
Then: `gh run watch <CI_ID> --exit-status` and `gh run watch <PERF_ID> --exit-status`.
Expected: both `success` (no-change perf gate should be trivially green; the new tests are exercised by the CI build jobs).

---

## Self-review notes (worker should verify)

- **Spec coverage**: §2 API → Task 1 (enum+decls) + Task 2 (impl). §3.1 struct → Task 1 Step 3. §3.2-3.3 fail+jmp arm → Task 2 Step 3. §3.4 pool no-change → asserted in Task 3 (proven, not re-implemented). §3.5 exit_status → Task 2 Step 3. §4 tests → Task 2 (standalone) + Task 3 (pool). §5 risks → Task 4 (sanitizers/valgrind). §6 release 1.2.0 → Task 5 (docs; version bump is a separate release decision, out of plan scope).
- **Type consistency**: `exit_code`/`exit_failed` names used identically in Task 1 (struct), Task 2 (impl + jmp arm). `LOOMWORKS_CORO_ERR_FAILED` appended once in Task 1, referenced in Task 2/3.
- **Ambiguity resolved**: `loom_coro_fail` outside a coroutine = no-op (Task 2 Step 3, `if (c == NULL) return;`), matching repo rule "never abort".
