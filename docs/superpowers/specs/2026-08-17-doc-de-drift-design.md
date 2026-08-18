# Documentation De-Drift Design (F2-F4 closure)

## 1. Background

A third-party design review (committed `c15ce20`, `docs/architecture-review.md`)
found no design defects but flagged three documentation drift findings:

- **F2 (Low)**: `docs/architecture.md` still uses ucontext/swapcontext
  terminology in several places although hand-written asm backends
  (x86-64/aarch64) are now the default and ucontext is only a compile-time
  fallback.
- **F3 (Low)**: `README.md` is stale — project-structure listing omits the asm
  sources and the ctx smoke test, the POSIX note claims `ucontext(3)` is
  required (it is fallback-only now), and the assertion counts are behind.
- **F4 (Low)**: `compile_commands.json` (git-tracked, exported via
  `CMAKE_EXPORT_COMPILE_COMMANDS`) is stale — missing the asm units, the new
  examples, and `tests/ctx_smoke.c`.

This is a pure documentation/tooling synchronization task. **No code changes.**

## 2. Goals / Non-goals

**Goals:**
- Every drift point in `docs/architecture.md` and `README.md` is corrected.
- `compile_commands.json` is regenerated and committed (keeps the existing
  git-tracking convention).

**Non-goals:**
- No behavior/code changes to the library, tests, or CMake configuration.
- No re-verification of the coroutine backend claims themselves (the review
  already validated them; §3.8 of architecture.md is already correct).
- No restructuring or rewriting of the documents beyond the drift fixes.

## 3. F2 — `docs/architecture.md` edits

| Loc | Current | Replacement |
|-----|---------|-------------|
| L14 (overview table) | `• ucontext save/restore` | `• asm save/restore` (ucontext is fallback-only) |
| L244 (§3.1 component diagram) | `ucontext_t ctx` | `loom_coro_ctx_t ctx` |
| L322-334 (§3.5 context-switch flowchart) | `getcontext` / `makecontext` / `swapcontext` call boxes + "swapcontext returns" | backend-neutral: `loom_coro_ctx_*` entry points |
| L371-372 (§3.7) | "`ucontext` is not thread-safe; `swapcontext` across threads is undefined" | "per-thread `g_scheduler` is `_Thread_local`; resuming on the wrong thread switches to the wrong scheduler stack" |
| L526 (§7.3 diagram note) | same ucontext-thread-safety wording | same replacement as L371-372 |
| L607 (§10 error table) | `swapcontext` row | `loom_coro_ctx_switch` row |

**Kept unchanged** (verified correct or historical):
- L386 (§3.8 backend description: asm default, ucontext fallback) — already accurate.
- L413 (glibc `makecontext` NULL-link historical analogy) — a historical
  comparison, not a claim that ucontext is the default backend.

## 4. F3 — `README.md` edits

1. **L5-8** assertion counts: `~12539 pool + ~5603 coroutine + ~78731
   integration` → current `~20744 pool + ~5611 coroutine + ~78746
   integration` (plus `test_ctx_smoke` 200014).
2. **L63** (coroutine feature table) and **L239** (design-constraints table):
   `makecontext` args cast via `uintptr_t → unsigned long` → backend-neutral
   ("argument registers, no makecontext dependency").
3. **L77-104** project structure:
   - `src/`: add `ctx_aarch64.S` and `ctx_x86_64.S`.
   - `tests/`: add `test_ctx_smoke.c` (and refresh the assertion-count label).
   - `examples/`: add `backpressure_demo.c`, `cancel_demo.c`,
     `coroutine_demo.c`, `priority_demo.c`, `resize_demo.c`.
   - `docs/`: add `architecture-review.md` and `risk-assessment.md`.
4. **L258** (Notes): "Requires a POSIX-compliant platform with `ucontext(3)`,
   `mmap(2)`, and `pthread(3)`" → ucontext(3) is only the fallback backend;
   x86-64/aarch64 use the asm backends.

**Kept unchanged:** L60 signal-handling longjmp description (still accurate).

## 5. F4 — `compile_commands.json`

- Regenerate with `cmake -S . -B build` (the export is already enabled at
  CMakeLists.txt:9).
- Commit the regenerated file to keep the existing tracking convention.

## 6. Verification

- No code changes → no clang-format, no build, no test reruns required.
- Post-edit greps:
  - `grep -n "getcontext\|makecontext\|swapcontext" docs/architecture.md
    README.md` → only the intentionally kept L413 analogy in architecture.md.
  - `grep -n "12539\|5603\|78731" README.md` → no hits.
  - `grep -c "ctx_smoke\|backpressure_demo\|ctx_x86_64" compile_commands.json`
    → > 0 (regenerated database contains the new units).
- `git status --porcelain` clean after the final commit.

## 7. Acceptance criteria

- AC1: All six architecture.md drift points corrected; L386 and L413 intact.
- AC2: All README drift points corrected (counts, structure, POSIX note,
  makecontext references).
- AC3: compile_commands.json regenerated, contains ctx_*.S / new examples /
  ctx_smoke.c entries, committed.
- AC4: No source, test, or CMake file modified.
- AC5: Working tree clean; two docs commits (`docs: de-drift architecture and
  README terminology (F2-F3)` and `docs: refresh compile_commands.json (F4)`).

## 8. Non-goals (restated)

No library behavior change. No test changes. No CMake changes.
