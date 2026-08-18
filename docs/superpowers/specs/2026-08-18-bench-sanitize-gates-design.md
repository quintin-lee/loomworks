# Performance Baselines + Sanitizer Gates Design

> **Status:** Approved 2026-08-18 by user during brainstorming. Scope locked:
> thread pool + coroutine only. No new public API (existing
> `loom_pool_submit_priority` covers REALTIME submission). No subagents
> (user standing ban).

## 1. Background

Risk register is clear (19/19 closed or recorded as accepted trade-offs).
This round moves from "closing risks" to "making performance and
correctness *provable*":

1. **Sanitizer matrix is nominal, not enforced**: `ci.yml` `sanitize:` job
   runs ASan/TSan/UBSan but every step is `continue-on-error: true` plus
   `|| true` — failures never block CI. ASan/UBSan additionally rely on a
   brittle `LD_PRELOAD` injection instead of the CMake-linked sanitizer.
2. **Benchmarks exist but under-exported**: `examples/bench.c` measures
   submit_latency / throughput / worker_scaling / parallel_scaling /
   bounded_queue / future_overhead / coro_create_destroy / queue_depth
   (~512 lines, single binary, `--json` mode). But JSON output only exports
   `submit_latency_avg_ns`, `throughput_tps`, `queue_depths`,
   `queue_depth_tps`; `perf.yml` only gates `queue_depth_tps` at 60%.

## 2. Goals (production-grade provability)

- **G1 — Sanitizer gates actually block**: ASan/UBSan become hard CI gates
  (no `continue-on-error`); TSan is best-effort with a documented strategy.
- **G2 — New benchmark scenarios**: priority fairness, tail latency
  (p50/p99/p999), coroutine switch latency.
- **G3 — Baseline hardened**: JSON export covers all gated scenarios;
  `bench_compare.py` extends regression gates to fairness + tail latency;
  `perf.yml` exercises them; a baseline document records expected values
  with CI-noise caveats.

Non-goals (scope fence): no new public API; no new benchmark binary; no
test-suite restructuring; no unrelated refactors of pool internals.

## 3. Scenario designs (three new functions in `examples/bench.c`)

### 3.1 `bench_priority_fairness` — REALTIME response under LOW flood

**Purpose**: verify the documented priority contract (lane bypass of ring)
under pressure: a REALTIME task submitted during a LOW-priority flood must
not be starved.

**Mechanics**:
- `worker_count = 0` (auto = 2×cpu, clamped 64). `queue_capacity = 0`.
- Phase 1 (flood): **one pthread producer submits LOW tasks in a loop for
  the entire duration of the probe phase** (not a fixed batch — a fixed
  batch of `g_task_count` drains in µs, far shorter than the probe window,
  so the flood must be *continuous* until probing ends). Flood task = tiny
  busy work (e.g. `volatile` counter loop) so workers stay saturated.
- Phase 2 (probe): while the flood runs, the main thread submits a REALTIME
  probe task (`LOOMWORKS_PRIORITY_REALTIME = 0`) whose fn stamps the time at
  entry into an atomic, then `loom_future_wait`s it. Repeat `g_iterations`
  probes at ~50 µs intervals.
- Metric: response latency = entry-stamp − submit-stamp, ns. Report
  avg/p50/p99 across probes. **Gate criterion**: p99 must stay under a
  sanity bound (e.g. < 10 ms REGARDLESS of flood depth) — starvation would
  show ms-scale wakeup or worse.
- JSON export: `fairness_resp_ns` object `{avg, p50, p99}`.

**Existing API used**: `loom_pool_submit_priority`, `loom_pool_submit_future_priority`,
`loom_future_wait`, standard pool lifecycle. No header changes.

### 3.2 `bench_tail_latency` — completion-latency p50/p99/p999

**Purpose**: surface lock contention / scheduler jitter that mean latency
hides. Production systems care about tails.

**Mechanics**:
- `worker_count = 4`, `queue_capacity = 0`.
- Submit `N = g_task_count` tasks; each task fn records
  `monotonic_now() − submit_ts` (submit_ts passed via `user_data` through a
  pre-allocated array; task index in `user_data` too) into a pre-allocated
  `double[N]`.
- After shutdown, sort the array (fixed window, `qsort`), report
  p50 / p99 / p999.
- Metric units: ns. Gate: relative regression check in `bench_compare.py`
  (baseline p99 drift beyond measured noise floor → fail), same pattern as
  queue_depth's 60% threshold but calibrated for tail metrics.
- JSON export: `tail_latency_ns` object `{p50, p99, p999}`.

### 3.3 `bench_coro_switch` — yield/resume round-trip latency

**Purpose**: quantify context-switch backend cost (asm vs ucontext). The
existing `coro_create_destroy` measures lifecycle only; switching is the
hot path.

**Mechanics**:
- Create one coroutine that loops `yield` `R` times; main thread
  `resume`s it `R` times, timing `R` resume→yield→resume round-trips.
- Metric: avg ns per resume-yield round-trip. `R` default
  `= g_task_count` (=10000); warmup 100 iterations to settle page faults.
- Report the backend in the output string for cross-checks (build-time
  `LOOMWORKS_CTX_BACKEND`).
- JSON export: `coro_switch_ns` (avg).

## 4. JSON + gate wiring

### 4.1 bench.c `--json` output (extended)

```json
{
  "benchmark": "loomworks",
  "iterations": 20,
  "task_count": 10000,
  "submit_latency_avg_ns": ...,
  "throughput_tps": ...,
  "queue_depths": [...],
  "queue_depth_tps": [...],
  "fairness_resp_ns": {"avg": ..., "p50": ..., "p99": ...},
  "tail_latency_ns": {"p50": ..., "p99": ..., "p999": ...},
  "coro_switch_ns": ...
}
```

`coro_create_destroy` stays human-readable-only (not gated).

### 4.2 `tools/bench_compare.py`

- Keep existing `queue_depth_tps` 60% gate unchanged.
- **New gate — fairness**: `fairness_resp_ns.p99` must not exceed
  BASE×threshold (initial 2.0, calibrated by measurement before enabling).
  Starvation manifests as orders-of-magnitude growth, far above noise.
- **New gate — tail latency**: `tail_latency_ns.p99` drift beyond
  calibrated floor (initial 2.0×) fails.
- New metrics missing in base (older bench build) → warning + skip, same
  pattern as the existing queue_depth back-compat path.

### 4.3 `.github/workflows/perf.yml`

- Unchanged invocation (`--json --iterations 20 --tasks 10000`) — new
  metrics arrive automatically in the JSON; `bench_compare.py` evaluates
  the new gates.
- No new CI job needed.

## 5. Sanitizer gating plan

### 5.1 Local triage (first implementation step)

Run all four tests under each of `ASan`, `TSan`, `UBSan` locally; record:

- ASan/UBSan pass/fail per test (expect pass; fix any real findings).
- TSan: enumerate races. Known-innocent lock-free patterns (ABA-tagged
  Treiber stack, Vyukov ring, Chase-Lev deques, node pool, metrics atomics)
  will show up — decide per-race: suppress (with justified comment) vs fix.

### 5.2 ci.yml `sanitize:` job rewrite

```yaml
sanitize:
  runs-on: ubuntu-latest
  strategy:
    matrix:
      build_type: [ASan, UBSan, TSan]   # TSan status = triage-dependent
  steps:
    ... configure/build with matrix.build_type ...
    - name: Test
      env:
        UBSAN_OPTIONS: halt_on_error=1   # UB must fail the job (default prints + continues)
        ASAN_OPTIONS: detect_leaks=1
      run: cd build && ctest --output-on-failure   # NO || true
```

- **ASan/UBSan**: remove `continue-on-error` and the `LD_PRELOAD` line —
  the CMake build-type flags already link the runtime; plain `ctest` runs
  instrumented binaries. Any leak/UB finding fails the job.
- **TSan**: decisions deferred to triage —
  - If races are few and suppressible → add `tools/tsan.supp`
    (documented) + `TSAN_OPTIONS="suppressions=... history_size=7"` +
    hard gate.
  - If TSan is unusable on the lock-free core → keep the job but with an
    explicit `:unstable:` comment and non-blocking report, recorded as an
    open item in the risk register (new row).
- Note: TSan + coroutine asm/ucontext backends are a known pairing risk;
  triage must record backend under test.

## 6. Baseline documentation

`docs/benchmark-baseline.md` (new):
- Table of the gated metrics + measured values on a quiet local machine:
  queue_depth_tps per depth, fairness p99, tail p50/p99/p999, coro_switch.
- **Explicit CI-noise caveat** (verbatim from bench_compare.py comments):
  identical binaries swing up to −58% on shared CI runners; gates are
  calibrated to catch algorithm-class regressions (O(n) enqueue = 10–100×,
  starvation = ms-scale) — NOT micro-noise.
- Front-matter note: values are indicative, re-measure before trusting.

## 7. Commits (expected shape)

1. `bench: add priority-fairness, tail-latency, coro-switch scenarios`
   (examples/bench.c)
2. `ci: enforce ASan/UBSan gates, best-effort TSan` (.github/workflows/ci.yml)
3. `ci: extend perf gates to fairness and tail latency`
   (tools/bench_compare.py, .github/workflows/perf.yml)
4. `docs: add benchmark baseline` (docs/benchmark-baseline.md, CHANGELOG
   entry)

No code changes to `src/` are anticipated (no new API, no internals
touched). If triage surfaces a genuine defect, it becomes its own
fix+test commit with a risk-register row.

## 8. Testing strategy

- New bench scenarios are not unit tests — they are measurement tools.
  Correctness is asserted indirectly: every number must be produced and
  exported to JSON (a crash/silent-miss = bench exits non-zero = CI gates
  fail).
- Existing test suite (20771 pool / 5611 coro / 78759 integration /
  ctx_smoke) unchanged and still expected green under all build types
  (Debug default; ASan/UBSan now hard-gated in CI).

## 9. Non-goals (restated)

- No new public API surface.
- No change to scheduler semantics (fairness scenario *measures* the
  existing contract, it does not alter it).
- No benchmark binary split; no fuzzing (separate round).
- No changes to `src/thread_pool.c` / `src/coroutine.c` unless triage
  finds a real defect.