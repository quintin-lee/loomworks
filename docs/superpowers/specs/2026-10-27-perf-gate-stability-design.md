# Performance-Gate Stability — Design

Date: 2026-10-27
Status: approved for implementation
Baseline: master @ `95d8553` (v1.1.0 released, CI 11/11 green; Performance
passing but needed 2 of 4 recent runs rerun by hand on p99-gate trips)

## 1. Objective

Make the Performance workflow's p99 latency gates first-try-stable without
weakening their ability to catch real regressions.

Observed failure mode: `bench_compare.py` compares a single `--json` run of
the current tree against a single `--json` run of the baseline tree and fails
when any p99 ratio exceeds `P99_LIMIT = 2.0x`. Two distinct sources of
instability were measured:

| Gate | Sampling | Root cause of flakiness |
|---|---|---|
| `fairness_resp_ns.p99` | `--iterations 20` → 20 samples, `p99 = samples[18]` (bench.c:541) | A "p99" over 20 samples is the 2nd-largest value — statistically meaningless; one scheduler blip decides it. Measured swing 0.31x–2.58x on identical binaries. |
| `tail_latency_ns.p99` | `--tasks 10000` → true 1% quantile | Ratio of two independent single-run tail percentiles; shared-runner background noise makes the two noise tails ratio badly. Measured 2.34x on a no-change comparison. |

The queue-depth throughput gate is already stable (best-of-10 **inside**
`bench_queue_depth`, bench.c:342) and is not part of this change.

User-confirmed decisions:
- Approach A: fix the sampling root cause AND medianize the gate.
- `--iterations 20 → 300` in the perf workflow only (bench.c defaults
  unchanged).
- No threshold changes (`P99_LIMIT` stays 2.0, `THRESHOLD` stays 0.65).
- +1–2 min wall-clock on the bench job is acceptable.
- No library code and no `bench.c` JSON-schema changes.

## 2. Mechanisms

### 2.1 Fairness probe count: 20 → 300 (perf.yml only)

Both bench invocations in the Performance workflow change from
`--iterations 20` to `--iterations 300`.

Effect on each `g_iterations`-scaled scenario (bench.c):
- **priority_fairness**: p99 moves from the 19th-largest of 20 to the
  ~3rd-largest of 300 — a true ~1% quantile. Cost: 300 probes × 50 µs
  inter-probe pause ≈ 15 ms of probes plus warmup — negligible.
- **submit_latency**: 300 instead of 20 timing samples — strictly better.
- **dag**: `bench_dag_throughput(g_iterations)` (bench.c:805) uses the same
  count — consistent with the new fairness sample size.
- **tail_latency**: driven by `--tasks` (10000), unaffected by iterations.

Not changed: `bench.c` defaults (`g_iterations = 100`), the local
`docs/perf/baseline-*.md` files, and any other workflow. Local quick-checks
keep their current cost.

### 2.2 Median-of-3 gate (bench_compare.py)

`bench_compare.py` accepts either the legacy 2-argument form
(`BASE NEW`) or a new 6-argument form
(`BASE1 BASE2 BASE3 NEW1 NEW2 NEW3`).

In 6-argument mode, before any gate runs, each side's three JSON files are
collapsed into one synthesized dict:
- For every reported percentile object (`tail_latency_ns`,
  `fairness_resp_ns`, `dag_latency_ns`, each `{p50, p99, p999}` or
  `{avg, p50, p99}`): take the **element-wise median of the three files**
  (median of 3 = the middle value; a key missing in one file uses the
  median of the present ones; missing in all three → stays missing,
  preserving the existing "warn + skip" contract in `gate_p99`/`gate_dag`).
- `dag_throughput_tps`: median of the three files.
- `queue_depth_tps`: element-wise median across the three files at each
  depth index (arrays share the fixed `queue_depths` ordering).

The existing `gate_dag`, `gate_p99` (×3 metrics), and queue-depth
throughput logic then run **unchanged** on the two synthesized dicts.
Thresholds and limits are untouched.

Required properties (both are test acceptance criteria):
1. **Blip immunity**: one of the three base files carrying a 2.3x tail-p99
   blip, the other two normal, and all three new files normal → gate
   PASSES.
2. **Real regression still caught**: base files consistent, all three new
   files ~2.6x above base on a p99 → gate FAILS.

Rationale for median-of-3 over alternatives:
- Raising only `P99_LIMIT` (approach B) silently weakens the gate; rejected.
- A full statistical test over raw distributions (approach C) requires
  exporting raw samples from `bench.c` and is disproportionate for a CI gate.
- best-of-3 (min) would be *anti*-regression-sensitive on percentiles
  (min of noise hides a real tail regression); median is the fair
  compromise: it survives one bad run, still trips when two of three
  agree on the regression.

The legacy 2-file form keeps working so local one-shot comparisons remain
fast; the 6-file form is what CI uses.

## 3. Perf workflow changes (`.github/workflows/perf.yml`)

- **Run benchmarks (current)**: loop 3, writing
  `bench_new_1.json … bench_new_3.json` (each `./examples/bench --json
  --iterations 300 --tasks 10000`).
- **Run benchmarks (baseline)**: loop 3, writing
  `bench_base_1.json … bench_base_3.json` in `/tmp/base/build`.
- **Compare and gate**:
  `python3 tools/bench_compare.py /tmp/base/build/bench_base_1.json
  /tmp/base/build/bench_base_2.json /tmp/base/build/bench_base_3.json
  build/bench_new_1.json build/bench_new_2.json build/bench_new_3.json`
- **Upload benchmark results**: keep all six JSONs as artifacts
  (cheap, and preserves per-run evidence when a gate trips).

Job has no `timeout-minutes` (default 6 h); four extra bench runs
(≈ 45–120 s total at these parameters) are negligible.

## 4. Files touched

| File | Change |
|---|---|
| `.github/workflows/perf.yml` | iterations 300; 3× per side; 6-arg compare; artifact keeps 6 files |
| `tools/bench_compare.py` | 6-arg form + per-side median-of-3 synthesis; legacy 2-arg form intact |
| `CHANGELOG.md` | `[Unreleased]` / new `### Changed` line: perf gate now median-of-3; fairness 300 probes |

No changes to `bench.c`, library sources, headers, or tests.

## 5. Testing

Throwaway fixture files (created in `/tmp`, **not** committed):
1. `blip` case — three base files with one 2.3x tail-p99 outlier + three
   normal new files → expect exit 0.
2. `regression` case — consistent bases, three new files ~2.6x on
   tail p99 → expect exit 1 with the FAIL line.
3. `legacy` case — two-file invocation → unchanged behavior (existing
   single-file semantics).

Verification sequence:
1. `python3 -m py_compile tools/bench_compare.py`
2. Run the three fixture cases.
3. Full local gate rehearsal: build Release, run the 3×3 bench flow, run
   the 6-arg compare on the real outputs.
4. Push → `gh run watch <perf-id> --exit-status`. Acceptance: the run
   passes **on the first attempt** (no manual rerun). One rerun is
   tolerated if the trip is clearly a fresh noise draw and is logged in
   the changelog note as follow-up evidence.

## 6. Release placement

CI/test-reliability change only; ships in **1.1.1** (patch: no public API
change, user-visible effect is "fewer spurious perf failures"). No
version-bump in this change itself; recorded under `[Unreleased]`.

## 7. Spec self-review

- No placeholders / TBD.
- Consistency: `P99_LIMIT`/`THRESHOLD` untouched everywhere they are
  mentioned; bench.c JSON schema untouched (2.1 + §4).
- Scope: three files, one purpose; single implementation plan.
- Ambiguity resolved: median-of-3 (not best/worst-of-3); 6-arg form
  synthesized per side, not per metric-pair; legacy 2-arg form retained.
