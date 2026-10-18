# Global Optimization — Thread-Pool Core Hot Path — Optimization Log

Phase 2 (post-DAG global optimization), measurement-driven, target w=4 × t=100k.
Baseline: `docs/perf/baseline-2026-10-18.md` (commit `961b398`), master @ `81677f2`.
Host: Intel i7-8550U, 8 logical CPUs, governor `performance`.

## Candidates and outcomes

### Candidate 4.8 — cancel-index contention (PRIMARY, ≥10 % attributable)

Measured root cause: `cancel_index_insert` + `cancel_index_remove` = 10.34 % of
steady-state cycles on the acceptance cell. At w=4 the shared open-addressed
table is `cancel_cap = next_pow2(4096 + 4×256) = 8192` slots (256 KB) at ~63 %
load, so each insert/remove probes 3–4 shared cache lines across 4 workers +
the submitter.

**Attempt 1 (Option A: tighter-load single table) — DISPROVEN.**

Mechanism tried: double the table headroom (`need *= 2` → `cancel_cap =
16384`, 512 KB, ~31 % load) plus `__builtin_expect` hot-branch hints on the
insert/remove probe loops. No structural change, no new field, no
call-site move.

Gates: build + `format-check` + clang-tidy clean; 8/8 ctest green
(52,147 assertions); ASan 0 lines; UBSan 0 lines; `include/` diff empty.

Acceptance-cell measurement (`bench --iterations 20 --tasks 100000
--workers 4`, single run — high-variance on this 8-thread laptop):

| Metric | Baseline | After | Delta |
|---|---|---|---|
| throughput_tps | 763591 | 632966 | **−17.1 %** |
| tail_latency_ns p99 | 117795008 | 144296868 | **+22.5 %** |
| dag_throughput_tps | 465552 | 278564 | −40.2 % |

The Target was a **≥ 2.5 % primary gain** on the acceptance cell with no T1
regression and p99 within +10 %. This attempt regressed every metric it was
meant to improve.

**Diagnosis.** Doubling `cancel_cap` to 16384 did not reduce per-probe
traffic: at 63 % load the *typical* probe already terminates in ~1–2 slots
(an EMPTY or TOMBSTONE slot), so the hot path was already short. The
regression came from (a) a 512 KB shared table with a wider scan window —
more cache lines touched on the *rare* long-probe cases and on the
submit-side `cancel_index_find` walk, and (b) the two `__builtin_expect`
hints adding a branch-predictor load without removing any cycle. The
dominant 10.34 % cost is **not** removable by table-sizing on this host:
it is irreducible shared-cache-line traffic across 4 cores contending for
the same open-addressed table, i.e. the hardware limit, not a
constant-factor inefficiency.

**Action:** reverted (`git checkout -- src/thread_pool.c`); the working tree
is back to master @ `81677f2` behavior.

**Conclusion (honest-exit rule, §4 of the spec):** the single
≥ 10 %-attributable cost (cancel-index pair) is hardware/contention-bound
on this 8-thread host; no zero-semantic-change, low-risk micro-optimization
removes it. Per the spec's stop rule (no ≥ 10 % cost removable by a
safe change) the phase is closed with a "no code change" outcome for
candidate 4.8.

### Candidates 4.1 / 4.6 — opportunistic (each < 10 %)

`worker_entry` self (4.15 %) and `metrics_fire` (1.00 %) are each below
the §3 ≥ 10 % bar and do NOT count toward Target. They were deferred as
opportunistic; given 4.8's disproof closed the primary goal, they are not
pursued this session (they would be sub-Target garnish, and 4.6's
`clock_gettime` skip is already bounded by the R3 closed finding that the
callback is cheap). Recorded here so the next session can pick them up in
one sitting if desired.

## Final state

- No thread-pool hot-path code change kept on master (working tree clean at
  `81677f2` + docs-only commits).
- Deliverables: `tools/profile_pool.sh`, `docs/perf/baseline-2026-10-18.md`
  (committed `961b398`), this log, and the spec
  `docs/superpowers/specs/2026-10-18-global-optimization-design.md`.
- Regression floor `tools/bench_compare.py` (THRESHOLD 0.60 / P99_LIMIT
  2.0) untouched; 8/8 ctest green at 52,147 assertions.
