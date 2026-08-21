# Benchmark Baseline

Gated metrics from `examples/bench --json` and their CI ceilings.  Values are
measured on a quiet development machine (2026-08-18); CI runners are noisy
(identical binaries swing up to -58% on shared runners), so treat every number
below as indicative, not contractual.  The gates catch algorithm-class
regressions (10-100x), not noise.

## Gated metrics

| Metric (JSON key) | What it measures | Gate |
|---|---|---|
| `queue_depth_tps[4]` | single-worker throughput at depths 1k/10k/50k/100k (best-of-10) | ≥ 60% of baseline (queue-depth O(n) regression guard) |
| `fairness_resp_ns.p99` | REALTIME response latency under continuous LOW flood | ≤ 2.0x baseline; hard sanity: < 10 ms |
| `tail_latency_ns.p99` | completion latency p99, 4 workers, N tasks | ≤ 2.0x baseline |
| `coro_switch_ns` | resume→yield→resume round-trip, asm backend | exported, not yet gated |

## Quiet-machine reference values (2026-08-18)

Machine: x86-64 Linux, gcc Debug build (`build/`), `LOOMWORKS_CTX_BACKEND=asm`.
Values: `bench --iterations 10 --tasks 2000`.

| Metric | Measured |
|---|---|
| `fairness_resp_ns` avg / p50 / p99 | 102666 / 61526 / 156420 ns |
| `tail_latency_ns` p50 / p99 / p999 | 3005971 / 4049389 / 4252102 ns |
| `coro_switch_ns` | 307.8 ns (Debug; real round-trip since multi-yield) |

> Values above are from a Debug build (unoptimized) and a loaded machine —
> indicative only.  Rerun `bench --json --iterations 100 --tasks 10000` on a
> quiet runner to refresh.  The 2.0x p99 limits are intentionally loose until
> real gate runs exist; the fairness `p99 < 10ms` sanity is the hard floor.
>
> The `coro_switch_ns` jump from ~22.7 ns to ~307.8 ns (2026-08-21) is a
> semantic change, not a regression: earlier builds timed a loop that the
> optimizer could collapse (single-yield coroutines could not re-resume),
> while the current number measures a genuine resume→yield→resume round-trip
> of a multi-yield coroutine — full stack switch plus scheduler bookkeeping.

## CLI reference

```bash
./build/examples/bench --json --iterations 20 --tasks 10000
python3 tools/bench_compare.py base.json new.json
```

`tools/bench_compare.py` exits non-zero when a gate fails: queue-depth
throughput below 60% of baseline, or `fairness_resp_ns.p99` /
`tail_latency_ns.p99` above 2.0x baseline.  A baseline JSON missing the p99
keys (older bench builds) is skipped with a warning.