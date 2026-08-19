#!/usr/bin/env python3
"""Compare two loomworks bench outputs; fail if queue-depth throughput
regressed more than the threshold.  Inputs are raw `bench --json`
stdout files (the trailing JSON object is extracted).

Threshold rationale (measured, not guessed):
  * Identical binaries on the same machine swing up to -43% worst-delta
    between consecutive runs (depth-1000 measures a sub-millisecond
    workload, dominated by worker wakeup latency).
  * Identical binaries on shared CI runners showed up to -58%.
  * The gate exists to catch O(n) enqueue regressions, which collapse
    throughput by orders of magnitude (10x-100x+), far beyond noise.
  * THRESHOLD = 0.60 sits above the measured noise floor (never fails a
    no-change push) while still catching any regression >= 2.5x."""
import json
import sys

THRESHOLD = 0.60  # 60% allowed regression on queue-depth throughput
P99_LIMIT = 2.0   # 200% allowed regression on p99 latency (initial, to be calibrated)


def load(path):
    with open(path) as f:
        text = f.read()
    start = text.find("{")
    if start < 0:
        print(f"error: no JSON object found in {path} (run bench with --json)", file=sys.stderr)
        sys.exit(1)
    return json.loads(text[start:])


def gate_p99(name, base, new):
    """Compare a percentile object {p50,p99,...} between base and new.
    Missing in the baseline (older bench build) -> warn and skip."""
    b = (base.get(name) or {}).get("p99")
    n = (new.get(name) or {}).get("p99")
    if b is None or n is None:
        print(f"warning: {name}.p99 missing in base (old bench build); skipping comparison")
        return 0
    ratio = n / b
    print(f"{name:>26}  base={b:>10.0f} ns  new={n:>10.0f} ns  ratio={ratio:>5.2f}x")
    if ratio > P99_LIMIT:
        print(f"FAIL: {name}.p99 regressed {ratio:.2f}x (limit {P99_LIMIT:.2f}x)")
        return 1
    return 0


def main():
    if len(sys.argv) != 3:
        print("usage: bench_compare.py BASE.txt NEW.txt", file=sys.stderr)
        return 2
    base = load(sys.argv[1])
    new = load(sys.argv[2])
    bq = base.get("queue_depth_tps", [])
    nq = new.get("queue_depth_tps", [])
    if not bq:
        # Baseline predates the queue_depth scenario (e.g. the merge that
        # introduced it).  Nothing to compare — pass with a warning.
        print("warning: base output has no queue_depth_tps (old bench build); skipping comparison")
        return 0
    if len(bq) != len(nq) or not nq:
        print("error: queue_depth_tps arrays differ or missing", file=sys.stderr)
        return 1
    print(f"{'depth':>10}  {'base':>12}  {'new':>12}  {'delta%':>8}")
    worst = 0.0
    for d, b, n in zip(base.get("queue_depths", []), bq, nq):
        delta = (n - b) / b * 100.0
        worst = min(worst, delta)
        print(f"{d:>10}  {b:>12.0f}  {n:>12.0f}  {delta:>7.1f}%")
    if worst < -THRESHOLD * 100.0:
        print(f"FAIL: queue-depth throughput regressed {worst:.1f}% (limit {THRESHOLD * 100:.0f}%)")
        return 1
    print(f"OK: worst delta {worst:.1f}% within {THRESHOLD * 100:.0f}% limit")
    rc = 0
    rc |= gate_p99("fairness_resp_ns", base, new)
    rc |= gate_p99("tail_latency_ns", base, new)
    if rc:
        print(f"FAIL: fairness/tail p99 above {P99_LIMIT:.2f}x limit")
        return 1
    print(f"OK: fairness/tail p99 within {P99_LIMIT:.2f}x limit (queue-depth gate above)")
    return rc


if __name__ == "__main__":
    sys.exit(main())
