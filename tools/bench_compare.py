#!/usr/bin/env python3
"""Compare two loomworks bench outputs; fail if queue-depth throughput
regressed more than the threshold.  Inputs are raw `bench --json`
stdout files (the trailing JSON object is extracted)."""
import json
import sys

THRESHOLD = 0.15  # 15% allowed regression on queue-depth throughput


def load(path):
    with open(path) as f:
        text = f.read()
    start = text.find("{")
    if start < 0:
        print(f"error: no JSON object found in {path} (run bench with --json)", file=sys.stderr)
        sys.exit(1)
    return json.loads(text[start:])


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
    return 0


if __name__ == "__main__":
    sys.exit(main())
