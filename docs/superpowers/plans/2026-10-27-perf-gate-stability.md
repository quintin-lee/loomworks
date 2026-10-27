# Perf-Gate Stability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Performance workflow's p99 latency gates first-try-stable: median-of-3 gate in `tools/bench_compare.py`, 3× per-side bench runs at `--iterations 300` in `.github/workflows/perf.yml`, no threshold changes, no library/`bench.c` changes.

**Architecture:** Each side's three bench JSON outputs are synthesized into one dict (element-wise median over the percentile objects, `dag_throughput_tps`, and index-aligned `queue_depth_tps`); the existing gate functions then run unchanged on the synthesized dicts. The tool keeps its legacy 2-file form for local one-shot checks.

**Tech Stack:** Python 3 (stdlib only), GitHub Actions YAML. Spec: `docs/superpowers/specs/2026-10-27-perf-gate-stability-design.md`.

**Repo context the worker does not know:**
- `tools/bench_compare.py` currently: `main()` takes exactly 2 files (`BASE NEW`); `gate_p99(name, base, new)` compares `p99` of three objects — `dag_latency_ns`, `fairness_resp_ns`, `tail_latency_ns` — against `P99_LIMIT = 2.0`; queue-depth throughput and `dag_throughput_tps` use `THRESHOLD = 0.65`. Both limits are OUT OF SCOPE — do not touch.
- `bench.c` fairness p99 at 20 probes is the 2nd-largest sample (`samples[(int)((20-1)*0.99)]` = index 18). At 300 it is index 296 — a true ~1% quantile. No `bench.c` code changes: `--iterations` is a plain CLI arg (`examples/bench.c:826`).
- `bench_queue_depth` already runs best-of-10 internally; the 65% throughput gate is stable and needs no medianing logic changes (we still median its input values — harmless, consistent).
- Commit style: Conventional Commits with emoji, e.g. `ci(perf): 👷 …`, `docs(changelog): 📝 …`.

---

## Task 1: bench_compare.py — median-of-3 with legacy fallback

**Files:**
- Modify: `tools/bench_compare.py` (add `PCTL_OBJECTS`, `median_of_3`, `synthesize`; rewrite `main()`'s argument handling only; gate functions untouched)

- [ ] **Step 1: Write the failing test (fixture generator + cases, in /tmp — throwaway, NOT committed)**

Run:

```bash
mkdir -p /tmp/pgfx && python3 - <<'EOF'
import json, pathlib
base = dict(queue_depths=[1000,10000,50000,100000],
            queue_depth_tps=[3100000,3100000,3100000,3100000],
            dag_throughput_tps=700000.0,
            dag_latency_ns={"p50":160000.0,"p99":300000.0},
            fairness_resp_ns={"avg":20000.0,"p50":15000.0,"p99":53000.0})
def mk(name, tail_p99):
    d = dict(base); d["tail_latency_ns"]={"p50":1100000.0,"p99":tail_p99,"p999":2400000.0}
    pathlib.Path(f"/tmp/pgfx/{name}.json").write_text(json.dumps(d))
for i in (1,2,3): mk(f"base{i}", 1500000.0)
mk("base_blip", 3300000.0)               # one blippy base run
for i in (1,2,3): mk(f"new{i}", 1500000.0)
for i in (1,2,3): mk(f"new_regr{i}", 4000000.0)
EOF
# Case A (blip immunity) — expect PASS rc=0 AFTER implementation
python3 tools/bench_compare.py \
  /tmp/pgfx/base1.json /tmp/pgfx/base_blip.json /tmp/pgfx/base2.json \
  /tmp/pgfx/new1.json /tmp/pgfx/new2.json /tmp/pgfx/new3.json; echo "A rc=$?"
# Case B (real regression) — expect FAIL rc=1 after implementation
python3 tools/bench_compare.py \
  /tmp/pgfx/base1.json /tmp/pgfx/base2.json /tmp/pgfx/base3.json \
  /tmp/pgfx/new_regr1.json /tmp/pgfx/new_regr2.json /tmp/pgfx/new_regr3.json; echo "B rc=$?"
# Case C (legacy 2-file form must still work)
python3 tools/bench_compare.py /tmp/pgfx/base1.json /tmp/pgfx/new1.json; echo "C rc=$?"
```

Expected BEFORE implementation: Case A and B fail with `usage` error rc=2 (6-arg form unsupported); Case C passes rc=0.

- [ ] **Step 2: Run the failing cases to confirm**

Run: (the three commands from Step 1). Expected: `A rc=2`, `B rc=2` (usage error), `C rc=0`.

- [ ] **Step 3: Add the synthesis code**

Insert after `gate_dag()` in `tools/bench_compare.py` (before `def main():`):

```python
PCTL_OBJECTS = ("tail_latency_ns", "fairness_resp_ns", "dag_latency_ns")


def median_of_3(values):
    """Middle value after sorting.  Non-numeric/missing inputs are
    excluded; all-missing -> None so callers keep their warn+skip
    contract.  With 2 present values the larger (conservative for p99)."""
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return None
    vals.sort()
    return vals[len(vals) // 2]


def synthesize(paths):
    """Collapse per-side bench JSON files into one dict: element-wise
    median across files for each percentile object, dag_throughput_tps,
    and the index-aligned queue_depth_tps array.  Gates then run on the
    stable middle value instead of one noisy run."""
    docs = [load(p) for p in paths]
    out = {}
    for name in PCTL_OBJECTS:
        fields = {}
        for doc in docs:
            obj = doc.get(name)
            if not isinstance(obj, dict):
                continue
            for metric, value in obj.items():
                fields.setdefault(metric, []).append(value)
        out[name] = {
            m: median_of_3(vals)
            for m, vals in fields.items()
            if median_of_3(vals) is not None
        }
    dag = median_of_3([d.get("dag_throughput_tps") for d in docs])
    if dag is not None:
        out["dag_throughput_tps"] = dag
    qd = [d.get("queue_depth_tps") for d in docs if isinstance(d.get("queue_depth_tps"), list)]
    if qd:
        width = min(len(q) for q in qd)
        out["queue_depth_tps"] = [
            median_of_3([q[i] for q in qd]) for i in range(width)
        ]
        out["queue_depths"] = docs[0].get("queue_depths", [])
    return out
```

- [ ] **Step 4: Rewrite only `main()`'s argument handling**

Replace the head of `main()` in `tools/bench_compare.py`:

```python
def main():
    if len(sys.argv) not in (3, 7):
        print(
            "usage: bench_compare.py BASE NEW | BASE1 BASE2 BASE3 NEW1 NEW2 NEW3",
            file=sys.stderr,
        )
        return 2
    if len(sys.argv) == 7:
        print("median-of-3 synthesis: base=argv[1:4], new=argv[4:7]")
        base = synthesize(sys.argv[1:4])
        new = synthesize(sys.argv[4:7])
    else:
        base = load(sys.argv[1])
        new = load(sys.argv[2])
```

Everything from `bq = base.get("queue_depth_tps", [])` down to `return rc` stays byte-identical.

- [ ] **Step 5: Run the passing cases**

Run:

```bash
python3 -m py_compile tools/bench_compare.py && echo COMPILED
python3 tools/bench_compare.py \
  /tmp/pgfx/base1.json /tmp/pgfx/base_blip.json /tmp/pgfx/base2.json \
  /tmp/pgfx/new1.json /tmp/pgfx/new2.json /tmp/pgfx/new3.json; echo "A rc=$?"
python3 tools/bench_compare.py \
  /tmp/pgfx/base1.json /tmp/pgfx/base2.json /tmp/pgfx/base3.json \
  /tmp/pgfx/new_regr1.json /tmp/pgfx/new_regr2.json /tmp/pgfx/new_regr3.json; echo "B rc=$?"
python3 tools/bench_compare.py /tmp/pgfx/base1.json /tmp/pgfx/new1.json; echo "C rc=$?"
python3 tools/bench_compare.py /tmp/pgfx/base1.json; echo "D rc=$?"
```

Expected: `A rc=0` with `OK: dag/fairness/tail p99 within 2.00x limit` (blip suppressed: base tail median = 1500000, new = 1500000, ratio 1.00x). `B rc=1` with `FAIL: tail_latency_ns.p99 regressed 2.67x (limit 2.00x)` (4000000/1500000). `C rc=0` (legacy form). `D rc=2` (usage error — 1 file is rejected).

- [ ] **Step 6: Commit**

```bash
git add tools/bench_compare.py
git commit -m "ci(perf): 👷 median-of-3 p99 gate with backward-compatible compare tool

bench_compare.py now accepts BASE1 BASE2 BASE3 NEW1 NEW2 NEW3 and
compares the element-wise median of each side's three runs, so a
single noisy bench run can no longer trip the 2.00x p99 gate;
consistent regressions on 2 of 3 runs still fail.  The legacy 2-file
form is unchanged for local one-shot checks."
```

---

## Task 2: perf.yml — 3× per side at 300 iterations, 6-arg gate

**Files:**
- Modify: `.github/workflows/perf.yml` (bench steps, compare step, artifact paths)

- [ ] **Step 1: Replace the two "Run benchmarks" steps**

In `.github/workflows/perf.yml`, replace:

```yaml
- name: Run benchmarks (current)
  run: |
    cd build
    ./examples/bench --json --iterations 20 --tasks 10000 2>&1 | tee bench_new.json
```

with:

```yaml
- name: Run benchmarks (current, 3 runs)
  run: |
    cd build
    for i in 1 2 3; do
      ./examples/bench --json --iterations 300 --tasks 10000 2>&1 | tee "bench_new_${i}.json"
    done
```

and replace:

```yaml
- name: Run benchmarks (baseline)
  run: |
    cd /tmp/base/build
    ./examples/bench --json --iterations 20 --tasks 10000 2>&1 | tee bench_base.json
```

with:

```yaml
- name: Run benchmarks (baseline, 3 runs)
  run: |
    cd /tmp/base/build
    for i in 1 2 3; do
      ./examples/bench --json --iterations 300 --tasks 10000 2>&1 | tee "bench_base_${i}.json"
    done
```

Rationale: `--iterations 300` makes the fairness/DAG p99 a true ~1% quantile (300 samples instead of 20); the 3 runs feed Task 1's median-of-3 gate.

- [ ] **Step 2: Point the compare step at 6 files**

Replace the "Compare and gate" step's `run` body:

```yaml
- name: Compare and gate
  run: |
    python3 tools/bench_compare.py /tmp/base/build/bench_base.json build/bench_new.json
```

with:

```yaml
- name: Compare and gate (median of 3)
  run: |
    python3 tools/bench_compare.py \
      /tmp/base/build/bench_base_1.json /tmp/base/build/bench_base_2.json /tmp/base/build/bench_base_3.json \
      build/bench_new_1.json build/bench_new_2.json build/bench_new_3.json
```

- [ ] **Step 3: Update the artifact upload paths**

In the "Upload benchmark results" step, replace:

```yaml
    path: |
      build/bench_new.json
      /tmp/base/build/bench_base.json
```

with:

```yaml
    path: |
      build/bench_new_1.json
      build/bench_new_2.json
      build/bench_new_3.json
      /tmp/base/build/bench_base_1.json
      /tmp/base/build/bench_base_2.json
      /tmp/base/build/bench_base_3.json
```

- [ ] **Step 4: Verify YAML sanity**

Run:

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/perf.yml')); print('YAML OK')"
```

Expected: `YAML OK`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/perf.yml
git commit -m "ci(perf): 👷 run bench 3x per side at 300 iterations

Feeds the median-of-3 gate: a single scheduler blip in any one run
can no longer fail the p99 limits, while a regression present on
2 of 3 runs still trips them.  Fairness/DAG probes rise from 20 to
300 so their p99 is a real ~1% quantile.  +4 bench runs (~45-120s),
negligible against the 6h job timeout."
```

---

## Task 3: CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md` (the `## [Unreleased]` section, currently empty)

- [ ] **Step 1: Add a Changed section under [Unreleased]**

In `CHANGELOG.md`, insert directly under `## [Unreleased]` (which is currently followed by a blank line then `## [1.1.0]`):

```markdown
## [Unreleased]

### Changed
- **Performance gate stability (CI)**: the CI p99 latency gate now
  compares the median of 3 benchmark runs per side instead of a single
  run, and the fairness/DAG scenarios use 300 probes instead of 20
  (a true ~1% quantile).  Eliminates spurious gate failures from
  single-run noise; consistent regressions still fail the unchanged
  2.00× p99 / 65% throughput limits.

```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): 📝 note perf-gate median-of-3 change"
```

---

## Task 4: Local rehearsal, push, watch Performance

**Files:** none (verification only).

- [ ] **Step 1: Local 3×3 rehearsal (no-change simulation)**

```bash
cmake --build build -j$(nproc)
cd build
for i in 1 2 3; do ./examples/bench --json --iterations 300 --tasks 10000 > /tmp/pgfx/re_new_$i.json; done
for i in 1 2 3; do cp /tmp/pgfx/re_new_$i.json /tmp/pgfx/re_base_$i.json; done
cd ..
python3 tools/bench_compare.py \
  /tmp/pgfx/re_base_1.json /tmp/pgfx/re_base_2.json /tmp/pgfx/re_base_3.json \
  /tmp/pgfx/re_new_1.json /tmp/pgfx/re_new_2.json /tmp/pgfx/re_new_3.json
```

Expected: every ratio ≈1.00x, `OK: queue-depth … within 65% limit`, `OK: dag/fairness/tail p99 within 2.00x limit`, exit 0. (Same binary on both sides — the no-change case the gate must pass.)

- [ ] **Step 2: Push and watch the Performance run to first-try green**

```bash
git push
gh run list --limit 1 --workflow perf.yml --json databaseId --jq '.[0].databaseId'
gh run watch <PERF_RUN_ID> --exit-status
```

Expected: run completes `success` WITHOUT needing `gh run rerun`. If it fails, read the `Compare and gate` log: any `FAIL: …p99 regressed` despite median-of-3 is an out-of-scope finding — record the three raw values (now uploaded as artifacts) and report; do NOT widen scope by relaxing thresholds.

- [ ] **Step 3: Verify CI job also stays green**

```bash
gh run list --limit 1 --workflow ci.yml --json databaseId,conclusion
```

Expected: `success` (perf.yml change does not affect ci.yml, but confirm the push was clean).
