# Global Optimization — Thread-Pool Core Hot Path — Design

Date: 2026-10-18
Status: approved for implementation
Baseline: master @ 81677f2 (Task DAG landed; 8/8 ctest green at 52,147 assertions)
Phase: post-DAG global optimization (phase 2 of the original request; phase 1 = Task DAG, complete)

## 1. Objective

Reduce the constant-factor cost and contention of the thread-pool core hot
path — `worker_entry` dequeue/dispatch, the three-tier queue
(ring/lane/deque), the submit/cancel index, and the task node pool — to
raise throughput and cut tail latency, with **zero observable behaviour
change**.

Four hard invariants bind every change:

1. **Public API/ABI frozen.** Only `.c` files and internal headers may change.
   No public header edit, no new public symbol, no ABI shift.
2. **Zero semantic change.** All 8 ctest suites (52,147 assertions) stay
   green, and every documented contract is preserved verbatim: R5 cancel
   linear scan, R7 LIFO-local/FIFO-steal ordering, priority routing,
   NORMAL+ring FIFO, full-queue → `ERR_INVALID`, shutdown → `ERR_SHUTDOWN`,
   future/cancel completion semantics.
3. **Build gates unchanged.** `format-check` + clang-tidy
   (`WarningsAsErrors: '*'`) pass; ASan/UBSan report zero.
4. **Regression floor untouched.** `tools/bench_compare.py`
   (THRESHOLD 0.60, P99_LIMIT 2.0) stays the regression floor, not the
   acceptance bar.

Non-goals: no new public API; no wake-mechanism / queue-protocol rewrite
(approach B was explicitly not chosen); no coroutine/DAG/pipeline/
task_group/metrics/runtime work beyond minimal touches where genuinely
affected; no memory-footprint or portability targets; no CI changes.

## 2. Measurement Methodology

Reproducible baseline: `tools/profile_pool.sh` (committed, `961b398`) —
fixed workloads (empty + fixed-work tasks), 1 warm-up round + 3 timed
rounds (median = round 2), `taskset -c 0-7` pinning, matrix =
workers {1,2,4,8} × `--tasks` {1k,10k,100k} × `--iterations 20`,
`perf stat` over `cycles:u, instructions:u, cache-references:u,
cache-misses:u, context-switches, cpu-migrations, branch-misses:u`,
plus `perf record -g` for attribution. Raw data in `/tmp/lw-prof/`,
summary committed at `docs/perf/baseline-2026-10-18.md`.

Host: Intel i7-8550U, 8 logical CPUs (4 cores + HT), governor `performance`,
hardware `:u` counters work unprivileged (`perf_event_paranoid = 2`).

Six baseline metrics per cell: T1 throughput (`throughput_tps`,
`queue_depth_tps[]`), T2 tail (`tail_latency_ns.p99`,
`fairness_resp_ns.p99`), T3 per-task scheduling cost
(`submit_latency_avg_ns`), T4 per-task cycles/instructions, T5 per-task
cache-misses/context-switches, T6 wake/idle behaviour.

**Anti-guessing rule:** a cost enters the optimization list only if it is
≥ 10 % of steady-state cycles AND attributable to specific lines via
`perf annotate`/`perf report`. Anything below is recorded as unproven and
left alone.

## 3. Measured Baseline (docs/perf/baseline-2026-10-18.md)

Acceptance cell **w=4 × t=100k (median of 3, taskset 0-7):**

| Metric | Baseline |
|---|---|
| wall_s | 6.012 |
| throughput_tps | 763591 |
| submit_latency_avg_ns | 2736.2 |
| tail_latency_ns p50 / p99 | 23 653 612 / 117 795 008 |
| fairness_resp_ns p50 / p99 | 1 148 512 / 3 168 804 |
| dag_throughput_tps | 465552 |
| dag_latency_ns p50 / p99 | 1 096 024 / 160 078 |
| cycles:u (r2) | 18 971 502 805 |
| instructions:u (r2) | 19 571 106 915 (IPC ≈ 1.03) |
| cache-misses:u (r2) | 15 608 951 |
| context-switches / cpu-migrations | 0 / 0 (pinning holds) |
| branch-misses:u (r2) | 14 671 299 |

First-order findings: (a) IPC ≈ 0.8–1.0 across all cells → not
memory-stall-bound at the machine level; (b) tail p99 grows ~4.5× with task
count even at w=1 → dominated by queueing/steal interference, not raw work;
(c) throughput drops at w=4/8 vs w=1/2 at t=1k (1530k → 560k → 554k tps) →
contention/steal overhead dominates small task sets.

**`perf report` self-share on the acceptance cell (worker.data, no-children):**

| Function | Self % |
|---|---|
| `cpu_task` (bench workload) | 53.15 % (not optimizable) |
| `cancel_index_remove` | 5.24 % |
| `cancel_index_insert` | 5.10 % |
| `worker_entry` self | 4.15 % |
| `deque_pop` | 2.79 % |
| `ring_try_enqueue` self | 2.43 % |
| `task_destroy; node_stack_push` | 1.29 % |
| `deque_push` | 1.10 % |
| `ring_bulk_try_dequeue` | 1.14 % |
| `metrics_fire` | 1.00 % |
| `lane_has_priority` | 0.86 % |
| `pthread_mutex_lock` | 0.83 % |

**The only ≥ 10 % attributable cost is the cancel-index pair**
(`cancel_index_insert` + `cancel_index_remove` ≈ 10.34 % of the whole
process). Root cause (`src/thread_pool.c`): `LOOMWORKS_RING_DEFAULT_SLOTS =
4096` (src:143) + `LOOMWORKS_DEQUE_CAPACITY = 256` (internal.h:106, fixed
`calloc`, no grow) → for w=4, `need = 4096 + 4×256 = 5120` →
`cancel_cap = next_pow2(5120) = 8192`; the shared 8192 × 32 B ≈ 256 KB
open-addressed table runs at 63 % load at t=100k, so every
insert/remove (`src/thread_pool.c:1383`/`:1424`, both probing
`h = (h+1) & (cap-1)` with relaxed atomic loads) pays long shared-cache-line
probes across 4 workers + the submitter.

## 4. Target Derivation (section-3 rule, strictly applied)

Only costs ≥ 10 % and line-attributable count toward Target:

| Cost | Share pᵢ | Conservative removable eᵢ |
|---|---|---|
| A: cancel-index pair (insert + remove) | 0.103 | 0.09 (typical-case probe is already ~1–2 slots; the removable share is the saturated-window cost) |

`worker_entry` self (4.15 %), `metrics_fire` (1.00 %), and the
deque/ring push+pop cluster (~5 %) are each < 10 % individually →
opportunistic only, NOT counted toward Target (supersedes any earlier
larger figure).

**Target = 0.5 × Σ eᵢ = 0.5 × 0.09 ≈ 4.5 % primary gain** on the w=4 × t=100k
cell.

**Acceptance (all must hold):** primary gain ≥ Target on the acceptance
cell; no T1 metric cell below baseline −5 %; `tail_latency_ns.p99` and
`fairness_resp_ns.p99` no worse than baseline +10 %; 8/8 ctest green
(52,147 assertions); ASan/UBSan zero reports; `format-check` + clang-tidy
pass; `tools/bench_compare.py` green.

**Honest-exit rule:** if verification shows no ≥ 10 % cost remains
removable, deliver the baseline report + a "no change" conclusion and the
reasons each candidate was empirically disproven — never make a risky
change just to hit a number. Stop after 2 consecutive disproven candidates.

## 5. Candidate List (each first proven by the baseline; one commit per
change; per-item verification protocol (a)–(e) below)

**4.8 — Two-level cancel index (PRIMARY; user-chosen; counts toward Target).**
Replace the single shared 8192-slot table with:
- **Level 1 (local, per-worker):** each worker owns a small open-addressed
  table sized `LOOMWORKS_DEQUE_CAPACITY` (256, power of two) × 32 B =
  8 KB/worker = 32 KB total — L2-resident, zero cross-core sharing. Tasks
  pushed to that worker's deque are indexed there (the pushing worker is
  known at push time: self-push or steal).
- **Level 2 (shared):** the existing table **resized to `ring_size`
  (4096)** = 128 KB shared — covers ring-enqueued tasks only.
- `loom_task_t` gains a 1-byte index tag (0 = shared/ring, 1..N = local
  worker N, reusing the stamped owner). `cancel_index_remove`
  (`worker_entry`, src:732) probes the tagged local table first, then
  shared. `cancel_index_find`/`cancel_index_claim` (user-side cancel,
  rare — R5) probe all local tables + shared: cancel cost grows by a small
  bounded constant, cancel semantics unchanged.
- Net memory: 32 + 128 = 160 KB (vs 256 KB today) — a small win.
- Zero observable behaviour change: a task findable today stays findable
  (its tag routes to exactly one of the two tables).
- Internal-header edit only (`thread_pool_internal.h`, `loom_thread_pool_t`
  / `loom_task_t` layouts) — allowed by invariant 1.
- **Keep criterion:** cancel-index self-share on the acceptance cell drops
  from ~10.3 % to < 5 % with no gate regression.
- Risk: medium (insert/remove/find/claim all touched); mitigated by the
  full contract test suite + ASan/UBSan.

**4.1 — Slim `worker_entry` fast path** (opportunistic): move/hint the cold
drain/shutdown/resize/timer/health checks out of the per-iteration decision
chain (`__builtin_expect` cold). Evidence: 4.15 % self. Risk: low —
logically equivalent reordering. Keep if `worker_entry` self drops ≥ 1 %
with no gate regression.

**4.6 — `metrics_fire` clock avoidance** (opportunistic): avoid the
`clock_gettime` syscall in steady state (evidence: 1.00 %; R3 closed:
callback fires synchronously on the worker — must stay cheap). Risk: low.
Keep on the same criteria.

**4.3 / 4.4 / 4.5 / 4.7** stay on the list at LOWER priority (each < 5 %
individually; 4.7 dequeue-batching is the highest-risk for the FIFO/
priority contracts — kept last or dropped): hoist `queue_len` relaxed loads;
shrink `enqueue_task`'s `pool->lock` lane critical section; task-node-pool
CAS retry; batching.

**Per-item verification protocol (all must pass or the change is reverted
and the disproof recorded):**
(a) full ctest 8/8 (52,147 assertions);
(b) before/after `perf stat` over the same 12-cell matrix
(`tools/profile_pool.sh compare`);
(c) `tools/bench_compare.py` vs the baseline JSON;
(d) ASan + UBSan, zero reports;
(e) `format-check` + clang-tidy.

**Keep criterion:** the item's own metric improves AND no gate regresses;
otherwise `git revert <sha>` (never force-push/rebase) and the empirical
disproof is written into `docs/perf/optimization-log.md`.

## 6. Risks / Mitigations

- **Lost wakeup / hang (if any candidate ever touched wake logic — 4.2 was
  NOT chosen):** 20 repeated ctest stress rounds; any hang = immediate
  revert; original `sem` semantics kept as the control.
- **Semantic drift (4.8 routing):** contract-preservation argument written
  BEFORE the change; the existing cancel/submit assertions must cover it;
  tests are not public API, so coverage may be extended without violating
  invariant 1.
- **Local win regressing tail latency:** section-4 guardrails — any change
  worsening `tail_latency_ns.p99` / `fairness_resp_ns.p99` by > 10 % is
  reverted.
- **Measurement noise (4-core/8-thread laptop):** `taskset` pinning,
  1 warm-up + median of 3, before/after measured ABBA in one session; extra
  rounds if variance > 10 %.
- **Hardware already at the limit:** honest-exit rule (deliver the
  "no change" conclusion).
- **Revert safety:** exactly one commit per change; reverts only via
  `git revert <sha>`; master stays linear; no force-push, no rebase.

## 7. Deliverables

1. `tools/profile_pool.sh` — **done** (commit `961b398`), reproducible
   baseline/compare script.
2. `docs/perf/baseline-2026-10-18.md` — **done** (commit `961b398`), T1–T6
   matrix + hotspot table.
3. `docs/perf/optimization-log.md` — per-change before/after evidence plus
   the keep-or-revert decision (created in Phase 2).
4. Code changes, one `perf(pool): ⚡ <change>` commit each (repo gitmoji
   convention), body carries the key before/after numbers.
5. **Zero public-header changes** — any such change is a violation of
   invariant 1.

## 8. Phases (to become plan tasks)

- **Phase 0 (DONE):** measurement harness + baseline matrix + hotspot
  attribution + this spec.
- **Phase 1:** implement candidate 4.8 (two-level cancel index) —
  contract-preservation argument → change → verification (a)–(e) → one
  commit, or revert + record.
- **Phase 2:** opportunistic candidates 4.1, 4.6 (same protocol, lower
  priority; stop after 2 consecutive disproofs).
- **Phase 3:** final comparison matrix (`tools/profile_pool.sh compare`
  vs `docs/perf/baseline-2026-10-18.md`), update
  `docs/perf/optimization-log.md`, conclusions report including disproven
  items.

## 9. Spec Self-Review

- No TBD/placeholder digits: every number above is measured from
  `docs/perf/baseline-2026-10-18.md` and the acceptance-cell `perf report`.
- Consistency: section 4's strict ≥ 10 % rule supersedes any earlier
  looser Target computation; candidates 4.1/4.3/4.6 are explicitly
  non-Target.
- Scope: one subsystem (thread-pool core hot path), one primary candidate
  (4.8) + two opportunistic (4.1, 4.6) — single-plan size.
- Ambiguity: "primary gain" is defined (section 4: throughput on the
  acceptance cell); "keep criterion" per candidate is named; the honest-exit
  rule is explicit.
