# Work-Steal Execution Order Documentation (R7 closure)

> **Status:** Approved 2026-08-18. Pure documentation change; zero behavior change.

## 1. Background

Risk register row R7 (risk-assessment.md L27) claims work-stealing executes
tasks out of submission order — LIFO local pops, FIFO cross-worker steal —
rated Low/Medium/Low. This write-up records the source-verified
investigation outcome and the accepted-trade-off decision.

## 2. Investigation findings (source-verified 2026-08-18)

1. **LIFO local pop, FIFO steal — Chase-Lev semantics, as implemented**:
   `deque_pop` (thread_pool.c:650) pops from `bottom-1` (the newest end) —
   LIFO for tasks submitted directly to a worker's own deque;
   `deque_steal` (thread_pool.c:701) takes from `top` (the oldest end) — FIFO
   from a victim worker's deque. Ring batches are FIFO. REALTIME/HIGH tasks
   bypass the ring entirely through the 256 priority buckets
   (architecture.md §2.3 Step 0/Step 4).
2. **Documentation already covers the contract in full**:
   architecture.md §2.3 "Worker Drain Order" (L91-115) specifies each drain
   step; risk-assessment.md R7 detail (L182-190) already states the
   mitigation — users needing strict ordering should use the pipeline (FIFO
   by design) or sequence numbers.
3. **Public API makes no ordering promise**: `thread_pool.h` and
   `pipeline.h` contain zero occurrences of "FIFO"/"order"/"sequence" —
   nothing misleads callers into assuming a global submission-order guarantee.
4. **Regression coverage already exists**: `steal-FIFO-order` and
   `steal-stress` (test_thread_pool.c) lock the steal semantics. No new
   tests required.

## 3. Decision: accept as documented design contract

Out-of-order execution is the intended cost of work-stealing scalability
(the 1.0.1 scheduler replaced the single shared queue precisely to remove
the 16–32 worker scaling plateau). Restoring strict global FIFO would
require a single global queue — reverting the known bottleneck this module
was built to eliminate — or cross-deque fencing that destroys steal
locality. Accepted: execution order is contractually unspecified; the
pipeline provides FIFO when ordering matters.

## 4. Changes (docs only)

- `docs/risk-assessment.md`:
  - Register row R7 → **accepted trade-off (2026-08-18)**.
  - Detailed section R7 → add **Status: ✅ ACCEPTED (2026-08-18)** paragraph
    recording the source verification.
  - Maintenance priority → append "R7 was accepted as a documented
    design contract on 2026-08-18".
- `README.md` / `CHANGELOG.md`: assertion-count sync (R10) — pool
  ~20771, coroutine ~5611, integration ~78759, ctx_smoke 200014.

## 5. Commits

- `chore: record work-steal order trade-off (R7 accepted)` — spec +
  risk-assessment.md.
- `docs: sync assertion counts (R10)` — README.md + CHANGELOG.md.
- Verification: working tree clean; git log shows the two new commits.
  No build/test run required (pure documentation).

## 6. Non-goals

- No code change to `deque_pop` / `deque_steal` / the drain loop.
- No global-FIFO ordering guarantee.
- No new tests (coverage already complete).