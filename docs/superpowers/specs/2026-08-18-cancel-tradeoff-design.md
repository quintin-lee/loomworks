# Cancel Index Trade-off Documentation (R5 closure)

> **Status:** Approved 2026-08-18. Pure documentation change; zero behavior change.

## 1. Background

Risk register row R5 (risk-assessment.md L25) claims
`loom_pool_cancel()` performs an O(cancel_cap) linear scan of the cancel
index on the fast path, rated Medium/Low/Low. This write-up records the
investigation outcome and the accepted-trade-off decision.

## 2. Investigation findings (source-verified 2026-08-18)

1. **`loom_pool_cancel_by_id` is already O(1) expected**: it goes through
   the lock-free open-addressing hash index `cancel_index_find`
   (thread_pool.c:801-821, task_id → task, EMPTY=0 / TOMBSTONE=1 /
   occupied=task_id+1, linear probing with bounded-probe fallback to the
   256-bucket lane walk). The register row's "O(n) linear scan" wording
   only ever applied to `loom_pool_cancel(data)`.
2. **`loom_pool_cancel(data)` is the only O(n) residue**: matching by
   `user_data` requires scanning all `cancel_cap` slots
   (thread_pool.c:1632-1650; one acquire atomic read + `cur <= 1` skip +
   `task->user_data != data` compare per slot). The index is keyed by
   task_id; there is no user_data → slot reverse structure.
3. **Actual cost**: ~`cancel_cap` atomic reads per call (default
   cancel_cap = 2^k ≥ ring_size + workers×64 ⇒ ≥1024 for 8 workers).
   Microseconds. Cancellation is rare relative to submit/execute, and the
   cancel contract already tolerates the "task may have already run" race.
4. **Existing regression coverage is complete**: `test_ring_cancel_data`
   (tests/test_thread_pool.c:1449) verifies OK / re-cancel-rejected
   semantics for `loom_pool_cancel`; `test_ring_cancel_by_id`,
   `test_cancel_by_id_after_shutdown`, `test_cancel_by_id_running`,
   `test_cancel_by_id_first_of_many`, plus the future-cancel family lock
   the cancel semantics. No new tests required.

## 3. Decision: accept as documented trade-off

Indexing `loom_pool_cancel` by user_data (secondary hash) would require
maintaining delete-chains for shared user_data, roughly double the index
memory, and add a new concurrency surface — a negative return on a Low
risk. Accepted: `cancel_by_id` (the O(1) path) is production-grade; the
`cancel(data)` scan stays as a documented, microseconds-scale trade-off.

## 4. Changes (docs only)

- `docs/risk-assessment.md`:
  - Register row R5 → **accepted trade-off (2026-08-18)** with corrected
    wording (O(cancel_cap) scan applies only to `loom_pool_cancel(data)`;
    `cancel_by_id` is hash-indexed O(1)).
  - Detailed section R5 → add **Status: ✅ ACCEPTED (2026-08-18)** paragraph
    with the investigation findings above.
  - Maintenance priority → append "R5 was accepted as a documented
    trade-off on 2026-08-18".

## 5. Commit

- `docs: record cancel index trade-off (R5 accepted)` — one file
  (docs/risk-assessment.md).
- Verification: working tree clean; git log shows the single new commit.
  No build/test run required (pure documentation).

## 6. Non-goals

- No code change to `loom_pool_cancel` or `cancel_index_*`.
- No secondary user_data hash index.
- No new tests (coverage already complete).
