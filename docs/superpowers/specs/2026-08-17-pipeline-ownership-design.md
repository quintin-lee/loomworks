# Pipeline Payload Ownership Design (R8 closure)

## 1. Background

`loom_pc_create(worker_count > 0, ...)` spawns an internal consumer pool whose
workers loop on `loom_pc_take()` and, **absent a discard handler, `free()` every
payload**. This default was a deliberate drain choice (risk-assessment R8), but
it is a silent hazard for callers that submit heap payloads and expect to
reclaim them via their own `take()` loop or a delayed scan: the memory is freed
out from under them by internal consumer threads, with no error and no way to
opt out. The maintenance-priority list proposed: "document loudly; consider an
explicit `LOOM_PC_OWN_PAYLOADS` flag in a future minor release."

This design implements that flag. The goal is a crisp, auditable ownership rule
fixed at create time: **the library frees a payload only when internal-consumer
mode is active AND no discard handler is installed AND ownership was not
claimed by the caller.** Every other combination leaves the payload untouched.

## 2. API

```c
#define LOOM_PC_OWN_PAYLOADS (1u << 0)

loom_result_t loom_pc_create_ex(uint32_t worker_count, uint32_t capacity,
                                uint32_t flags,
                                void (*discard)(void *data, void *ctx), void *discard_ctx,
                                loom_pc_t **pc);
```

- `flags` is a bitmask. `LOOM_PC_OWN_PAYLOADS` asserts **caller ownership**: the
  library must never `free()` a payload submitted to this pipeline.
- `discard` / `discard_ctx` are the discard-handler pair, supplied **atomically
  at create** so the create-time validation can see the full state. Passing the
  handler here is equivalent to calling `loom_pc_set_discard_handler` before
  any producer starts (including for pipelines that also call it later — the
  later call simply replaces this one).
- `loom_pc_create(...)` becomes a zero-flag wrapper:
  `return loom_pc_create_ex(worker_count, capacity, 0, NULL, NULL, pc);` — the
  current behavior and every existing caller are unaffected (source and ABI
  compatible; `struct loom_pc` gains a `flags` field but is opaque).

### Validation (in order)

1. `pc == NULL` → `LOOMWORKS_ERR_INVALID`.
2. Any **unknown flag bit** (`flags & ~LOOM_PC_OWN_PAYLOADS`) → `ERR_INVALID` —
   future flag bits are rejected rather than silently ignored (production-grade
   defense).
3. `LOOM_PC_OWN_PAYLOADS` **and** `worker_count > 0` **and** `discard == NULL`
   → `ERR_INVALID`. Rationale: in internal-consumer mode, every item is consumed
   by a worker; with ownership claimed and no handler, there is no hook through
   which the caller can ever see or reclaim an item — a heap payload leaks
   silently. Rejecting the combination at create makes the leak impossible
   rather than merely documented. Callers in this position must supply a
   handler (which receives each item on the consuming and the destroy-drain
   paths) or run external-take mode (`worker_count = 0`).
4. `LOOM_PC_OWN_PAYLOADS` with `worker_count == 0` is a **valid no-op**: the
   library never touches payloads in external-take mode; the flag documents the
   caller's ownership and is harmless. (Not rejected — a uniform flags argument
   may legally be passed in both modes.)

`struct loom_pc` gains `uint32_t flags;` stored from `create_ex`.

## 3. Ownership semantics (tri-state)

| internal consumers | discard handler | `OWN_PAYLOADS` | library behavior on consume |
|---|---|---|---|
| yes | yes | either | call handler — library never frees |
| yes | no | no | **`free(item)`** (current default; the R8 hazard, now explicit) |
| yes | no | yes | **rejected at create** (leak-only combination) |
| no (workers=0) | either | either | library never touches payloads; destroy-drain with no handler drops without freeing |

Destroy-time behavior is unchanged and remains consistent: drain calls the
handler when set, otherwise drops the item without `free()`. With ownership
claimed, "drop without free" is the correct terminal state — the caller owns
the payload and either registered a reclaiming handler or accepts the drop.

## 4. Consumer implementation

`consumer_pool_task` (src/pipeline.c L82-96) changes one line:

```c
if (pc->on_discard)      pc->on_discard(item, pc->discard_ctx);
else if (!(pc->flags & LOOM_PC_OWN_PAYLOADS)) free(item);
```

**Safety proof:** the `else if` branch is dead code whenever ownership was
claimed in internal-consumer mode, because create-time validation (rule 3)
guarantees a handler exists in exactly that combination. The invariant is
enforced at the boundary instead of at each consume.

No clock, ring, or pool changes. `CLOCK_REALTIME` in `loom_pc_submit`'s
bounded wait stays untouched (out of scope, per R9's exclusion of pipeline.c).

## 5. Tests (5 new + regression)

All in `tests/test_thread_pool.c`'s pipeline section (existing `loom_pc_create`
tests range L2874-L3169); `pc_discard_handler`-style counting uses the
file's existing atomic-gate convention.

1. `test_pc_create_ex_owns_no_handler_rejected` — `create_ex(2, 16,
   LOOM_PC_OWN_PAYLOADS, NULL, NULL, &pc)` → `ERR_INVALID`; `*pc` untouched.
2. `test_pc_create_ex_owns_handler_ok` — `create_ex(1, 64, OWN_PAYLOADS,
   counting_handler, &st, &pc)` → OK; N=5000 submitted heap payloads; handler
   reclaims each **exactly once** (`st.discarded == N`); library did not free
   (handler counts live payloads it frees, so double-free would double-count or
   crash — the count assertion is the canary); shutdown; destroy.
3. `test_pc_create_ex_owns_external_take_ok` — `create_ex(0, 10,
   OWN_PAYLOADS, NULL, NULL, &pc)` → OK (no-op); submit 1000 payloads; caller
   `take()` loop reclaims all 1000 (`free` by test, `taken_count == 1000`);
   destroy drains 0.
4. `test_pc_create_ex_unknown_flag_rejected` — `create_ex(0, 10, 0x80000000, ...)`
   → `ERR_INVALID`.
5. `test_pc_create_ex_null_pc` — `create_ex(0, 10, 0, NULL, NULL, NULL)` →
   `ERR_INVALID`.

Regression gates: existing `test_pipeline_discard_queued` (integration, handler
count == N) and all `loom_pc_create` tests must stay green — the wrapper must
not change behavior. `examples/pipeline_demo.c` stays green unchanged (its
fire-and-forget contract is correct under the default). Baselines: test_thread_pool
12734/0, test_coroutine 5611/0, test_integration 78759/0 (±40), test_ctx_smoke
200014; new assertions only increase the pool count.

## 6. Docs

- `docs/api-reference.md` pipeline section: document `loom_pc_create_ex`,
  `LOOM_PC_OWN_PAYLOADS`, the tri-state ownership rule, the create-time
  rejection, and that `loom_pc_create` is the zero-flag wrapper.
- `docs/risk-assessment.md`: R8 register row → ✅ resolved (2026-08-17, ownership
  flag); detailed R8 section gains a resolved-status paragraph (flag +
  create-time rejection + handler-as-reclaim-hook + regression locks); the
  maintenance-priority item 2 reworded/removed per the resolution.
- `CHANGELOG.md` `[Unreleased]` `### Added`/`### Changed`: `create_ex` +
  ownership flag bullet(s).
- `docs/design-decisions.md`: append decision 19 — why ownership is a create-time
  flag with an atomic handler parameter, and why the leak-only combination is
  rejected instead of documented.

## 7. Acceptance criteria

- AC1 `loom_pc_create_ex` exists with the signature above; unknown flag bits
  reject with `ERR_INVALID`.
- AC2 `LOOM_PC_OWN_PAYLOADS` + workers>0 + no handler → `ERR_INVALID` at create;
  the leak-only combination cannot be constructed.
- AC3 With ownership claimed and a handler, every consumed payload reaches the
  handler exactly once (internal consumers and destroy drain), and the library
  never calls `free` on a payload.
- AC4 With ownership claimed in external-take mode, create succeeds and the
  caller reclaims via `take()`; destroy drops queued items without freeing them.
- AC5 `loom_pc_create` behaves identically to before (wrapper); all existing
  pipeline tests and the demo stay green.
- AC6 Docs updated per §6; risk-assessment R8 marked resolved.

## 8. Non-goals

- No `LOOM_PC_FREE_PAYLOADS` default flip (breaking change — rejected in
  brainstorming).
- No per-item ownership API.
- No changes to pipeline clocks or the bounded-wait timeout path.
- No demo changes.