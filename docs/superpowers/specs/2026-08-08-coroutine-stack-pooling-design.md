# Coroutine Stack Pooling — Design

> **Status:** Approved design (2026-08-08)
> **Scope:** Internal performance optimization of the coroutine subsystem. No public API change.

## Problem

Every `loom_coro_create` performs `mmap` + `mprotect` (2 syscalls), and every `loom_coro_destroy` performs `munmap` (1 syscall) — 3 syscalls per coroutine lifecycle ([coroutine.c:121](coroutine.c#L121), [coroutine.c:155](coroutine.c#L155)). Coroutine-heavy workloads (many short-lived coroutines, e.g. per-request tasks) pay this cost repeatedly.

The thread pool received the equivalent treatment (task node pool + bucketized O(1) queue); the coroutine subsystem is the remaining performance hotspot in the library.

## Goals / Non-Goals

**Goals:**
- Reuse mmap'd stack mappings across create/destroy cycles, eliminating the syscall overhead on the steady-state path
- Zero behavior change for users — identical API, semantics, and error codes

**Non-Goals:**
- Windows port (separate future item, Low priority)
- Pooling the *scheduler* stack (per-thread, intentionally never freed — documented design decision)
- Pooling the `loom_coroutine_t` struct itself (`calloc`/`free` of ~100 bytes is cheap next to syscalls)

## Approach: global mutex-protected free-list, exact size matching

### Rationale for the approach

- **Global, not per-thread:** coroutines are cross-thread capable (per-thread scheduler context, but any thread may create/destroy a coroutine). A per-thread pool would split reuse across threads and multiply resident memory.
- **Mutex, not lock-free:** matches the established task node pool pattern in the thread pool; create/destroy is not the hot path vs task submit, so lock contention is negligible.
- **Exact size matching, not size-class bucketing:** linear scan over ≤ 64 entries is trivial; avoids rounding a 64 KiB request up to a 1 MiB bucket. The mapping layout (`[GUARD][usable][GUARD]`, guard pages `PROT_NONE`, usable `PROT_READ|PROT_WRITE`) persists in the mapping, so a recycled stack needs **zero syscalls** — no re-`mprotect` required.

### Pool structure (internal to `coroutine.c`)

```c
#define LOOMWORKS_CORO_STACK_POOL_CAP 64u /* max pooled stack mappings */

typedef struct coro_stack_node {
    struct coro_stack_node *next;
    size_t  stack_size;        /* exact-match key (requested usable size) */
    void   *mmap_base;         /* mapping metadata for reuse */
    size_t  mmap_size;
    void   *stack_start;
    void   *stack_end;
    uintptr_t valgrind_stack_id;
} coro_stack_node_t;

static coro_stack_node_t *g_stack_pool;      /* free-list head */
static pthread_mutex_t    g_stack_pool_lock; /* guards the pool */
static size_t             g_stack_pool_count;/* ≤ CAP */
```

The existing `struct loom_coroutine` fields (`mmap_base`, `mmap_size`, `stack_start`, `stack_end`, `valgrind_stack_id`) are reused unchanged; the pool node mirrors them so a pooled mapping can be re-attached to a fresh coroutine.

### `allocate_stack(c)` — acquire

1. Lock the pool; linear-scan for a node with `stack_size == c->stack_size` (n ≤ 64); unlink on hit; unlock.
2. **Hit:** copy `mmap_base`/`mmap_size`/`stack_start`/`stack_end` into `c`, re-register the valgrind stack. **Zero syscalls** (guards and RW permissions already in place).
3. **Miss:** unlock, fall through to the existing `mmap` + `mprotect` path (unchanged).

### `deallocate_stack(c)` — release

1. Lock the pool; if `g_stack_pool_count < LOOMWORKS_CORO_STACK_POOL_CAP`: copy the mapping metadata into a fresh node, deregister the valgrind stack, push to head, `count++`; unlock. **No syscall.**
2. **Full:** unlock, fall through to the existing `munmap` path (unchanged).

### Edge cases

- **Cap accounting:** count-based (64 total entries), per design decision. Worst case 64 × 64 KiB = 4 MiB resident. A 1 MiB stack occupies one entry — it does not starve 64 KiB coroutines because matching is by exact size.
- **Valgrind:** register on every acquire (fresh or pooled), deregister on every release (munmap or pool push) — consistent with the existing `VALGRIND_STACK_REGISTER`/`VALGRIND_STACK_DEREGISTER` ifdefs and their no-op fallback when valgrind headers are absent.
- **Error handling:** pool miss on acquire → the `mmap` path may still fail with `LOOMWORKS_CORO_ERR_ALLOC` (unchanged semantics). Pool full on release → `munmap` (failure ignored, as today). **No new error codes.**
- **Process exit:** extend the `coro_atexit` destructor to `munmap` any remaining pooled mappings (it currently frees scheduler stacks only) — otherwise pooled mappings would leak at exit (invisible to the OS, but visible to valgrind's leak check).
- **`loom_coro_exit`:** frees only the per-thread scheduler stack; the stack pool is global, so there is no interaction.

## Testing

### New test (`tests/test_coroutine.c`)

- **Pool-reuse proof (deterministic):** create/destroy a coroutine 1000× with the default 64 KiB stack; on the second wave assert `loom_coro_stack_info` returns the *same* address range as the first wave (same mapping recycled — pooling is active, not merely "no crash").
- **Size isolation:** destroy a 64 KiB coroutine, then create a 256 KiB coroutine and assert it does **not** receive the pooled 64 KiB mapping (exact-match key respected).
- **Guard pages intact on reuse:** after a pooled stack is reused, a guard-page overflow must still trap to `LOOMWORKS_CORO_ERR_GUARD`.

### Regression gates

- All 1571 coroutine assertions + 62749 integration assertions, zero failures
- ASan + UBSan clean
- `-Wall -Wextra -Werror -pedantic` + clang-tidy + clang-format clean

### Microbenchmark (`examples/bench.c`)

Add a coroutine scenario: create + destroy × 100000 with a trivial (non-yielding) entry function. Measure wall time before/after the change and report the delta — syscall removal is expected to dominate.

## Files Touched

| File | Change |
|------|--------|
| `src/coroutine.c` | Pool structs + acquire/release logic + `coro_atexit` extension |
| `src/coroutine_internal.h` | Comment update only (pool documented) |
| `tests/test_coroutine.c` | New pool-reuse / size-isolation / guard-on-reuse tests |
| `examples/bench.c` | Coroutine create+destroy microbenchmark scenario |

**No public API change.** `loom_coro_create` / `loom_coro_destroy` / all other public symbols keep their exact signatures and semantics.
