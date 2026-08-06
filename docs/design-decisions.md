# ctpool Design Decisions

This document records key design choices and their rationales, for future maintenance and extension reference.

---

## 1. Why ucontext Instead of makecontext/jmp_buf?

**Decision:** Use POSIX `ucontext` family (`getcontext`/`setcontext`/`swapcontext`/`makecontext`) for coroutine context save and restore.

**Rationale:**
- `ucontext` is a POSIX.1-2001 standard API, natively supported on Linux/glibc, no platform-specific code needed
- `swapcontext` is better suited than `setjmp`/`longjmp`: it saves and restores contexts atomically, naturally fitting coroutine switching
- `makecontext` supports passing 1–5 `unsigned long` arguments, satisfying 64-bit pointer-safe parameter passing

**Alternatives compared:**

| Approach | Pros | Cons |
|----------|------|------|
| `ucontext` (adopted) | POSIX standard, cross-platform | Marked deprecated in glibc 2.16+ (still fully supported on Linux) |
| `setjmp`/`longjmp` | Lighterweight | Cannot save register context, does not support "resume from middle" |
| `makecontext` + `swapcontext` (adopted) | Standard API, full context | Same as above |
| Hand-written assembly | Optimal performance | Platform-bound, high maintenance cost |

**Conclusion:** `ucontext` is the best compromise — stable on Linux/x86_64 with a clean API.

---

## 2. Why mmap + PROT_NONE Instead of malloc Stack?

**Decision:** Allocate coroutine stacks via `mmap(PROT_NONE)` and activate the usable region with `mprotect`.

**Rationale:**
- **Stack overflow detection:** `PROT_NONE` guard pages trigger SIGSEGV/SIGBUS; the signal handler can catch the overflow and return an error code instead of crashing
- **Memory control:** Does not depend on `pthread_attr_setstacksize()`; all stack space is fully controlled by the library
- **Clear address space:** `mmap`-returned addresses are queryable via `stack_info()`, aiding debugging

**Stack layout design:**
```
[PROT_NONE]  ← Bottom guard page (prevents downward stack overflow)
[PROT_NONE]  ← Second guard page
[PROT_RW]    ← Usable stack space
[PROT_NONE]  ← Top guard page (prevents upward stack overflow)
```

**Alternatives compared:**

| Approach | Pros | Cons |
|----------|------|------|
| `mmap` + PROT_NONE (adopted) | Precise control, guard page detection | Requires signal handling, higher complexity |
| `pthread_attr_setstacksize` | Simple | No guard pages, overflow crashes directly |
| `malloc` large buffer | Simple | No overflow detection, wastes memory |
| `sigaltstack` | POSIX standard | Only for signal handlers, not for coroutine stacks |

---

## 3. Why Per-Thread Instead of Global Scheduler?

**Decision:** `g_scheduler` (scheduler context) and `g_guard_jmp` are declared as `_Thread_local`.

**Rationale:**
- `ucontext_t` contains thread stack pointers and register state; using it across threads causes stack corruption (SIGSEGV)
- Testing revealed: if `g_scheduler` is a global variable and coroutines are resumed from thread pool worker threads, `swapcontext` uses the main thread's scheduler context, causing a glibc malloc assertion failure (`chunk_is_mmapped` conflict)
- `_Thread_local` ensures each thread has its own scheduler; coroutines can be used safely within thread pool workers

**Performance impact:** The first coroutine API call per thread allocates a 128KB scheduler stack (`malloc`), which is then reused. The OS reclaims it when the thread exits; explicit free is omitted (standard practice).

---

## 4. Why Single Lock Instead of Sharded Locks?

**Decision:** The entire queue is protected by a single `pthread_mutex_t`.

**Rationale:**
- Queue operations are O(1) linked-list insert/delete; lock hold time is short
- Multi-lock/sharded-lock designs add code complexity and bug risk, plus require additional routing logic
- For most application scenarios (tasks with significant granularity), a single lock is not a bottleneck
- Cache-line alignment ensures locks and other hot fields do not share cache lines, reducing false sharing

**When to consider sharded locks:** If task granularity is extremely small (nanosecond-level) and concurrency is very high (thousands of workers), consider splitting the queue into N independent locks. This design does not apply to the current use case.

---

## 5. Why Does `destroy` Reject NULL-Pointer Arguments?

**Decision:** `ctpool_coro_destroy(ctpool_coroutine_t **coro)` handles NULL `coro` by checking `!*coro`, but does not accept garbage pointers like `0xDEAD`.

**Rationale:**
- Accepting a NULL `coro` pointer itself (the double pointer) is a common C API convention that simplifies caller code
- Accepting arbitrary addresses (e.g., `0xDEAD`) would dereference invalid memory, causing SIGSEGV
- Current implementation: `if (!coro || !*coro) return;` handles both NULL cases

**Design trade-off:**
- Strict mode: Add pointer validity checks (e.g., verify address is within mmap range), but increases overhead and cannot detect all invalid pointers
- Lenient mode: Rely on callers to guarantee valid pointers (current choice)

---

## 6. Why No Dynamic Library (.so) Support?

**Decision:** The library supports static linking only (`.a`), not shared libraries (`.so`).

**Rationale:**
- Coroutines use `_Thread_local` variables (`g_scheduler`, `g_current`, `g_guard_jmp`); linking as a shared object produces `R_X86_64_TPOFF32` relocation errors. glibc does not support TLS `local-exec` model in shared libraries
- Compile error: `relocation R_X86_64_TPOFF32 against 'g_current' can not be used when making a shared object`
- Workaround: `-fPIC -ftls-model=initial-exec` enables shared library support, but adds complexity and slight performance overhead

**Current limitation:** The library is used via static linking only (`libctpool.a`).

---

## 7. Why Two Guard Pages Per Side Instead of One?

**Decision:** Each side uses 2 PROT_NONE guard pages (`CTPPOOL_CORO_GUARD_PAGES_EACH * 2`).

**Rationale:**
- First guard page: serves as the boundary of the usable stack
- Second guard page: acts as a "buffer" — even if the stack overflows past the first guard page, it does not immediately access other mmap regions
- 2 guard pages provide a larger safety margin, reducing false-trigger probability

**Signal handler logic:** Only access to `base` (first page) or `end-ps` (last page) triggers longjmp. If an intermediate PROT_NONE page is also accessed, the default signal handler is invoked (crash), ensuring that true out-of-bounds accesses are not silently ignored.

---

## 8. Why `uintptr_t → unsigned long` Cast for makecontext?

**Decision:** `makecontext(&ctx, entry, 1, (unsigned long)(uintptr_t)coro)`.

**Rationale:**
- POSIX `makecontext` requires arguments of type `unsigned long`, with a maximum of 5 arguments
- On x86_64 Linux, `unsigned long` is 64-bit and can hold a pointer without loss
- Using `(uintptr_t)` as an intermediate cast ensures correct pointer-to-integer semantics, rather than direct truncation
- This approach works correctly in both ILP32 (32-bit) and LP64 (64-bit) data models

**64-bit safety verification:**
```c
// Correct: uintptr_t → unsigned long (guaranteed lossless conversion)
makecontext(&ctx, entry, 1, (unsigned long)(uintptr_t)coro);

// Incorrect: direct truncation of 64-bit pointer to 32-bit
makecontext(&ctx, entry, 1, (unsigned long)coro);  // May truncate on ILP32
```

---

## 9. Why Not Use C11 `_Generic` for Unified API?

**Decision:** Thread pool and coroutine APIs remain in separate namespaces (`ctpool_pool_*` vs `ctpool_coro_*`).

**Rationale:**
- `_Generic` selectors are complex to implement in C11 and provide poor IDE autocomplete
- Separate namespaces are clearer and easier to understand and maintain
- The two subsystems have very different functionality; a unified API adds little value

---

## 10. Cache-Line Alignment Strategy

**Decision:** All locks, queue pointers use `__attribute__((aligned(64)))` or padding fields for separation.

**Rationale:**
- Modern CPU cache line size is 64 bytes (x86_64)
- Multi-threaded concurrent access to different fields in the same cache line causes false sharing, with 10–100× performance degradation
- Queue head/tail pointers are separated from locks onto different cache lines, ensuring worker dequeue operations do not contend with main-thread lock operations

**Layout verification:**
```c
struct ctpool_thread_pool {
    pthread_mutex_t lock              ← cache line 0 (64B)
    pthread_cond_t  cond              ← cache line 1 (64B)
    pthread_cond_t  drain_cond        ← cache line 2 (64B)
    bool            shutdown          ← shares cache line 2 with drain_cond
    bool            draining
    ...
    ctpool_task_t  *queue_head        ← cache line 3 (8B + padding)
    ctpool_task_t  *queue_tail        ← same cache line as queue_head
    uint32_t        queue_len
};
```
