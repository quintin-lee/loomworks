# asm Context Backend Design

Date: 2026-08-16
Status: Draft (pending user review)
Type: Design spec (architecture improvement)
Authors: Sisyphus + user (brainstorming session)

## 1. Background

The coroutine subsystem currently drives all context switching exclusively through
`src/coro_ctx.h`, a thin inline wrapper over the ucontext(3) API. This keeps the
deprecated POSIX `ucontext` dependency in exactly one place — but only as a
stopgap:

- `ucontext` is marked obsolescent in POSIX.1-2008 and **removed on macOS**
  (Apple Silicon has no `makecontext`/`swapcontext` at all).
- Each `swapcontext` saves/restores the full FPU environment via `fxsave`-class
  instructions — hundreds of cycles per switch, far beyond what a coroutine
  scheduler needs (see §4.3 of Design: only FP *control* words outlive a yield;
  SIMD data registers are caller-clobbered by the C ABI and are never live
  across a yield).
- The header comment already anticipates this: *"swapping the backend (e.g.
  asm ctx pro/cto) only requires reimplementing these [functions]."*

Risk R2 in `docs/risk-assessment.md` tracks this as accepted-but-mitigated; this
design is that mitigation.

## 2. Goals

- G1. Replace ucontext as the *default* context backend with handwritten
  assembly on **both x86-64 (SysV ABI) and aarch64 (AAPCS64)**, in lockstep.
- G2. **Exact semantic parity** with the current ucontext backend for every
  behavior the coroutine subsystem relies on:
  - `resume` / `yield` / `terminate` control flow (scheduler ⇄ coroutine
    swaps in both directions),
  - FP control state (`mxcsr` + x87 control word; `fpcr` + `fpsr`) preserved
    across yields,
  - fn-return trampoline contract: coroutine function returns → switch to link
    context; `link == NULL` → `exit(EXIT_FAILURE)` (verbatim
    `makecontext` + `uc_link` semantics).
- G3. Keep ucontext as a **compile-time fallback backend** (zero behavior
  change when selected), so arbitrary platforms and CI parity runs keep
  working.
- G4. Add **aarch64 cross-compile CI coverage** (`gcc-aarch64-linux-gnu` +
  QEMU user-mode) so the ARM backend is *executed*, not just syntax-checked.
- G5. Add **backend parity CI** (same suite forced through ucontext) proving
  both backends produce identical assertion counts.

## 3. Non-Goals

- NG1. Performance benchmarking/deliverable numbers (measured after
  implementation; optional follow-up commit).
- NG2. Windows / other-arch support (x86-64 and aarch64 only).
- NG3. Full FPU state (`fxsave`-class full SIMD spill) — explicitly *not*
  needed (see decision D2).
- NG4. Runtime backend dispatch table — compile-time selection only (decision
  D6).
- NG5. Changing `coroutine.c` structure: stack allocation / guard pages / mmap /
  mprotect / Valgrind registration / signal-guard `setjmp`/`longjmp` / state
  machine stay exactly where they are. The context backend replaces only the
  `loom_coro_ctx_t` storage layout and the jump path.

## 4. Decisions (from brainstorming session, user-approved)

| # | Decision | Rationale |
|---|---|---|
| D1 | Both x86-64 and aarch64 asm backends (not single-arch) | Covers Linux/arm64 + macOS Apple Silicon where ucontext is dead |
| D2 | Minimal callee-saved GPR set + FP *control* words only | Full ucontext semantic parity at ~tens of cycles/switch; SIMD data regs are caller-clobbered and never live across yield (option 2 chosen over minimal-GPR and over full fxsave) |
| D3 | fn-return → switch to link; NULL link → `exit(EXIT_FAILURE)` | Verbatim ucontext replication; 100% parity with baseline behavior |
| D4 | Separate `.S` files per architecture (approach A) | Full `.cfi_*` unwind + `.note.GNU-stack`, initial-frame/trampoline naturally expressible; inline-asm (B) and runtime dispatch (C) rejected |
| D5 | Compile-time macro selection (`LOOMWORKS_CTX_ASM_X86_64` / `LOOMWORKS_CTX_ASM_AARCH64`); ucontext remains the fallback when neither is defined | No hot-path indirection; zero behavior change in fallback mode |
| D6 | CMake picks backend by `CMAKE_SYSTEM_PROCESSOR`; explicit `LOOMWORKS_CTX_BACKEND=asm|ucontext` cache override (default asm); unknown arch → ucontext | One obvious build switch; parity job is a single re-configure |

## 5. Design

### 5.1 `loom_coro_ctx_t` storage layout (Section 1)

```c
#if LOOMWORKS_CTX_ASM_X86_64
typedef struct loom_coro_ctx {
    void   *pc;          /* return address: after_ret (get) or after_swap (swap) */
    uint64_t rsp;
    uint64_t rbx, rbp, r12, r13, r14, r15;   /* callee-saved GPRs */
    uint32_t mxcsr;
    uint16_t x87cw;
    void   *entry_fn;    /* make(): coroutine entry, invoked by trampoline */
    void   *entry_arg;
    struct loom_coro_ctx *link;              /* set_link target (may be NULL) */
} loom_coro_ctx_t;

#elif LOOMWORKS_CTX_ASM_AARCH64
typedef struct loom_coro_ctx {
    void   *pc;          /* return address: after_ret (get) or after_swap (swap) */
    uint64_t sp;
    uint64_t regs[12];   /* x19..x30 (x30 = lr) */
    uint32_t fpcr;
    uint32_t fpsr;
    void   *entry_fn;
    void   *entry_arg;
    struct loom_coro_ctx *link;
} loom_coro_ctx_t;

#else
typedef ucontext_t loom_coro_ctx_t;   /* ucontext fallback: zero behavior change */
#endif
```

- `pc` = saved return address (`leaq .Lafter, %rax`-style or `x30`), so the
  unified resume path ends with `jmp *pc` rather than gcc-generated `ret` —
  giving exact control over where execution continues.
- Zero-init means *invalid* context; activating before `get()`/`make()` is UB.
  loomworks never does (scheduler always goes through `ensure_scheduler`'s
  `get`; coroutines always through `resume`'s `make`).
- FP control words (2 instructions per architecture):
  - x86-64: `stmxcsr` / `ldmxcsr`, `fnstcw` / `fldcw` (use `fnstcw`/`fldcw`
    rather than `fstcw` — same encoding, `fn` variant avoids x87-wait
    overhead),
  - aarch64: `mrs` / `msr` `fpcr` and `fpsr`.

### 5.2 Backend selection & build integration (Section 3.6)

```cmake
# CMake
if(LOOMWORKS_CTX_BACKEND STREQUAL "asm" AND
   CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    set(LOOMWORKS_CTX_ASM_X86_64 ON)
    list(APPEND LOOMWORKS_SOURCES src/ctx_x86_64.S)
elseif(LOOMWORKS_CTX_BACKEND STREQUAL "asm" AND
       CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    set(LOOMWORKS_CTX_ASM_AARCH64 ON)
    list(APPEND LOOMWORKS_SOURCES src/ctx_aarch64.S)
endif()
# macros injected on both library targets + test targets via
# target_compile_definitions (tests consume only the public API, so
# assertion counts are backend-independent by construction)
```

- Default `LOOMWORKS_CTX_BACKEND=asm`. `-DLOOMWORKS_CTX_BACKEND=ucontext`
  selects the existing inline-ucontext path.
- `.S` files are written in GNU `as` syntax (portable across the linux
  toolchain; no LLVM-only directives) so the same source serves gcc and clang
  native builds and the aarch64 cross build.
- `.S` is excluded from clang-format (glob already covers only `.c`/`.h`);
  clang-tidy does not analyze assembly. Correctness is enforced by
  `_Static_assert` offset locks (§5.5) + the test suite.

### 5.3 Assembly file skeleton (Section 3)

Both `src/ctx_x86_64.S` and `src/ctx_aarch64.S`:

| Element | Specification |
|---|---|
| `.text` | code section |
| `.section .note.GNU-stack,"",@progbits` | non-executable stack (hard requirement) |
| `.cfi_startproc` / `.cfi_endproc` | on swap, swap_to_link, trampoline |
| `.cfi_def_cfa_offset` | kept in sync after every push/pop |
| `.type <sym>,@function` + `.size` | all code symbols |
| `.hidden` | everything except the two exported symbols |
| `.align 16` | function entry points |

Exported (global) symbols — the only two the C layer calls:

- `loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to)`
- `loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx)`

Everything else (`ctx_trampoline`, `after_ret`, `after_swap`, link-switch
helper) is `.hidden` or `.L`-local.

C-side declarations under the asm macros:

```c
int loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to);
int loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx);
/* get/make/set_stack/set_link/has_link remain static inline, pure C */
```

### 5.4 The seven operations → semantics mapping (Section 2)

| # | Operation | asm-backend semantics |
|---|---|---|
| 1 | `loom_coro_ctx_get(ctx)` | **Pure save**: callee-saved GPRs + `rsp`/`sp` + FP control words; `pc ← after_ret`; returns 0. Does **not** touch the stack (no push/pop) — x86-64 red zone untouched. Cannot fail. |
| 2 | `loom_coro_ctx_set_stack(ctx, sp, size)` | Pure C field write (stack base/limit for `make`). Idempotent on scheduler re-entry. |
| 3 | `loom_coro_ctx_set_link(ctx, target)` | Pure C field write; `target` may be NULL. |
| 4 | `loom_coro_ctx_has_link(ctx)` | Pure C: `ctx->link != NULL`. |
| 5 | `loom_coro_ctx_make(ctx, fn, arg)` | C fills `entry_fn`, `entry_arg`, `sp` → stack-top aligned per ABI, `pc ← ctx_trampoline`. No asm in `make` itself. |
| 6 | `loom_coro_ctx_swap(from, to)` | Pure asm: save `from` (rsp, GPRs, FP ctrl, `pc ← after_swap`) → restore `to` → `jmp to->pc`. **Bilaterally symmetric** — one function covers scheduler→coro and coro→scheduler. `from == to` → natural no-op. Cannot fail. |
| 7 | `loom_coro_ctx_swap_to_link(ctx)` | Pure asm; reads `to = ctx->link`, behaves as `swap(ctx, to)`. NULL link = UB (documented; all current call sites have non-NULL link — same status quo as ucontext). |

Unified resume path: restore `rsp`, callee-saved GPRs, FP control words, then
`jmp ctx->pc`. Both `after_ret` and `after_swap` continue with `eax=0`
(aarch64 `w0=0`), matching `getcontext`/`swapcontext` returning 0 — so the
existing `if (loom_coro_ctx_get(&g_scheduler) != 0)` check in
`ensure_scheduler` and resume/yield return handling keep exact semantics.

### 5.5 Offset drift protection (Section 3.4)

`.S` files hard-code offsets (`.equ PC_OFF 0`, `.equ RSP_OFF 8`, …). To make
layout drift a compile-time hard failure instead of silent memory corruption,
the asm branches of `coro_ctx.h` carry `_Static_assert` locks:

```c
#if LOOMWORKS_CTX_ASM_X86_64
_Static_assert(offsetof(struct loom_coro_ctx, pc)   == 0,  "ctx layout drift: pc (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, rsp)  == 8,  "ctx layout drift: rsp (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, rbx)  == 16, "ctx layout drift: rbx (see ctx_x86_64.S)");
/* ... one per field in order ... */
_Static_assert(offsetof(struct loom_coro_ctx, link) == 88, "ctx layout drift: link (see ctx_x86_64.S)");
#endif
```

Either side changes a field → build breaks loudly. (Exact offsets verified at
implementation time.)

### 5.6 make() trampoline protocol (Section 2)

```asm
ctx_trampoline:
    mov  entry_arg → arg reg (rdi | x0)   ; arg loaded from ctx via dedicated reg
    call entry_fn                          ; rsp already ABI-aligned by make
    ; fn returned:
    cmp  link, 0
    je   .Lno_link
    jmp  swap_to_link-body                 ; switch to link (reuses swap body)
.Lno_link:
    mov  EXIT_FAILURE → edi
    call exit                              ; verbatim makecontext NULL-uc_link contract
```

Key invariant: **fn return never `ret`s back to the caller** — control always
switches to link (or exits). This is how a *dropped* coroutine stack can still
yield back to the scheduler (the coroutine entry `coro_entry` in coroutine.c
explicitly ends with `if (has_link) swap_to_link;`).

### 5.7 Call-site compatibility (verified against src/coroutine.c)

Existing call sites, all preserved unchanged:

- `ensure_scheduler`: `get(&g_scheduler)` → `set_stack(&g_scheduler, g_scheduler_stack, 131072)` → `set_link(&g_scheduler, NULL)` (scheduler has NO link — it must never return; `get` position semantics keep `pc=after_ret`).
- `coro_entry` (fn-return path): `has_link(&c->ctx)` → `swap_to_link(&c->ctx)` — coroutine link is always `&g_scheduler` in loomworks usage.
- `resume` (NEW path): `get(&coro->ctx)` → `set_stack(&coro->ctx, coro->stack_start, …)` → `set_link(&coro->ctx, &g_scheduler)` → `make(&coro->ctx, coro_entry, coro)` → `swap(&g_scheduler, &coro->ctx)`.
- `yield`: `swap(&cur->ctx, &g_scheduler)`.
- `terminate` (self): `swap(&coro->ctx, &g_scheduler)`.

`g_scheduler` is `_Thread_local` (`static _Thread_local loom_coro_ctx_t
g_scheduler;`), its stack is the 128 KiB `_Thread_local` scheduler stack.
No TLS access is performed in assembly — the asm functions only touch the
context structs passed by pointer.

### 5.8 Error handling (Section 4.1)

| Call | Failure condition | Behavior |
|---|---|---|
| `get(ctx)` | — | Cannot fail (pure saves); returns 0 |
| `swap(from,to)` | — | Cannot fail; `from==to` → no-op; NULL pointers are UB (same as ucontext status quo — coroutine.c never passes NULL) |
| `swap_to_link(ctx)` | `ctx->link == NULL` | UB, documented |
| `make/set_stack/set_link/has_link` | — | Pure C, cannot fail |

Deliberately NOT added: glibc's `-1/EINVAL` on NULL from/to. None of the
current call sites can produce NULL; adding branches would tax the hot path
for an impossible case.

### 5.9 Test matrix (Section 4.2)

Local x86-64 (default `asm` backend):

- Full 3-test suite (baseline assertion counts 12541 / 5603 / 78765) — the asm
  backend runs every coroutine path (`resume`/`yield`/`terminate`/guard-page
  jumps/stack-pool reuse).
- New `ctx_smoke.c`: 100k `swap` round-trips (rsp/GPR integrity); FP control
  word survives a yield (`fesetround` round-trip); trampoline NULL-link→`exit`
  path verified in a forked child process (never kills the main test process).
- clang-tidy + format checks unchanged.

CI (extending, not changing, the existing matrix):

1. Existing gcc/clang × Debug/Release + ASan/TSan/UBSan matrix — exercises the
   `asm` default path.
2. **Backend parity job**: `-DLOOMWORKS_CTX_BACKEND=ucontext`, re-run
   `test_coroutine` + `test_integration`; assertion counts must equal the asm
   run → both backends behaviorally identical.
3. **aarch64 cross job**: `gcc-aarch64-linux-gnu` cross-compiles static lib +
   test binaries; QEMU user-mode executes `test_coroutine` + `test_integration`
   to green (assertion counts identical to host — coroutine semantics are
   arch-independent).
4. Valgrind job unchanged (still proves no uninitialized jumps through asm
   frames).

Acceptance threshold: any backend/arch assertion-count mismatch vs baseline =
red. Parity mismatch between backends = red (semantic split detection).

### 5.10 Documentation updates (Section 4.3)

1. `src/coro_ctx.h` header comment: backend matrix (asm x86-64 / asm aarch64 /
   ucontext), how selection works, why it exists.
2. `docs/architecture.md` ch. 3 (Coroutines): new "Context Backend" subsection
   — swap timing diagram, register-save inventory, trampoline protocol.
3. `docs/design-decisions.md` §1: append asm-backend rationale (macOS removed
   ucontext, performance), why x86-64+aarch64 in lockstep, and the FP-control-
   words fidelity trade-off — forming a complete decision chain with the
   original ucontext argument.

## 6. Out of scope for this design (implementation-phase items)

- Per-instruction `.S` content.
- `ctx_smoke.c` concrete assertions.
- CI YAML final diff.
- Performance numbers (optional follow-up commit after implementation).

## 7. Acceptance criteria

- AC1. Default build on x86-64 uses the asm backend; all 3 test suites green
  with unchanged assertion counts (12541 / 5603 / 78765 ±ε).
- AC2. `-DLOOMWORKS_CTX_BACKEND=ucontext` build produces identical assertion
  counts (parity proven locally before CI).
- AC3. `_Static_assert` offset locks present in both asm branches.
- AC4. `.note.GNU-stack` present in both `.S` files; executables have
  non-executable stack.
- AC5. aarch64 cross-compile + QEMU run of `test_coroutine` +
  `test_integration` green.
- AC6. Docs updated (3 files per §5.10).