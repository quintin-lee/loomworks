/*
 * Context backend for coroutine switching.
 *
 * Backend matrix (compile-time selection driven by CMakeLists.txt):
 *   LOOMWORKS_CTX_ASM_X86_64   - hand-written assembly, x86-64 SysV ABI
 *   LOOMWORKS_CTX_ASM_AARCH64  - hand-written assembly, aarch64 AAPCS64
 *   (neither)                  - POSIX ucontext fallback (glibc/musl/BSD)
 *
 * CMake picks asm for x86_64/AMD64 and aarch64/arm64 hosts unless the
 * LOOMWORKS_CTX_BACKEND cache variable is set to "ucontext"; any other
 * host falls back to ucontext with zero behavior change.  The asm
 * backends exist because macOS removed ucontext and because a hand
 * written backend needs only callee-saved GPRs plus the FP control
 * words (mxcsr+x87cw / fpcr+fpsr), not the full fxsave-class state.
 *
 * The coroutine layer consumes exactly this surface:
 *   get(ctx)            - save current CPU state into ctx, return 0
 *   set_stack(ctx,s,n)  - point ctx at a fresh stack of n bytes
 *   set_link(ctx,t)     - set ctx's "return to" context (may be NULL)
 *   has_link(ctx)       - nonzero if ctx has a link context
 *   make(ctx,fn,arg)    - arm ctx to run fn(arg) on its stack
 *   swap(from,to)       - switch to 'to', returning 0 when switched back
 *   swap_to_link(ctx)   - switch to ctx->link (requires a link)
 *
 * Swapping the backend only requires reimplementing these operations;
 * src/coroutine.c is backend-agnostic.  A context that is zero-initialized
 * is invalid; it must be armed with get/make before it is activated.
 */
#ifndef LOOMWORKS_CORO_CTX_H
#define LOOMWORKS_CORO_CTX_H

#include <stddef.h>
#include <stdint.h>

#if defined(LOOMWORKS_CTX_ASM_X86_64)

/* Layout must match src/ctx_x86_64.S exactly. */
typedef struct loom_coro_ctx {
    void *pc;                   /* 0  resume address                 */
    uint64_t rsp;               /* 8  stack pointer                  */
    uint64_t rbx;               /* 16 callee-saved GPR               */
    uint64_t rbp;               /* 24                                */
    uint64_t r12;               /* 32                                */
    uint64_t r13;               /* 40                                */
    uint64_t r14;               /* 48                                */
    uint64_t r15;               /* 56                                */
    uint32_t mxcsr;             /* 64 FP control word                */
    uint16_t x87cw;             /* 68                                */
    void (*entry_fn)(void *);   /* 72 fn to run on first resume      */
    void *entry_arg;            /* 80 fn argument                    */
    struct loom_coro_ctx *link; /* 88 context to switch to on return */
} loom_coro_ctx_t;

_Static_assert(offsetof(struct loom_coro_ctx, pc) == 0,
               "ctx layout drift: pc (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, rsp) == 8,
               "ctx layout drift: rsp (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, rbx) == 16,
               "ctx layout drift: rbx (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, rbp) == 24,
               "ctx layout drift: rbp (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, r12) == 32,
               "ctx layout drift: r12 (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, r13) == 40,
               "ctx layout drift: r13 (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, r14) == 48,
               "ctx layout drift: r14 (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, r15) == 56,
               "ctx layout drift: r15 (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, mxcsr) == 64,
               "ctx layout drift: mxcsr (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, x87cw) == 68,
               "ctx layout drift: x87cw (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, entry_fn) == 72,
               "ctx layout drift: entry_fn (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, entry_arg) == 80,
               "ctx layout drift: entry_arg (see ctx_x86_64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, link) == 88,
               "ctx layout drift: link (see ctx_x86_64.S)");
_Static_assert(sizeof(struct loom_coro_ctx) == 96,
               "ctx layout drift: sizeof (see ctx_x86_64.S)");

/* Implemented in src/ctx_x86_64.S.  ctx_trampoline is the resume target
 * armed by make(); declared as a char array (object type) so that the
 * -Wpedantic "conversion of function pointer to object pointer" warning
 * can never fire under -Werror -pedantic. */
int loom_coro_ctx_get(loom_coro_ctx_t *ctx);
int loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to);
int loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx);
extern char ctx_trampoline[];

static inline void
loom_coro_ctx_set_stack(loom_coro_ctx_t *ctx, void *sp, size_t size)
{
    ctx->rsp = (uint64_t)((char *)sp + size); /* top, exclusive */
}

static inline void
loom_coro_ctx_set_link(loom_coro_ctx_t *ctx, loom_coro_ctx_t *target)
{
    ctx->link = target;
}

static inline int
loom_coro_ctx_has_link(const loom_coro_ctx_t *ctx)
{
    return ctx->link != NULL;
}

static inline void
loom_coro_ctx_make(loom_coro_ctx_t *ctx, void (*fn)(void *), void *arg)
{
    ctx->entry_fn = fn;
    ctx->entry_arg = arg;
    /* Trampoline is entered with rsp % 16 == 8 so that `call entry_fn`
     * leaves entry_fn with rsp % 16 == 0 (SysV ABI). */
    ctx->rsp = (ctx->rsp & ~(uint64_t)0xF) - 8u;
    ctx->r15 = (uint64_t)ctx;       /* ctx is passed to the trampoline */
    ctx->mxcsr = 0x1F80u;           /* default control word (mask all) */
    ctx->x87cw = 0x037Fu;
    ctx->pc = (void *)ctx_trampoline;
}

#elif defined(LOOMWORKS_CTX_ASM_AARCH64)

/* Layout must match src/ctx_aarch64.S exactly. */
typedef struct loom_coro_ctx {
    void *pc;                   /* 0   resume address                 */
    uint64_t sp;                /* 8   stack pointer                  */
    uint64_t regs[12];          /* 16  x19..x29, x30 (lr)             */
    uint32_t fpcr;              /* 112 FP control register            */
    uint32_t fpsr;              /* 116 FP status register             */
    void (*entry_fn)(void *);   /* 120 fn to run on first resume      */
    void *entry_arg;            /* 128 fn argument                    */
    struct loom_coro_ctx *link; /* 136 context to switch to on return */
} loom_coro_ctx_t;

_Static_assert(offsetof(struct loom_coro_ctx, pc) == 0,
               "ctx layout drift: pc (see ctx_aarch64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, sp) == 8,
               "ctx layout drift: sp (see ctx_aarch64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, regs) == 16,
               "ctx layout drift: regs (see ctx_aarch64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, fpcr) == 112,
               "ctx layout drift: fpcr (see ctx_aarch64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, fpsr) == 116,
               "ctx layout drift: fpsr (see ctx_aarch64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, entry_fn) == 120,
               "ctx layout drift: entry_fn (see ctx_aarch64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, entry_arg) == 128,
               "ctx layout drift: entry_arg (see ctx_aarch64.S)");
_Static_assert(offsetof(struct loom_coro_ctx, link) == 136,
               "ctx layout drift: link (see ctx_aarch64.S)");
_Static_assert(sizeof(struct loom_coro_ctx) == 144,
               "ctx layout drift: sizeof (see ctx_aarch64.S)");

int loom_coro_ctx_get(loom_coro_ctx_t *ctx);
int loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to);
int loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx);
extern char ctx_trampoline[];

static inline void
loom_coro_ctx_set_stack(loom_coro_ctx_t *ctx, void *sp, size_t size)
{
    ctx->sp = (uint64_t)((char *)sp + size); /* top, exclusive */
}

static inline void
loom_coro_ctx_set_link(loom_coro_ctx_t *ctx, loom_coro_ctx_t *target)
{
    ctx->link = target;
}

static inline int
loom_coro_ctx_has_link(const loom_coro_ctx_t *ctx)
{
    return ctx->link != NULL;
}

static inline void
loom_coro_ctx_make(loom_coro_ctx_t *ctx, void (*fn)(void *), void *arg)
{
    ctx->entry_fn = fn;
    ctx->entry_arg = arg;
    /* Trampoline is entered with sp % 16 == 8 so that `blr entry_fn`
     * leaves entry_fn with sp % 16 == 0 (AAPCS64). */
    ctx->sp = (ctx->sp & ~(uint64_t)0xF) - 8u;
    ctx->regs[0] = (uint64_t)ctx;   /* ctx is passed to the trampoline */
    ctx->fpcr = 0u;
    ctx->fpsr = 0u;
    ctx->pc = (void *)ctx_trampoline;
}

#else /* ucontext fallback */

#include <ucontext.h>

typedef ucontext_t loom_coro_ctx_t;

static inline int
loom_coro_ctx_get(loom_coro_ctx_t *ctx)
{
    return getcontext(ctx);
}

static inline void
loom_coro_ctx_set_stack(loom_coro_ctx_t *ctx, void *sp, size_t size)
{
    ctx->uc_stack.ss_sp = sp;
    ctx->uc_stack.ss_size = size;
    ctx->uc_stack.ss_flags = 0;
}

static inline void
loom_coro_ctx_set_link(loom_coro_ctx_t *ctx, loom_coro_ctx_t *target)
{
    ctx->uc_link = target;
}

static inline int
loom_coro_ctx_has_link(const loom_coro_ctx_t *ctx)
{
    return ctx->uc_link != NULL;
}

static inline void
loom_coro_ctx_make(loom_coro_ctx_t *ctx, void (*fn)(void *), void *arg)
{
    makecontext(ctx, (void (*)(void))fn, 1, (unsigned long)(uintptr_t)arg);
}

static inline int
loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to)
{
    return swapcontext(from, to);
}

static inline int
loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx)
{
    return swapcontext(ctx, ctx->uc_link);
}

#endif /* LOOMWORKS_CTX_ASM_X86_64 || LOOMWORKS_CTX_ASM_AARCH64 */

#endif /* LOOMWORKS_CORO_CTX_H */