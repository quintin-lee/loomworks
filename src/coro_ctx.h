#ifndef LOOMWORKS_CORO_CTX_H
#define LOOMWORKS_CORO_CTX_H

#include <stddef.h>
#include <stdint.h>
#include <ucontext.h>

/* ================================================================
 *  Context backend
 *
 *  coroutine.c drives coroutines exclusively through this header so
 *  the ucontext(3) dependency stays in one place: swapping the backend
 *  (e.g. an assembly-based ctx pro/cto pair) only requires reimplementing
 *  these inline functions with the same semantics.  The ucontext backend
 *  is the default and the only one shipped.
 * ================================================================ */
typedef ucontext_t loom_coro_ctx_t;

/* Capture the current execution context into ctx (getcontext). */
static inline int loom_coro_ctx_get(loom_coro_ctx_t *ctx)
{
    return getcontext(ctx);
}

/* Configure the stack area a makecontext'd function will run on. */
static inline void loom_coro_ctx_set_stack(loom_coro_ctx_t *ctx, void *sp, size_t size)
{
    ctx->uc_stack.ss_sp    = sp;
    ctx->uc_stack.ss_size  = size;
    ctx->uc_stack.ss_flags = 0;
}

/* Point ctx's "return" link at target; NULL means "no return path". */
static inline void loom_coro_ctx_set_link(loom_coro_ctx_t *ctx, loom_coro_ctx_t *target)
{
    ctx->uc_link = target;
}

static inline int loom_coro_ctx_has_link(const loom_coro_ctx_t *ctx)
{
    return ctx->uc_link != NULL;
}

/* Populate ctx to start fn(arg) on its configured stack.  fn must be
 * callable through the standard ucontext trampoline — coroutine.c only
 * ever starts a void(void*) entry, which this signature captures. */
static inline void loom_coro_ctx_make(loom_coro_ctx_t *ctx, void (*fn)(void *), void *arg)
{
    makecontext(ctx, (void (*)(void))fn, 1, (unsigned long)(uintptr_t)arg);
}

/* Switch from `from` into `to`. */
static inline int loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to)
{
    return swapcontext(from, to);
}

/* Switch from ctx into ctx's link target (a natural coroutine return). */
static inline int loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx)
{
    return swapcontext(ctx, ctx->uc_link);
}

#endif /* LOOMWORKS_CORO_CTX_H */
