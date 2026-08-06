#ifndef CTPPOOL_COROUTINE_INTERNAL_H
#define CTPPOOL_COROUTINE_INTERNAL_H

#include "ctpool/coroutine.h"
#include <ucontext.h>
#include <stdbool.h>

/* Cache line alignment for hot fields. */
#define CTPPOOL_CACHELINE_ALIGN  __attribute__((aligned(64)))

/**
 * @brief Internal coroutine structure.
 *
 * Stack layout (allocated via mmap):
 *   [GUARD][GUARD][STACK][GUARD][GUARD]
 *   where GUARD pages are PROT_NONE.
 */
struct ctpool_coroutine {
    ctpool_coro_state_t   state;
    ctpool_coro_fn        entry_fn;
    void                 *user_data;
    size_t                stack_size;

    /* Context for save/restore. */
    ucontext_t            ctx;

    /* Stack allocation metadata. */
    void                 *mmap_base;    /* Base address returned by mmap. */
    size_t                mmap_size;    /* Total mmap region size (includes guards). */
    void                 *stack_start;  /* Start of usable stack. */
    void                 *stack_end;    /* End of usable stack (exclusive). */

    /* Padding for cache-line alignment. */
    uint64_t              padding[6];
};

/* Internal: signal handler for SIGSEGV/SIGBUS guard page hits. */
void ctpool_coro_install_guard_handler(void);

/* Internal: uninstall the signal handler. */
void ctpool_coro_uninstall_guard_handler(void);

#endif /* CTPPOOL_COROUTINE_INTERNAL_H */
