#ifndef LOOMWORKS_COROUTINE_INTERNAL_H
#define LOOMWORKS_COROUTINE_INTERNAL_H

#include "loomworks/coroutine.h"
#include <pthread.h>
#include <stdbool.h>
#include <ucontext.h>

/* One cache line on modern x86-64.  Fields that are accessed by
 * different threads concurrently (e.g. the mutex, condition variables)
 * are aligned to avoid false sharing. */
#define LOOMWORKS_CACHELINE_ALIGN __attribute__((aligned(64)))

/**
 * @brief Internal coroutine structure.
 *
 * Stack layout (mmap with PROT_NONE guard pages on each side):
 *   [GUARD][GUARD][STACK][GUARD][GUARD]
 *   where GUARD pages are PROT_NONE.
 * Freed stacks are pooled in coroutine.c (exact-size match, capped
 * at 64 mappings); a pooled mapping is reused with zero syscalls on
 * the next create of the same requested size.
 *
 * Context switching uses POSIX ucontext(3).  The scheduler context
 * (g_scheduler) is thread-local and persistent across all coroutines
 * created on the same thread.
 */
struct loom_coroutine {
    loom_coro_state_t state;      /**< Current state (NEW/RUNNING/SUSPENDED/DONE/ERROR). */
    loom_coro_fn      entry_fn;   /**< User entry function. */
    void             *user_data;  /**< Opaque argument passed to entry_fn. */
    size_t            stack_size; /**< Requested stack size in bytes. */
    pthread_t         owner;      /**< Thread that created the coroutine. */

    /* Lifetime rule: resume/terminate are only valid from owner; calling
     * them from another thread is user error, guarded at runtime with
     * LOOMWORKS_CORO_ERR_INVALID in coroutine.c because the ucontext
     * machinery is not safe to touch from multiple threads. */

    ucontext_t ctx; /**< Saved context for swapcontext(). */

    void  *mmap_base;   /**< Base address from mmap(). */
    size_t mmap_size;   /**< Total size of the mmap region (includes guards). */
    void  *stack_start; /**< Start of the usable (mprotect'd) region. */
    void  *stack_end;   /**< End of the usable region (exclusive). */

    uintptr_t valgrind_stack_id; /**< Valgrind stack registration ID. */
    uint64_t  padding[5];        /**< Pad to 64-byte cache-line boundary. */
};

/**
 * @brief Install the SIGSEGV/SIGBUS guard-page handler (idempotent).
 *
 * Must be called before any coroutine resume that might trigger a guard
 * page violation.  Uses atomic load to avoid redundant sigaction() calls
 * across threads.
 */
void loom_coro_install_guard_handler(void);

/**
 * @brief Remove the guard-page signal handler and restore defaults.
 */
void loom_coro_uninstall_guard_handler(void);

#endif /* LOOMWORKS_COROUTINE_INTERNAL_H */
