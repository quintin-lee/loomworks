#ifndef LOOMWORKS_COROUTINE_INTERNAL_H
#define LOOMWORKS_COROUTINE_INTERNAL_H

#include "coro_ctx.h"
#include "loomworks/coroutine.h"
#include <pthread.h>
#include <stdbool.h>

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
 * Context switching goes through the pluggable backend in coro_ctx.h
 * (default: POSIX ucontext(3); see "Context backend" there).  The
 * scheduler context (g_scheduler) is thread-local and persistent across
 * all coroutines created on the same thread.
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

    loom_coro_ctx_t ctx; /**< Saved context (abstracted backend). */

    void  *mmap_base;   /**< Base address from mmap(). */
    size_t mmap_size;   /**< Total size of the mmap region (includes guards). */
    void  *stack_start; /**< Start of the usable (mprotect'd) region. */
    void  *stack_end;   /**< End of the usable region (exclusive). */

    uintptr_t valgrind_stack_id; /**< Valgrind stack registration ID. */

    /* ASan fiber bookkeeping: under AddressSanitizer the current fake-stack
     * pointer must be saved across a raw context switch (see the macros in
     * coroutine.c).  NULL when not built with ASan. */
    void *fake_stack_save;

    uint64_t task_id;          /* Pool task id (0 for stand-alone coroutines). */
    int64_t  wake_deadline_ns; /* 0 = not sleeping; CLOCK_MONOTONIC absolute. */
    uint32_t worker_idx;       /* Owner worker slot; stamped at create time. */
    void    *sleep_reg_ctx;    /* Pool pointer for the sleep_reg hook (NULL = stand-alone). */
    void    *task_node;        /* Pool loom_task_t* carrying this coroutine (NULL = stand-alone). */
    /* Optional pool hook: registers this coroutine's deadline with the pool
     * timer heap. NULL = stand-alone (pure suspension; caller resumes). */
    void (*sleep_reg)(void *ctx, uint64_t task_id, int64_t deadline_ns);

    uint64_t padding[4]; /**< Pad to 64-byte cache-line boundary. */
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
