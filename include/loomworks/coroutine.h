#ifndef LOOMWORKS_COROUTINE_H
#define LOOMWORKS_COROUTINE_H

/**
 * @file coroutine.h
 * @brief Stackful coroutine library with guard pages.
 *
 * Features:
 *   - Stack allocated via mmap with PROT_NONE guard pages
 *   - Full context save/restore via ucontext
 *   - Yield, suspend, and resume primitives
 *   - Nested coroutine support
 *   - Pure C11 with POSIX context switching
 *
 * Guard page strategy:
 *   Each coroutine stack is allocated with mmap using a region 2 guard pages
 *   larger than the requested size. The first and last guard pages are set to
 *   PROT_NONE. A guard page immediately inside the usable region is also set
 *   to PROT_NONE to catch stack overflows growing inward from either end.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque coroutine handle. */
typedef struct loom_coroutine loom_coroutine_t;

/**
 * @brief Coroutine state.
 */
typedef enum {
    LOOMWORKS_CORO_NEW,       /**< Coroutine created but not started. */
    LOOMWORKS_CORO_RUNNING,   /**< Currently executing. */
    LOOMWORKS_CORO_SUSPENDED, /**< Paused via yield or initial suspend. */
    LOOMWORKS_CORO_DONE,      /**< Completed execution. */
    LOOMWORKS_CORO_ERROR,     /**< Error state (e.g., guard page hit). */
} loom_coro_state_t;

/**
 * @brief Result codes for coroutine operations.
 */
typedef enum {
    LOOMWORKS_CORO_OK = 0,       /**< Operation succeeded. */
    LOOMWORKS_CORO_ERR_ALLOC,    /**< Memory allocation failed. */
    LOOMWORKS_CORO_ERR_CONTEXT,  /**< Context creation failed. */
    LOOMWORKS_CORO_ERR_MPROTECT, /**< mprotect call failed. */
    LOOMWORKS_CORO_ERR_INVALID,  /**< Invalid coroutine handle or state. */
    LOOMWORKS_CORO_ERR_GUARD,    /**< Guard page violation detected. */
    LOOMWORKS_CORO_ERR_RUNNING,  /**< Operation invalid in current state. */
} loom_coro_result_t;

/**
 * @brief Coroutine entry function signature.
 *
 * @param user_data  Opaque pointer provided at creation time.
 */
typedef void (*loom_coro_fn)(void *user_data);

/**
 * @brief Default coroutine stack size.
 */
#define LOOMWORKS_CORO_DEFAULT_STACK_SIZE ((size_t)(64 * 1024)) /* 64 KiB */

/**
 * @brief Number of guard pages on each side of the stack.
 */
#define LOOMWORKS_CORO_GUARD_PAGES_EACH 1u

/**
 * @brief Create a new coroutine.
 *
 * The coroutine is in the NEW state. Call loom_coro_resume() to start it.
 *
 * @param fn       Entry function for the coroutine.
 * @param data     Opaque user data passed to the entry function.
 * @param stack_size  Stack size in bytes (0 uses default).
 * @param coro     Output pointer for the created coroutine handle.
 * @return         LOOMWORKS_CORO_OK on success, error code otherwise.
 */
loom_coro_result_t
loom_coro_create(loom_coro_fn fn, void *data, size_t stack_size, loom_coroutine_t **coro);

/**
 * @brief Start or resume a coroutine.
 *
 * If the coroutine is in the NEW state, this starts it for the first time.
 * If it is SUSPENDED, this resumes execution from the yield point.
 *
 * @param coro  The coroutine handle.
 * @return      LOOMWORKS_CORO_OK on success, error code otherwise.
 */
loom_coro_result_t loom_coro_resume(loom_coroutine_t *coro);

/**
 * @brief Yield control back to the caller (scheduler).
 *
 * Must be called from within a running coroutine. Execution will resume
 * at the same point on the next loom_coro_resume() call.
 */
void loom_coro_yield(void);

/**
 * @brief Suspend the current coroutine, returning control to the caller.
 *
 * Equivalent to yield but explicitly marks the coroutine as SUSPENDED.
 */
void loom_coro_suspend(void);

/**
 * @brief Terminate a coroutine before it completes naturally.
 *
 * The coroutine moves to the DONE state. Any pending tasks in a thread
 * pool will be cleaned up automatically.
 *
 * @param coro  The coroutine handle.
 * @return      LOOMWORKS_CORO_OK on success, error code otherwise.
 */
loom_coro_result_t loom_coro_terminate(loom_coroutine_t *coro);

/**
 * @brief Destroy a coroutine and free all resources.
 *
 * Must be called after the coroutine is in DONE state, or after
 * loom_coro_terminate() has been called.
 *
 * @param coro  Pointer to the coroutine handle (set to NULL on return).
 */
void loom_coro_destroy(loom_coroutine_t **coro);

/**
 * @brief Get the current state of a coroutine.
 *
 * @param coro  The coroutine handle.
 * @return      Current state, or LOOMWORKS_CORO_ERROR if handle is invalid.
 */
loom_coro_state_t loom_coro_state(const loom_coroutine_t *coro);

/**
 * @brief Get the stack address range for debugging.
 *
 * @param coro       The coroutine handle.
 * @param start      Output pointer for stack start address.
 * @param end        Output pointer for stack end address (exclusive).
 * @return           LOOMWORKS_CORO_OK on success.
 */
loom_coro_result_t loom_coro_stack_info(const loom_coroutine_t *coro, void **start, void **end);

/**
 * @brief Get a human-readable string for a result code.
 */
const char *loom_coro_result_str(loom_coro_result_t result);

/**
 * @brief Clean up per-thread coroutine resources.
 *
 * Frees the scheduler stack for the calling thread.
 * Call this from threads that create/use coroutines but are not
 * managed by the coroutine runtime (e.g., pool worker threads).
 */
void loom_coro_exit(void);

/**
 * @brief Install the SIGSEGV/SIGBUS guard-page handler.
 *
 * Idempotent: safe to call multiple times.  Automatically invoked
 * by loom_coro_resume() so manual calls are rarely needed.
 */
void loom_coro_install_guard_handler(void);

/**
 * @brief Remove the guard-page handler and restore SIGSEGV/SIGBUS defaults.
 */
void loom_coro_uninstall_guard_handler(void);

#ifdef __cplusplus
}
#endif

#endif /* LOOMWORKS_COROUTINE_H */
