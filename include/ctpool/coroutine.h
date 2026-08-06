#ifndef CTPPOOL_COROUTINE_H
#define CTPPOOL_COROUTINE_H

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

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque coroutine handle. */
typedef struct ctpool_coroutine ctpool_coroutine_t;

/**
 * @brief Coroutine state.
 */
typedef enum {
    CTPPOOL_CORO_NEW,       /**< Coroutine created but not started. */
    CTPPOOL_CORO_RUNNING,   /**< Currently executing. */
    CTPPOOL_CORO_SUSPENDED, /**< Paused via yield or initial suspend. */
    CTPPOOL_CORO_DONE,      /**< Completed execution. */
    CTPPOOL_CORO_ERROR,     /**< Error state (e.g., guard page hit). */
} ctpool_coro_state_t;

/**
 * @brief Result codes for coroutine operations.
 */
typedef enum {
    CTPPOOL_CORO_OK = 0,        /**< Operation succeeded. */
    CTPPOOL_CORO_ERR_ALLOC,     /**< Memory allocation failed. */
    CTPPOOL_CORO_ERR_CONTEXT,   /**< Context creation failed. */
    CTPPOOL_CORO_ERR_MPROTECT,  /**< mprotect call failed. */
    CTPPOOL_CORO_ERR_INVALID,   /**< Invalid coroutine handle or state. */
    CTPPOOL_CORO_ERR_GUARD,     /**< Guard page violation detected. */
    CTPPOOL_CORO_ERR_RUNNING,   /**< Operation invalid in current state. */
} ctpool_coro_result_t;

/**
 * @brief Coroutine entry function signature.
 *
 * @param user_data  Opaque pointer provided at creation time.
 */
typedef void (*ctpool_coro_fn)(void *user_data);

/**
 * @brief Default coroutine stack size.
 */
#define CTPPOOL_CORO_DEFAULT_STACK_SIZE (64 * 1024)  /* 64 KiB */

/**
 * @brief Number of guard pages on each side of the stack.
 */
#define CTPPOOL_CORO_GUARD_PAGES_EACH 1

/**
 * @brief Create a new coroutine.
 *
 * The coroutine is in the NEW state. Call ctpool_coro_resume() to start it.
 *
 * @param fn       Entry function for the coroutine.
 * @param data     Opaque user data passed to the entry function.
 * @param stack_size  Stack size in bytes (0 uses default).
 * @param coro     Output pointer for the created coroutine handle.
 * @return         CTPPOOL_CORO_OK on success, error code otherwise.
 */
ctpool_coro_result_t ctpool_coro_create(ctpool_coro_fn fn,
                                         void *data,
                                         size_t stack_size,
                                         ctpool_coroutine_t **coro);

/**
 * @brief Start or resume a coroutine.
 *
 * If the coroutine is in the NEW state, this starts it for the first time.
 * If it is SUSPENDED, this resumes execution from the yield point.
 *
 * @param coro  The coroutine handle.
 * @return      CTPPOOL_CORO_OK on success, error code otherwise.
 */
ctpool_coro_result_t ctpool_coro_resume(ctpool_coroutine_t *coro);

/**
 * @brief Yield control back to the caller (scheduler).
 *
 * Must be called from within a running coroutine. Execution will resume
 * at the same point on the next ctpool_coro_resume() call.
 */
void ctpool_coro_yield(void);

/**
 * @brief Suspend the current coroutine, returning control to the caller.
 *
 * Equivalent to yield but explicitly marks the coroutine as SUSPENDED.
 */
void ctpool_coro_suspend(void);

/**
 * @brief Terminate a coroutine before it completes naturally.
 *
 * The coroutine moves to the DONE state. Any pending tasks in a thread
 * pool will be cleaned up automatically.
 *
 * @param coro  The coroutine handle.
 * @return      CTPPOOL_CORO_OK on success, error code otherwise.
 */
ctpool_coro_result_t ctpool_coro_terminate(ctpool_coroutine_t *coro);

/**
 * @brief Destroy a coroutine and free all resources.
 *
 * Must be called after the coroutine is in DONE state, or after
 * ctpool_coro_terminate() has been called.
 *
 * @param coro  Pointer to the coroutine handle (set to NULL on return).
 */
void ctpool_coro_destroy(ctpool_coroutine_t **coro);

/**
 * @brief Get the current state of a coroutine.
 *
 * @param coro  The coroutine handle.
 * @return      Current state, or CTPPOOL_CORO_ERROR if handle is invalid.
 */
ctpool_coro_state_t ctpool_coro_state(const ctpool_coroutine_t *coro);

/**
 * @brief Get the stack address range for debugging.
 *
 * @param coro       The coroutine handle.
 * @param start      Output pointer for stack start address.
 * @param end        Output pointer for stack end address (exclusive).
 * @return           CTPPOOL_CORO_OK on success.
 */
ctpool_coro_result_t ctpool_coro_stack_info(const ctpool_coroutine_t *coro,
                                             void **start,
                                             void **end);

/**
 * @brief Get a human-readable string for a result code.
 */
const char *ctpool_coro_result_str(ctpool_coro_result_t result);

#ifdef __cplusplus
}
#endif

#endif /* CTPPOOL_COROUTINE_H */
