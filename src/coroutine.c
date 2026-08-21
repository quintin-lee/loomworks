#define _POSIX_C_SOURCE 200809L
#include "portability.h" /* MAP_ANON fallback for the guard-page mmap */
#include "loomworks/coroutine.h"
#include "coroutine_internal.h"

#include <errno.h>
#include <pthread.h>
#include <setjmp.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>
#if defined(__has_include) && __has_include(<valgrind/valgrind.h>)
#include <valgrind/valgrind.h>
#endif
/* AddressSanitizer fiber annotations: the coroutine backend performs raw
 * context switches onto privately allocated stacks, which ASan's fake-stack
 * machinery cannot see.  Wrap every switch with start/finish_switch_fiber so
 * interceptors inside the coroutine resolve against the coroutine stack.
 * Only compiled when the TU itself is instrumented by ASan. */
#if defined(__SANITIZE_ADDRESS__) || (defined(__has_feature) && __has_feature(address_sanitizer))
#include <sanitizer/common_interface_defs.h>
#define LOOMWORKS_ASAN 1
/* The coroutine's fake-stack pointer, saved on yield/sleep/terminate
 * switches and restored on the next re-entry (per-thread, same as the
 * rest of the scheduler state). */
static _Thread_local void *g_fake_stack_save = NULL;
#endif

/* ================================================================
 *  Globals
 *
 *  All scheduler state is _Thread_local so that any thread can create
 *  and drive coroutines independently: there is no global "current
 *  coroutine" — g_current belongs to whichever thread is executing.
 *  g_guard_installed is the one process-global atomic (signal handlers
 *  are process-wide).
 * ================================================================ */
static _Thread_local loom_coroutine_t *g_current = NULL;
static _Thread_local loom_coro_ctx_t   g_scheduler; /* per-thread scheduler context */
static _Thread_local char             *g_scheduler_stack  = NULL;
static _Thread_local bool              g_scheduler_inited = false;
static _Atomic bool                    g_guard_installed  = false;
static sigjmp_buf                      g_guard_jmp; /* longjmp target for guard violations */
/* Prior SIGSEGV/SIGBUS dispositions, saved on first install so they can be
 * chained to: uninstall restores them and a fault that is not on a coroutine
 * guard page is re-raised through them.  Zero-init means "SIG_DFL" if no
 * handler was ever installed. */
static struct sigaction g_prev_segv;
/* Recursion guard: prevents nested signal handler invocation. */
static _Thread_local bool g_in_handler = false;
static struct sigaction   g_prev_bus;
/* Linked list of all scheduler stacks for atexit cleanup. */
typedef struct scheduler_stack_node {
    char                        *stack;
    struct scheduler_stack_node *next;
} scheduler_stack_node_t;

static scheduler_stack_node_t *g_scheduler_stacks = NULL;

/* Serializes all mutations of g_scheduler_stacks.  Three writers:
 *  - ensure_scheduler()           appends (a thread's first coro resume)
 *  - loom_coro_exit()             unlinks + frees (worker loop top)
 *  - free_all_scheduler_stacks()  atexit teardown
 * A dedicated lock keeps this registry independent of the stack pool;
 * the only cross-lock ordering that can occur is pool->lock ->
 * g_scheduler_lock (loom_coro_exit runs under the pool lock and no
 * coroutine API path takes g_scheduler_lock then a pool lock), so there
 * is no lock-cycle.  free_all_scheduler_stacks additionally assumes all
 * threads have been joined before process exit (pthread contract). */
static pthread_mutex_t g_scheduler_lock = PTHREAD_MUTEX_INITIALIZER;

/* ================================================================
 *  ASan fiber annotations
 *
 *  The backend performs raw context switches (registers + stack pointer)
 *  that ASan's fake-stack machinery cannot see: after switching onto a
 *  coroutine's private stack, the fake-stack pointer still points at the
 *  caller's frame, so any interceptor call inside the coroutine crashes.
 *  These four macros bracket every switch, saving/restoring the
 *  appropriate fake-stack pointer on each side.  They compile to nothing
 *  unless the TU is built with AddressSanitizer.
 * ================================================================ */
#ifdef LOOMWORKS_ASAN
/* scheduler→coroutine: save thread fake-stack into coro->fake_stack_save.
 * bottom is the low address of the coroutine's usable stack region. */
#define ASAN_SWITCH_TO_CORO(coro)                                                                  \
    __sanitizer_start_switch_fiber(                                                                \
        &(coro)->fake_stack_save,                                                                  \
        (coro)->stack_start,                                                                       \
        (size_t)((char *)(coro)->stack_end - (char *)(coro)->stack_start))
/* coroutine→scheduler (return path): restore from coro->fake_stack_save */
#define ASAN_SWITCH_BACK_TO_THREAD(coro)                                                           \
    __sanitizer_finish_switch_fiber((coro)->fake_stack_save, NULL, NULL)
/* coroutine→scheduler: save coroutine fake-stack into thread-local slot;
 * bottom is the low address of the per-thread scheduler stack. */
#define ASAN_SWITCH_TO_SCHEDULER()                                                                 \
    __sanitizer_start_switch_fiber(&g_fake_stack_save, g_scheduler_stack, 131072)
/* scheduler→coroutine (re-entry path): restore from thread-local slot */
#define ASAN_SWITCH_BACK_TO_CORO() __sanitizer_finish_switch_fiber(g_fake_stack_save, NULL, NULL)
/* coroutine entry (first arrival on this stack): finalize the inbound hop
 * started by ASAN_SWITCH_TO_CORO() — restores the scheduling thread's
 * fake-stack.  Without this, ASan still considers the switch in progress
 * and rejects the coroutine's first outbound start. */
#define ASAN_FINISH_CORO_ENTRY(coro)                                                               \
    __sanitizer_finish_switch_fiber((coro)->fake_stack_save, NULL, NULL)
#else
#define ASAN_SWITCH_TO_CORO(coro) ((void)0)
#define ASAN_SWITCH_BACK_TO_THREAD(coro) ((void)0)
#define ASAN_SWITCH_TO_SCHEDULER() ((void)0)
#define ASAN_SWITCH_BACK_TO_CORO() ((void)0)
#define ASAN_FINISH_CORO_ENTRY(coro) ((void)0)
#endif

/* ================================================================
 *  Stack pool — reuse mmap'd coroutine stacks across create/destroy
 *  cycles.  Exact-size matching; global mutex; cap of 64 mappings
 *  (~4 MiB worst case at 64 KiB each).  A pooled mapping needs zero
 *  syscalls on reuse (guard pages + RW permissions persist in the
 *  mapping; only the valgrind registration is re-done on acquire).
 * ================================================================ */
#define LOOMWORKS_CORO_STACK_POOL_CAP 64u

typedef struct coro_stack_node {
    struct coro_stack_node *next;
    size_t                  stack_size; /* exact-match key (requested size, pre-rounding) */
    void                   *mmap_base;
    size_t                  mmap_size;
    void                   *stack_start;
    void                   *stack_end;
    uintptr_t               valgrind_stack_id;
} coro_stack_node_t;

static coro_stack_node_t *g_stack_pool       = NULL;
static pthread_mutex_t    g_stack_pool_lock  = PTHREAD_MUTEX_INITIALIZER;
static size_t             g_stack_pool_count = 0;
/* ================================================================
 *  Guard-page signal handler
 *
 *  SIGSEGV/SIGBUS land here when a coroutine overflows its stack.
 *  A fault is attributed to our coroutine only if it falls inside the
 *  current coroutine's mmap region AND on the first/last page (the
 *  PROT_NONE guards).  Anything else (wild pointer, unrelated SEGV)
 *  is re-raised with the default handler so the process still dies
 *  with a normal core dump.
 *
 *  On a genuine guard hit the handler records LOOMWORKS_CORO_ERROR and
 *  longjmps out of the coroutine's context back into the setjmp in
 *  loom_coro_resume() — the coroutine is effectively dead after this,
 *  and only loom_coro_destroy() may be used on it.
 * ================================================================ */
static void guard_handler(int sig, siginfo_t *info, void *uctx)
{
    (void)uctx;
    loom_coroutine_t *c = g_current;
    if (c != NULL && c->mmap_base != NULL) {
        size_t ps = (size_t)sysconf(_SC_PAGESIZE);
        if (ps == 0) {
            ps = 4096;
        }
        uintptr_t fault = (uintptr_t)info->si_addr;
        uintptr_t base  = (uintptr_t)c->mmap_base;
        uintptr_t end   = base + c->mmap_size;
        if (fault >= base && fault < end) {
            uintptr_t fp = (fault / ps) * ps;
            if (fp == base || fp == end - ps) {
                c->state  = LOOMWORKS_CORO_ERROR;
                g_current = NULL;
                siglongjmp(g_guard_jmp, 1);
            }
        }
    }
    /* Not our guard page ' fall through to the default handler.
     * We deliberately do NOT chain to the previous handler here because:
     *   1. Saving/restoring sigactions across signal boundaries is racy.
     *   2. A re-raised signal could recurse into this handler.
     *   3. The default handler will produce a normal core dump for debugging.
     * If we are already inside the handler (nested signal), just exit to
     * avoid an infinite loop. */
    if (g_in_handler) {
        _exit(128 + sig);
    }
    g_in_handler = true;
    sigaction(sig, &(struct sigaction){.sa_handler = SIG_DFL}, NULL);
    raise(sig);
}

void loom_coro_install_guard_handler(void)
{
    /* Idempotent, process-global: installing once covers every thread. */
    if (atomic_load_explicit(&g_guard_installed, memory_order_relaxed)) {
        return;
    }
    struct sigaction sa;
    sa.sa_sigaction = guard_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_SIGINFO;
    if (sigaction(SIGSEGV, &sa, &g_prev_segv) != 0 || sigaction(SIGBUS, &sa, &g_prev_bus) != 0) {
        fprintf(stderr, "loomworks: sigaction failed: %s\n", strerror(errno));
        return;
    }
    atomic_store_explicit(&g_guard_installed, true, memory_order_relaxed);
}

void loom_coro_uninstall_guard_handler(void)
{
    /* Restore the handlers that were installed before us, so embedding
     * applications keep their own SIGSEGV/SIGBUS handling. */
    if (!atomic_load_explicit(&g_guard_installed, memory_order_relaxed)) {
        return;
    }
    sigaction(SIGSEGV, &g_prev_segv, NULL);
    sigaction(SIGBUS, &g_prev_bus, NULL);
    atomic_store_explicit(&g_guard_installed, false, memory_order_relaxed);
}

/* ================================================================
 *  Stack allocation with PROT_NONE guard pages
 *
 *  Layout (low -> high):
 *    [GUARD] [GUARD] [usable stack] [top guard]
 *  mmap reserves the whole region as PROT_NONE, then we mprotect
 *  the usable part to PROT_READ|PROT_WRITE.
 * ================================================================ */
static loom_coro_result_t allocate_stack(loom_coroutine_t *c)
{
    /* Fast path: reuse an exact-size pooled mapping (zero syscalls). */
    pthread_mutex_lock(&g_stack_pool_lock);
    coro_stack_node_t **pp = &g_stack_pool;
    while (*pp != NULL) {
        if ((*pp)->stack_size == c->stack_size) {
            coro_stack_node_t *node = *pp;
            *pp                     = node->next;
            g_stack_pool_count--;
            pthread_mutex_unlock(&g_stack_pool_lock);

            c->mmap_base   = node->mmap_base;
            c->mmap_size   = node->mmap_size;
            c->stack_start = node->stack_start;
            c->stack_end   = node->stack_end;
#ifdef VALGRIND_STACK_REGISTER
            c->valgrind_stack_id = (uintptr_t)VALGRIND_STACK_REGISTER(c->stack_start, c->stack_end);
#else
            (void)c->stack_start;
            (void)c->stack_end;
            c->valgrind_stack_id = 0;
#endif
            free(node);
            return LOOMWORKS_CORO_OK;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&g_stack_pool_lock);

    /* Miss — fall through to the existing mmap + mprotect path. */
    long psl = sysconf(_SC_PAGESIZE);
    if (psl <= 0) {
        psl = 4096;
    }
    size_t ps        = (size_t)psl;
    size_t guard_nb  = (size_t)(LOOMWORKS_CORO_GUARD_PAGES_EACH * 2);
    size_t usable_pg = (c->stack_size + ps - 1) / ps;
    size_t total_pg  = guard_nb + usable_pg;
    size_t total_sz  = total_pg * ps;

    void *base = mmap(NULL, total_sz, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) {
        return LOOMWORKS_CORO_ERR_ALLOC;
    }

    c->mmap_base = base;
    c->mmap_size = total_sz;

    /* The usable region starts after the bottom guard pages. */
    size_t offset    = guard_nb * ps;
    size_t usable_sz = usable_pg * ps;
    c->stack_start   = (char *)base + offset;
    c->stack_end     = (char *)base + offset + usable_sz;
#ifdef VALGRIND_STACK_REGISTER
    c->valgrind_stack_id = (uintptr_t)VALGRIND_STACK_REGISTER(c->stack_start, c->stack_end);
#else
    (void)c->stack_start;
    (void)c->stack_end;
#endif

    if (mprotect(c->stack_start, usable_sz, PROT_READ | PROT_WRITE) != 0) {
        munmap(base, total_sz);
        c->mmap_base = NULL;
        return LOOMWORKS_CORO_ERR_MPROTECT;
    }
    return LOOMWORKS_CORO_OK;
}

static void deallocate_stack(loom_coroutine_t *c)
{
    if (c->mmap_base != NULL) {
#ifdef VALGRIND_STACK_DEREGISTER
        VALGRIND_STACK_DEREGISTER((unsigned)c->valgrind_stack_id);
#endif
        /* Fast path: cache the mapping for reuse (respecting the cap).
         * The mapping keeps its guard pages and RW permissions, so a
         * future loom_coro_create() with the same stack_size can adopt
         * it with zero syscalls. */
        pthread_mutex_lock(&g_stack_pool_lock);
        if (g_stack_pool_count < LOOMWORKS_CORO_STACK_POOL_CAP) {
            coro_stack_node_t *node = (coro_stack_node_t *)malloc(sizeof(*node));
            if (node != NULL) {
                node->next              = g_stack_pool;
                node->stack_size        = c->stack_size;
                node->mmap_base         = c->mmap_base;
                node->mmap_size         = c->mmap_size;
                node->stack_start       = c->stack_start;
                node->stack_end         = c->stack_end;
                node->valgrind_stack_id = 0; /* re-registered on acquire */
                g_stack_pool            = node;
                g_stack_pool_count++;
                pthread_mutex_unlock(&g_stack_pool_lock);

                c->mmap_base         = NULL;
                c->mmap_size         = 0;
                c->stack_start       = NULL;
                c->stack_end         = NULL;
                c->valgrind_stack_id = 0;
                return;
            }
        }
        pthread_mutex_unlock(&g_stack_pool_lock);

        /* Pool full or node alloc failed — munmap (unchanged semantics:
         * failure is ignored, as today). */
        munmap(c->mmap_base, c->mmap_size);
        c->mmap_base = NULL;
        c->mmap_size = 0;
    }
    c->stack_start       = NULL;
    c->stack_end         = NULL;
    c->valgrind_stack_id = 0;
}

/* ================================================================
 *  Scheduler – a single persistent context with its own stack.
 *  Per-thread to support cross-thread coroutine use.
 * ================================================================ */
static bool ensure_scheduler(void)
{
    if (g_scheduler_inited) {
        return true;
    }
    g_scheduler_stack = (char *)malloc(131072);
    if (!g_scheduler_stack) {
        return false;
    }
    if (loom_coro_ctx_get(&g_scheduler) != 0) {
        free(g_scheduler_stack);
        g_scheduler_stack = NULL;
        return false;
    }
    loom_coro_ctx_set_stack(&g_scheduler, g_scheduler_stack, 131072);
    loom_coro_ctx_set_link(&g_scheduler, NULL);
    g_scheduler_inited = true;

    /* Track for cleanup — append must be serialized against concurrent
     * loom_coro_exit() unlinks and the atexit walk. */
    scheduler_stack_node_t *node = (scheduler_stack_node_t *)malloc(sizeof(*node));
    if (node) {
        pthread_mutex_lock(&g_scheduler_lock);
        node->stack        = g_scheduler_stack;
        node->next         = g_scheduler_stacks;
        g_scheduler_stacks = node;
        pthread_mutex_unlock(&g_scheduler_lock);
    }
    return true;
}

/* ================================================================
 *  Coroutine entry point
 *
 *  Runs on the coroutine's own stack via makecontext().  Normal return
 *  (no yield) flips state to DONE and swaps back to the scheduler
 *  through uc_link.  A guard-page fault instead longjmps out of this
 *  frame, so this function never completes in that path.
 * ================================================================ */
static void coro_entry(void *arg)
{
    loom_coroutine_t *c = (loom_coroutine_t *)arg;
#ifdef LOOMWORKS_ASAN
    /* First arrival on the coroutine stack: complete the switch that
     * loom_coro_resume() started before swapping to us. */
    ASAN_FINISH_CORO_ENTRY(c);
#endif
    g_current = c;
    c->state  = LOOMWORKS_CORO_RUNNING;
    c->entry_fn(c->user_data);
    /* entry_fn returned normally (no yield) → mark done and return to scheduler */
    c->state  = LOOMWORKS_CORO_DONE;
    g_current = NULL;
    if (loom_coro_ctx_has_link(&c->ctx)) {
        ASAN_SWITCH_TO_SCHEDULER();
        loom_coro_ctx_swap_to_link(&c->ctx);
        ASAN_SWITCH_BACK_TO_CORO();
    }
}

/* ================================================================
 *  Public API
 * ================================================================ */
/* A coroutine may only be driven (resume/terminate) by the thread that
 * created it: ucontext state is not safe to touch from another thread.
 * pthread_equal() handles pthread_t being an opaque/aggregate type. */
static bool coro_owned_by_this_thread(const loom_coroutine_t *c)
{
    return pthread_equal(c->owner, pthread_self()) != 0;
}

loom_coro_result_t
loom_coro_create(loom_coro_fn fn, void *data, size_t stack_size, loom_coroutine_t **coro)
{
    if (fn == NULL || coro == NULL) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }

    loom_coroutine_t *c = (loom_coroutine_t *)calloc(1, sizeof(loom_coroutine_t));
    if (!c) {
        return LOOMWORKS_CORO_ERR_ALLOC;
    }

    c->state     = LOOMWORKS_CORO_NEW;
    c->entry_fn  = fn;
    c->user_data = data;
    c->owner     = pthread_self();
    /* 0 means "use the default" (LOOMWORKS_CORO_DEFAULT_STACK_SIZE); the
     * stack is mapped right here in create via allocate_stack, which may
     * serve it from the exact-size reuse pool. */
    c->stack_size  = (stack_size > 0) ? stack_size : LOOMWORKS_CORO_DEFAULT_STACK_SIZE;
    c->mmap_base   = NULL;
    c->mmap_size   = 0;
    c->stack_start = NULL;
    c->stack_end   = NULL;

    loom_coro_result_t rc = allocate_stack(c);
    if (rc != LOOMWORKS_CORO_OK) {
        free(c);
        return rc;
    }

    *coro = c;
    return LOOMWORKS_CORO_OK;
}

loom_coro_result_t loom_coro_resume(loom_coroutine_t *coro)
{
    if (coro == NULL) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    if (!coro_owned_by_this_thread(coro)) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }

    loom_coro_install_guard_handler();
    /* Guard-page longjmp target: a stack overflow inside this coroutine
     * (or any coroutine on this thread) unwinds here with a nonzero
     * setjmp value, reporting LOOMWORKS_CORO_ERR_GUARD. */
    if (sigsetjmp(g_guard_jmp, 1) != 0) {
        return LOOMWORKS_CORO_ERR_GUARD;
    }
    if (!ensure_scheduler()) {
        return LOOMWORKS_CORO_ERR_CONTEXT;
    }

    if (coro->state == LOOMWORKS_CORO_DONE || coro->state == LOOMWORKS_CORO_ERROR) {
        return LOOMWORKS_CORO_ERR_RUNNING;
    }

    if (coro->state == LOOMWORKS_CORO_NEW) {
        /* First run: bind the coroutine context to its mmap'd stack and
         * point uc_link at this thread's scheduler so a natural return
         * hops back to swapcontext() below. */
        if (loom_coro_ctx_get(&coro->ctx) != 0) {
            return LOOMWORKS_CORO_ERR_CONTEXT;
        }
        loom_coro_ctx_set_stack(&coro->ctx,
                                coro->stack_start,
                                (size_t)((char *)coro->stack_end - (char *)coro->stack_start));
        loom_coro_ctx_set_link(&coro->ctx, &g_scheduler);
        loom_coro_ctx_make(&coro->ctx, coro_entry, coro);
    }

    /* SLEEPING coroutines resume only after their deadline, matching the
     * contract of loom_coro_sleep_until.  An early resume is a scheduling
     * miss: keep SLEEPING and report ERR_RUNNING ("not resumable yet").
     * The pool timer thread wakes the owner at the deadline; stand-alone
     * callers must wait out the deadline themselves. */
    if (coro->state == LOOMWORKS_CORO_SLEEPING) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        int64_t now_ns = (int64_t)ts.tv_sec * 1000000000 + (int64_t)ts.tv_nsec;
        if (now_ns < coro->wake_deadline_ns) {
            return LOOMWORKS_CORO_ERR_RUNNING;
        }
        coro->wake_deadline_ns = 0; /* deadline met: clear and resume below */
    }

    /* (Re)enter as RUNNING for every resume: NEW (first run), SUSPENDED
     * (multi-yield: the coroutine may yield again and expect the next
     * resume to continue), and SLEEPING (see loom_coro_sleep_until).
     * The coroutine's own yield() requires state == RUNNING. */
    coro->state = LOOMWORKS_CORO_RUNNING;

    ASAN_SWITCH_TO_CORO(coro);
    if (loom_coro_ctx_swap(&g_scheduler, &coro->ctx) != 0) {
        ASAN_SWITCH_BACK_TO_THREAD(coro);
        coro->state = LOOMWORKS_CORO_ERROR;
        return LOOMWORKS_CORO_ERR_CONTEXT;
    }
    ASAN_SWITCH_BACK_TO_THREAD(coro);
    return LOOMWORKS_CORO_OK;
}

void loom_coro_yield(void)
{
    /* Only meaningful inside a running coroutine on this thread. */
    loom_coroutine_t *cur = g_current;
    if (cur == NULL || cur->state != LOOMWORKS_CORO_RUNNING) {
        return;
    }
    /* Pause here: save our context, switch to the scheduler, and
     * resume when the caller next invokes loom_coro_resume(). */
    cur->state = LOOMWORKS_CORO_SUSPENDED;
    ASAN_SWITCH_TO_SCHEDULER();
    if (loom_coro_ctx_swap(&cur->ctx, &g_scheduler) != 0) {
        cur->state = LOOMWORKS_CORO_ERROR;
    }
    ASAN_SWITCH_BACK_TO_CORO();
}

void loom_coro_suspend(void)
{
    loom_coro_yield();
}

loom_coro_result_t loom_coro_sleep_until(int64_t deadline_ns)
{
    loom_coroutine_t *cur = g_current;
    if (cur == NULL || cur->state != LOOMWORKS_CORO_RUNNING) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    cur->state            = LOOMWORKS_CORO_SLEEPING;
    cur->wake_deadline_ns = deadline_ns;
    if (cur->sleep_reg != NULL) {
        /* Pool coroutine task: register the deadline with the pool timer
         * heap. The timer thread resumes us via the owner worker. */
        cur->sleep_reg(cur, cur->task_id, deadline_ns);
    }
    /* Return control to the scheduler. The resume() that eventually runs
     * us again validates the deadline (SLEEPING branch above). */
    ASAN_SWITCH_TO_SCHEDULER();
    if (loom_coro_ctx_swap(&cur->ctx, &g_scheduler) != 0) {
        cur->state = LOOMWORKS_CORO_ERROR;
        return LOOMWORKS_CORO_ERR_CONTEXT;
    }
    ASAN_SWITCH_BACK_TO_CORO();
    return LOOMWORKS_CORO_OK;
}

loom_coro_result_t loom_coro_sleep(int64_t duration_ns)
{
    if (duration_ns < 0) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t now_ns = (int64_t)ts.tv_sec * 1000000000 + (int64_t)ts.tv_nsec;
    return loom_coro_sleep_until(now_ns + duration_ns);
}

loom_coro_result_t loom_coro_terminate(loom_coroutine_t *coro)
{
    if (coro == NULL) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    if (!coro_owned_by_this_thread(coro)) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    if (coro->state == LOOMWORKS_CORO_DONE || coro->state == LOOMWORKS_CORO_ERROR) {
        return LOOMWORKS_CORO_OK;
    }
    /* Mark done.  Only when the coroutine is terminating ITSELF (it is
     * g_current and actually running) must we switch back to the
     * scheduler — the swap returns the coroutine's stack to the
     * scheduler's, unwinding the coroutine's own frame permanently.
     * Terminating an externally-suspended coroutine (g_current == NULL
     * on this thread, or state != RUNNING) is just a state transition:
     * the stack is idle, so no swap is needed or legal. */
    bool self   = (coro == g_current && coro->state == LOOMWORKS_CORO_RUNNING);
    coro->state = LOOMWORKS_CORO_DONE;
    if (self) {
        g_current = NULL;
        ASAN_SWITCH_TO_SCHEDULER();
        if (loom_coro_ctx_swap(&coro->ctx, &g_scheduler) != 0) {
            return LOOMWORKS_CORO_ERR_CONTEXT;
        }
        ASAN_SWITCH_BACK_TO_CORO();
    }
    return LOOMWORKS_CORO_OK;
}

loom_coro_result_t loom_coro_destroy(loom_coroutine_t **coro)
{
    if (!coro || !*coro) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    loom_coroutine_t *c = *coro;
    /* Reject destroying a coroutine whose stack may still be live:
     * RUNNING/SUSPENDED means the context has not swizzled back into
     * the scheduler, so freeing the stack would be use-after-free. */
    if (c->state == LOOMWORKS_CORO_RUNNING || c->state == LOOMWORKS_CORO_SUSPENDED ||
        c->state == LOOMWORKS_CORO_SLEEPING) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    deallocate_stack(c);
    if (g_current == c) {
        g_current = NULL;
    }
    free(c);
    *coro = NULL;
    return LOOMWORKS_CORO_OK;
}

loom_coro_state_t loom_coro_state(const loom_coroutine_t *coro)
{
    if (!coro) {
        return LOOMWORKS_CORO_ERROR;
    }
    return coro->state;
}

loom_coro_result_t loom_coro_stack_info(const loom_coroutine_t *coro, void **start, void **end)
{
    if (!coro) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    if (start) {
        *start = coro->stack_start;
    }
    if (end) {
        *end = coro->stack_end;
    }
    return LOOMWORKS_CORO_OK;
}

void loom_coro_exit(void)
{
    /* Free the scheduler stack for this thread, if any.
     * Also remove from the global list so coro_atexit doesn't double-free.
     * Called with the pool lock held (worker loop); the unlink must be
     * serialized against concurrent ensure_scheduler() appends from other
     * threads.  free(stack) stays outside the lock: after the unlink the
     * stack is owned solely by this thread. */
    char *stack = g_scheduler_stack;
    if (stack) {
        g_scheduler_stack = NULL;
        pthread_mutex_lock(&g_scheduler_lock);
        /* Remove from linked list */
        scheduler_stack_node_t **pp = &g_scheduler_stacks;
        while (*pp) {
            if ((*pp)->stack == stack) {
                scheduler_stack_node_t *node = *pp;
                *pp                          = node->next;
                free(node);
                break;
            }
            pp = &(*pp)->next;
        }
        pthread_mutex_unlock(&g_scheduler_lock);
        free(stack);
    }
}

static void free_all_scheduler_stacks(void)
{
    /* Snapshot the head under the lock (mirrors free_all_pooled_stacks);
     * at exit all other threads are joined, so the walk is single-threaded. */
    pthread_mutex_lock(&g_scheduler_lock);
    scheduler_stack_node_t *cur = g_scheduler_stacks;
    g_scheduler_stacks          = NULL;
    pthread_mutex_unlock(&g_scheduler_lock);
    while (cur) {
        scheduler_stack_node_t *next = cur->next;
        free(cur->stack);
        free(cur);
        cur = next;
    }
}

static void free_all_pooled_stacks(void)
{
    pthread_mutex_lock(&g_stack_pool_lock);
    coro_stack_node_t *cur = g_stack_pool;
    g_stack_pool           = NULL;
    g_stack_pool_count     = 0;
    pthread_mutex_unlock(&g_stack_pool_lock);

    while (cur != NULL) {
        coro_stack_node_t *next = cur->next;
        if (cur->mmap_base != NULL) {
            munmap(cur->mmap_base, cur->mmap_size);
        }
        free(cur);
        cur = next;
    }
}

/* Runs at process exit (via __attribute__((destructor))).  By then every
 * worker thread has been joined, so the per-thread scheduler stacks that
 * loom_coro_exit() did not already free are reclaimed here, along with any
 * coroutine stacks still sitting in the reuse pool. */
static __attribute__((destructor)) void coro_atexit(void)
{
    free_all_scheduler_stacks();
    free_all_pooled_stacks();
}

const char *loom_coro_result_str(loom_coro_result_t result)
{
    switch (result) {
    case LOOMWORKS_CORO_OK:
        return "OK";
    case LOOMWORKS_CORO_ERR_ALLOC:
        return "Allocation failed";
    case LOOMWORKS_CORO_ERR_CONTEXT:
        return "Context error";
    case LOOMWORKS_CORO_ERR_MPROTECT:
        return "mprotect error";
    case LOOMWORKS_CORO_ERR_INVALID:
        return "Invalid argument";
    case LOOMWORKS_CORO_ERR_GUARD:
        return "Guard page violation";
    case LOOMWORKS_CORO_ERR_RUNNING:
        return "Invalid state";
    default:
        return "Unknown";
    }
}
