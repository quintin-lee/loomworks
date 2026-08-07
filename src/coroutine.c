#define _POSIX_C_SOURCE 200809L
#include "loomworks/coroutine.h"
#include "coroutine_internal.h"

#include <errno.h>
#include <setjmp.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <valgrind/valgrind.h>

/* ================================================================
 *  Globals
 * ================================================================ */
static _Thread_local loom_coroutine_t *g_current = NULL;
static _Thread_local ucontext_t        g_scheduler; /* per-thread scheduler context */
static _Thread_local char             *g_scheduler_stack  = NULL;
static _Thread_local bool              g_scheduler_inited = false;
static _Atomic bool                    g_guard_installed  = false;
static _Thread_local jmp_buf           g_guard_jmp; /* longjmp target for guard violations */
/* Linked list of all scheduler stacks for atexit cleanup. */
typedef struct scheduler_stack_node {
    char                        *stack;
    struct scheduler_stack_node *next;
} scheduler_stack_node_t;

static scheduler_stack_node_t *g_scheduler_stacks = NULL;
/* ================================================================
 *  Guard-page signal handler
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
                longjmp(g_guard_jmp, 1);
            }
        }
    }
    /* Not our guard page — reinstall default handler and re-raise. */
    {
        struct sigaction sa;
        sa.sa_handler = SIG_DFL;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        sigaction(sig, &sa, NULL);
        raise(sig);
    }
}

void loom_coro_install_guard_handler(void)
{
    if (atomic_load_explicit(&g_guard_installed, memory_order_relaxed)) {
        return;
    }
    struct sigaction sa;
    sa.sa_sigaction = guard_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_SIGINFO;
    if (sigaction(SIGSEGV, &sa, NULL) != 0 || sigaction(SIGBUS, &sa, NULL) != 0) {
        fprintf(stderr, "loomworks: sigaction failed: %s\n", strerror(errno));
        return;
    }
    atomic_store_explicit(&g_guard_installed, true, memory_order_relaxed);
}

void loom_coro_uninstall_guard_handler(void)
{
    if (!atomic_load_explicit(&g_guard_installed, memory_order_relaxed)) {
        return;
    }
    struct sigaction sa;
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGSEGV, &sa, NULL);
    sigaction(SIGBUS, &sa, NULL);
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
    size_t offset        = guard_nb * ps;
    size_t usable_sz     = usable_pg * ps;
    c->stack_start       = (char *)base + offset;
    c->stack_end         = (char *)base + offset + usable_sz;
    c->valgrind_stack_id = (uintptr_t)VALGRIND_STACK_REGISTER(c->stack_start, c->stack_end);

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
        VALGRIND_STACK_DEREGISTER((unsigned)c->valgrind_stack_id);
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
    if (getcontext(&g_scheduler) != 0) {
        free(g_scheduler_stack);
        g_scheduler_stack = NULL;
        return false;
    }
    g_scheduler.uc_stack.ss_sp    = g_scheduler_stack;
    g_scheduler.uc_stack.ss_size  = 131072;
    g_scheduler.uc_stack.ss_flags = 0;
    g_scheduler.uc_link           = NULL;
    g_scheduler_inited            = true;

    /* Track for cleanup. */
    scheduler_stack_node_t *node = (scheduler_stack_node_t *)malloc(sizeof(*node));
    if (node) {
        node->stack        = g_scheduler_stack;
        node->next         = g_scheduler_stacks;
        g_scheduler_stacks = node;
    }
    return true;
}

/* ================================================================
 *  Coroutine entry point
 * ================================================================ */
static void coro_entry(void *arg)
{
    loom_coroutine_t *c = (loom_coroutine_t *)arg;
    g_current           = c;
    c->state            = LOOMWORKS_CORO_RUNNING;
    c->entry_fn(c->user_data);
    /* entry_fn returned normally (no yield) → mark done and return to scheduler */
    c->state  = LOOMWORKS_CORO_DONE;
    g_current = NULL;
    if (c->ctx.uc_link != NULL) {
        swapcontext(&c->ctx, (ucontext_t *)c->ctx.uc_link);
    }
}

/* ================================================================
 *  Public API
 * ================================================================ */
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

    c->state       = LOOMWORKS_CORO_NEW;
    c->entry_fn    = fn;
    c->user_data   = data;
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

    loom_coro_install_guard_handler();
    if (setjmp(g_guard_jmp) != 0) {
        return LOOMWORKS_CORO_ERR_GUARD;
    }
    if (!ensure_scheduler()) {
        return LOOMWORKS_CORO_ERR_CONTEXT;
    }

    if (coro->state == LOOMWORKS_CORO_DONE || coro->state == LOOMWORKS_CORO_ERROR) {
        return LOOMWORKS_CORO_ERR_RUNNING;
    }

    if (coro->state == LOOMWORKS_CORO_NEW) {
        if (getcontext(&coro->ctx) != 0) {
            return LOOMWORKS_CORO_ERR_CONTEXT;
        }
        coro->ctx.uc_stack.ss_sp    = coro->stack_start;
        coro->ctx.uc_stack.ss_size  = (size_t)((char *)coro->stack_end - (char *)coro->stack_start);
        coro->ctx.uc_stack.ss_flags = 0;
        coro->ctx.uc_link           = &g_scheduler;
        makecontext(&coro->ctx, (void (*)(void))coro_entry, 1, (unsigned long)(uintptr_t)coro);
        coro->state = LOOMWORKS_CORO_RUNNING;
    }

    if (swapcontext(&g_scheduler, &coro->ctx) != 0) {
        coro->state = LOOMWORKS_CORO_ERROR;
        return LOOMWORKS_CORO_ERR_CONTEXT;
    }
    return LOOMWORKS_CORO_OK;
}

void loom_coro_yield(void)
{
    loom_coroutine_t *cur = g_current;
    if (cur == NULL || cur->state != LOOMWORKS_CORO_RUNNING) {
        return;
    }
    cur->state = LOOMWORKS_CORO_SUSPENDED;
    if (swapcontext(&cur->ctx, &g_scheduler) != 0) {
        cur->state = LOOMWORKS_CORO_ERROR;
    }
}

void loom_coro_suspend(void)
{
    loom_coro_yield();
}

loom_coro_result_t loom_coro_terminate(loom_coroutine_t *coro)
{
    if (coro == NULL) {
        return LOOMWORKS_CORO_ERR_INVALID;
    }
    if (coro->state == LOOMWORKS_CORO_DONE || coro->state == LOOMWORKS_CORO_ERROR) {
        return LOOMWORKS_CORO_OK;
    }
    coro->state = LOOMWORKS_CORO_DONE;
    if (coro == g_current) {
        g_current = NULL;
        if (swapcontext(&coro->ctx, &g_scheduler) != 0) {
            return LOOMWORKS_CORO_ERR_CONTEXT;
        }
    }
    return LOOMWORKS_CORO_OK;
}

void loom_coro_destroy(loom_coroutine_t **coro)
{
    if (!coro || !*coro) {
        return;
    }
    loom_coroutine_t *c = *coro;
    deallocate_stack(c);
    if (g_current == c) {
        g_current = NULL;
    }
    free(c);
    *coro = NULL;
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
     * Also remove from the global list so coro_atexit doesn't double-free. */
    char *stack = g_scheduler_stack;
    if (stack) {
        g_scheduler_stack = NULL;
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
        free(stack);
    }
}

static void free_all_scheduler_stacks(void)
{
    scheduler_stack_node_t *cur = g_scheduler_stacks;
    g_scheduler_stacks          = NULL;
    while (cur) {
        scheduler_stack_node_t *next = cur->next;
        free(cur->stack);
        free(cur);
        cur = next;
    }
}

static __attribute__((destructor)) void coro_atexit(void)
{
    free_all_scheduler_stacks();
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
