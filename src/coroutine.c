#define _POSIX_C_SOURCE 200809L
#include "ctpool/coroutine.h"
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
#include <unistd.h>
#include <sys/mman.h>

/* ================================================================
 *  Globals
 * ================================================================ */
static _Thread_local ctpool_coroutine_t *g_current     = NULL;
static _Thread_local ucontext_t          g_scheduler;     /* per-thread scheduler context */
static _Thread_local char               *g_scheduler_stack = NULL;
static _Thread_local bool                g_scheduler_inited  = false;
static _Atomic bool                      g_guard_installed = false;
static _Thread_local jmp_buf             g_guard_jmp;     /* longjmp target for guard violations */

/* ================================================================
 *  Guard-page signal handler
 * ================================================================ */
static void guard_handler(int sig, siginfo_t *info, void *uctx)
{
    (void)uctx;
    ctpool_coroutine_t *c = g_current;
    if (c != NULL && c->mmap_base != NULL) {
        size_t ps = (size_t)sysconf(_SC_PAGESIZE);
        if (ps == 0) ps = 4096;
        uintptr_t fault = (uintptr_t)info->si_addr;
        uintptr_t base  = (uintptr_t)c->mmap_base;
        uintptr_t end   = base + c->mmap_size;
        if (fault >= base && fault < end) {
            uintptr_t fp = (fault / ps) * ps;
            if (fp == base || fp == end - ps) {
                c->state = CTPPOOL_CORO_ERROR;
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

void ctpool_coro_install_guard_handler(void)
{
    if (atomic_load_explicit(&g_guard_installed, memory_order_relaxed)) return;
    struct sigaction sa;
    sa.sa_sigaction = guard_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_SIGINFO;
    if (sigaction(SIGSEGV, &sa, NULL) != 0 ||
        sigaction(SIGBUS,  &sa, NULL) != 0) {
        fprintf(stderr, "ctpool: sigaction failed: %s\n", strerror(errno));
        return;
    }
    atomic_store_explicit(&g_guard_installed, true, memory_order_relaxed);
}

void ctpool_coro_uninstall_guard_handler(void)
{
    if (!atomic_load_explicit(&g_guard_installed, memory_order_relaxed)) return;
    struct sigaction sa;
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGSEGV, &sa, NULL);
    sigaction(SIGBUS,  &sa, NULL);
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
static ctpool_coro_result_t allocate_stack(ctpool_coroutine_t *c)
{
    long psl = sysconf(_SC_PAGESIZE);
    if (psl <= 0) psl = 4096;
    size_t ps       = (size_t)psl;
    size_t guard_nb = CTPPOOL_CORO_GUARD_PAGES_EACH * 2;
    size_t usable_pg = (c->stack_size + ps - 1) / ps;
    size_t total_pg  = guard_nb + usable_pg;
    size_t total_sz  = total_pg * ps;

    void *base = mmap(NULL, total_sz, PROT_NONE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) return CTPPOOL_CORO_ERR_ALLOC;

    c->mmap_base  = base;
    c->mmap_size  = total_sz;

    /* The usable region starts after the bottom guard pages. */
    size_t offset    = guard_nb * ps;
    size_t usable_sz = usable_pg * ps;
    c->stack_start   = (char *)base + offset;
    c->stack_end     = (char *)base + offset + usable_sz;

    if (mprotect(c->stack_start, usable_sz,
                 PROT_READ | PROT_WRITE) != 0) {
        munmap(base, total_sz);
        c->mmap_base = NULL;
        return CTPPOOL_CORO_ERR_MPROTECT;
    }
    return CTPPOOL_CORO_OK;
}

static void deallocate_stack(ctpool_coroutine_t *c)
{
    if (c->mmap_base != NULL) {
        munmap(c->mmap_base, c->mmap_size);
        c->mmap_base = NULL;
        c->mmap_size = 0;
    }
    c->stack_start = NULL;
    c->stack_end   = NULL;
}

/* ================================================================
 *  Scheduler – a single persistent context with its own stack.
 *  Per-thread to support cross-thread coroutine use.
 * ================================================================ */
static bool ensure_scheduler(void)
{
    if (g_scheduler_inited) return true;
    g_scheduler_stack = (char *)malloc(131072);
    if (!g_scheduler_stack) return false;
    if (getcontext(&g_scheduler) != 0) {
        free(g_scheduler_stack);
        g_scheduler_stack = NULL;
        return false;
    }
    g_scheduler.uc_stack.ss_sp     = g_scheduler_stack;
    g_scheduler.uc_stack.ss_size   = 131072;
    g_scheduler.uc_stack.ss_flags  = 0;
    g_scheduler.uc_link            = NULL;
    g_scheduler_inited             = true;
    return true;
}

/* ================================================================
 *  Coroutine entry point
 * ================================================================ */
static void coro_entry(void *arg)
{
    ctpool_coroutine_t *c = (ctpool_coroutine_t *)arg;
    g_current = c;
    c->state = CTPPOOL_CORO_RUNNING;
    c->entry_fn(c->user_data);
    /* entry_fn returned normally (no yield) → mark done and return to scheduler */
    c->state = CTPPOOL_CORO_DONE;
    g_current = NULL;
    if (c->ctx.uc_link != NULL) {
        swapcontext(&c->ctx, (ucontext_t *)c->ctx.uc_link);
    }
}

/* ================================================================
 *  Public API
 * ================================================================ */
ctpool_coro_result_t ctpool_coro_create(ctpool_coro_fn fn,
                                         void *data,
                                         size_t stack_size,
                                         ctpool_coroutine_t **coro)
{
    if (fn == NULL || coro == NULL) return CTPPOOL_CORO_ERR_INVALID;

    ctpool_coroutine_t *c =
        (ctpool_coroutine_t *)calloc(1, sizeof(ctpool_coroutine_t));
    if (!c) return CTPPOOL_CORO_ERR_ALLOC;

    c->state           = CTPPOOL_CORO_NEW;
    c->entry_fn        = fn;
    c->user_data       = data;
    c->stack_size      = (stack_size > 0) ? stack_size
                                           : CTPPOOL_CORO_DEFAULT_STACK_SIZE;
    c->mmap_base       = NULL;
    c->mmap_size       = 0;
    c->stack_start     = NULL;
    c->stack_end       = NULL;

    ctpool_coro_result_t rc = allocate_stack(c);
    if (rc != CTPPOOL_CORO_OK) { free(c); return rc; }

    *coro = c;
    return CTPPOOL_CORO_OK;
}

ctpool_coro_result_t ctpool_coro_resume(ctpool_coroutine_t *coro)
{
    if (coro == NULL) return CTPPOOL_CORO_ERR_INVALID;

    ctpool_coro_install_guard_handler();
    if (setjmp(g_guard_jmp) != 0) return CTPPOOL_CORO_ERR_GUARD;
    if (!ensure_scheduler()) return CTPPOOL_CORO_ERR_CONTEXT;

    if (coro->state == CTPPOOL_CORO_DONE ||
        coro->state == CTPPOOL_CORO_ERROR) {
        return CTPPOOL_CORO_ERR_RUNNING;
    }

    if (coro->state == CTPPOOL_CORO_NEW) {
        if (getcontext(&coro->ctx) != 0) return CTPPOOL_CORO_ERR_CONTEXT;
        coro->ctx.uc_stack.ss_sp     = coro->stack_start;
        coro->ctx.uc_stack.ss_size   =
            (size_t)((char *)coro->stack_end - (char *)coro->stack_start);
        coro->ctx.uc_stack.ss_flags  = 0;
        coro->ctx.uc_link            = &g_scheduler;
        makecontext(&coro->ctx, (void (*)(void))coro_entry, 1,
                    (unsigned long)(uintptr_t)coro);
        coro->state = CTPPOOL_CORO_RUNNING;
    }

    if (swapcontext(&g_scheduler, &coro->ctx) != 0) {
        coro->state = CTPPOOL_CORO_ERROR;
        return CTPPOOL_CORO_ERR_CONTEXT;
    }
    return CTPPOOL_CORO_OK;
}

void ctpool_coro_yield(void)
{
    ctpool_coroutine_t *cur = g_current;
    if (cur == NULL || cur->state != CTPPOOL_CORO_RUNNING) return;
    cur->state = CTPPOOL_CORO_SUSPENDED;
    if (swapcontext(&cur->ctx, &g_scheduler) != 0) {
        cur->state = CTPPOOL_CORO_ERROR;
    }
}

void ctpool_coro_suspend(void)
{
    ctpool_coro_yield();
}

ctpool_coro_result_t ctpool_coro_terminate(ctpool_coroutine_t *coro)
{
    if (coro == NULL) return CTPPOOL_CORO_ERR_INVALID;
    if (coro->state == CTPPOOL_CORO_DONE ||
        coro->state == CTPPOOL_CORO_ERROR) {
        return CTPPOOL_CORO_OK;
    }
    coro->state = CTPPOOL_CORO_DONE;
    if (coro == g_current) {
        g_current = NULL;
        if (swapcontext(&coro->ctx, &g_scheduler) != 0) {
            return CTPPOOL_CORO_ERR_CONTEXT;
        }
    }
    return CTPPOOL_CORO_OK;
}

void ctpool_coro_destroy(ctpool_coroutine_t **coro)
{
    if (!coro || !*coro) return;
    ctpool_coroutine_t *c = *coro;
    deallocate_stack(c);
    if (g_current == c) g_current = NULL;
    free(c);
    *coro = NULL;
}

ctpool_coro_state_t ctpool_coro_state(const ctpool_coroutine_t *coro)
{
    if (!coro) return CTPPOOL_CORO_ERROR;
    return coro->state;
}

ctpool_coro_result_t ctpool_coro_stack_info(const ctpool_coroutine_t *coro,
                                             void **start,
                                             void **end)
{
    if (!coro) return CTPPOOL_CORO_ERR_INVALID;
    if (start) *start = coro->stack_start;
    if (end)   *end   = coro->stack_end;
    return CTPPOOL_CORO_OK;
}

const char *ctpool_coro_result_str(ctpool_coro_result_t result)
{
    switch (result) {
        case CTPPOOL_CORO_OK:            return "OK";
        case CTPPOOL_CORO_ERR_ALLOC:     return "Allocation failed";
        case CTPPOOL_CORO_ERR_CONTEXT:   return "Context error";
        case CTPPOOL_CORO_ERR_MPROTECT:  return "mprotect error";
        case CTPPOOL_CORO_ERR_INVALID:   return "Invalid argument";
        case CTPPOOL_CORO_ERR_GUARD:     return "Guard page violation";
        case CTPPOOL_CORO_ERR_RUNNING:   return "Invalid state";
        default:                         return "Unknown";
    }
}
