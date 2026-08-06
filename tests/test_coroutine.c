#define _POSIX_C_SOURCE 200809L
#include "ctpool/coroutine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

static int g_passes   = 0;
static int g_failures = 0;

#define ASSERT(expr, msg) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s at %s:%d\n", msg, __FILE__, __LINE__); \
        g_failures++; \
    } else { \
        g_passes++; \
    } \
} while (0)

/* ---------- Helpers ---------- */
static void simple_coro_fn(void *arg)
{
    int *counter = (int *)arg;
    __sync_fetch_and_add(counter, 1);
}

static void yield_coro_fn(void *arg)
{
    int *counter = (int *)arg;
    __sync_fetch_and_add(counter, 1);
    ctpool_coro_yield();
    __sync_fetch_and_add(counter, 1);
}

static void nested_coro_fn(void *arg)
{
    int *state = (int *)arg;
    *state = 1;
    ctpool_coro_yield();
    *state = 2;
}

/* ---------- Test: null args ---------- */
static void test_null_args(void)
{
    ctpool_coro_result_t rc;
    rc = ctpool_coro_create(NULL, NULL, 0, NULL);
    ASSERT(rc == CTPPOOL_CORO_ERR_INVALID, "null fn + null out");

    rc = ctpool_coro_resume(NULL);
    ASSERT(rc == CTPPOOL_CORO_ERR_INVALID, "null resume");

    rc = ctpool_coro_terminate(NULL);
    ASSERT(rc == CTPPOOL_CORO_ERR_INVALID, "null terminate");

    ASSERT(ctpool_coro_state(NULL) == CTPPOOL_CORO_ERROR, "null state");

    void *s = NULL, *e = NULL;
    rc = ctpool_coro_stack_info(NULL, &s, &e);
    ASSERT(rc == CTPPOOL_CORO_ERR_INVALID, "null stack info");
}

/* ---------- Test: simple run to completion ---------- */
static void test_simple_run(void)
{
    ctpool_coroutine_t *coro = NULL;
    int counter = 0;
    ctpool_coro_result_t rc = ctpool_coro_create(simple_coro_fn, &counter, 0, &coro);
    ASSERT(rc == CTPPOOL_CORO_OK,                      "create coroutine");
    ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_NEW, "state is NEW");

    rc = ctpool_coro_resume(coro);
    ASSERT(rc == CTPPOOL_CORO_OK,    "resume coroutine");
    ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_DONE, "state is DONE");
    ASSERT(counter == 1,             "counter incremented");

    rc = ctpool_coro_resume(coro);
    ASSERT(rc == CTPPOOL_CORO_ERR_RUNNING, "resume done coroutine fails");

    ctpool_coro_destroy(&coro);
    ASSERT(coro == NULL, "destroy sets to null");
}

/* ---------- Test: yield and resume ---------- */
static void test_yield_resume(void)
{
    ctpool_coroutine_t *coro = NULL;
    int counter = 0;
    ctpool_coro_create(yield_coro_fn, &counter, 0, &coro);

    ctpool_coro_resume(coro);
    ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_SUSPENDED, "state SUSPENDED after yield");
    ASSERT(counter == 1, "counter is 1 after first yield");

    ctpool_coro_resume(coro);
    ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_DONE,    "state DONE after second resume");
    ASSERT(counter == 2, "counter is 2 after second resume");

    ctpool_coro_destroy(&coro);
}

/* ---------- Test: multiple coroutines ---------- */
static void test_multiple_coroutines(void)
{
    ctpool_coroutine_t *c1 = NULL, *c2 = NULL;
    int counter1 = 0, counter2 = 0;

    ctpool_coro_create(simple_coro_fn, &counter1, 0, &c1);
    ctpool_coro_create(simple_coro_fn, &counter2, 0, &c2);

    ctpool_coro_resume(c1);
    ASSERT(counter1 == 1,           "coro1 completed");
    ASSERT(ctpool_coro_state(c1) == CTPPOOL_CORO_DONE, "coro1 DONE");

    ctpool_coro_resume(c2);
    ASSERT(counter2 == 1,           "coro2 completed");
    ASSERT(ctpool_coro_state(c2) == CTPPOOL_CORO_DONE, "coro2 DONE");

    ctpool_coro_destroy(&c1);
    ctpool_coro_destroy(&c2);
}

/* ---------- Test: stack info ---------- */
static void test_stack_info(void)
{
    ctpool_coroutine_t *coro = NULL;
    int counter = 0;
    ctpool_coro_create(simple_coro_fn, &counter, 0, &coro);

    void *start = NULL, *end = NULL;
    ctpool_coro_result_t rc = ctpool_coro_stack_info(coro, &start, &end);
    ASSERT(rc == CTPPOOL_CORO_OK,   "stack info");
    ASSERT(start != NULL,           "stack start not null");
    ASSERT(end   != NULL,           "stack end   not null");
    ASSERT((char *)end > (char *)start, "end > start");

    ctpool_coro_resume(coro);
    ASSERT(counter == 1, "counter incremented after resume");
    ctpool_coro_destroy(&coro);
}

/* ---------- Test: terminate ---------- */
static void test_terminate(void)
{
    ctpool_coroutine_t *coro = NULL;
    int state = 0;
    ctpool_coro_create(nested_coro_fn, &state, 0, &coro);

    ctpool_coro_resume(coro);
    ASSERT(state == 1,                    "state is 1 after first resume");
    ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_SUSPENDED, "state SUSPENDED");

    ctpool_coro_result_t rc = ctpool_coro_terminate(coro);
    ASSERT(rc == CTPPOOL_CORO_OK,       "terminate");
    ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_DONE,    "state DONE after terminate");

    ctpool_coro_destroy(&coro);
}

/* ---------- Test: result string ---------- */
static void test_result_str(void)
{
    ASSERT(ctpool_coro_result_str(CTPPOOL_CORO_OK)           != NULL, "result str OK");
    ASSERT(ctpool_coro_result_str(CTPPOOL_CORO_ERR_ALLOC)    != NULL, "result str ALLOC");
    ASSERT(ctpool_coro_result_str(CTPPOOL_CORO_ERR_CONTEXT)  != NULL, "result str CONTEXT");
    ASSERT(ctpool_coro_result_str(99)                       != NULL, "result str unknown");
}

/* ---------- Test: custom stack size ---------- */
static void test_custom_stack_size(void)
{
    ctpool_coroutine_t *coro = NULL;
    int counter = 0;
    ctpool_coro_result_t rc = ctpool_coro_create(simple_coro_fn, &counter, 8192, &coro);
    ASSERT(rc == CTPPOOL_CORO_OK, "create with 8KB stack");
    ctpool_coro_resume(coro);
    ASSERT(ctpool_coro_state(coro) == CTPPOOL_CORO_DONE, "state DONE");
    ctpool_coro_destroy(&coro);
}

/* ---------- Test: destroy null ---------- */
static void test_destroy_null(void)
{
    ctpool_coroutine_t *coro = NULL;
    ctpool_coro_destroy(NULL);
    ctpool_coro_destroy(&coro);
    ASSERT(coro == NULL, "destroy null is safe");
}

/* ================================================================
 *  Main
 * ================================================================ */

int main(void)
{
    printf("=== Coroutine Tests ===\n");

    test_null_args();
    test_simple_run();
    test_yield_resume();
    test_multiple_coroutines();
    test_stack_info();
    test_terminate();
    test_result_str();
    test_custom_stack_size();
    test_destroy_null();

    printf("\nResults: %d passed, %d failed\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
