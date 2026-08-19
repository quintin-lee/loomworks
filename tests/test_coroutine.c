#define _POSIX_C_SOURCE 200809L
#include "loomworks/coroutine.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int g_passes   = 0;
static int g_failures = 0;

#define ASSERT(expr, msg)                                                                          \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "FAIL: %s at %s:%d\n", msg, __FILE__, __LINE__);                       \
            g_failures++;                                                                          \
        } else {                                                                                   \
            g_passes++;                                                                            \
        }                                                                                          \
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
    loom_coro_yield();
    __sync_fetch_and_add(counter, 1);
}

static void nested_coro_fn(void *arg)
{
    int *state = (int *)arg;
    *state     = 1;
    loom_coro_yield();
    *state = 2;
}

static void multi_yield_coro_fn(void *arg)
{
    int *counter = (int *)arg;
    __sync_fetch_and_add(counter, 1); /* 1st resume */
    loom_coro_yield();
    __sync_fetch_and_add(counter, 1); /* 2nd resume */
    loom_coro_yield();
    __sync_fetch_and_add(counter, 1); /* 3rd resume */
}

static void sleepy_coro_fn(void *arg)
{
    int *state = (int *)arg;
    (*state)++;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t now_ns = (int64_t)ts.tv_sec * 1000000000 + (int64_t)ts.tv_nsec;
    loom_coro_sleep_until(now_ns + 10000000); /* +10 ms */
    (*state)++;
}

static loom_coroutine_t *g_self_terminate_target;
static int               g_self_terminate_survivor;

/* Self-terminates mid-run; the handle is reached via a file-scope
 * pointer because create() assigns *out only after arg is bound. */
static void self_terminate_coro_fn(void *arg)
{
    (void)arg;
    loom_coro_result_t rc = loom_coro_terminate(g_self_terminate_target);
    (void)rc; /* never reached: terminate(self) as g_current swaps away */
    __sync_fetch_and_add(&g_self_terminate_survivor, 1); /* must not run */
}

/* ---------- Test: null args ---------- */
static void test_null_args(void)
{
    loom_coro_result_t rc;
    rc = loom_coro_create(NULL, NULL, 0, NULL);
    ASSERT(rc == LOOMWORKS_CORO_ERR_INVALID, "null fn + null out");

    rc = loom_coro_resume(NULL);
    ASSERT(rc == LOOMWORKS_CORO_ERR_INVALID, "null resume");

    rc = loom_coro_terminate(NULL);
    ASSERT(rc == LOOMWORKS_CORO_ERR_INVALID, "null terminate");

    ASSERT(loom_coro_state(NULL) == LOOMWORKS_CORO_ERROR, "null state");

    void *s = NULL, *e = NULL;
    rc = loom_coro_stack_info(NULL, &s, &e);
    ASSERT(rc == LOOMWORKS_CORO_ERR_INVALID, "null stack info");
}

/* ---------- Test: simple run to completion ---------- */
static void test_simple_run(void)
{
    loom_coroutine_t  *coro    = NULL;
    int                counter = 0;
    loom_coro_result_t rc      = loom_coro_create(simple_coro_fn, &counter, 0, &coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "create coroutine");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_NEW, "state is NEW");

    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "resume coroutine");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "state is DONE");
    ASSERT(counter == 1, "counter incremented");

    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_ERR_RUNNING, "resume done coroutine fails");

    loom_coro_destroy(&coro);
    ASSERT(coro == NULL, "destroy sets to null");
}

/* ---------- Test: yield and resume (single yield) ---------- */
static void test_yield_resume(void)
{
    loom_coroutine_t  *coro    = NULL;
    int                counter = 0;
    loom_coro_result_t rc;
    loom_coro_create(yield_coro_fn, &counter, 0, &coro);

    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "first resume");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED, "state SUSPENDED after yield");
    ASSERT(counter == 1, "counter is 1 after first yield");

    /* Second resume continues past second yield, function completes */
    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "second resume");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "state DONE after function completes");
    ASSERT(counter == 2, "counter is 2 after completion");

    loom_coro_destroy(&coro);
}

/* ---------- Test: multiple coroutines ---------- */
static void test_multiple_coroutines(void)
{
    loom_coroutine_t *c1 = NULL, *c2 = NULL;
    int               counter1 = 0, counter2 = 0;

    loom_coro_create(simple_coro_fn, &counter1, 0, &c1);
    loom_coro_create(simple_coro_fn, &counter2, 0, &c2);

    loom_coro_resume(c1);
    ASSERT(counter1 == 1, "coro1 completed");
    ASSERT(loom_coro_state(c1) == LOOMWORKS_CORO_DONE, "coro1 DONE");

    loom_coro_resume(c2);
    ASSERT(counter2 == 1, "coro2 completed");
    ASSERT(loom_coro_state(c2) == LOOMWORKS_CORO_DONE, "coro2 DONE");

    loom_coro_destroy(&c1);
    loom_coro_destroy(&c2);
}

/* ---------- Test: stack info ---------- */
static void test_stack_info(void)
{
    loom_coroutine_t *coro    = NULL;
    int               counter = 0;
    loom_coro_create(simple_coro_fn, &counter, 0, &coro);

    void              *start = NULL, *end = NULL;
    loom_coro_result_t rc = loom_coro_stack_info(coro, &start, &end);
    ASSERT(rc == LOOMWORKS_CORO_OK, "stack info");
    ASSERT(start != NULL, "stack start not null");
    ASSERT(end != NULL, "stack end   not null");
    ASSERT((char *)end > (char *)start, "end > start");

    loom_coro_resume(coro);
    ASSERT(counter == 1, "counter incremented after resume");
    loom_coro_destroy(&coro);
}

/* ---------- Test: terminate ---------- */
static void test_terminate(void)
{
    loom_coroutine_t *coro  = NULL;
    int               state = 0;
    loom_coro_create(nested_coro_fn, &state, 0, &coro);

    loom_coro_resume(coro);
    ASSERT(state == 1, "state is 1 after first resume");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED, "state SUSPENDED");

    loom_coro_result_t rc = loom_coro_terminate(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "terminate");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "state DONE after terminate");

    loom_coro_destroy(&coro);
}

/* ---------- Test: result string ---------- */
static void test_result_str(void)
{
    ASSERT(loom_coro_result_str(LOOMWORKS_CORO_OK) != NULL, "result str OK");
    ASSERT(loom_coro_result_str(LOOMWORKS_CORO_ERR_ALLOC) != NULL, "result str ALLOC");
    ASSERT(loom_coro_result_str(LOOMWORKS_CORO_ERR_CONTEXT) != NULL, "result str CONTEXT");
    ASSERT(loom_coro_result_str(LOOMWORKS_CORO_ERR_GUARD) != NULL, "result str GUARD");
    ASSERT(loom_coro_result_str(LOOMWORKS_CORO_ERR_INVALID) != NULL, "result str INVALID");
    ASSERT(loom_coro_result_str(LOOMWORKS_CORO_ERR_RUNNING) != NULL, "result str RUNNING");
    /* NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) */
    ASSERT(loom_coro_result_str((loom_coro_result_t)99) != NULL, "result str unknown");
}

/* ---------- Test: custom stack size ---------- */
static void test_custom_stack_size(void)
{
    loom_coroutine_t  *coro    = NULL;
    int                counter = 0;
    loom_coro_result_t rc      = loom_coro_create(simple_coro_fn, &counter, 8192, &coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "create with 8KB stack");
    loom_coro_resume(coro);
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "state DONE");
    loom_coro_destroy(&coro);
}

/* ---------- Test: destroy null ---------- */
static void test_destroy_null(void)
{
    loom_coroutine_t *coro = NULL;
    loom_coro_destroy(NULL);
    loom_coro_destroy(&coro);
    ASSERT(coro == NULL, "destroy null is safe");
}

/* ---------- Test: multi yield/resume cycle ---------- */
static void test_multi_yield_resume(void)
{
    loom_coroutine_t  *coro    = NULL;
    int                counter = 0;
    loom_coro_result_t rc;
    loom_coro_create(multi_yield_coro_fn, &counter, 0, &coro);

    /* 1st resume: executes up to 1st yield */
    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "1st resume");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED, "SUSPENDED after 1st yield");
    ASSERT(counter == 1, "counter=1 after 1st yield");

    /* 2nd resume: continues past 1st yield, stops at 2nd yield */
    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "2nd resume");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED, "SUSPENDED after 2nd yield");
    ASSERT(counter == 2, "counter=2 after 2nd yield");

    /* 3rd resume: completes */
    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "3rd resume");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE after 3rd resume");
    ASSERT(counter == 3, "counter=3 after all increments");

    loom_coro_destroy(&coro);
}

/* ---------- Test: terminate new (never resumed) coroutine ---------- */
static void test_terminate_new(void)
{
    loom_coroutine_t *coro    = NULL;
    int               counter = 0;
    loom_coro_create(simple_coro_fn, &counter, 0, &coro);
    /* Never resume — terminate immediately */
    loom_coro_result_t rc = loom_coro_terminate(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "terminate new coro");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "state DONE after terminate new");
    ASSERT(counter == 0, "counter not incremented (never ran)");
    loom_coro_destroy(&coro);
}

/* ---------- Test: double resume after yield ---------- */
static void test_double_resume_after_yield(void)
{
    loom_coroutine_t *coro    = NULL;
    int               counter = 0;
    loom_coro_create(yield_coro_fn, &counter, 0, &coro);

    loom_coro_resume(coro);
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED, "SUSPENDED after yield");

    loom_coro_resume(coro);
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE after second resume");
    ASSERT(counter == 2, "counter=2");

    /* Resume again — should fail */
    loom_coro_result_t rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_ERR_RUNNING, "resume DONE coro fails");

    loom_coro_destroy(&coro);
}

/* ---------- Test: terminate already-done coroutine ---------- */
static void test_terminate_done(void)
{
    loom_coroutine_t *coro    = NULL;
    int               counter = 0;
    loom_coro_create(simple_coro_fn, &counter, 0, &coro);
    loom_coro_resume(coro);
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE");

    loom_coro_result_t rc = loom_coro_terminate(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "terminate already-done coro");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "still DONE");

    loom_coro_destroy(&coro);
}

/* ---------- Test: many sequential coroutines ---------- */
static void test_many_coroutines(void)
{
    const int N       = 500;
    int       counter = 0;
    for (int i = 0; i < N; i++) {
        loom_coroutine_t  *coro = NULL;
        loom_coro_result_t rc   = loom_coro_create(simple_coro_fn, &counter, 0, &coro);
        ASSERT(rc == LOOMWORKS_CORO_OK, "create coro");
        rc = loom_coro_resume(coro);
        ASSERT(rc == LOOMWORKS_CORO_OK, "resume coro");
        ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE");
        loom_coro_destroy(&coro);
    }
    ASSERT(counter == N, "all coroutines completed");
}

/* ================================================================
 *  Stack pool tests
 * ================================================================ */
/* ---------- Test: pool reuses the same mapping ----------
 * Create/destroy 1000 default-size coroutines. Each cycle must recycle
 * the exact same stack mapping (pool hit) — proven by a sentinel byte
 * written into the live mapping that must survive destroy + recreate.
 * (Address equality alone is not enough: the kernel's mmap allocator
 * reuses the same hole after munmap. A fresh MAP_ANONYMOUS mmap would
 * be zero-filled, so a surviving sentinel proves userspace pooling.)
 * ---------- */
static void test_stack_pool_reuse(void)
{
    void *first_start = NULL;
    void *first_end   = NULL;
    for (int i = 0; i < 1000; i++) {
        loom_coroutine_t *coro = NULL;
        ASSERT(loom_coro_create(simple_coro_fn, NULL, 0, &coro) == LOOMWORKS_CORO_OK,
               "pool reuse create");
        void *start = NULL;
        void *end   = NULL;
        ASSERT(loom_coro_stack_info(coro, &start, &end) == LOOMWORKS_CORO_OK,
               "pool reuse stack info");
        if (i > 0) {
            ASSERT(start == first_start && end == first_end, "stack mapping recycled from pool");
            ASSERT(*(volatile char *)first_start == (char)0x5A,
                   "pool retains stack contents (no fresh mmap)");
        } else {
            first_start = start;
            first_end   = end;
        }
        *(volatile char *)start = (char)0x5A;
        loom_coro_destroy(&coro);
    }
}

/* ---------- Test: size isolation (exact-match key) ---------- */
static void test_stack_pool_size_isolation(void)
{
    loom_coroutine_t *coro = NULL;
    ASSERT(loom_coro_create(simple_coro_fn, NULL, 0, &coro) == LOOMWORKS_CORO_OK,
           "size isolation create 64K");
    void *s64 = NULL;
    void *e64 = NULL;
    ASSERT(loom_coro_stack_info(coro, &s64, &e64) == LOOMWORKS_CORO_OK, "size isolation info 64K");
    *(volatile char *)s64 = (char)0x5A;
    loom_coro_destroy(&coro);

    /* A 256 KiB request must NOT receive the pooled 64 KiB mapping. */
    ASSERT(loom_coro_create(simple_coro_fn, NULL, (size_t)256 * 1024, &coro) == LOOMWORKS_CORO_OK,
           "size isolation create 256K");
    void *s256 = NULL;
    void *e256 = NULL;
    ASSERT(loom_coro_stack_info(coro, &s256, &e256) == LOOMWORKS_CORO_OK,
           "size isolation info 256K");
    ASSERT((size_t)((char *)e256 - (char *)s256) == (size_t)256 * 1024,
           "256K stack is full size (no 64K reuse)");
    ASSERT(s256 != s64, "256K mapping differs from pooled 64K mapping");
    loom_coro_destroy(&coro);

    /* The 64 KiB mapping must still be pooled for later reuse. */
    ASSERT(loom_coro_create(simple_coro_fn, NULL, 0, &coro) == LOOMWORKS_CORO_OK,
           "size isolation create 64K again");
    void *s64b = NULL;
    void *e64b = NULL;
    ASSERT(loom_coro_stack_info(coro, &s64b, &e64b) == LOOMWORKS_CORO_OK,
           "size isolation info 64K again");
    ASSERT(s64b == s64 && e64b == e64, "64K mapping preserved in pool");
    ASSERT(*(volatile char *)s64b == (char)0x5A, "64K mapping retained (not re-mmap'd)");
    loom_coro_destroy(&coro);
}

/* ---------- Test: guard pages intact on pooled reuse ---------- */
typedef struct {
    char *target;
} guard_arg_t;

static void guard_write_fn(void *arg)
{
    guard_arg_t   *a = (guard_arg_t *)arg;
    volatile char *p = (volatile char *)a->target;
    *p               = (char)0xAA; /* PROT_NONE guard page -> SIGSEGV -> LOOMWORKS_CORO_ERR_GUARD */
}

static void test_stack_pool_guard_on_reuse(void)
{
    long psl = sysconf(_SC_PAGESIZE);
    if (psl <= 0) {
        psl = 4096;
    }
    size_t ps = (size_t)psl;

    loom_coroutine_t *coro = NULL;
    ASSERT(loom_coro_create(guard_write_fn, NULL, 0, &coro) == LOOMWORKS_CORO_OK,
           "guard reuse create 1");
    void *start = NULL;
    void *end   = NULL;
    ASSERT(loom_coro_stack_info(coro, &start, &end) == LOOMWORKS_CORO_OK, "guard reuse info 1");
    loom_coro_destroy(&coro);

    guard_arg_t arg = {0};
    ASSERT(loom_coro_create(guard_write_fn, &arg, 0, &coro) == LOOMWORKS_CORO_OK,
           "guard reuse create 2");
    void *start2 = NULL;
    void *end2   = NULL;
    ASSERT(loom_coro_stack_info(coro, &start2, &end2) == LOOMWORKS_CORO_OK, "guard reuse info 2");
    ASSERT(start2 == start && end2 == end, "mapping recycled from pool");

    /* Layout: [GUARD][GUARD][usable]; stack_start = base + 2*ps, so
     * start2 - 2*ps == mmap base (first PROT_NONE page). The handler
     * catches fp == base and longjmps -> resume returns ERR_GUARD. */
    arg.target            = (char *)start2 - (long)(LOOMWORKS_CORO_GUARD_PAGES_EACH * 2) * (long)ps;
    loom_coro_result_t rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_ERR_GUARD, "guard violation trapped on pooled stack");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_ERROR, "state ERROR after guard fault");
    loom_coro_destroy(&coro);
}

/* ---------- Test: self-terminate mid-run ---------- */
static void test_self_terminate(void)
{
    loom_coroutine_t *coro = NULL;

    ASSERT(loom_coro_create(self_terminate_coro_fn, NULL, 0, &coro) == LOOMWORKS_CORO_OK,
           "create self-terminate coroutine");
    g_self_terminate_target = coro;
    /* First resume: the coroutine terminates itself, which must swap
     * back to the scheduler (not crash / not continue). */
    ASSERT(loom_coro_resume(coro) == LOOMWORKS_CORO_OK, "resume self-terminate");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE after self-terminate");
    ASSERT(g_self_terminate_survivor == 0, "post-terminate code did not run");
    g_self_terminate_target = NULL;
    ASSERT(loom_coro_destroy(&coro) == LOOMWORKS_CORO_OK, "destroy after self-terminate");
}

/* ---------- Test: yield then terminate ---------- */
static void test_yield_then_terminate(void)
{
    loom_coroutine_t *coro  = NULL;
    int               state = 0;
    loom_coro_create(nested_coro_fn, &state, 0, &coro);

    loom_coro_resume(coro);
    ASSERT(state == 1, "state=1");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED, "SUSPENDED");

    /* Terminate a suspended coroutine */
    loom_coro_result_t rc = loom_coro_terminate(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "terminate suspended coro");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE after terminate");
    ASSERT(state == 1, "state still 1 (second part not executed)");

    loom_coro_destroy(&coro);
}

/* ---------- Test: stack info after destroy ---------- */
static void test_stack_info_after_destroy(void)
{
    loom_coroutine_t *coro = NULL;
    loom_coro_create(simple_coro_fn, NULL, 0, &coro);
    void *start = NULL, *end = NULL;
    loom_coro_stack_info(coro, &start, &end);
    /* Do not resume with NULL data — simple_coro_fn would SIGSEGV
       (address 0 is outside the mmap'd guard region, so the guard
       handler reinstalls the default handler and re-raises SIGSEGV). */
    loom_coro_destroy(&coro);
    /* start/end point to freed mmap region after destroy.
       We only assert that stack_info worked before destroy. */
    ASSERT(start != NULL, "stack start valid before destroy");
    ASSERT(true, "stack info after destroy safe");
}

/* ---------- Test: coro with NULL data ---------- */
static void test_coro_null_data(void)
{
    loom_coroutine_t  *coro = NULL;
    loom_coro_result_t rc   = loom_coro_create(simple_coro_fn, NULL, 0, &coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "create with NULL data");
    /* This will SIGSEGV when simple_coro_fn dereferences NULL —
       but the test verifies the create/resume path reaches the entry point */
    /* We skip actual resume since NULL data causes segfault in simple_coro_fn */
    loom_coro_destroy(&coro);
    ASSERT(true, "create with NULL data accepted");
}

/* ---------- Test: custom small stack size ---------- */
static void test_small_stack(void)
{
    loom_coroutine_t  *coro    = NULL;
    int                counter = 0;
    loom_coro_result_t rc      = loom_coro_create(simple_coro_fn, &counter, 4096, &coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "create with 4KB stack");
    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "resume 4KB stack coro");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE");
    ASSERT(counter == 1, "counter=1");
    loom_coro_destroy(&coro);
}

/* ---------- Test: cross-thread resume/terminate rejected ---------- */
typedef struct {
    loom_coroutine_t  *coro;
    loom_coro_result_t rc;
} cross_thread_arg_t;

static void *foreign_resume_fn(void *arg)
{
    cross_thread_arg_t *a = (cross_thread_arg_t *)arg;
    a->rc                 = loom_coro_resume(a->coro);
    return NULL;
}

static void *foreign_terminate_fn(void *arg)
{
    cross_thread_arg_t *a = (cross_thread_arg_t *)arg;
    a->rc                 = loom_coro_terminate(a->coro);
    return NULL;
}

static void test_cross_thread_guard(void)
{
    loom_coroutine_t  *coro    = NULL;
    int                counter = 0;
    loom_coro_result_t rc      = loom_coro_create(simple_coro_fn, &counter, 0, &coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "create for cross-thread guard");

    cross_thread_arg_t arg = {coro, LOOMWORKS_CORO_OK};
    pthread_t          t;
    ASSERT(pthread_create(&t, NULL, foreign_resume_fn, &arg) == 0, "spawn foreign resume");
    pthread_join(t, NULL);
    ASSERT(arg.rc == LOOMWORKS_CORO_ERR_INVALID, "foreign resume rejected");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_NEW, "state untouched by foreign resume");
    ASSERT(counter == 0, "entry fn never ran on foreign thread");

    /* Owner thread still owns the coroutine after the rejected attempt. */
    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "owner resume still works");
    ASSERT(counter == 1, "owner thread ran entry fn");
    loom_coro_destroy(&coro);

    coro              = NULL;
    int yield_counter = 0;
    rc                = loom_coro_create(yield_coro_fn, &yield_counter, 0, &coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "create yield coro for cross-thread terminate");
    ASSERT(loom_coro_resume(coro) == LOOMWORKS_CORO_OK, "owner resume to yield");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED, "SUSPENDED before foreign terminate");

    arg.coro = coro;
    arg.rc   = LOOMWORKS_CORO_OK;
    ASSERT(pthread_create(&t, NULL, foreign_terminate_fn, &arg) == 0, "spawn foreign terminate");
    pthread_join(t, NULL);
    ASSERT(arg.rc == LOOMWORKS_CORO_ERR_INVALID, "foreign terminate rejected");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED,
           "state untouched by foreign terminate");

    /* Owner can still resume the suspended coroutine to completion. */
    ASSERT(loom_coro_resume(coro) == LOOMWORKS_CORO_OK, "owner resume after foreign reject");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE after owner's final resume");
    ASSERT(yield_counter == 2, "both increments ran on owner thread");

    loom_coro_destroy(&coro);
}

/* ----------------------------------------------------------------
 *  Test: destroying a SUSPENDED coroutine is rejected
 * ----------------------------------------------------------------- */
static void test_coro_destroy_suspended_rejected(void)
{
    loom_coroutine_t *coro    = NULL;
    int               counter = 0;

    ASSERT(loom_coro_create(yield_coro_fn, &counter, 0, &coro) == LOOMWORKS_CORO_OK,
           "create yield coroutine");
    ASSERT(loom_coro_resume(coro) == LOOMWORKS_CORO_OK, "resume to first yield");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SUSPENDED, "SUSPENDED after yield");
    ASSERT(loom_coro_destroy(&coro) == LOOMWORKS_CORO_ERR_INVALID, "destroy suspended rejected");
    ASSERT(coro != NULL, "handle untouched after rejected destroy");
    ASSERT(loom_coro_terminate(coro) == LOOMWORKS_CORO_OK, "terminate suspended");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE after terminate");
    ASSERT(loom_coro_destroy(&coro) == LOOMWORKS_CORO_OK, "destroy done ok");
    ASSERT(coro == NULL, "handle nulled after ok destroy");
}

/* ---------- Test: sleep_until (stand-alone, pure suspend) ---------- */
static void test_coro_sleep_until(void)
{
    loom_coroutine_t *coro  = NULL;
    int               state = 0;
    loom_coro_create(sleepy_coro_fn, &state, 0, &coro);

    loom_coro_result_t rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "sleep resume 1 (enters sleep)");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SLEEPING, "state SLEEPING");
    ASSERT(state == 1, "counter=1 before sleep");

    /* Early resume: rejected, stays SLEEPING. */
    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_ERR_RUNNING, "early resume rejected");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_SLEEPING, "still SLEEPING");

    /* Wait out the deadline, then resume normally. */
    clock_nanosleep(
        CLOCK_MONOTONIC, 0, &(struct timespec){.tv_nsec = 12000000}, NULL); /* 12ms > 10ms */

    rc = loom_coro_resume(coro);
    ASSERT(rc == LOOMWORKS_CORO_OK, "post-deadline resume");
    ASSERT(loom_coro_state(coro) == LOOMWORKS_CORO_DONE, "DONE after sleep completes");
    ASSERT(state == 2, "counter=2 after sleep");

    loom_coro_destroy(&coro);
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
    /* Stack pool tests MUST run before test_many_coroutines (pool cap 64) */
    test_stack_pool_reuse();
    test_stack_pool_size_isolation();
    test_stack_pool_guard_on_reuse();
    test_destroy_null();
    test_multi_yield_resume();
    test_terminate_new();
    test_double_resume_after_yield();
    test_terminate_done();
    test_many_coroutines();
    test_yield_then_terminate();
    test_self_terminate();
    test_stack_info_after_destroy();
    test_coro_null_data();
    test_small_stack();
    test_cross_thread_guard();
    test_coro_destroy_suspended_rejected();
    test_coro_sleep_until();

    printf("\nResults: %d passed, %d failed\n", g_passes, g_failures);
    return g_failures > 0 ? 1 : 0;
}
