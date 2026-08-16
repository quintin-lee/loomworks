/*
 * ctx_smoke.c - backend stress test for the context switching layer.
 *
 * Exercises the assembly (or ucontext) backend through the
 * src/coro_ctx.h primitives:
 *   1. 100,000 swap round-trips between two contexts (rsp/GPR integrity)
 *   2. FP control word survives a yield (rounding-mode round-trip)
 *   3. trampoline NULL-link -> exit(EXIT_SUCCESS), verified in a child
 *
 * (glibc's __start_context exits 0 on a NULL uc_link; the asm trampolines
 * mirror that exactly, so both backends are byte-for-byte equivalent.)
 *
 * Assertions are counted in g_checks; prints "ctx_smoke: N checks passed"
 * and exits 0 on success, or prints the first failing line and exits 1.
 */
#include <fenv.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include "coro_ctx.h"

#define ITERATIONS 100000

static int g_checks;
#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);\
            exit(1);                                                       \
        }                                                                  \
        g_checks++;                                                        \
    } while (0)

/* ---- Test 1: 100k swap round-trips ---------------------------------- */
static loom_coro_ctx_t g_main;
static loom_coro_ctx_t g_coro;
static char g_stack[65536] __attribute__((aligned(16)));
static int g_swaps;

static void
round_trip_entry(void *arg)
{
    (void)arg;
    for (int i = 0; i < ITERATIONS; i++) {
        g_swaps++;
        CHECK(loom_coro_ctx_swap(&g_coro, &g_main) == 0);
    }
    /* Natural return: the trampoline switches back to g_main via the
     * link, so main's final swap returns normally. */
}

static void
test_round_trips(void)
{
    CHECK(loom_coro_ctx_get(&g_main) == 0);
    /* POSIX requires makecontext to operate on a getcontext-initialized
     * context; glibc leaves fpregs unset otherwise (SIGSEGV on swap).
     * coroutine.c's resume path does the same get-before-make. */
    CHECK(loom_coro_ctx_get(&g_coro) == 0);
    loom_coro_ctx_set_stack(&g_coro, g_stack, sizeof g_stack);
    loom_coro_ctx_set_link(&g_coro, &g_main);
    loom_coro_ctx_make(&g_coro, round_trip_entry, NULL);

    for (int i = 0; i < ITERATIONS; i++)
        CHECK(loom_coro_ctx_swap(&g_main, &g_coro) == 0);

    CHECK(g_swaps == ITERATIONS);
}

/* ---- Test 2: FP control word survives a yield ----------------------- */
static loom_coro_ctx_t fp_main;
static loom_coro_ctx_t fp_coro;
static char fp_stack[65536] __attribute__((aligned(16)));

static void
fp_entry(void *arg)
{
    (void)arg;
    CHECK(fesetround(FE_DOWNWARD) == 0);
    CHECK(fegetround() == FE_DOWNWARD);
    CHECK(loom_coro_ctx_swap(&fp_coro, &fp_main) == 0);
    /* Rounding mode must have survived the second swap too. */
    CHECK(fegetround() == FE_DOWNWARD);
}

static void
test_fp_control(void)
{
    CHECK(fesetround(FE_TONEAREST) == 0); /* deterministic baseline */
    CHECK(loom_coro_ctx_get(&fp_main) == 0);
    CHECK(loom_coro_ctx_get(&fp_coro) == 0); /* getcontext-init before make */
    loom_coro_ctx_set_stack(&fp_coro, fp_stack, sizeof fp_stack);
    loom_coro_ctx_set_link(&fp_coro, &fp_main);
    loom_coro_ctx_make(&fp_coro, fp_entry, NULL);

    CHECK(loom_coro_ctx_swap(&fp_main, &fp_coro) == 0);
    /* Main's own rounding mode must be untouched by the yield. */
    CHECK(fegetround() == FE_TONEAREST);
    CHECK(loom_coro_ctx_swap(&fp_main, &fp_coro) == 0);
}

/* ---- Test 3: NULL-link fn return exits with EXIT_FAILURE (in a child) */
static loom_coro_ctx_t nl_main;
static loom_coro_ctx_t nl_coro;
static char nl_stack[65536] __attribute__((aligned(16)));

static void
empty_entry(void *arg)
{
    (void)arg;
    /* Return normally: the trampoline must then see the NULL link and
     * exit(EXIT_SUCCESS), which is what the parent asserts below. */
}

static void
test_null_link_exit(void)
{
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(1);
    }
    if (pid == 0) {
        /* Child: fn returns with a NULL link -> trampoline must call
         * exit(EXIT_SUCCESS), so the parent sees that status. */
        if (loom_coro_ctx_get(&nl_main) != 0)
            _exit(2);
        if (loom_coro_ctx_get(&nl_coro) != 0) /* getcontext-init before make */
            _exit(2);
        loom_coro_ctx_set_stack(&nl_coro, nl_stack, sizeof nl_stack);
        loom_coro_ctx_set_link(&nl_coro, NULL);
        loom_coro_ctx_make(&nl_coro, empty_entry, NULL);
        if (loom_coro_ctx_swap(&nl_main, &nl_coro) != 0)
            _exit(2);
        _exit(3); /* unreachable */
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        perror("waitpid");
        exit(1);
    }
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == EXIT_SUCCESS);
}

int
main(void)
{
    test_round_trips();
    test_fp_control();
    test_null_link_exit();
    printf("ctx_smoke: %d checks passed\n", g_checks);
    return 0;
}