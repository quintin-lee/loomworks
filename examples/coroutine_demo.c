/**
 * @file coroutine_demo.c
 * @brief Coroutine demo — cooperative yield/resume state machine,
 *        early termination, and stack introspection.
 *
 * Two coroutines alternate: each yields control back to the scheduler
 * (main) a few times, then completes.  The demo drives them explicitly,
 * printing the state transitions NEW → SUSPENDED → DONE, then creates a
 * third coroutine that is terminated early, and finally inspects the
 * guard-paged stack range of a live coroutine.
 *
 * Build:
 *   cmake --build build --target example_coroutine
 * Run:
 *   ./build/example_coroutine
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/coroutine.h"

#include <stdio.h>
#include <string.h>

typedef struct {
    const char *name;
    int         rounds;
    int         step;
} coro_ctx_t;

/* Cooperative task: runs @p rounds steps, yielding between each. */
static void ping_pong_coro(void *arg)
{
    coro_ctx_t *ctx = (coro_ctx_t *)arg;
    printf("  [%s] started (first yield)\n", ctx->name);
    loom_coro_yield();
    for (int i = 0; i < ctx->rounds; i++) {
        printf("  [%s] step %d/%d\n", ctx->name, i + 1, ctx->rounds);
        loom_coro_yield();
    }
    printf("  [%s] finished\n", ctx->name);
}

/* Coroutine that would loop forever — the demo terminates it early. */
static void runaway_coro(void *arg)
{
    coro_ctx_t *ctx = (coro_ctx_t *)arg;
    for (;;) {
        printf("  [%s] iteration (would loop forever)\n", ctx->name);
        loom_coro_yield();
        (void)ctx;
    }
}

static const char *state_str(loom_coro_state_t s)
{
    switch (s) {
    case LOOMWORKS_CORO_NEW:       return "NEW";
    case LOOMWORKS_CORO_RUNNING:   return "RUNNING";
    case LOOMWORKS_CORO_SUSPENDED: return "SUSPENDED";
    case LOOMWORKS_CORO_DONE:      return "DONE";
    case LOOMWORKS_CORO_ERROR:     return "ERROR";
    }
    return "?";
}

int main(void)
{
    int failed = 0;

    /* Two coroutines alternating under manual scheduling. */
    coro_ctx_t ca = {.name = "A", .rounds = 3, .step = 0};
    coro_ctx_t cb = {.name = "B", .rounds = 2, .step = 0};

    loom_coroutine_t *a = NULL;
    loom_coroutine_t *b = NULL;
    if (loom_coro_create(ping_pong_coro, &ca, 0, &a) != LOOMWORKS_CORO_OK ||
        loom_coro_create(ping_pong_coro, &cb, 0, &b) != LOOMWORKS_CORO_OK) {
        fprintf(stderr, "FAIL: coroutine create\n");
        return 1;
    }
    printf("Coroutine A state: %s, B state: %s\n", state_str(loom_coro_state(a)),
           state_str(loom_coro_state(b)));

    /* Round-robin: resume A until suspended, then B, and so on. */
    for (int round = 0; round < 8; round++) {
        loom_coroutine_t *cur = (round % 2 == 0) ? a : b;
        if (loom_coro_state(cur) == LOOMWORKS_CORO_DONE) {
            continue;
        }
        loom_coro_result_t rc = loom_coro_resume(cur);
        if (rc != LOOMWORKS_CORO_OK) {
            fprintf(stderr, "FAIL: resume %s returned %s\n",
                    (cur == a) ? "A" : "B", loom_coro_result_str(rc));
            failed = 1;
            break;
        }
        printf("  -> %s state: %s\n", (cur == a) ? "A" : "B", state_str(loom_coro_state(cur)));
    }
    if (loom_coro_state(a) != LOOMWORKS_CORO_DONE || loom_coro_state(b) != LOOMWORKS_CORO_DONE) {
        fprintf(stderr, "FAIL: A=%s B=%s expected both DONE\n",
                state_str(loom_coro_state(a)), state_str(loom_coro_state(b)));
        failed = 1;
    }

    /* Early termination of a runaway coroutine. */
    coro_ctx_t cr = {.name = "R", .rounds = 0, .step = 0};
    loom_coroutine_t *r = NULL;
    if (loom_coro_create(runaway_coro, &cr, 0, &r) != LOOMWORKS_CORO_OK) {
        fprintf(stderr, "FAIL: runaway create\n");
        return 1;
    }
    loom_coro_resume(r);
    printf("Runaway state after first resume: %s\n", state_str(loom_coro_state(r)));
    if (loom_coro_terminate(r) != LOOMWORKS_CORO_OK ||
        loom_coro_state(r) != LOOMWORKS_CORO_DONE) {
        fprintf(stderr, "FAIL: terminate\n");
        failed = 1;
    }
    printf("Runaway state after terminate: %s\n", state_str(loom_coro_state(r)));

    /* Stack introspection on a fresh coroutine (guard pages at both ends). */
    coro_ctx_t cs = {.name = "S", .rounds = 1, .step = 0};
    loom_coroutine_t *s = NULL;
    if (loom_coro_create(ping_pong_coro, &cs, (size_t)64 * 1024, &s) != LOOMWORKS_CORO_OK) {
        fprintf(stderr, "FAIL: stack-info create\n");
        return 1;
    }
    void *start = NULL;
    void *end   = NULL;
    if (loom_coro_stack_info(s, &start, &end) == LOOMWORKS_CORO_OK) {
        printf("Stack range: [%p, %p) — %zu KiB\n", start, end,
               (size_t)((const char *)end - (const char *)start) / (size_t)1024);
        if (start == NULL || end <= start) {
            fprintf(stderr, "FAIL: invalid stack range\n");
            failed = 1;
        }
    } else {
        fprintf(stderr, "FAIL: stack_info\n");
        failed = 1;
    }

    loom_coro_destroy(&a);
    loom_coro_destroy(&b);
    loom_coro_destroy(&r);
    loom_coro_destroy(&s);

    if (!failed) {
        printf("PASS: coroutine lifecycle, early termination, and stack info OK.\n");
        return 0;
    }
    return 1;
}
