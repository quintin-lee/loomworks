/* ctx_x86_64.S - hand-written context backend, x86-64 System V ABI.
 *
 * Layout of struct loom_coro_ctx (see src/coro_ctx.h - offsets are
 * locked by _Static_assert to prevent silent drift):
 *   PC_OFF 0   rsp 8   rbx 16   rbp 24   r12 32   r13 40   r14 48
 *   r15 56   mxcsr 64   x87cw 68   entry_fn 72   entry_arg 80   link 88
 *
 * Conventions:
 *   - get()/swap() never touch the stack (no push/pop), so the red zone
 *     is preserved and unwinding stays sane.
 *   - make() arms the context with r15 == &ctx; the trampoline reads
 *     entry_fn/entry_arg/link through r15 (callee-saved across the
 *     entry_fn call).
 *   - Resume always lands with eax == 0 (after_ret/after_swap), which is
 *     what `if (loom_coro_ctx_get(...) != 0)` relies on.
 *   - PIC-safe: only PC-relative addressing; no GOT/TLS access.
 */
        .equ    PC_OFF,         0
        .equ    RSP_OFF,        8
        .equ    RBX_OFF,        16
        .equ    RBP_OFF,        24
        .equ    R12_OFF,        32
        .equ    R13_OFF,        40
        .equ    R14_OFF,        48
        .equ    R15_OFF,        56
        .equ    MXCSR_OFF,      64
        .equ    X87CW_OFF,      68
        .equ    ENTRY_FN_OFF,   72
        .equ    ENTRY_ARG_OFF,  80
        .equ    LINK_OFF,       88

        .text

/* int loom_coro_ctx_get(loom_coro_ctx_t *ctx);  -- save only, returns 0 */
        .globl  loom_coro_ctx_get
        .hidden loom_coro_ctx_get
        .type   loom_coro_ctx_get, @function
        .align  16
loom_coro_ctx_get:
        .cfi_startproc
        movq    %rsp, RSP_OFF(%rdi)
        movq    %rbx, RBX_OFF(%rdi)
        movq    %rbp, RBP_OFF(%rdi)
        movq    %r12, R12_OFF(%rdi)
        movq    %r13, R13_OFF(%rdi)
        movq    %r14, R14_OFF(%rdi)
        movq    %r15, R15_OFF(%rdi)
        stmxcsr MXCSR_OFF(%rdi)
        fnstcw  X87CW_OFF(%rdi)
        leaq    .Lafter_ret(%rip), %rax
        movq    %rax, PC_OFF(%rdi)
        xorl    %eax, %eax
        ret
.Lafter_ret:
        ret
        .cfi_endproc
        .size   loom_coro_ctx_get, .-loom_coro_ctx_get

/* int loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to);
 * Bilaterally symmetric: saves from, restores to, returns 0 at the
 * after_swap point of the caller.  from == to is a natural no-op. */
        .globl  loom_coro_ctx_swap
        .type   loom_coro_ctx_swap, @function
        .align  16
loom_coro_ctx_swap:
        .cfi_startproc
.Lswap_core:
        movq    %rsp, RSP_OFF(%rdi)
        movq    %rbx, RBX_OFF(%rdi)
        movq    %rbp, RBP_OFF(%rdi)
        movq    %r12, R12_OFF(%rdi)
        movq    %r13, R13_OFF(%rdi)
        movq    %r14, R14_OFF(%rdi)
        movq    %r15, R15_OFF(%rdi)
        stmxcsr MXCSR_OFF(%rdi)
        fnstcw  X87CW_OFF(%rdi)
        leaq    .Lafter_swap(%rip), %rax
        movq    %rax, PC_OFF(%rdi)

        movq    RSP_OFF(%rsi), %rsp
        movq    RBX_OFF(%rsi), %rbx
        movq    RBP_OFF(%rsi), %rbp
        movq    R12_OFF(%rsi), %r12
        movq    R13_OFF(%rsi), %r13
        movq    R14_OFF(%rsi), %r14
        movq    R15_OFF(%rsi), %r15
        ldmxcsr MXCSR_OFF(%rsi)
        fldcw   X87CW_OFF(%rsi)
        xorl    %eax, %eax
        jmp     *PC_OFF(%rsi)
.Lafter_swap:
        ret
        .cfi_endproc
        .size   loom_coro_ctx_swap, .-loom_coro_ctx_swap

/* int loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx);
 * Switches to ctx->link.  A NULL link is undefined behavior by contract
 * (every call site in src/coroutine.c arms a non-NULL link first). */
        .globl  loom_coro_ctx_swap_to_link
        .type   loom_coro_ctx_swap_to_link, @function
        .align  16
loom_coro_ctx_swap_to_link:
        .cfi_startproc
        movq    LINK_OFF(%rdi), %rsi
        jmp     .Lswap_core
        .cfi_endproc
        .size   loom_coro_ctx_swap_to_link, .-loom_coro_ctx_swap_to_link

/* ctx_trampoline: entered with rsp % 16 == 8 (see loom_coro_ctx_make).
 * Runs entry_fn(entry_arg) with r15 == ctx.  On return: if ctx->link is
 * non-NULL switch to it; otherwise exit(EXIT_FAILURE).  It never rets to
 * the arming frame - the coroutine stack is abandoned on purpose. */
        .globl  ctx_trampoline
        .hidden ctx_trampoline
        .type   ctx_trampoline, @function
        .align  16
ctx_trampoline:
        .cfi_startproc
        movq    ENTRY_ARG_OFF(%r15), %rdi
        call    *ENTRY_FN_OFF(%r15)
        movq    LINK_OFF(%r15), %rsi
        testq   %rsi, %rsi
        je      .Ltramp_no_link
        movq    %r15, %rdi
        jmp     .Lswap_core
.Ltramp_no_link:
        /* rsp % 16 == 0 here (entry had 8, call pushed 8); exit is a
         * normal ABI call, so restore the 8-byte offset first. */
        subq    $8, %rsp
        movl    $1, %edi        /* EXIT_FAILURE */
        call    exit
        hlt                     /* unreachable */
        .cfi_endproc
        .size   ctx_trampoline, .-ctx_trampoline

        .section .note.GNU-stack,"",@progbits