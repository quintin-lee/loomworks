/* ctx_aarch64.S - hand-written context backend, aarch64 AAPCS64.
 *
 * Layout of struct loom_coro_ctx (see src/coro_ctx.h - offsets are
 * locked by _Static_assert):
 *   PC_OFF 0   sp 8   regs[12] at 16 (x19..x30, x30 = lr)
 *   fpcr 112   fpsr 116   entry_fn 120   entry_arg 128   link 136
 *
 * Conventions:
 *   - Callee-saved GPRs x19..x29 plus lr (x30) are preserved; x0..x18
 *     and the v-registers are caller-saved and deliberately not saved.
 *   - fpcr/fpsr are saved with mrs/msr (control/status words only -
 *     no vector register save).
 *   - x9 is the scratch register (caller-saved, safe to clobber).
 *   - make() arms the context with x19 == &ctx; the trampoline reads
 *     entry_fn/entry_arg/link through x19.
 *   - Resume always lands with w0 == 0.
 */
        .equ    PC_OFF,         0
        .equ    SP_OFF,         8
        .equ    REGS_OFF,       16
        .equ    FPCR_OFF,       112
        .equ    FPSR_OFF,       116
        .equ    ENTRY_FN_OFF,   120
        .equ    ENTRY_ARG_OFF,  128
        .equ    LINK_OFF,       136

        .text

/* int loom_coro_ctx_get(loom_coro_ctx_t *ctx);  -- save only, returns 0 */
        .globl  loom_coro_ctx_get
        .hidden loom_coro_ctx_get
        .type   loom_coro_ctx_get, %function
        .align  4
loom_coro_ctx_get:
        .cfi_startproc
        mov     x9, sp
        str     x9, [x0, #SP_OFF]
        stp     x19, x20, [x0, #REGS_OFF]
        stp     x21, x22, [x0, #(REGS_OFF+16)]
        stp     x23, x24, [x0, #(REGS_OFF+32)]
        stp     x25, x26, [x0, #(REGS_OFF+48)]
        stp     x27, x28, [x0, #(REGS_OFF+64)]
        stp     x29, x30, [x0, #(REGS_OFF+80)]
        mrs     x9, fpcr
        str     w9, [x0, #FPCR_OFF]
        mrs     x9, fpsr
        str     w9, [x0, #FPSR_OFF]
        adr     x9, .Lafter_ret
        str     x9, [x0, #PC_OFF]
        mov     w0, wzr
        ret
.Lafter_ret:
        ret
        .cfi_endproc
        .size   loom_coro_ctx_get, .-loom_coro_ctx_get

/* int loom_coro_ctx_swap(loom_coro_ctx_t *from, loom_coro_ctx_t *to);
 * Bilaterally symmetric; from == to is a natural no-op. */
        .globl  loom_coro_ctx_swap
        .hidden loom_coro_ctx_swap
        .type   loom_coro_ctx_swap, %function
        .align  4
loom_coro_ctx_swap:
        .cfi_startproc
.Lswap_core:
        mov     x9, sp
        str     x9, [x0, #SP_OFF]
        stp     x19, x20, [x0, #REGS_OFF]
        stp     x21, x22, [x0, #(REGS_OFF+16)]
        stp     x23, x24, [x0, #(REGS_OFF+32)]
        stp     x25, x26, [x0, #(REGS_OFF+48)]
        stp     x27, x28, [x0, #(REGS_OFF+64)]
        stp     x29, x30, [x0, #(REGS_OFF+80)]
        mrs     x9, fpcr
        str     w9, [x0, #FPCR_OFF]
        mrs     x9, fpsr
        str     w9, [x0, #FPSR_OFF]
        adr     x9, .Lafter_swap
        str     x9, [x0, #PC_OFF]

        ldr     x9, [x1, #SP_OFF]
        mov     sp, x9
        ldp     x19, x20, [x1, #REGS_OFF]
        ldp     x21, x22, [x1, #(REGS_OFF+16)]
        ldp     x23, x24, [x1, #(REGS_OFF+32)]
        ldp     x25, x26, [x1, #(REGS_OFF+48)]
        ldp     x27, x28, [x1, #(REGS_OFF+64)]
        ldp     x29, x30, [x1, #(REGS_OFF+80)]
        ldr     w9, [x1, #FPCR_OFF]
        msr     fpcr, x9
        ldr     w9, [x1, #FPSR_OFF]
        msr     fpsr, x9
        mov     w0, wzr
        ldr     x9, [x1, #PC_OFF]
        br      x9
.Lafter_swap:
        ret
        .cfi_endproc
        .size   loom_coro_ctx_swap, .-loom_coro_ctx_swap

/* int loom_coro_ctx_swap_to_link(loom_coro_ctx_t *ctx);
 * Switches to ctx->link; NULL link is undefined behavior by contract. */
        .globl  loom_coro_ctx_swap_to_link
        .hidden loom_coro_ctx_swap_to_link
        .type   loom_coro_ctx_swap_to_link, %function
        .align  4
loom_coro_ctx_swap_to_link:
        .cfi_startproc
        ldr     x1, [x0, #LINK_OFF]
        b       .Lswap_core
        .cfi_endproc
        .size   loom_coro_ctx_swap_to_link, .-loom_coro_ctx_swap_to_link

/* ctx_trampoline: entered with sp % 16 == 8 (see loom_coro_ctx_make).
 * Runs entry_fn(entry_arg) with x19 == ctx.  On return: switch to
 * ctx->link if present, else exit(EXIT_FAILURE).  Never rets to the
 * arming frame.  `bl` needs sp % 16 == 0 at the interface; blr pushed
 * the return address (16 bytes), which already restores alignment. */
        .globl  ctx_trampoline
        .hidden ctx_trampoline
        .type   ctx_trampoline, %function
        .align  4
ctx_trampoline:
        .cfi_startproc
        ldr     x0, [x19, #ENTRY_ARG_OFF]
        ldr     x9, [x19, #ENTRY_FN_OFF]
        blr     x9
        ldr     x9, [x19, #LINK_OFF]
        cbz     x9, .Ltramp_no_link
        mov     x0, x19
        mov     x1, x9
        b       .Lswap_core
.Ltramp_no_link:
        /* post-blr sp % 16 == 0; drop 16 and call exit (sp % 16 == 0). */
        sub     sp, sp, #16
        mov     w0, #1          /* EXIT_FAILURE */
        bl      exit
        .cfi_endproc
        .size   ctx_trampoline, .-ctx_trampoline

        .section .note.GNU-stack,"",@progbits
