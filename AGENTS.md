# Repository Guidelines

## Project Overview

**loomworks** is an industrial-grade C11 concurrency library (v1.0.1, ~14k LOC) providing:
- **Thread pool** with priority scheduling, work-stealing, future results, cancellation, resize, graceful drain
- **Stackful coroutines** with mmap guard pages and hand-written ASM context switching
- **Unified runtime** routing thread/coroutine submissions through one API
- **Pipeline**, **task group**, and **metrics** subsystems built on the core

Zero external dependencies beyond POSIX pthreads + mmap. CI covers gcc/clang × Debug/Release, ASan/UBSan, TSan, valgrind memcheck, aarch64 QEMU cross-execution, and asm/ucontext backend parity.

Test count: ~21,874 pool + ~5,627 coroutine + ~79,288 integration + ~200,014 ctx_smoke assertions — all passing.

## Architecture & Data Flow

### Subsystems (layered on core thread pool)

```
loom_runtime_t              ← single-entry routing layer (THREAD vs CORO flag)
├── loom_thread_pool_t      ← core scheduler
│   ├── Priority lanes (256 buckets, REALTIME=0 … LOW=10)
│   │   └── Lock-free Vyukov ring (NORMAL tasks) → spill to lanes
│   ├── Per-worker Chase-Lev work-stealing deques (LIFO local / FIFO steal)
│   ├── Task node pool (ABA-tagged Treiber stack)
│   ├── Cancel index (open addressing)
│   ├── Coroutine ready FIFO + timer min-heap + lazy timer thread
│   └── Metrics wiring + future wrapping
├── loom_coroutine_t        ← per-coroutine mmap stack, SIGSEGV/SIGBUS longjmp recovery
├── loom_pc_t               ← bounded FIFO pipeline (optional internal pool)
├── loom_task_group_t       ← grouped lifecycle with timed wait + self-deadlock guard
└── loom_metrics_t          ← atomic counters + latency tracking + callbacks
    loom_metrics_shm_t      ← lock-free uint64 mmap'd to /dev/shm/ for external readers
```

### Key architectural decisions

- **Three-tier queue**: priority lanes → Vyukov ring → Chase-Lev deques. Submit funnels into ring (fast path) or lanes (priority); workers pop local deque, steal from neighbors.
- **Cache-line alignment**: locks and hot pointers use `__attribute__((aligned(64)))` to prevent false sharing.
- **Coroutine stacks**: `mmap` with `PROT_NONE` guard pages on both ends; SIGSEGV/SIGBUS handler uses `longjmp` to return error state. Stacks pooled (cap 64 mappings).
- **Context backend**: hand-written ASM (x86_64/aarch64) by default; POSIX `ucontext` fallback forced via `LOOMWORKS_CTX_BACKEND=ucontext`.
- **No C++**: pure C11, no C99 extensions, `_POSIX_C_SOURCE 200809L` allowed.
/**
 * @brief Coroutine state.
 */
typedef enum {
    LOOMWORKS_CORO_NEW,       /**< Coroutine created but not started. */
    LOOMWORKS_CORO_RUNNING,   /**< Currently executing. */
    LOOMWORKS_CORO_SUSPENDED, /**< Paused via yield or initial suspend. */
    LOOMWORKS_CORO_SLEEPING,  /**< Sleeping until a deadline; resumable only after it. */
    LOOMWORKS_CORO_DONE,      /**< Completed execution. */
    LOOMWORKS_CORO_ERROR,     /**< Error state (e.g., guard page hit). */
    LOOMWORKS_CORO_TIMEOUT,   /**< Execution time limit exceeded (forced yield). */
} loom_coro_state_t;
## Key Directories

| Path | Purpose |
|------|---------|
| `include/loomworks/` | Six public headers; `loomworks.h` is the single convenince include |
| `src/` | All implementation; internal headers (`*_internal.h`) are non-public |
| `tests/` | Five test executables, hand-rolled assertion framework (no GoogleTest/etc.) |
struct loom_coroutine {
    loom_coro_state_t state;      /**< Current state (NEW/RUNNING/SUSPENDED/DONE/ERROR/TIMEOUT). */
    loom_coro_fn      entry_fn;   /**< User entry function. */
    void             *user_data;  /**< Opaque argument passed to entry_fn. */
    size_t            stack_size; /**< Requested stack size in bytes. */
    pthread_t         owner;      /**< Thread that created the coroutine. */

    /* Lifetime rule: resume/terminate are only valid from owner; calling
     * them from another thread is user error, guarded at runtime with
     * LOOMWORKS_CORO_ERR_INVALID in coroutine.c because the ucontext
     * machinery is not safe to touch from multiple threads. */

    loom_coro_ctx_t ctx; /**< Saved context (abstracted backend). */

    void  *mmap_base;   /**< Base address from mmap(). */
    size_t mmap_size;   /**< Total size of the mmap region (includes guards). */
    void  *stack_start; /**< Start of the usable (mprotect'd) region. */
    void  *stack_end;   /**< End of the usable region (exclusive). */

    uintptr_t valgrind_stack_id; /**< Valgrind stack registration ID. */

    /* ASan fiber bookkeeping: under AddressSanitizer the current fake-stack
     * pointer must be saved across a raw context switch (see the macros in
     * coroutine.c).  NULL when not built with ASan. */
    void *fake_stack_save;

    uint64_t task_id;          /* Pool task id (0 for stand-alone coroutines). */
    int64_t  wake_deadline_ns; /* 0 = not sleeping; CLOCK_MONOTONIC absolute. */
    uint32_t worker_idx;       /* Owner worker slot; stamped at create time. */
    void    *sleep_reg_ctx;    /* Pool pointer for the sleep_reg hook (NULL = stand-alone). */
    void    *task_node;        /* Pool loom_task_t* carrying this coroutine (NULL = stand-alone). */
    /* Optional pool hook: registers this coroutine's deadline with the pool
     * timer heap. NULL = stand-alone (pure suspension; caller resumes). */
    loom_coro_result_t (*sleep_reg)(void *ctx, uint64_t task_id, int64_t deadline_ns);

    /* Execution timeout fields (0 = disabled). Set by loom_coro_set_timeout(). */
    int64_t  execution_start_ns;  /**< CLOCK_MONOTONIC time when coroutine started running. */
    int64_t  max_execution_ns;    /**< Maximum allowed execution time (0 = unlimited). */

    uint64_t padding[2]; /**< Pad to 64-byte cache-line boundary. */
};

```bash
# Configure (ASM backend auto-selected; force ucontext with -DLOOMWORKS_CTX_BACKEND=ucontext)
cmake -S . -B build

# Build (clang-format check runs first if clang-format is installed)
cmake --build build

# Run all tests
/**
 * @brief Default coroutine stack size.
 */
#define LOOMWORKS_CORO_DEFAULT_STACK_SIZE ((size_t)(64 * 1024)) /* 64 KiB */

/**
 * @brief Number of guard pages on each side of the stack.
 */
#define LOOMWORKS_CORO_GUARD_PAGES_EACH 1u

/**
 * @brief Default coroutine execution timeout (100ms).
 *        Set to 0 to disable timeout.
 */
#define LOOMWORKS_CORO_DEFAULT_TIMEOUT_NS ((int64_t)(100 * 1000000))
./build/tests/test_coroutine
./build/tests/test_integration
./build/tests/test_runtime
./build/tests/ctx_smoke

# Benchmark
./build/examples/bench --json --iterations 20 --tasks 10000

# Direct GCC compile (all five TUs required)
SRCS="src/thread_pool.c src/coroutine.c src/pipeline.c src/task_group.c src/metrics.c"
gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread -I include $SRCS \
    tests/test_thread_pool.c -o test_thread_pool && ./test_thread_pool
```

**Custom build types** (set `CMAKE_BUILD_TYPE`):
- `Debug` — `-g -O0`
- `Release` — `-O3`
- `ASan` — `-fsanitize=address`
- `TSan` — `-fsanitize=thread` (continue-on-error in CI)
- `UBSan` — `-fsanitize=undefined`

**Portability simulation**: `-DLOOMWORKS_POSIX_FALLBACK=ON` forces the `pthread_tryjoin` fallback path regardless of platform.

## Code Conventions & Patterns

### Naming

| Kind | Prefix | Example |
|------|--------|---------|
| Public functions | `loom_` | `loom_pool_create()`, `loom_coro_resume()` |
| Internal functions | `pool_` / `coro_` | `pool_init()`, `coro_entry()` |
| Opaque types | `_t` suffix | `loom_thread_pool_t`, `loom_coroutine_t` |
| Enum values / macros | `LOOMWORKS_` / `LOOMWORKS_CORO_` | `LOOMWORKS_OK`, `LOOMWORKS_CORO_NEW` |

### Error handling

- **Every syscall must be checked**: `pthread_*`, `malloc`, `mmap`, `mprotect` return values are all validated.
- **Return error codes, never call `abort()`/`exit()`**: functions return `loom_result_t` or `loom_coro_result_t`.
- **Every allocation path needs a free path**: destroy functions must clean up everything allocated on success paths.
- **NULL-safe destroy**: `destroy(&ptr)` where `ptr` is NULL is a no-op (safe).

```c
// ✅ Correct
if (pthread_mutex_init(&pool->lock, NULL) != 0) {
    free(pool);
    return LOOMWORKS_ERR_ALLOC;
}

// ❌ Incorrect
pthread_mutex_init(&pool->lock, NULL);  // return value unchecked
```

### Formatting

- `.clang-format`: LLVM style, 4-space indent, column limit 100, Linux brace style.
- Portability headers sort with top priority.
- `format-check` custom CMake target runs before every build when `clang-format` is installed (`ENABLE_CLANG_FORMAT=ON` by default).

### Thread safety

- Shared mutable state protected by mutexes or `stdatomic.h` atomics.
- `_Thread_local` scheduler context per thread; never share `ucontext_t` across threads.
- Signal handlers (`SIGSEGV`/`SIGBUS`) must only use async-signal-safe operations + `longjmp`.

### Test patterns

- **No external test framework**: hand-rolled `ASSERT(expr, msg)` macro with pass/fail counters.
- **Gate/park mechanism**: atomic booleans (`g_gate_started`, `g_gate_parked`, `g_gate_release`) hold workers hostage so queued tasks stay pending for cancel/priority/shutdown tests.
- **Valgrind scaling**: `#ifdef __VALGRIND__` reduces loop counts (~4×) to stay within CI budget.
- **Spin-wait with timeout**: `WAIT_UNTIL(sec, cond)` macro with 2B spin limit + monotonic clock fallback.
- **Internal struct access**: `test_thread_pool.c` includes `thread_pool_internal.h` to assert on `pool->deques`, `pool->ring_count`, `pool->queue_len`.

## Important Files

| File | Role |
|------|------|
| `CMakeLists.txt` | Root build: static+shared targets, ASM backend selection, tests, examples, install/export |
| `cmake/BuildTypes.cmake` | Custom build types (Debug/Release/ASan/TSan/UBSan) with shared warning flags |
| `include/loomworks/loomworks.h` | Single-header convenience include (pulls all 6 public headers) |
| `include/loomworks/thread_pool.h` | Core public API — pool create/destroy, submit variants, future, cancel, resize, shutdown |
| `include/loomworks/coroutine.h` | Coroutine public API — lifecycle, yield/resume/sleep, state, guard handler |
| `include/loomworks/runtime.h` | Unified runtime API — single submit entry, backs a thread pool internally |
| `src/thread_pool.c` | Core implementation (~2865 lines): ring, deque, cancel index, timer, resize, drain |
| `src/thread_pool_internal.h` | Internal types — read this to understand pool internals for testing |
| `src/coroutine.c` | Coroutine impl: mmap stack, guard pages, signal handler, stack pooling |
| `src/coro_ctx.h` | Context-switch backend abstraction layer |
| `src/ctx_x86_64.S` | x86-64 ASM context switch (SysV ABI) |
| `src/ctx_aarch64.S` | aarch64 ASM context switch (AAPCS64) |
| `tests/test_thread_pool.c` | Largest test file (~4200 lines): pool, groups, pipeline, priority, resize, metrics |
| `tests/test_coroutine.c` | Coroutine lifecycle, stack pooling, guard pages, sleep, cross-thread safety |
| `tests/test_integration.c` | Cross-component stress: 50k tasks, pool+coroutine interop, scheduler race |
| `tests/ctx_smoke.c` | Low-level context-switch stress: 100k swap round-trips, FP register survival |
| `tests/test_runtime.c` | Unified runtime: submit thread/coro, cancel, futures, metrics, resize |
| `.github/workflows/ci.yml` | Full CI matrix + sanitizers + valgrind + QEMU aarch64 |
| `.github/workflows/perf.yml` | Benchmark regression gate (throughput >60% drop or p99 >2× = fail) |
| `tools/tag-release.sh` | Version sync script (bumps CMakeLists, CHANGELOG, docs/migration, reconfigures all build dirs) |
| `docs/architecture.md` | Detailed component diagram + pseudocode for drain/cancel/resize |
| `docs/design-decisions.md` | 14 recorded ADRs with alternatives and rationale |
| `docs/risk-assessment.md` | 22-risk register with likelihood×impact grid and resolution status |

## Runtime & Tooling

- **Language**: C11 (`-std=c11`, `CMAKE_C_EXTENSIONS OFF`). No C++, no C99 extensions.
- **Build system**: CMake ≥ 3.16. No autoconf, no Meson.
- **Package manager**: none — pure POSIX C, no dependencies.
- **Required tools**: `gcc` or `clang`, `make` or `ninja`, `ctest`.
- **Optional but recommended**: `clang-format` (format-check target), `clang-tidy` (static analysis on every compile, `ENABLE_CLANG_TIDY=ON` by default), `valgrind` (CI memcheck).
- **Platforms**: Linux x86_64 (primary), Linux aarch64 (CI QEMU), any POSIX platform with `ucontext` fallback.
- **CI**: GitHub Actions — 2×2 compiler×build-type matrix, sanitizer builds, ucontext parity build, valgrind memcheck, aarch64 cross-compile + QEMU smoke.

## Testing & QA

### Running tests

```bash
cmake -S . -B build && cmake --build build
cd build && ctest --output-on-failure
```

Five test executables, each a self-contained C program returning 0 (pass) or 1 (fail):

| Test | Name | Coverage | Timeout |
|------|------|----------|---------|
| `tests/test_thread_pool.c` | `ThreadPoolTests` | Pool, groups, pipeline, priority, ring, deque, metrics, resize | 900s |
| `tests/test_coroutine.c` | `CoroutineTests` | Lifecycle, yield/resume, stack pool, guard pages, sleep, cross-thread | 900s |
| `tests/test_integration.c` | `IntegrationTests` | 50k stress, pool+coroutine interop, scheduler race, bounded queue | 900s |
| `tests/ctx_smoke.c` | `test_ctx_smoke` | ASM/ucontext layer: 100k swap round-trips, FP register survival | 900s |
| `tests/test_runtime.c` | `RuntimeTests` | Unified runtime: submit, cancel, futures, metrics, resize | 120s |

### Test-writing expectations

- Every new feature or fix must add corresponding test cases.
- Cover: boundary conditions (NULL args, null handles, double destroy), error paths (alloc failure, guard page trigger), normal paths (full lifecycle), concurrency (multi-threaded submission, multi-coroutine).
- Use the gate/park pattern to hold workers for state-assertion tests.
- Wrap large loops with `#ifdef __VALGRIND__` scaling.

### Pre-submit checks

```bash
# Format check (runs automatically with CMake if clang-format installed)
cmake --build build --target format-check

# Full test suite
cd build && ctest --output-on-failure

# Sanitizer build (select one)
cmake -S . -B build_asan -DCMAKE_BUILD_TYPE=ASan && cmake --build build_asan
cmake -S . -B build_tsan -DCMAKE_BUILD_TYPE=TSan && cmake --build build_tsan
cmake -S . -B build_ubsan -DCMAKE_BUILD_TYPE=UBSan && cmake --build build_ubsan
```

### Commit format (Conventional Commits)

```
<type>(<scope>): 🎯 subject
```

| Type | Emoji |
|------|-------|
| feat | ✨ |
| fix | 🐛 |
| docs | 📝 |
| style | 🎨 |
| refactor | ♻️ |
| test | ✅ |
| build | 📦 |
| ci | 👷 |
| chore | 🧹 |

Subject is lowercase imperative mood.

Example:
```
fix(coroutine): 🐛 prevent crash when destroying cross-thread coroutine

The scheduler context was global, causing SIGSEGV when coroutines
were resumed from different threads. Made g_scheduler _Thread_local.

Closes #42
```
