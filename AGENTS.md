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
- **Destroy safety**: NULL-pointer-safe destroy on every public handle.

### Data flow: task submission

1. `loom_pool_submit()` → check shutdown → bump metrics → acquire cancel slot → allocate task node from Treiber pool → insert into priority lane or Vyukov ring → signal worker condvar.
2. Worker loop → pop local deque (LIFO) → if empty, steal from neighbor (FIFO) → if still empty, drain ring → execute task.
3. `loom_pool_shutdown()` → set shutdown flag → wake all workers → each worker drains remaining tasks before exiting.

## Key Directories

| Path | Purpose |
|------|---------|
| `include/loomworks/` | Six public headers; `loomworks.h` is the single convenince include |
| `src/` | All implementation; internal headers (`*_internal.h`) are non-public |
| `tests/` | Five test executables, hand-rolled assertion framework (no GoogleTest/etc.) |
| `examples/` | Demo programs + `bench` benchmark harness + `monitor_demo` |
| `cmake/` | BuildTypes.cmake (Debug/Release/ASan/TSan/UBSan), package config template |
| `docs/` | architecture, api-reference, contributing, design-decisions, faq, migration, risk-assessment |
| `tools/` | `tag-release.sh` (version sync across 4 files), `bench_compare.py` (regression gate) |
| `.github/workflows/` | `ci.yml` (full matrix + sanitizers + QEMU), `perf.yml` (benchmark comparison) |

## Development Commands

```bash
# Configure (ASM backend auto-selected; force ucontext with -DLOOMWORKS_CTX_BACKEND=ucontext)
cmake -S . -B build

# Build (clang-format check runs first if clang-format is installed)
cmake --build build

# Run all tests
cd build && ctest --output-on-failure

# Individual test binaries
./build/tests/test_thread_pool
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
