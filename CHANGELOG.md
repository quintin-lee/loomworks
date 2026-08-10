# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Bucketized priority queue: 256 O(1) FIFO buckets with 256-bit occupancy bitmap (replaces the single linked list)
- Lock-free node pool: ABA-tagged Treiber stack recycles task nodes (`LOOMWORKS_NODE_POOL_CAP`)
- Lock-free Vyukov bounded ring as the NORMAL-priority fast path, spilling to the priority lanes when full
- Open-addressing cancel index: `loom_pool_cancel()` / `cancel_by_id()` / `cancel_all()` can retract not-yet-started tasks
- POSIX counting semaphore (`work_sem`) worker wakeup, replacing the condition variable (no lost wakeups)
- Coroutine stack pooling: capped at 64 exact-size mmap mappings, reused across create/destroy cycles with zero syscalls
- Serialize the scheduler-stack registry with `g_scheduler_lock` — fixes a heap-corruption race when pool workers run coroutines concurrently
- Scheduler-stack race stress regression test (pool + coroutine interop under 64 workers)
- `examples/monitor_demo.c` — metrics monitoring demo
- Full project rename from `ctpool` to `loomworks`
- `docs/faq.md` — frequently asked questions
- `docs/migration.md` — migration guide from ctpool
- FAQ section added to README.md
- Expanded Notes section in README.md with signal handler and scheduler details
- Complete usage example in docs/api-reference.md
- Future work section in docs/design-decisions.md
- `--queue-depth` benchmark scenario with JSON output (`examples/bench`)
- `tools/bench_compare.py` — A/B queue-depth regression comparator
- CI performance gate (`perf.yml`) — compares queue-depth throughput against base ref at a 15% regression threshold

### Changed
- All API prefixes: `ctpool_` -> `loom_`, `CTPPOOL_` -> `LOOMWORKS_`
- Include directory: `include/ctpool/` -> `include/loomworks/`
- Main header: `ctpool.h` -> `loomworks.h`
- CMake project name: `ctpool` -> `loomworks`
- Library output: `libctpool.a` -> `libloomworks.a`
- Task queue redesigned from O(n) single linked list to O(1) per-priority FIFO buckets with a 256-bit occupancy bitmap (enqueue and dequeue now constant time)
- Task nodes pooled on a bounded free-list (`LOOMWORKS_NODE_POOL_CAP`) to cut alloc/free churn on submit/drain
- `clock_gettime` calls in the worker loop now lazy: only invoked when metrics collection is enabled (zero overhead otherwise)
- Shared library (`libloomworks.so`, SOVERSION 1) now builds and works at runtime, including coroutines

### Fixed
- Fixed misleading blocks syntax (`^()`) in README examples; replaced with standard C function pointers
- `bench_future_overhead` hung at shutdown: future pool destroyed without a prior `loom_pool_shutdown` (contract violation); now shuts down before destroy
- Scheduler-stack registry race: concurrent `loom_coro_exit()` / `ensure_scheduler()` from pool worker threads corrupted the global list, causing heap corruption at process exit. List mutations are now serialized by `g_scheduler_lock`.

---

## [1.0.0]

### Added
- Thread pool with configurable worker count, stack size, and bounded/unbounded queue
- Future-based async result retrieval (`loom_pool_submit_future` + `loom_future_wait`)
- Stackful coroutine subsystem with mmap-allocated stacks
- PROT_NONE guard pages for stack overflow detection
- SIGSEGV/SIGBUS signal handler for safe stack overflow recovery
- Per-thread scheduler context (`_Thread_local`) for cross-thread safety
- Cache-line aligned structures to prevent false sharing
- Comprehensive test suite: ~10455 pool assertions, ~5587 coroutine assertions, ~68750 integration assertions
- Full API documentation in `docs/api-reference.md`
- Architecture documentation in `docs/architecture.md`
- Design decisions documentation in `docs/design-decisions.md`
- Contributing guide in `docs/contributing.md`

### Features
- Pure C11 implementation (no C++ dependencies)
- Graceful shutdown with task draining
- Opaque pointer API — internal struct definitions hidden from users
- 64-bit safe `makecontext` argument passing
- POSIX.1-2008 compliant (`_POSIX_C_SOURCE 200809L`)

### Build
- CMake build system with static and shared library targets
- Test integration via CTest
- gcc and clang compatible with `-Wall -Wextra -Werror -pedantic`

[Unreleased]: https://github.com/.../loomworks/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/.../loomworks/releases/tag/v1.0.0
