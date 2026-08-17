# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Pluggable context-switching backend**: new `src/coro_ctx.h` abstracts
  `getcontext`/`makecontext`/`swapcontext` (default: POSIX ucontext), so an
  alternative backend can be dropped in without touching `src/coroutine.c`.

### Changed
- **`group_wait()` no longer shuts down the backing pool**: it now waits only
  for this group's tracked tasks to finish, and the pool stays fully usable
  afterwards. Tasks are tracked by `task_id` (not `user_data` pointer), so
  tasks with `NULL` data are tracked and cancellable too; cancellation goes
  through `loom_pool_cancel_by_id()`, and `group_destroy()` blocks until
  in-flight wrapped tasks finish before releasing the group.
- **Pipeline `take()` no longer polls**: it blocks on a condvar until an item
  arrives or the pipeline is shut down. A discard handler
  (`loom_pc_set_discard_handler()`) can reclaim payloads dropped by internal
  consumers and by `pc_destroy()` on queued items, closing the historical
  teardown leak.
- **Coroutines reject cross-thread driving**: each coroutine records the
  `pthread_t owner` that created it; `resume()` and `terminate()` from any
  other thread return `LOOMWORKS_CORO_ERR_INVALID`.
- Build: merged the duplicate clang-format block in `CMakeLists.txt` (also
  gating the lint target on the tool actually being found).

### Fixed
- **Lifecycle destroy gates (2026-08-17)**: `loom_pool_destroy` requires
  `loom_pool_shutdown` first; `loom_future_destroy` requires the future to
  be complete; `loom_coro_destroy` requires a quiescent coroutine (state
  `NEW`/`DONE`/`ERROR`). All three entry points now return a result code
  (`loom_result_t` / `loom_coro_result_t`) instead of `void`, so misuse is
  detected and reported instead of corrupting memory or hanging.
- **Cancelled futures complete with `LOOMWORKS_ERR_CANCELLED`**: a future
  whose task is cancelled before it runs completes with
  `LOOMWORKS_ERR_CANCELLED` (both `wait` variants), so waiters never block
  forever on a cancellation that cannot be signalled.
- **Worker crashes are reported**: each worker slot tracks a
  `clean_exit` flag set at the worker's own normal exit paths; a worker
  that dies abnormally (crash or `pthread_exit` inside a task) fires
  `LOOMWORKS_METRIC_FAILED` at shutdown and is counted by
  `loom_metrics_failed()`.
- **Group self-wait deadlock rejected**: `loom_task_group_wait` and
  `loom_task_group_destroy` return `LOOMWORKS_ERR_INVALID` when called from
  a worker of the group's own pool (detected via a thread-local
  `loom_pool_current()` marker), instead of stalling the pool forever.
- **Timeout waits now use `CLOCK_MONOTONIC`**: queue-space and future
  condition variables are created with a monotonic clock attribute, and
  `wait_for_space` / `loom_future_wait_timeout` read monotonic time —
  deadline arguments in the headers are documented as monotonic, so
  wall-clock adjustments can no longer expire or stretch timeouts.
- **Timed group wait (`loom_task_group_wait_timeout`)**: bounds an external
  group wait with an absolute `CLOCK_MONOTONIC` deadline, returning
  `LOOMWORKS_ERR_TIMEOUT` on expiry while leaving the group fully usable
  (pending accounting + tracking list untouched); `NULL` deadline means
  "wait forever". `wait()` is now the `NULL`-deadline case.
- **Resize rollback hygiene**: a failed `loom_pool_resize` grow rolled back
  its created workers but left stale `thread_clean_exit` flags in their
  slots, so a later grow reusing those slots could hide a worker crash from
  the FAILED metric; rollback now resets the flag (symmetric with shrink).
- **Resize rollback no longer deadlocks**: freshly spawned workers blocked on
  the work semaphore were joined with a plain `pthread_join` and no wake
  tokens; both grow-rollback loops now use `tryjoin` + token re-post
  (shrink-symmetric).
- **Lane-only pools now execute tasks**: when per-worker deques cannot be
  allocated the pool degrades to lane-only mode, but NORMAL tasks still
  routed to the ring were never consumed (workers drain the ring only via
  the deques), wedging shutdown; ring submits now require deques to exist.
- **Lane-only fallback no longer frees garbage**: the slots fallback freed
  uninitialized pointer memory after a `realloc` extension; the tail is now
  zeroed, mirroring `thread_alive`/`thread_clean_exit`.
- **Work stealing can no longer strand tasks**: the `try*2` victim stride
  visited only half the workers on even worker counts, so the deque owner
  could stay unreachable and runnable tasks went unexecuted; once the ring
  is dry, steals scan every other worker exactly once.
- **Resize fault-injection suite**: a test-only one-shot allocation
  fault-injection hook plus 8 tests covering every grow-path allocation
  call site, proving the rollback guarantee and the lane-only degrade
  contract, and locking the fixes above against regression.

## [1.0.1] - 2026-08-14

### Added
- **Work-stealing scheduler**: per-worker Chase-Lev deques (256 slots) with LIFO local pops and FIFO cross-worker stealing, plus bulk ring→deque batches (`LOOMWORKS_BULK_DEQUEUE` = 8) replacing the single-shared-queue drain path — eliminates the 16–32 worker scaling plateau
- `deque_total` aggregate counter: shutdown waits for deque-resident tasks, not just ring + lanes
- Regression tests: shutdown-drains-deque, resize-down-spills-deque, steal-trigger, steal-FIFO-order, steal-stress (worker_count=2/8, 20k tasks)
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
- Thread pool with configurable worker count, stack size, and bounded/unbounded queue
- Future-based async result retrieval (`loom_pool_submit_future` + `loom_future_wait`)
- Stackful coroutine subsystem with mmap-allocated stacks
- PROT_NONE guard pages for stack overflow detection
- SIGSEGV/SIGBUS signal handler for safe stack overflow recovery
- Per-thread scheduler context (`_Thread_local`) for cross-thread safety
- Cache-line aligned structures to prevent false sharing
- Comprehensive test suite: ~12604 pool assertions, ~5611 coroutine assertions, ~78763 integration assertions
- Full API documentation in `docs/api-reference.md`
- Architecture documentation in `docs/architecture.md`
- Design decisions documentation in `docs/design-decisions.md`
- Contributing guide in `docs/contributing.md`
- Pure C11 implementation (no C++ dependencies)
- Graceful shutdown with task draining
- Opaque pointer API — internal struct definitions hidden from users
- 64-bit safe `makecontext` argument passing
- POSIX.1-2008 compliant (`_POSIX_C_SOURCE 200809L`); the pool additionally uses `_GNU_SOURCE` for `pthread_tryjoin_np` in the resize-shrink path
- CMake build system with static and shared library targets
- Test integration via CTest
- gcc and clang compatible with `-Wall -Wextra -Werror -pedantic`

### Changed
- Worker drain order: priority lanes (REALTIME/HIGH) → own deque (LIFO) → bulk ring claim → steal from random victim → p≥5 lanes; workers never re-sleep after shutdown
- `deque_pop()` fast path: Chase-Lev canonical optimization — skip the `seq_cst` fence when more than one element remains
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
- Shutdown hang when tasks sat in per-worker deques: exit check now accounts `deque_total`; workers that re-sleep after shutdown steal shutdown tokens and starve the join (never re-sleep — `sched_yield()` instead)
- Resize-down hang: displaced workers could starve for wake tokens; now joined via `pthread_tryjoin_np` with token re-posting until exit, and their deque contents spill back to the shared queue first
- `deque_total` was dead (initialized 0, never updated by deque ops) — now maintained by every push/pop/steal
- Deques grow in lockstep with the threads array on `loom_pool_resize()` grow (realloc could move the array under live workers)
- Fixed misleading blocks syntax (`^()`) in README examples; replaced with standard C function pointers
- `bench_future_overhead` hung at shutdown: future pool destroyed without a prior `loom_pool_shutdown` (contract violation); now shuts down before destroy
- Scheduler-stack registry race: concurrent `loom_coro_exit()` / `ensure_scheduler()` from pool worker threads corrupted the global list, causing heap corruption at process exit. List mutations are now serialized by `g_scheduler_lock`.
