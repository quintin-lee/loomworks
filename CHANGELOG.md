# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Unified runtime (`loom_runtime_t`)**: single-entry-point abstraction that
  routes thread and coroutine submissions through one API. `loom_runtime_submit()`
  takes a `loom_submit_flag_t` (THREAD / CORO) and a `loom_fn_union_t` to avoid
  object-to-function pointer casts; the backing pool handles M:N coroutine
  multiplexing via the per-worker ready FIFO.
- **Runtime convenience APIs**: `loom_runtime_submit_future()`,
  `loom_runtime_resize()`, `loom_runtime_set_metrics_callback()`,
  `loom_runtime_pool()` (test accessor). Full lifecycle:
  `create → submit → cancel → shutdown → destroy`.
- **Standalone metrics callback path**: `loom_pool_set_metrics_callback()` now
  actually fires callbacks when no collector is attached, in addition to the
  existing collector path via `loom_metrics_create()`.
- **Pluggable context-switching backend**: new `src/coro_ctx.h` abstracts
  `getcontext`/`makecontext`/`swapcontext` (default: POSIX ucontext), so an
  alternative backend can be dropped in without touching `src/coroutine.c`.
- **Pipeline payload ownership flag**: new `loom_pc_create_ex()` takes a
  flags bitmask in which `LOOM_PC_OWN_PAYLOADS` promises the library will
  never `free()` payloads; the discard handler is installable atomically at
  creation, and the leak-only combination (ownership flag + internal pool +
  no handler) is rejected at creation. Legacy `loom_pc_create()` is an
  unchanged zero-flag wrapper.
- **Portable join (R12)**: the resize/shrink join loops no longer need
  `pthread_tryjoin_np` (a GNU extension) — they poll the per-worker
  `clean_exit` flag and join explicitly, closing the last hard
  platform dependency. A CMake switch `LOOMWORKS_POSIX_FALLBACK=ON`
  forces the portable path on Linux so CI proves both code paths behave
  identically; platform floor is now macOS 10.12+ / Linux.
- **Metrics callback contract lockdown (R3)**: the callback contract —
  synchronous, on the event-producing thread, always outside the pool lock,
  must be cheap — is now enforced by regression tests
  (`test_metrics_callback_on_worker_thread`,
  `test_metrics_callback_outside_lock`,
  `test_metrics_callback_lifecycle_counts`), so any future refactor that
  invokes the callback under the lock or drops events fails the suite.
- **Benchmark scenarios**: `examples/bench` gains three scenarios —
  `priority_fairness` (REALTIME response latency under a continuous LOW
  flood, p99 must stay < 10 ms), `tail_latency` (completion latency
  p50/p99/p999 across N tasks on 4 workers), and `coro_switch` (coroutine
  resume→yield→resume round-trip with the active backend reported). The
  `--json` output exports `fairness_resp_ns`, `tail_latency_ns`, and
  `coro_switch_ns` so the CI perf gate can compare them.
- **Sanitizer hard gates (R19/R20)**: the CI `sanitize` job now runs
  ASan (with `detect_leaks=1`) and UBSan (with `halt_on_error=1`)
  as hard gates — a leak or undefined behavior fails the job. TSan stays
  best-effort (`continue-on-error`) because ThreadSanitizer cannot follow
  the coroutine context switch and crashes inside its own interceptors.
  The triage this enforced surfaced two real defects — a resize
  rollback leak on allocation failure and misaligned 64-byte-aligned
  deques arrays — both fixed with regression coverage.
- **Coroutine multi-yield semantics**: a coroutine may now call
  `loom_coro_yield()` any number of times; each `loom_coro_resume()`
  continues from the last yield point (`SUSPENDED` → `RUNNING`), so
  generator-style coroutines work. Previously the second resume entered
  undefined territory (the entry trampoline was re-run).
- **Coroutine sleeping**: new `loom_coro_sleep_until(deadline_ns)`
  (absolute `CLOCK_MONOTONIC`) and `loom_coro_sleep(duration_ns)` park the
  calling coroutine in a `SLEEPING` state until its deadline passes.
  Early resumes are rejected with `LOOMWORKS_CORO_ERR_RUNNING` instead of
  corrupting the schedule. Inside pool coroutine tasks, deadlines are
  registered with a per-pool timer thread (lazily started, min-heap under
  `timer_lock`) that moves expired entries to the owner worker's ready FIFO
  and posts a wake token — it never resumes a coroutine itself, preserving
  the one-thread-per-coroutine affinity rule. Standalone coroutines must be
  resumed by their owner after the deadline.
- **Pool coroutine tasks**: new `loom_pool_submit_coroutine(pool, fn, arg,
  stack_size, &task_id)` runs a coroutine as a fire-and-forget pool task: a
  worker resumes it, re-queues it on every yield, parks it via the timer heap
  on sleep, and destroys it on completion. Failures surface through the
  existing `LOOMWORKS_METRIC_FAILED` metric; the returned `task_id` supports
  the usual cancellation path.

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
- **Execution order documented as a design contract (R7)**: work-stealing
  ordering — LIFO local pops, FIFO cross-worker steals — is recorded in the
  risk register as an accepted trade-off (2026-08-18), with
  pipeline/sequence-number guidance for callers needing strict ordering;
  the public headers make no FIFO/order promise.
- **Assertion counts synchronized (R10)**: README and CHANGELOG now reflect
  the current suite — ~20771 pool + ~5616 coroutine + ~78759 integration +
  ~200014 ctx_smoke.

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
