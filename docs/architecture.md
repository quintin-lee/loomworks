# loomworks Architecture

## 1. System Overview

loomworks is a pure C11 concurrency library comprising five subsystems:

```
┌────────────────────────────────────────────────────────────────────────┐
│                              loomworks                                    │
├───────────────┬───────────────┬──────────────┬──────────────┬───────────┤
│ Thread Pool   │  Coroutine    │  Pipeline    │ Task Group   │  Metrics  │
│ thread_pool.h │ coroutine.h   │ pipeline.h   │ task_group.h │ metrics.h │
├───────────────┼───────────────┼──────────────┼──────────────┼───────────┤
│ • Worker mgmt │ • ucontext    │ • Bounded/   │ • Tracked    │ • Event   │
│ • 256 prio    │   save/restore│   unbounded  │   submissions│   counters│
│   buckets     │ • mmap guard  │   FIFO       │ • Cancel by  │ • Latency │
│ • Lock-free   │   stacks      │ • Internal   │   pointer    │   sum/max │
│   Vyukov ring │ • Stack pool  │   consumers  │   equality   │ • Snapshot│
│ • Cancel idx  │ • Scheduler   │ • Shutdown   │ • wait()     │ • Callback│
│ • Node pool   │   stack       │              │   = drain    │   wiring  │
└───────────────┴───────────────┴──────────────┴──────────────┴───────────┘
```

The thread pool is the core; pipeline, task group, and metrics are thin layers
built on top of it. The coroutine subsystem is independent (no pool
dependency) but interoperates: pool workers may legally run coroutines, so the
coroutine internals must be concurrency-safe even from many worker threads.

---

## 2. Thread Pool Architecture

### 2.1 Component Relationships

```
                    ┌──────────────────────────┐
                    │    loom_thread_pool_t    │
                    │        (opaque)          │
                    └──────┬─────────┬─────────┘
                           │         │
        ┌──────────────────┼─────────┼──────────────────┐
        ▼                  ▼         ▼                  ▼
┌──────────────┐  ┌───────────────┐  ┌──────────────┐  ┌──────────────┐
│ 256 prio     │  │ lock + work_  │  │ Vyukov ring  │  │ cancel       │
│ lane buckets │  │ sem + space_  │  │ (NORMAL fast │  │ slots        │
│ (buckets_    │  │ cond + drain_ │  │  path)       │  │ (open-       │
│  head[256],  │  │ cond          │  ├──────────────┤  │  addressing) │
│  tail[256],  │  │ (aligned 64B) │  │ ring_cell_t  │  ├──────────────┤
│  nonempty_   │  └───────────────┘  │ seq-protocol │  │ node_pool    │
│  bits[4])    │                     └──────────────┘  │ (ABA-tagged  │
└──────────────┘                                      │  Treiber)    │
                                                       └──────────────┘
```

### 2.2 Queue Layer — Two Paths Plus Per-Worker Deques

The task queue is a **single shared structure** with two insertion paths,
plus a **per-worker Chase-Lev work-stealing deque** layered on top for
NORMAL-priority throughput:

1. **Priority lanes** (all non-NORMAL priorities; NORMAL when no ring is
   configured): 256 FIFO buckets (`buckets_head[256]` / `buckets_tail[256]`)
   protected by one pool lock, with a 256-bit occupancy bitmap
   (`nonempty_bits[4]`) enabling O(1) lowest-priority scan via `ctz`.
   Priorities: `LOW=10`, `NORMAL=5`, `HIGH=1`, `REALTIME=0` — **lower value
   runs first**.

2. **Lock-free Vyukov ring** (NORMAL-priority fast path, when `ring != NULL`):
   bounded array of `ring_cell_t { _Atomic size_t seq; _Atomic(loom_task_t*) task; }`
   following the standard Vyukov protocol:

   - `seq == pos`        → cell empty
   - `seq == pos + 1`    → cell full (producer owns it)
   - `seq == pos + ring_size` → cell released (consumer returned it)

   Producers `CAS` the tail, store the task, insert the cancel slot, then
   release the cell (`seq = want + 1`, `release` order). Consumers `CAS` the
   head, load the task, remove the cancel slot, then publish
   `seq = want + ring_size`. When the ring is full, submission **spills to
   the NORMAL lane bucket** instead of blocking.

3. **Per-worker Chase-Lev deques** (`deques[worker_count]`, allocated in
   lockstep with the thread array): each worker owns a bounded array-based
   deque (256 slots, `bottom`/`top` atomics). Workers bulk-claim up to
   `LOOMWORKS_BULK_DEQUEUE` (8) tasks from the ring into their own deque and
   pop from the bottom (LIFO, newest first — cache friendly). Idle workers
   **steal from a random victim's deque top** (FIFO, oldest first). A pool
   aggregate `deque_total` tracks the number of deque-resident tasks so the
   shutdown drain check is O(1).

### 2.3 Worker Drain Order

`worker_entry()` (one thread per worker index) runs this loop:

```
lock
  ├─ re-read deques (resize may realloc the array, moving it in memory)
  ├─ exit if (idx >= worker_count && !shutdown)      // resized down (spill own deque first)
  ├─ exit if (shutdown && queue_len == 0 && ring_count == 0 && deque_total == 0)  // drained
  ├─ loom_coro_exit()        // free this thread's coroutine scheduler stack, if any
  ├─ Step 0: lane_has_priority(pool, 4) → dequeue lowest-priority task with p <= 4
  │           (REALTIME/HIGH) under the lock — a full local deque must never
  │           starve high-priority work
  ├─ Step 1: deque_pop(pool, my)        → LIFO pop from own deque (newest first)
  ├─ Step 2: ring_bulk_try_dequeue()    → claim up to 8 from the Vyukov ring into
  │           own deque, then pop one (deque full → spill back to the shared queue)
  ├─ Step 3: if worker_count > 1 → steal deque_steal() from victim
  │           (idx+1+2*try) % worker_count, FIFO (oldest first), LOOMWORKS_STEAL_TRIES tries
  ├─ Step 4: if ring empty → dequeue_lowest_priority_unlocked(255) from lanes
  └─ none available → unlock, sem_wait(&pool->work_sem) (EINTR → retry;
                        after shutdown: sched_yield + continue — never re-sleep)
run fn(data) with active_workers++/-- and metrics around it
```

The priority-aware ordering means REALTIME/HIGH tasks bypass the ring, so a
flood of NORMAL tasks cannot starve high-priority work. Deque tasks were
ring-accounted at submit (`queue_len++`); the run boundary decrements
`queue_len` and (only for ring-sourced tasks) removes the cancel-index entry
exactly once.

The wakeup primitive is a **POSIX counting semaphore** (`work_sem`): every
successful enqueue posts exactly one token, so there are no lost wakeups;
workers that find no work wait on `sem_wait`. (`space_cond` is used by
submitters waiting for capacity in bounded-queue mode; `drain_cond` signals
shutdown completion.)

### 2.4 Cancel Index

Cancellation of **not-yet-started** tasks uses an open-addressing hash table
(`cancel_slots`, capacity `2 * ring_size`, slot keyed by `task_id & (cap-1)`):

- `0` = EMPTY, `1` = TOMBSTONE, `id + 1` = occupied (task ids start at 2, so
  `id + 1` never collides with the sentinels).
- Submit inserts a slot *before* making the task visible to workers; the
  worker removes it after dequeue. `cancel_by_id` finds + claims (CAS to
  TOMBSTONE); `cancel(data)` scans slots matching `user_data`; `cancel_all`
  claims every occupied slot.
- A worker that pops a TOMBSTONEd task frees it (and its user data if
  `free_data`) and continues — no dangling pointers.

### 2.5 Task Node Pool

`loom_task_t` nodes are recycled through a lock-free **ABA-tagged Treiber
stack** (`node_stack`: low 32 bits = top, high 32 bits = ABA tag). `pop`
returns a pooled node or falls back to `malloc`; `push` returns it to the
pool (nodes outside the pool are simply `free`d).

### 2.6 Cache-Line Layout

Locks and hot flags are separated on cache lines to prevent false sharing:

```c
struct loom_thread_pool {
    // scalars: worker_count, stack_size, queue_capacity, …
    __attribute__((aligned(64)))
    pthread_mutex_t lock;       // serializes lane buckets, submit funnel, shutdown
    pthread_cond_t  drain_cond; // shutdown completion
    sem_t           work_sem;   // counting semaphore: 1 token per enqueued task
    pthread_cond_t  space_cond; // bounded-queue capacity wait

    _Atomic bool shutdown; _Atomic bool draining; _Atomic bool joined;
    loom_task_t *buckets_head[256]; loom_task_t *buckets_tail[256];
    _Atomic uint64_t nonempty_bits[4];      // 256-bit occupancy bitmap
    _Atomic uint32_t queue_len;             // lane count
    // Vyukov ring: ring_head, ring_tail, ring, ring_size, ring_mask, _Atomic ring_count
    // cancel index: cancel_slots, cancel_cap
    // node pool: node_pool array, _Atomic node_stack (top|ABA tag)
    _Atomic uint32_t active_workers; _Atomic uint64_t next_task_id;
    pthread_t *threads; uint32_t max_worker_count;
    // Chase-Lev deques: loom_work_deque_t *deques; _Atomic uint32_t deque_total;
    // thread liveness: _Atomic bool *thread_alive (parallel to threads[])
    // metrics pointer + callback + user data
};
```

### 2.7 Submit Funnel

```
enqueue_task(pool, task)
  ├─ shutdown? → ERR_SHUTDOWN
  ├─ queue full (cap > 0 && queue_len >= cap)?
  │    └─ wait on space_cond up to 60 s (CLOCK_REALTIME timedwait)
  │         ├─ timeout → ERR_TIMEOUT
  │         └─ shutdown/draining → ERR_SHUTDOWN
  ├─ priority == NORMAL && ring configured? → ring enqueue (spill to lane if full)
  └─ else → lane enqueue under lock
```

Futures (`loom_future_t`) wrap the task with a caller-supplied result buffer;
`future_wait` / `future_wait_timeout` (returns `ERR_TIMEOUT` on deadline
expiry) block on a per-future mutex/cond until the worker signals completion.

### 2.8 Shutdown Flow

```
shutdown()
  ├─ lock; joined? → return (idempotent)
  ├─ shutdown = true; draining = true
  ├─ sem_post(work_sem) × worker_count    // wake every idle worker
  ├─ unlock
  ├─ pthread_join over threads[0 .. max_worker_count)
  └─ draining = false; joined = true; broadcast(drain_cond)

pool_destroy()
  ├─ free lane buckets, cancel slots, ring, node pool, worker threads array
  ├─ destroy space_cond, drain_cond, work_sem, lock
  ├─ clear metrics pointer / callback
  └─ free(pool)
```

Shutdown drains all pending tasks — including cancelled ring tasks awaiting a
tombstone drain and tasks still resident in per-worker deques — and is safe to
call after `resize`. Workers never re-sleep after shutdown: every shutdown token
is posted exactly once per worker, and a worker that slept again could consume
a token meant for a still-sleeping peer, exhausting the supply and deadlocking
the join. Instead, workers that find no work post-shutdown `sched_yield()` and
loop until the drain check fires.

### 2.9 Resize

`resize(count)` grows or shrinks the worker set: the thread array is
reallocated if needed (the deques array grows in lockstep, initialized like
`pool_init`), `worker_count` is updated, new workers are spawned (failure
rolls back), and `work_sem` is posted `old_count` times — enough to wake every
live worker including displaced ones. Workers whose index falls beyond the new
count **spill their deque back to the shared queue** then self-exit at the top
of their loop; the shrinking side joins them with `pthread_tryjoin_np`,
re-posting wake tokens until each displaced thread actually exits (a surviving
worker that wakes and finds no work may sleep again, consuming a token, so a
naive single round of posts can starve a displaced worker forever). A parallel
`thread_alive[]` atomic array tracks which slots have live threads so
shutdown/join skip slots whose workers already exited.

---

## 3. Coroutine Architecture

### 3.1 Component Relationships

```
                    ┌──────────────────────┐
                    │  loom_coroutine_t  │  (opaque, per-coroutine)
                    │  ┌────────────────┐  │
                    │  │ ucontext_t ctx │  │  ← save/restore points
                    │  ├────────────────┤  │
                    │  │ mmap stack     │  │  ← [GUARD][usable][GUARD][GUARD]
                    │  │ (pooled)       │  │
                    │  └────────────────┘  │
                    └──────────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
┌───────────────────┐ ┌───────────────────┐ ┌──────────────────┐
│  g_scheduler      │ │  g_current (TL)   │ │  guard_jmp (TL) │
│  _Thread_local    │ │  _Thread_local    │ │  _Thread_local   │
│  (per-thread)     │ │  (per-thread)     │ │  (per-thread)    │
├───────────────────┤ ├───────────────────┤ ├──────────────────┤
│  128 KiB malloc   │ │  points to        │ │  longjmp target │
│  tracked in       │ │  active coro      │ │  for guard hit  │
│  registry list    │ └───────────────────┘ └──────────────────┘
└───────────────────┘
```

### 3.2 Stack Layout

```
High address ┌──────────────────────┐
             │  GUARD (PROT_NONE)   │  ← prevents upward stack overflow
             ├──────────────────────┤
             │                      │
             │    Usable stack      │  PROT_READ | PROT_WRITE
             │    (64 KiB default)  │
             │                      │
             ├──────────────────────┤
             │  GUARD (PROT_NONE)   │  ← prevents downward stack overflow
             │  GUARD (PROT_NONE)   │
Low address  └──────────────────────┘
              ↑ mmap_base
                   ↑ stack_start
                        ↑ stack_end
```

`LOOMWORKS_CORO_GUARD_PAGES_EACH = 1u`; the bottom uses two guards
(`guard_nb = 2`). The whole region is `mmap`ed `PROT_NONE` then the usable
part is `mprotect`ed `RW`.

### 3.3 Stack Pool

Coroutine stacks are **reused across create/destroy cycles**: a global pool
(cap 64 mappings) matches exact requested stack sizes under
`g_stack_pool_lock`. A pool hit requires zero system calls (the mapping keeps
its guard pages and RW permissions; only the valgrind registration is
redone). Pool misses `mmap` fresh; on destroy, hits are returned to the pool,
misses are `munmap`ed.

### 3.4 Scheduler Stack & Registry

Each thread that runs a coroutine lazily allocates a 128 KiB scheduler stack
(`ensure_scheduler()`, called from `loom_coro_resume()`). The allocation is
tracked in a process-global linked list so it can be reclaimed:

- `loom_coro_exit()` — frees the *current thread's* scheduler stack and
  removes its node from the registry (called by pool workers at the top of
  every loop iteration; a no-op for threads that never used coroutines).
- `coro_atexit()` (a `__attribute__((destructor))` handler) — frees any
  remaining nodes at process exit.

**Concurrency:** pool workers may run coroutines, so `ensure_scheduler()`
(appends) and `loom_coro_exit()` (unlinks) can run concurrently across many
threads. All registry mutations are serialized by `g_scheduler_lock` (a
static pthread mutex) — without it, concurrent read-modify-writes corrupted
`next` pointers and caused heap corruption at exit.

### 3.5 Context Switching Flow

```
Main thread                      Coroutine stack
  │                                  │
  │  loom_coro_resume(coro)       │
  │  ├─ setjmp(g_guard_jmp)         │
  │  ├─ ensure_scheduler()          │  (alloc + registry append, once)
  │  ├─ getcontext(&coro->ctx)      │
  │  ├─ makecontext(ctx, coro_entry, 1, coro_ptr)
  │  └─ swapcontext(&scheduler, &coro->ctx)
  │          ◄───────────────────────  switch to coroutine stack
  │                                  │  coro_entry(coro_ptr)
  │                                  │  ├─ g_current = coro
  │                                  │  ├─ coro->entry_fn(data)
  │                                  │  │    ├─ ... user code ...
  │                                  │  │    └─ loom_coro_yield()
  │                                  │  │         └─ swapcontext(&coro->ctx, &scheduler)
  │                                  │  └─ coro->state = DONE / SUSPENDED
  │          ───────────────────────►  switch back to scheduler stack
  │  swapcontext returns              │
  │  return LOOMWORKS_CORO_OK           │
  │                                  │
```

### 3.6 Signal Handling Flow

```
Coroutine stack overflow → access PROT_NONE page
    │
    ▼
SIGSEGV / SIGBUS signal
    │
    ▼
guard_handler(sig, info, uctx)
    │
    ├─ c = g_current
    ├─ Verify fault_addr is within current coroutine mmap range
    ├─ Verify fault_addr is on a guard page (base or end-ps)
    ├─ c->state = LOOMWORKS_CORO_ERROR
    ├─ g_current = NULL
    └─ longjmp(g_guard_jmp, 1)
            │
            ▼
    setjmp(g_guard_jmp) returns non-zero
            │
            ▼
    return LOOMWORKS_CORO_ERR_GUARD
```

Faults outside the current coroutine's guard pages reinstall the default
handler and re-raise, so genuine segfaults still crash normally. The handler
is installed idempotently (`_Atomic g_guard_installed`).

### 3.7 Lifecycle Rules

- `NEW → resume → (yield/resume)* → terminate → destroy`
- The **entire lifecycle must stay on one thread** (`ucontext` is not
  thread-safe; `swapcontext` across threads is undefined). Running a
  coroutine inside a pool worker is fine — create/resume/destroy all happen
  inside that worker.
- A coroutine records the `pthread_t owner` that created it; `resume()` and
  `terminate()` from any other thread are rejected with
  `LOOMWORKS_CORO_ERR_INVALID`. `destroy()` is deliberately not guarded
  (stack reclamation is internally synchronized) but is only valid once the
  coroutine is `DONE`/`ERROR`.

### 3.8 Context Backend

The switch between the scheduler and a coroutine (and back) is performed
by a context backend selected at compile time. By default, hand-written
assembly is used on x86-64 (SysV) and aarch64 (AAPCS64); every other
platform falls back to POSIX ucontext with identical semantics.

Swap timing (single-threaded, one stack per party):

    scheduler                          coroutine
    --------                           ---------
    resume(): get(&sched); make(&c)
              swap(&sched, &c) ------> restore c; jmp c.pc (entry trampoline)
                                              |  entry_fn(user_data) runs
              ...scheduler blocked...        |  yield(): swap(&c, &sched)
    <-----------------------------------------+  (returns 0 at after_swap)
    resume continues, returns OK
              ...
              swap(&sched, &c) ------> restore c; resume after its swap
              ...                        or: fn returns ->
              ...                        trampoline: swap_to_link(&c)
    <-----------------------------------------+  (to = c.link = &sched)

Register save inventory: only callee-saved GPRs plus the FP control
words are preserved - mxcsr + x87cw on x86-64, fpcr + fpsr on aarch64.
Caller-saved GPRs and the vector registers are not saved. FP control
state therefore round-trips a yield; full FPU register state does not.

Trampoline protocol: a freshly made context starts in `ctx_trampoline`,
which calls `entry_fn(entry_arg)`. When the function returns, control
switches to the context's link (`set_link`); if there is no link, the
process calls `exit(EXIT_SUCCESS)` - the same NULL-link behaviour glibc
exhibits for a `makecontext` function that returns. A coroutine function
must never `return` to the scheduler frame - returning is the terminal
transition, and the trampoline always switches away or exits rather than
returning into a frame that may no longer be on the stack.

---

## 4. Pipeline Architecture

`loom_pc_t` is an application-level FIFO of `void *` items with optional
internal consumption:

- **Queue**: singly linked list under a mutex; `capacity > 0` bounds pending
  items (`submit` waits up to 60 s when full); `capacity = 0` is unbounded.
- **Internal consumers**: if `worker_count > 0`, `pc_create` spins up an
  internal thread pool and submits one consumer task per worker. Each
  consumer loops `loom_pc_take()` and hands the item to the registered
  discard handler for reclamation: `loom_pc_set_discard_handler(pc, discard,
  ctx)` makes `discard(item, ctx)` run instead of the historical bare
  `free(item)`. The handler is not synchronized with concurrent
  `take()`/`submit()` — install it once, right after `create()` — and, when
  set, is also called by `pc_destroy()` on every item still queued at close,
  so no payload leaks on teardown. Callers that `take()` themselves typically
  skip the handler and own the items they receive.
- **Close**: `pc_shutdown()` sets the flag and broadcasts the cond; the
  broadcast is what wakes blocked `take()` calls, so no polling or pool
  broadcast is needed. `submit` after close → `ERR_SHUTDOWN`; `take` after
  close → `SHUTDOWN` with `*item = NULL`.
- **Counters**: `pending_count` under the lock; `submitted_count` /
  `taken_count` atomics.

---

## 5. Task Group Architecture

`loom_task_group_t` tracks the `task_id` of every task submitted through it
(not the payload pointer, so tasks with `NULL` data are tracked and
cancellable too):

- `group_submit()` / `group_submit_future()` submit a completion wrapper to
  the pool and record the returned `task_id` in an internal node list. A
  per-task `pending` counter is incremented on submit. Each wrapper runs the
  real function on a worker thread, decrements `pending` under the group
  lock, broadcasts the done cond when it reaches zero, then frees itself.
- `group_cancel()` cancels every tracked pending task via
  `loom_pool_cancel_by_id()`. A successful cancel (`ERR_OK`) means the task
  never runs: the group reclaims the wrapper and drops `pending`. A
  `ERR_INVALID` result means the task is running or already finished — the
  group only drops the tracking node and leaves reclamation to the wrapper,
  so the wrapper is never double-freed.
- `group_destroy()` marks the group destroyed, cancels all tracked tasks,
  then **blocks until every in-flight wrapped task finishes** (`pending`
  reaches zero) before releasing the group, so no worker can touch a freed
  group. It does not touch the backing pool.
- `group_wait()` blocks until `pending` reaches zero and returns; it does
  **not** shut down the backing pool — the pool stays fully usable for new
  submissions afterwards.
- `group_pending_count()` returns the number of tasks **currently tracked**
  (submitted but not yet cancelled or completed-tracking-dropped); it is not
  reset by `wait()`.

---

## 6. Metrics Architecture

`loom_metrics_t` exposes counters and latency stats fed by the pool:

- **Events**: SUBMITTED, STARTED, COMPLETED, CANCELLED, FAILED — incremented
  at the corresponding points in the submit funnel and worker loop.
- **Latency**: sum (atomic add) and max (CAS loop) updated on task
  completion; `avg_latency_ns = sum / completed`.
- **Wiring**: `pool_set_metrics_callback(pool, cb, user_data)` makes the pool
  fire `cb(event, pool, user_data)` on every event; `pool_set_metrics`
  attaches the counters object; `record_latency` lets application code
  contribute timings.
- **Snapshot**: consistent point-in-time read of all counters under the
  metrics lock (the counters themselves are lock-free atomics, so getters are
  cheap).

---

## 7. Thread Safety Model

### 7.1 Thread Pool

- **Locks**: one `pthread_mutex_t` (`lock`) protects the lane buckets, submit
  funnel, and state transitions; workers also touch the lock-free ring
  without it. `work_sem` is a counting semaphore (post per enqueue, wait per
  no-work); `space_cond` + `drain_cond` handle capacity waits and shutdown
  completion.
- **Lock-free ops**: `ring_try_enqueue` / `ring_bulk_try_dequeue` (Vyukov
  protocol), the per-worker Chase-Lev deques (`deque_push`/`deque_pop`/
  `deque_steal` with the classic seq_cst-fence slow path for the last-element
  race), the node-pool Treiber stack, the cancel-slot CAS claims, and the
  queue/ring/deque counters are all lock-free under relaxed/acquire/release
  orders.

### 7.2 Coroutines

- **Per-thread scheduler**: `g_scheduler`, `g_current`, `g_guard_jmp` are
  `_Thread_local`.
- **`g_guard_installed`**: `_Atomic bool`; concurrent installs run
  `sigaction` once.
- **Registry list**: process-global, serialized by `g_scheduler_lock`
  (concurrent `ensure_scheduler` / `loom_coro_exit` from pool workers are
  safe).

### 7.3 Why coroutines do not support cross-thread resume

```
Thread A: coro = loom_coro_create(...)
Thread A: loom_coro_resume(coro)   ← runs on Thread A's g_scheduler
Thread B: loom_coro_resume(coro)   ← runs on Thread B's g_scheduler
        ↑ UNSAFE: ucontext_t is not thread-safe; swapcontext across threads is undefined behavior
```

To use a coroutine safely, the entire lifecycle (create → resume → destroy)
must occur within a single thread.

---

## 8. Memory Model

### 8.1 Thread Pool Memory Allocation

```
loom_pool_create()
  ├─ calloc(1, sizeof(loom_thread_pool_t))
  ├─ init lock, drain_cond, work_sem, space_cond
  ├─ ring = next_pow2(queue_capacity) cells (4096 default when unbounded)
  ├─ calloc cancel_slots (2 * ring_size)  — failure → lane-only mode, ring freed
  ├─ calloc node_pool nodes; init node_stack (ABA tag)
  └─ calloc threads array; spawn workers

loom_pool_submit()
  └─ loom_task_create(): pop node_stack (if any) or malloc(sizeof(loom_task_t))

loom_pool_submit_future()
  ├─ calloc(1, sizeof(loom_future_t)) + init mutex/cond
  ├─ malloc(sizeof(future_task_ctx_t))  ← task wrapper context
  └─ enqueue wrapper task with free_data = true

loom_pool_destroy()
  ├─ free lane buckets, cancel slots, ring, node pool
  ├─ destroy space_cond, drain_cond, sem, lock
  ├─ free(threads)
  └─ free(pool)
```

### 8.2 Coroutine Memory Allocation

```
loom_coro_create()
  ├─ calloc(1, sizeof(loom_coroutine_t))
  ├─ stack pool hit? → reuse mapping (zero syscalls)
  └─ miss → mmap(total_sz, PROT_NONE) + mprotect(usable, RW)

loom_coro_resume()
  └─ ensure_scheduler(): malloc(131072) scheduler stack (once per thread),
       appended to the registry under g_scheduler_lock

loom_coro_destroy()
  ├─ stack pool has room? → cache the mapping (cap 64) else munmap
  └─ free(c)

loom_coro_exit() / coro_atexit()
  └─ free scheduler stack (+ node) — under g_scheduler_lock for the registry
```

---

## 9. Documentation Index

| Document | Description |
|----------|-------------|
| [API Reference](api-reference.md) | Complete public API with function signatures and parameters |
| [FAQ](faq.md) | Frequently asked questions about usage and limitations |
| [Migration Guide](migration.md) | How to migrate from ctpool to loomworks |
| [Design Decisions](design-decisions.md) | Rationale behind key architectural choices |
| [Contributing](contributing.md) | Coding standards and submission process |

---

## 10. Error Handling Strategy

| Operation | Failure behavior |
|-----------|-----------------|
| `pthread_mutex_init` / `pthread_cond_init` / `sem_init` | Return `LOOMWORKS_ERR_ALLOC`, destroy already-initialized primitives |
| `pthread_create` | Set `shutdown=true`, post `work_sem` per worker, join created threads, free all resources |
| `malloc` / `calloc` | Return `LOOMWORKS_ERR_ALLOC` |
| `mmap` | Return `LOOMWORKS_CORO_ERR_ALLOC` |
| `mprotect` | munmap the already-allocated region, return `LOOMWORKS_CORO_ERR_MPROTECT` |
| `ring` / cancel-slot calloc failure | Fall back to lane-only mode (correct, slower) |
| `sigaction` | Print stderr error, do not abort, continue running (no guard protection) |
| `swapcontext` | Set `state = LOOMWORKS_CORO_ERROR`, return `LOOMWORKS_CORO_ERR_CONTEXT` |
| Stack overflow (guard page) | `longjmp` to `g_guard_jmp`, coroutine → `ERROR`, return `LOOMWORKS_CORO_ERR_GUARD` |
| Bounded queue full, 60 s wait | Return `LOOMWORKS_ERR_TIMEOUT` |

All error paths guarantee correct resource cleanup with no memory leaks.