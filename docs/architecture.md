# loomworks Architecture

## 1. System Overview

loomworks is a pure C11 concurrency library comprising two independent subsystems:

```
┌─────────────────────────────────────────────────────────┐
│                        loomworks                           │
├─────────────────────────┬───────────────────────────────┤
│     Thread Pool         │       Coroutine               │
│  thread_pool.h/c        │  coroutine.h/c                │
├─────────────────────────┼───────────────────────────────┤
│  • Worker thread mgmt   │  • ucontext save/restore       │
│  • Bounded/unbounded    │  • mmap PROT_NONE guard stack │
│    task queue           │  • SIGSEGV/SIGBUS guard hit   │
│  • Future async results │  • Per-thread scheduler       │
│  • Graceful shutdown    │  • 64-bit safe makecontext    │
│  • Cache-line aligned   │  • Thread-safe signal handler │
└─────────────────────────┴───────────────────────────────┘
```

The two subsystems share the same API conventions (opaque pointers, result codes, C11 standard) but have completely independent internal implementations with no cross-dependencies.

---

## 2. Thread Pool Architecture

### 2.1 Component Relationships

```
                    ┌──────────────────────┐
                    │   loom_thread_pool │
                    │   (opaque handle)    │
                    └───────┬──────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐  ┌───────────────┐  ┌────────────────┐
│  lock + cond  │  │  linked list  │  │  worker[]     │
│  (cache line 0)│ │  (queue head) │  │  (per-index   │
├───────────────┤  ├───────────────┤  │   worker_ctx) │
│  drain_cond   │  │  (queue tail) │  ├───────────────┤
│ (cache line 2)│  │  (queue_len)  │  │  task_queue_* │
└───────────────┘  └───────────────┘  │  [cache line  │
                                       │   3,4...]      │
                                       └───────────────┘
```

### 2.2 Cache-Line Layout

```c
struct loom_thread_pool {
    // ── Shared state (frequently written, must span different cache lines) ──
    pthread_mutex_t lock              __attribute__((aligned(64)));  // cache line 0
    pthread_cond_t  cond              __attribute__((aligned(64)));  // cache line 1
    pthread_cond_t  drain_cond        __attribute__((aligned(64)));  // cache line 2
    bool            shutdown;                                         // same cache line as drain_cond
    bool            draining;

    // ── Queue (frequently read/written, independent cache line) ──
    loom_task_t  *queue_head;                                      // cache line 3
    loom_task_t  *queue_tail;
    uint32_t        queue_len;

    // ── Worker array (each worker独占 its own cache line) ──
    loom_worker_ctx_t workers[];
};

struct loom_worker_ctx {
    uint64_t padding[7];            // prevents false sharing with adjacent workers
    loom_task_t *task_queue_head;
    loom_task_t *task_queue_tail;
    uint32_t       task_queue_len;
    uint64_t padding2[7];
};
```

### 2.3 Task Lifecycle

```
submit() ──► task_create() ──► enqueue() ──► worker dequeue()
    │              │                  │                │
    ▼              ▼                  ▼                ▼
  User call    malloc alloc      lock+insert        lock+remove
                                           call fn(data)
                                           free task node
```

### 2.4 Shutdown Flow

```
shutdown()
  ├─ Acquire lock, set shutdown=true, draining=true
  ├─ Broadcast cond (wake all waiting workers)
  └─ Join all worker threads
       ├─ Worker detects shutdown && queue_len==0 → break
       └─ Drain remaining tasks from queue (if any)

pool_destroy()
  ├─ Acquire lock, free all remaining task nodes in queue
  ├─ destroy drain_cond, cond, lock
  └─ free(threads), free(workers), free(pool)
```

---

## 3. Coroutine Architecture

### 3.1 Component Relationships

```
                    ┌──────────────────────┐
                    │  loom_coroutine_t  │  (opaque, per-coroutine)
                    │  ┌────────────────┐  │
                    │  │ ucontext_t ctx │  │  ← save/restore points
                    │  ├────────────────┤  │
                    │  │ mmap stack     │  │  ← [GUARD][GUARD][STACK][GUARD]
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
│  ss_sp = malloc   │ │  points to        │ │  longjmp target │
│  ss_size = 128KB  │ │  active coro      │ │  for guard hit  │
└───────────────────┘ └───────────────────┘ └──────────────────┘
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
             │  GUARD (PROT_NONE)   │  ← prevents downward stack overflow (starting guard)
             │  GUARD (PROT_NONE)   │
Low address  └──────────────────────┘
              ↑ mmap_base
                   ↑ stack_start
                        ↑ stack_end
```

### 3.3 Context Switching Flow

```
Main thread                      Coroutine stack
  │                                  │
  │  loom_coro_resume(coro)       │
  │  ├─ setjmp(g_guard_jmp)         │
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

### 3.4 Signal Handling Flow

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

---

## 4. Thread Safety Model

### 4.1 Thread Pool

- **Mutex**: Single `pthread_mutex_t` protects the queue and state, shared by all workers and the main thread
- **Condition variables**:
  - `cond` — workers block waiting for new tasks; main thread signals after submit
  - `drain_cond` — main thread waits after shutdown for all workers to join
- **Lock-free queue ops**: `loom_enqueue_unlocked()` / `loom_dequeue_unlocked()` must be called while holding the lock

### 4.2 Coroutines

- **Per-thread scheduler**: `g_scheduler` and `g_guard_jmp` are `_Thread_local`, each thread maintains its own
- **`g_guard_installed`**: Uses `_Atomic bool`; multiple threads calling install concurrently only execute `sigaction` once
- **`g_current`**: `_Thread_local` pointer, read directly in the signal handler without locking

### 4.3 Why coroutines do not support cross-thread resume

```
Thread A: coro = loom_coro_create(...)
Thread A: loom_coro_resume(coro)   ← runs on Thread A's g_scheduler
Thread B: loom_coro_resume(coro)   ← runs on Thread B's g_scheduler
        ↑ UNSAFE: ucontext_t is not thread-safe; swapcontext across threads is undefined behavior
```

To use a coroutine safely across threads, the entire lifecycle (create → resume → destroy) must occur within a single thread.

---

## 5. Memory Model

### 5.1 Thread Pool Memory Allocation

```
loom_pool_create()
  ├─ calloc(1, sizeof(loom_thread_pool_t))    ← pool structure
  ├─ calloc(worker_count, sizeof(loom_worker_ctx_t))  ← worker context array
  ├─ calloc(worker_count, sizeof(pthread_t))     ← thread handle array
  └─ pthread_mutex_init / pthread_cond_init × 3

loom_pool_submit()
  └─ malloc(sizeof(loom_task_t))               ← per-task node

loom_pool_submit_future()
  ├─ calloc(1, sizeof(loom_future_t))          ← future structure
  ├─ pthread_mutex_init(&fut->mutex)
  ├─ pthread_cond_init(&fut->cond)
  └─ malloc(sizeof(future_task_ctx_t))            ← task wrapper context

loom_pool_destroy()
  ├─ Traverse and free all task nodes in queue
  ├─ pthread_cond_destroy × 2
  ├─ pthread_mutex_destroy
  ├─ free(threads)
  ├─ free(workers)
  └─ free(pool)
```

### 5.2 Coroutine Memory Allocation

```
loom_coro_create()
  ├─ calloc(1, sizeof(loom_coroutine_t))       ← coroutine structure
  └─ mmap(total_sz, PROT_NONE)                   ← full stack region including guards
     └─ mprotect(stack_start, usable_sz, RW)     ← usable region set readable/writable

loom_coro_resume()
  └─ malloc(131072)                              ← scheduler stack (per-thread, allocated once)

loom_coro_destroy()
  ├─ munmap(mmap_base, mmap_size)                ← free entire stack region (including guards)
  └─ free(c)                                     ← free coroutine structure

Note: The scheduler stack (g_scheduler_stack) is intentionally not freed (process-level常驻, ~128KB).
```

---

## 6. Error Handling Strategy

| Operation | Failure behavior |
|-----------|-----------------|
| `pthread_mutex_init` | Return `LOOMWORKS_ERR_ALLOC`, free allocated resources |
| `pthread_cond_init` | Return `LOOMWORKS_ERR_ALLOC`, destroy already-initialized mutex |
| `pthread_create` | Set `shutdown=true`, broadcast cond, join created threads, free all resources |
| `malloc` / `calloc` | Return `LOOMWORKS_ERR_ALLOC` |
| `mmap` | Return `LOOMWORKS_CORO_ERR_ALLOC` |
| `mprotect` | munmap the already-allocated region, return `LOOMWORKS_CORO_ERR_MPROTECT` |
| `sigaction` | Print stderr error, do not abort, continue running (no guard page protection) |
| `swapcontext` | Set `state = LOOMWORKS_CORO_ERROR`, return `LOOMWORKS_CORO_ERR_CONTEXT` |
| Stack overflow (guard page) | `longjmp` to `g_guard_jmp`, return `LOOMWORKS_CORO_ERR_GUARD` |

All error paths guarantee correct resource cleanup with no memory leaks.
