# ctpool 架构设计文档

## 1. 系统概览

ctpool 是一个纯 C11 并发库，包含两个独立子系统：

```
┌─────────────────────────────────────────────────────────┐
│                        ctpool                           │
├─────────────────────────┬───────────────────────────────┤
│      线程池子模块        │       协程子模块               │
│  thread_pool.h/c        │  coroutine.h/c                │
├─────────────────────────┼───────────────────────────────┤
│  • 工作线程管理          │  • ucontext 上下文切换          │
│  • 有界/无界任务队列     │  • mmap PROT_NONE 保护栈       │
│  • Future 异步返回值     │  • SIGSEGV/SIGBUS 保护页捕获   │
│  • 优雅关闭与任务排空    │  • 跨线程安全的 per-thread 调度 │
│  • 缓存行对齐防伪共享    │  • 64 位 makecontext 安全传参   │
└─────────────────────────┴───────────────────────────────┘
```

两个子模块共享同一套 API 规范（不透明指针、错误码、C11 标准），但内部实现完全独立，无交叉依赖。

---

## 2. 线程池架构

### 2.1 组件关系

```
                    ┌──────────────────────┐
                    │   ctpool_thread_pool │
                    │   (opaque handle)    │
                    └───────┬──────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐  ┌───────────────┐  ┌────────────────┐
│  lock + cond  │  │  linked list  │  │  worker[]     │
│  (cacheline 0)│  │  (queue head) │  │  (per-index   │
├───────────────┤  ├───────────────┤  │   worker_ctx) │
│  drain_cond   │  │  (queue tail) │  ├───────────────┤
│ (cacheline 1) │  │  (queue_len)  │  │  task_queue_* │
└───────────────┘  └───────────────┘  │  [cache line  │
                                      │   2,3,4...]   │
                                      └───────────────┘
```

### 2.2 缓存行布局

```
struct ctpool_thread_pool {
    // ── 共享状态（写时频繁竞争，必须对齐到不同 cache line）──
    pthread_mutex_t lock              __attribute__((aligned(64)));  // cache line 0
    pthread_cond_t  cond              __attribute__((aligned(64)));  // cache line 1
    pthread_cond_t  drain_cond        __attribute__((aligned(64)));  // cache line 2
    bool            shutdown;                                        // 与 lock 同 cache line
    bool            draining;

    // ── 队列（读写频繁，独立 cache line）──
    ctpool_task_t  *queue_head;                                     // cache line 3
    ctpool_task_t  *queue_tail;
    uint32_t        queue_len;

    // ── 工作线程数组（每个 worker 独占 cache line）──
    ctpool_worker_ctx_t workers[];
};

struct ctpool_worker_ctx {
    uint64_t padding[7];            // 防止与相邻 worker 的伪共享
    ctpool_task_t *task_queue_head;
    ctpool_task_t *task_queue_tail;
    uint32_t       task_queue_len;
    uint64_t padding2[7];
};
```

### 2.3 任务生命周期

```
submit() ──► task_create() ──► enqueue() ──► worker dequeue()
    │              │                  │                │
    ▼              ▼                  ▼                ▼
 用户调用      malloc 分配        持锁插入链表       持锁弹出链表
                                           调用 fn(data)
                                           释放 task 节点
```

### 2.4 关闭流程

```
shutdown()
  ├─ 持锁置 shutdown=true, draining=true
  ├─ broadcast cond (唤醒所有等待 worker)
  └─ join 所有 worker 线程
       ├─ worker 检测到 shutdown && queue_len==0 → break
       └─ 释放队列中剩余任务（如有）

pool_destroy()
  ├─ 持锁释放队列中所有 task 节点
  ├─ destroy drain_cond, cond, lock
  └─ free(threads), free(workers), free(pool)
```

---

## 3. 协程架构

### 3.1 组件关系

```
                    ┌──────────────────────┐
                    │  ctpool_coroutine_t  │  (opaque, per-coroutine)
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

### 3.2 栈布局

```
高地址 ┌──────────────────────┐
       │  GUARD (PROT_NONE)   │  ← 防止栈向上溢出
       ├──────────────────────┤
       │                      │
       │    可用栈空间         │  PROT_READ | PROT_WRITE
       │    (64 KiB 默认)     │
       │                      │
       ├──────────────────────┤
       │  GUARD (PROT_NONE)   │  ← 防止栈向下溢出（起始保护页）
       │  GUARD (PROT_NONE)   │
低地址 └──────────────────────┘
         ↑  mmap_base
              ↑ stack_start
                   ↑ stack_end
```

### 3.3 上下文切换流程

```
主线程                         协程栈
  │                              │
  │  ctpool_coro_resume(coro)   │
  │  ├─ setjmp(g_guard_jmp)     │
  │  ├─ getcontext(&coro->ctx)  │
  │  ├─ makecontext(ctx, coro_entry, 1, coro_ptr)
  │  └─ swapcontext(&scheduler, &coro->ctx)
  │          ◄──────────────────── 切换到协程栈
  │                              │  coro_entry(coro_ptr)
  │                              │  ├─ g_current = coro
  │                              │  ├─ coro->entry_fn(data)
  │                              │  │    ├─ ... 用户代码 ...
  │                              │  │    └─ ctpool_coro_yield()
  │                              │  │         └─ swapcontext(&coro->ctx, &scheduler)
  │                              │  └─ coro->state = DONE / SUSPENDED
  │          ───────────────────►  切回调度器栈
  │  swapcontext 返回             │
  │  return CTPPOOL_CORO_OK      │
  │                              │
```

### 3.4 信号处理流程

```
协程栈溢出 → 访问 PROT_NONE 页
    │
    ▼
SIGSEGV / SIGBUS 信号
    │
    ▼
guard_handler(sig, info, uctx)
    │
    ├─ c = g_current
    ├─ 校验 fault_addr 在当前协程 mmap 范围内
    ├─ 校验 fault_addr 在 guard page（base 或 end-ps）
    ├─ c->state = CTPPOOL_CORO_ERROR
    ├─ g_current = NULL
    └─ longjmp(g_guard_jmp, 1)
            │
            ▼
    setjmp(g_guard_jmp) 返回非零值
            │
            ▼
    return CTPPOOL_CORO_ERR_GUARD
```

---

## 4. 线程安全模型

### 4.1 线程池

- **互斥锁**：单个 `pthread_mutex_t` 保护队列和状态，所有 worker 和主线程共享
- **条件变量**：
  - `cond` — worker 等待新任务时阻塞，主线程 submit 后 signal
  - `drain_cond` — shutdown 后主线程等待所有 worker join
- **无锁队列操作**：`ctpool_enqueue_unlocked()` / `ctpool_dequeue_unlocked()` 必须在持锁状态下调用

### 4.2 协程

- **per-thread 调度器**：`g_scheduler` 和 `g_guard_jmp` 均为 `_Thread_local`，每个线程独立维护
- **`g_guard_installed`**：使用 `_Atomic bool`，多个线程同时 install 时只执行一次 `sigaction`
- **`g_current`**：`_Thread_local` 指针，信号处理器中直接读取，无需锁

### 4.3 为什么协程不支持跨线程 resume

```
线程 A：coro = ctpool_coro_create(...)
线程 A：ctpool_coro_resume(coro)   ← 在线程 A 的 g_scheduler 上运行
线程 B：ctpool_coro_resume(coro)   ← 在线程 B 的 g_scheduler 上运行
        ↑ 不安全：ucontext_t 不是线程安全的，swapcontext 跨线程使用未定义行为
```

如需跨线程使用协程，必须在同一线程内完成 create → resume → destroy 完整生命周期。

---

## 5. 内存模型

### 5.1 线程池内存分配

```
ctpool_pool_create()
  ├─ calloc(1, sizeof(ctpool_thread_pool_t))   ← pool 结构体
  ├─ calloc(worker_count, sizeof(ctpool_worker_ctx_t))  ← 工作线程上下文数组
  ├─ calloc(worker_count, sizeof(pthread_t))   ← 线程句柄数组
  └─ pthread_mutex_init / pthread_cond_init × 3

ctpool_pool_submit()
  └─ malloc(sizeof(ctpool_task_t))             ← 每个任务节点

ctpool_pool_submit_future()
  ├─ calloc(1, sizeof(ctpool_future_t))        ← future 结构体
  ├─ pthread_mutex_init(&fut->mutex)
  ├─ pthread_cond_init(&fut->cond)
  └─ malloc(sizeof(future_task_ctx_t))          ← 任务包装上下文

ctpool_pool_destroy()
  ├─ 遍历释放 queue_head → queue_tail 所有 task
  ├─ pthread_cond_destroy × 2
  ├─ pthread_mutex_destroy
  ├─ free(threads)
  ├─ free(workers)
  └─ free(pool)
```

### 5.2 协程内存分配

```
ctpool_coro_create()
  ├─ calloc(1, sizeof(ctpool_coroutine_t))     ← 协程结构体
  └─ mmap(total_sz, PROT_NONE)                 ← 含保护页的整个栈区域
     └─ mprotect(stack_start, usable_sz, RW)   ← 可用区域设为可读可写

ctpool_coro_resume()
  └─ malloc(131072)                            ← 调度器栈（per-thread，仅分配一次）

ctpool_coro_destroy()
  ├─ munmap(mmap_base, mmap_size)              ← 释放整个栈区域（含保护页）
  └─ free(c)                                   ← 释放协程结构体

注：调度器栈 (g_scheduler_stack) 不释放（进程级常驻，约 128KB），符合常规做法。
```

---

## 6. 错误处理策略

| 操作 | 失败时行为 |
|------|-----------|
| `pthread_mutex_init` | 返回 `CTPPOOL_ERR_ALLOC`，释放已分配资源 |
| `pthread_cond_init` | 返回 `CTPPOOL_ERR_ALLOC`，销毁已初始化的互斥锁 |
| `pthread_create` | 置 `shutdown=true`，broadcast cond，join 已创建线程，释放所有资源 |
| `malloc` / `calloc` | 返回 `CTPPOOL_ERR_ALLOC` |
| `mmap` | 返回 `CTPPOOL_CORO_ERR_ALLOC` |
| `mprotect` | munmap 已 mmap 区域，返回 `CTPPOOL_CORO_ERR_MPROTECT` |
| `sigaction` | 打印 stderr 错误信息，不 abort，继续运行（无保护页功能） |
| `swapcontext` | 置 `state = CTPPOOL_CORO_ERROR`，返回 `CTPPOOL_CORO_ERR_CONTEXT` |
| 栈溢出（guard page） | `longjmp` 到 `g_guard_jmp`，返回 `CTPPOOL_CORO_ERR_GUARD` |

所有错误路径均保证资源正确释放，无内存泄漏。
