# Phase 1: 健壮性/安全加固设计

**日期**: 2026-08-25  
**状态**: 实现中  
**范围**: 信号递归防护 + 协程执行超时

---

## 背景

loomworks 是一个工业级 C11 并发库，当前存在以下安全性问题：

1. **信号递归风险**: `guard_handler` 在递归嵌套场景（如 longjmp 到另一个栈溢出点）可能导致未定义行为
2. **协程饥饿**: 无时间片限制的协程调度可能让恶意或 bug 协程无限占用 worker
3. **资源上限**: 缺乏对 mmap/calloc 失败路径的严格处理

---

## 设计方案

### 1. 信号递归防护

**问题**: 
- `g_guard_jmp` 是进程全局 sigjmp_buf，嵌套长跳转会导致未定义行为
- 信号处理器可能在中断上下文中被递归调用

**解决方案**:
- 添加原子标志位 `g_in_guard_handler` 防止重入
- 使用 `_Thread_local` 的嵌套计数器追踪重入深度
- 递归重入时静默忽略，避免崩溃

**修改文件**:
- `src/coroutine.c`: 修改 `guard_handler` 函数
- `include/loomworks/coroutine.h`: 添加配置常量

### 2. 协程执行超时

**问题**:
- 协程必须主动 yield/sleep，buggy 协程可无限占用 worker
- worker 被占用期间无法调度其他任务

**解决方案**:
- 添加 `loom_coro_set_timeout(pool, timeout_ns)` API
- worker 记录协程开始运行的 monotonic 时间戳
- 在协程 yield/sleep 点检查已运行时间
- 超过阈值的协程被强制挂起，标记 `LOOMWORKS_CORO_TIMEOUT` 状态

**修改文件**:
- `include/loomworks/thread_pool.h`: 添加超时配置 API
- `src/thread_pool.c`: 添加超时检查和强制 yield 逻辑
- `src/coroutine.c`: 添加超时验证和状态标记
- `include/loomworks/coroutine.h`: 添加 `LOOMWORKS_CORO_TIMEOUT` 状态码

---

## API 变更

### 新增函数

```c
// 设置协程执行超时阈值（纳秒）
// timeout_ns == 0 表示禁用超时（默认行为）
void loom_coro_set_timeout(loom_thread_pool_t *pool, int64_t timeout_ns);
```

### 新增状态码

```c
typedef enum {
    LOOMWORKS_CORO_NEW,
    LOOMWORKS_CORO_RUNNING,
    LOOMWORKS_CORO_SUSPENDED,
    LOOMWORKS_CORO_SLEEPING,
    LOOMWORKS_CORO_DONE,
    LOOMWORKS_CORO_ERROR,
    LOOMWORKS_CORO_TIMEOUT,  // 新增：执行超时
} loom_coro_state_t;
```

---

## 实现细节

### 信号递归防护

```c
// 在 coroutine.c 中添加
static _Atomic int g_guard_recurse_depth = 0;

static void guard_handler(int sig, siginfo_t *info, void *uctx)
{
    // 防止递归重入
    if (atomic_fetch_add_explicit(&g_guard_recurse_depth, 1, memory_order_relaxed) > 0) {
        atomic_fetch_sub_explicit(&g_guard_recurse_depth, 1, memory_order_relaxed);
        _exit(128 + sig);
    }
    
    // ... 原有逻辑 ...
    
    // 退出前递减计数器
    atomic_fetch_sub_explicit(&g_guard_recurse_depth, 1, memory_order_relaxed);
}
```

### 协程超时机制

```c
// 在 thread_pool_internal.h 中添加
struct loom_coroutine {
    // ... 现有字段 ...
    int64_t execution_start_ns;  // 新增：执行开始时间
    int64_t max_execution_ns;    // 新增：最大执行时间（0 = 无限制）
};

// 在 worker_entry 中添加超时检查
static void check_coro_timeout(loom_coroutine_t *coro)
{
    if (coro->max_execution_ns <= 0) return;
    
    int64_t now = monotonic_now_ns();
    if (now - coro->execution_start_ns > coro->max_execution_ns) {
        coro->state = LOOMWORKS_CORO_TIMEOUT;
        coro_yield();  // 强制 yield
    }
}
```

---

## 测试计划

### 信号处理测试

1. `test_signal_recursion`: 模拟嵌套栈溢出，验证不会死锁
2. `test_signal_handler_restore`: 验证卸载后恢复原始处理器

### 协程超时测试

1. `test_coro_timeout_basic`: 验证超时后被正确挂起
2. `test_coro_timeout_disabled`: 验证超时禁用时行为正常
3. `test_coro_timeout_integration`: 验证超时后 worker 可调度其他任务

### 集成测试

1. 验证所有现有测试仍通过
2. 验证信号处理不影响非协程代码

---

## 风险控制

| 风险 | 缓解措施 |
|------|----------|
| 性能开销 | 原子操作使用 relaxed 顺序，仅在保护点检查 |
| 兼容性 | 默认禁用超时，向后兼容 |
| 信号安全 | 仅使用 async-signal-safe 函数 |

---

## 后续阶段

- **Phase 2**: 取消性能优化（O(1) hash 索引查找）
- **Phase 3**: 资源上限强化 + 栈池 per-pool 化
