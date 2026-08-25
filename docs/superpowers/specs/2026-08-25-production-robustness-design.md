# Production Robustness Hardening Design

**日期**: 2026-08-25  
**状态**: 设计中  
**范围**: Worker 自动恢复、Health Check API、Fault Injection 扩展、Backpressure 策略

---

## 背景

loomworks 已通过 ASan/UBSan/valgrind 验证，但生产环境需要更强的运行时健壮性保障：
1. Worker 异常退出后 pool 容量永久损失
2. 缺乏运行时健康状态查询 API
3. Fault injection 测试覆盖不足
4. 资源压力时无 backpressure 响应

---

## 设计方案

### 1. Worker 自动恢复 (`loom_pool_worker_recover`)

**问题**: 当前 worker 异常退出（非 clean_exit）时，`thread_alive[i]` 保持 true，但实际无 worker 执行任务，导致 pool 容量永久损失。

**方案**:
- 在 timer thread 中添加 worker liveness 检查循环
- 每 100ms 检查一次：若 `thread_alive[i] == true` 但 `thread_clean_exit[i] == false` 且超过 `LOOMWORKS_WORKER_RECOVERY_TIMEOUT_NS` (默认 5s)，则重建
- 重建流程：pthread_create + 重置相关状态

**API**:
```c
// 配置恢复超时（0 = 禁用）
void loom_pool_set_worker_recovery_timeout(loom_thread_pool_t *pool, int64_t timeout_ns);
// 返回异常退出的 worker 数量
uint32_t loom_pool_abnormal_worker_count(const loom_thread_pool_t *pool);
```

**文件变更**:
- `src/thread_pool.c`: timer thread 循环中添加 recover 逻辑
- `include/loomworks/thread_pool.h`: 新增 API 声明
- `src/thread_pool_internal.h`: 新增配置字段

---

### 2. Health Check API (`loom_pool_health_t`)

**问题**: 外部监控无法获取 pool 内部状态（worker 健康度、队列深度趋势等）。

**方案**:
- 创建独立的 `loom_pool_health_t` 结构，包含采样定时器
- 提供 snapshot API，返回当前健康状态
- 使用 lock-free 读取路径最小化对 pool 性能影响

**数据结构**:
```c
typedef struct loom_pool_health {
    loom_thread_pool_t *pool;
    pthread_t monitor_thread;
    _Atomic bool running;
    // 采样结果（lock-free 写入）
    struct {
        uint32_t worker_count;
        uint32_t active_count;
        uint32_t pending_count;
        uint32_t abnormal_workers;
        int64_t  uptime_ns;
        double   utilization;
    } last_sample;
} loom_pool_health_t;
```

**API**:
```c
loom_result_t loom_pool_health_create(loom_thread_pool_t *pool, loom_pool_health_t **out);
loom_result_t loom_pool_health_check(const loom_pool_health_t *h, loom_health_status_t *out);
void loom_pool_health_destroy(loom_pool_health_t **h);
```

**文件变更**:
- `include/loomworks/thread_pool.h`: 新增 health API
- `src/thread_pool.c`: 实现 health monitor thread

---

### 3. Fault Injection 扩展

**问题**: 现有 `loom_test_arm_alloc_failure(n)` 仅覆盖 malloc/calloc 失败，无法模拟其他故障场景。

**方案**: 扩展现有框架，支持多种 fault hook：

```c
// 已有
void loom_test_arm_alloc_failure(long n);

// 新增
void loom_test_arm_sigsegv(long n);              // 模拟 SIGSEGV
void loom_test_arm_coro_timeout(int64_t ns, long n);  // 强制协程超时
void loom_test_arm_timer_heap_full(long n);      // 堆满时失败
```

**实现方式**: 使用 atomic counter + hook point 宏

```c
#define LOOMWORKS_FAULT_CHECK(name, condition, result) do { \
    static _Atomic long g_##name##_arm = 0; \
    if (atomic_load_explicit(&g_##name##_arm, memory_order_relaxed) > 0) { \
        if ((condition)) { \
            atomic_fetch_sub_explicit(&g_##name##_arm, 1, memory_order_relaxed); \
            return (result); \
        } \
    } \
} while(0)
```

**文件变更**:
- `src/thread_pool.c`: 添加 fault hooks 到关键路径
- `src/coroutine.c`: 添加 coroutine fault hooks
- `tests/`: 新增 fault injection 测试用例

---

### 4. Backpressure 策略

**问题**: 资源压力时无响应，直接 OOM 或拒绝。

**方案**:
- 栈内存: 90% 阈值时阻塞新协程创建（非拒绝），等待现有协程销毁释放内存
- 队列深度: >80% capacity 时触发回调告警
- 新增回调 API 供外部监控系统注册

**阈值配置**:
```c
typedef struct {
    double stack_usage_warn_ratio;  /* default 0.9 */
    double queue_depth_warn_ratio;  /* default 0.8 */
    int64_t queue_wait_timeout_ns;  /* default 60s */
} loom_backpressure_config_t;

void loom_pool_set_backpressure_config(loom_thread_pool_t *pool, 
                                        const loom_backpressure_config_t *cfg);
void loom_pool_set_backpressure_callback(loom_thread_pool_t *pool,
                                          void (*cb)(void *ctx, loom_backpressure_event_t event),
                                          void *ctx);
```

**事件类型**:
```c
typedef enum {
    LOOM_BACKPRESSURE_STACK_HIGH,     /* 栈使用 > 90% */
    LOOM_BACKPRESSURE_QUEUE_HIGH,     /* 队列深度 > 80% */
    LOOM_BACKPRESSURE_STACK_BLOCKED,  /* 协程创建被阻塞 */
    LOOM_BACKPRESSURE_QUEUE_BLOCKED,  /* submit 被阻塞 */
} loom_backpressure_event_t;
```

**文件变更**:
- `src/thread_pool.c`: 添加 backpressure 检查点
- `src/coroutine.c`: 添加栈内存阻塞逻辑
- `include/loomworks/thread_pool.h`: 新增 API

---

## 文件变更清单

| 文件 | 变更类型 | 说明 |
|------|----------|------|
| `include/loomworks/thread_pool.h` | 修改 | 新增 health API、backpressure API |
| `src/thread_pool.c` | 修改 | 实现 worker recovery、health monitor、backpressure |
| `src/coroutine.c` | 修改 | 添加 fault hooks、栈阻塞逻辑 |
| `src/thread_pool_internal.h` | 修改 | 新增 health/recovery 结构体定义 |
| `tests/test_fault_injection.c` | 新增 | fault injection 测试套件 |
| `tests/test_health.c` | 新增 | health check 测试 |
| `docs/risk-assessment.md` | 更新 | 标记新问题为 closed |

---

## 测试计划

### Worker Recovery
1. 手动触发 worker abort → 验证自动重建
2. 验证重建后新任务可正常执行
3. 验证 capacity 恢复

### Health Check
1. 创建 health monitor → 验证 snapshot 正确
2. 提交任务 → 验证 active_count 变化
3. 销毁 pool → 验证 monitor 线程退出

### Fault Injection
1. arm alloc failure → 验证错误路径
2. arm coro timeout → 验证超时触发
3. arm timer heap full → 验证错误传播

### Backpressure
1. 压测队列深度 → 验证回调触发
2. 压测栈内存 → 验证阻塞行为
3. 验证回调并发安全

---

## 风险控制

| 风险 | 缓解措施 |
|------|----------|
| Health monitor 线程增加延迟 | 使用 lock-free 采样，仅在读路径加锁 |
| Worker recovery 死循环 | 设置最大重建次数，超限后标记 pool 不可用 |
| Fault injection 影响生产 | 仅在 `#ifdef LOOMWORKS_TEST` 编译 |
| Backpressure 回调开销 | 使用 atomic flag 节流，避免高频回调 |

---

## 后续阶段

- **Phase 5**: Metrics 集成 health backpressure 指标
- **Phase 6**: 跨进程健康报告（SHM 导出）
