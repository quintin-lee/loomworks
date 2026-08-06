# ctpool API 参考文档

> 本文档描述 ctpool 库的所有公共 API。所有句柄均为不透明指针，头文件中不包含结构体定义。

---

## 目录

1. [线程池 API](#1-线程池-api)
2. [协程 API](#2-协程-api)
3. [结果码速查](#3-结果码速查)
4. [线程安全性](#4-线程安全性)

---

## 1. 线程池 API

### 1.1 创建与销毁

```c
ctpool_result_t ctpool_pool_create(const ctpool_pool_config_t *config,
                                   ctpool_thread_pool_t **pool);

void ctpool_pool_destroy(ctpool_thread_pool_t **pool);
```

| 参数 | 说明 |
|------|------|
| `config` | 配置结构体，传 `NULL` 使用默认值（worker_count 自动适配，queue_capacity=0 无界） |
| `pool` | 输出参数，创建成功后指向新池句柄 |

**默认配置：**
- `worker_count`：`min(hardware_concurrency * 2, 64)`
- `stack_size`：128 KiB
- `queue_capacity`：0（无界）

**使用注意：** 必须先用 `ctpool_pool_shutdown()` 等待任务排空，再调用 `ctpool_pool_destroy()`。

### 1.2 提交任务

```c
ctpool_result_t ctpool_pool_submit(ctpool_thread_pool_t *pool,
                                   ctpool_task_fn fn,
                                   void *data);

ctpool_result_t ctpool_pool_submit_future(ctpool_thread_pool_t *pool,
                                          ctpool_task_fn_result fn,
                                          void *data,
                                          ctpool_future_t **future);
```

| 参数 | 说明 |
|------|------|
| `fn` | 任务函数，签名 `void (*)(void*)` 或 `void* (*)(void*)` |
| `data` | 传给任务函数的 opaque 指针 |
| `future` | 输出参数，仅 `submit_future` 使用 |

**区别：**
- `submit`：fire-and-forget，无返回值
- `submit_future`：返回 `future` 句柄，可用 `ctpool_future_wait()` 获取结果

### 1.3 获取 Future 结果

```c
ctpool_result_t ctpool_future_wait(ctpool_future_t *future, void **result);
void            ctpool_future_destroy(ctpool_future_t *future);
```

| 参数 | 说明 |
|------|------|
| `future` | `submit_future` 返回的句柄 |
| `result` | 输出参数，指向任务函数返回的指针（由调用者负责释放） |

**注意：** `result` 指向的内存由任务函数内部 `malloc` 分配，调用者需在 `ctpool_future_destroy()` 之前释放。

### 1.4 关闭与查询

```c
void ctpool_pool_shutdown(ctpool_thread_pool_t *pool);
uint32_t ctpool_pool_worker_count(const ctpool_thread_pool_t *pool);
uint32_t ctpool_pool_pending_count(const ctpool_thread_pool_t *pool);
```

- `shutdown()`：阻塞直到所有已提交任务执行完毕，然后 join 所有 worker 线程
- `worker_count()`：返回实际创建的工作线程数（含自动计算后的值）
- `pending_count()`：返回当前队列中等待执行的任务数（可能不准确，因并发操作）

### 1.5 配置结构体

```c
typedef struct {
    uint32_t worker_count;     /* 0 = 自动，上限 64 */
    size_t   stack_size;       /* 0 = 128 KiB */
    uint32_t queue_capacity;   /* 0 = 无界，最大 1M */
} ctpool_pool_config_t;
```

---

## 2. 协程 API

### 2.1 创建与销毁

```c
ctpool_coro_result_t ctpool_coro_create(ctpool_coro_fn fn,
                                        void *data,
                                        size_t stack_size,
                                        ctpool_coroutine_t **coro);

void ctpool_coro_destroy(ctpool_coroutine_t **coro);
```

| 参数 | 说明 |
|------|------|
| `fn` | 协程入口函数，签名 `void (*)(void*)` |
| `data` | 传给入口函数的 opaque 指针 |
| `stack_size` | 栈大小（字节），0 使用默认 64 KiB |
| `coro` | 输出参数 |

**销毁注意：** 必须在协程处于 `DONE` 或 `ERROR` 状态时调用。

### 2.2 启动与恢复

```c
ctpool_coro_result_t ctpool_coro_resume(ctpool_coroutine_t *coro);
```

| 状态 | 行为 |
|------|------|
| `NEW` | 首次启动，分配并设置 ucontext，执行入口函数 |
| `SUSPENDED` | 从上次 yield 点恢复执行 |
| `DONE` / `ERROR` | 返回 `CTPPOOL_CORO_ERR_RUNNING` |

### 2.3 让步与终止

```c
void ctpool_coro_yield(void);
void ctpool_coro_suspend(void);

ctpool_coro_result_t ctpool_coro_terminate(ctpool_coroutine_t *coro);
```

- `yield()` / `suspend()`：让出控制权，返回到最近一次 `resume()` 的调用点。`suspend()` 是 `yield()` 的别名。
- `terminate()`：强制终止协程，置 `state=DONE`，恢复执行到调度器。若协程已在当前线程中运行则立即切换。

### 2.4 状态与调试

```c
ctpool_coro_state_t ctpool_coro_state(const ctpool_coroutine_t *coro);

ctpool_coro_result_t ctpool_coro_stack_info(const ctpool_coroutine_t *coro,
                                             void **start,
                                             void **end);

const char *ctpool_coro_result_str(ctpool_coro_result_t result);
```

- `state()`：返回当前状态枚举值
- `stack_info()`：获取栈地址范围（用于调试/内存检查）
- `result_str()`：将结果码转换为可读字符串

### 2.5 配置常量

```c
#define CTPPOOL_CORO_DEFAULT_STACK_SIZE    (64 * 1024)   /* 64 KiB */
#define CTPPOOL_CORO_GUARD_PAGES_EACH      1              /* 每端保护页数 */
```

---

## 3. 结果码速查

### 线程池 (`ctpool_result_t`)

| 值 | 常量 | 含义 |
|----|------|------|
| 0 | `CTPPOOL_OK` | 成功 |
| 1 | `CTPPOOL_ERR_ALLOC` | 内存分配失败 |
| 2 | `CTPPOOL_ERR_THREAD` | 线程创建失败 |
| 3 | `CTPPOOL_ERR_INVALID` | 参数无效或队列已满 |
| 4 | `CTPPOOL_ERR_SHUTDOWN` | 池已关闭 |
| 5 | `CTPPOOL_ERR_TIMEOUT` | 超时（预留） |

### 协程 (`ctpool_coro_result_t`)

| 值 | 常量 | 含义 |
|----|------|------|
| 0 | `CTPPOOL_CORO_OK` | 成功 |
| 1 | `CTPPOOL_CORO_ERR_ALLOC` | mmap 分配失败 |
| 2 | `CTPPOOL_CORO_ERR_CONTEXT` | ucontext 操作失败 |
| 3 | `CTPPOOL_CORO_ERR_MPROTECT` | mprotect 失败 |
| 4 | `CTPPOOL_CORO_ERR_INVALID` | 参数无效 |
| 5 | `CTPPOOL_CORO_ERR_GUARD` | 保护页触发（栈溢出） |
| 6 | `CTPPOOL_CORO_ERR_RUNNING` | 非法状态操作 |

### 协程状态 (`ctpool_coro_state_t`)

| 值 | 常量 | 含义 |
|----|------|------|
| 0 | `CTPPOOL_CORO_NEW` | 已创建，未启动 |
| 1 | `CTPPOOL_CORO_RUNNING` | 正在执行 |
| 2 | `CTPPOOL_CORO_SUSPENDED` | 已 yield，等待 resume |
| 3 | `CTPPOOL_CORO_DONE` | 正常完成 |
| 4 | `CTPPOOL_CORO_ERROR` | 发生错误 |

---

## 4. 线程安全性

| API | 线程安全 | 说明 |
|-----|---------|------|
| `ctpool_pool_create` | ✅ 安全 | 线程内调用 |
| `ctpool_pool_submit` | ✅ 安全 | 多并发调用，内部持锁 |
| `ctpool_pool_submit_future` | ✅ 安全 | 多并发调用，内部持锁 |
| `ctpool_pool_shutdown` | ⚠️ 仅调用一次 | 必须在所有 submit 完成后调用 |
| `ctpool_pool_destroy` | ✅ 安全（NULL 检查） | 必须在 shutdown 后调用 |
| `ctpool_future_wait` | ✅ 安全 | 内部自旋+条件变量等待 |
| `ctpool_future_destroy` | ✅ 安全 | 必须在 wait 完成后调用 |
| `ctpool_coro_create` | ✅ 安全 | 线程内调用 |
| `ctpool_coro_resume` | ✅ 安全 | 单线程内调用 |
| `ctpool_coro_yield` | ✅ 安全 | 在协程内部调用 |
| `ctpool_coro_destroy` | ✅ 安全 | 必须在协程 DONE 后调用 |
| `ctpool_coro_stack_info` | ✅ 安全 | 读操作，无需锁 |

**禁止行为：**
- 在 `shutdown()` 之后调用 `submit()` / `submit_future()`
- 跨线程调用 `resume()` / `yield()` / `terminate()` 同一个协程
- 在协程未完成（非 DONE/ERROR）时调用 `destroy()`
