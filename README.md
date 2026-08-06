# ctpool

工业级 C11 并发库，包含 **线程池** 与 **有栈协程** 两个子系统。

```
测试结果：1025 pool + 36 coroutine + 50511 integration 全部通过
编译：gcc -Wall -Wextra -Werror -pedantic -std=c11 -pthread 零警告
```

## 特性

### 线程池 (`ctpool_thread_pool_t`)

| 特性 | 说明 |
|------|------|
| 不透明指针 API | 头文件不暴露内部结构体 |
| 可配置 worker 数 | `0` 自动适配 `hardware_concurrency * 2`，上限 64 |
| 有界/无界队列 | `queue_capacity > 0` 时阻塞提交，`0` 时无限队列 |
| Future 返回值 | `ctpool_pool_submit_future()` + `ctpool_future_wait()` 获取异步结果 |
| 优雅关闭 | `ctpool_pool_shutdown()` 等待所有任务完成后再退出 |
| 缓存行对齐 | 锁与队列头尾指针 64 字节对齐，消除伪共享 |

### 协程 (`ctpool_coroutine_t`)

| 特性 | 说明 |
|------|------|
| 不透明指针 API | 头文件不暴露内部结构体 |
| mmap 栈分配 | 使用 `mmap` + `mprotect` 分配堆栈，两端各设置 `PROT_NONE` 保护页 |
| 信号处理 | SIGSEGV/SIGBUS 信号捕获栈溢出，`longjmp` 返回错误状态 |
| per-thread 调度器 | 调度上下文使用 `_Thread_local` 存储，支持跨线程安全使用 |
| 完整生命周期 | `create → resume → yield/resume → terminate → destroy` |
| 64 位安全 | `makecontext` 传参使用 `uintptr_t → unsigned long` 强转 |

## 项目结构

```
ctpool/
├── CMakeLists.txt
├── include/ctpool/
│   ├── ctpool.h            # 统一包含头文件
│   ├── thread_pool.h       # 线程池公共 API
│   └── coroutine.h         # 协程公共 API
├── src/
│   ├── thread_pool.c       # 线程池实现
│   ├── thread_pool_internal.h
│   ├── coroutine.c         # 协程实现
│   └── coroutine_internal.h
└── tests/
    ├── test_thread_pool.c   # 1025 个断言
    ├── test_coroutine.c     # 36 个断言
    └── test_integration.c   # 50511 个断言
```

## 构建与测试

### 使用 CMake

```bash
cmake -S . -B build_cmake
cmake --build build_cmake
cmake --build build_cmake --target test
# 或直接
cd build_cmake && ctest --output-on-failure
```

### 使用 GCC 直接编译

```bash
gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include \
    src/thread_pool.c src/coroutine.c \
    tests/test_thread_pool.c -o test_thread_pool && ./test_thread_pool

gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include \
    src/thread_pool.c src/coroutine.c \
    tests/test_coroutine.c -o test_coroutine && ./test_coroutine

gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include \
    src/thread_pool.c src/coroutine.c \
    tests/test_integration.c -o test_integration && ./test_integration
```

## 快速使用

### 线程池

```c
#include "ctpool/thread_pool.h"

// 创建线程池（默认参数：worker_count=0, queue_capacity=0）
ctpool_thread_pool_t *pool = NULL;
ctpool_pool_create(NULL, &pool);

// 提交任务
int sum = 0;
ctpool_pool_submit(pool, ^(void *arg) {
    int *s = arg;
    __sync_fetch_and_add(s, 1);
}, &sum);

// 提交带返回值的任务
ctpool_future_t *fut = NULL;
ctpool_pool_submit_future(pool, result_fn, NULL, &fut);
void *result = NULL;
ctpool_future_wait(fut, &result);

// 关闭并销毁
ctpool_pool_shutdown(pool);
ctpool_pool_destroy(&pool);
```

### 协程

```c
#include "ctpool/coroutine.h"

// 创建协程
ctpool_coroutine_t *coro = NULL;
ctpool_coro_create(my_coro_fn, user_data, 0, &coro);  // 0 = 默认 64 KiB 栈

// 启动或恢复
ctpool_coro_result_t rc = ctpool_coro_resume(coro);
if (rc != CTPPOOL_CORO_OK) { /* 处理错误 */ }

// 协程内部让步
void my_coro_fn(void *arg) {
    // ... 执行 ...
    ctpool_coro_yield();   // 让出控制权，下次 resume 从这里继续
    // ... 执行 ...
}

// 强制终止
ctpool_coro_terminate(coro);

// 销毁
ctpool_coro_destroy(&coro);
```

## API 参考

### 线程池结果码

```c
typedef enum {
    CTPPOOL_OK,          // 成功
    CTPPOOL_ERR_ALLOC,   // 内存分配失败
    CTPPOOL_ERR_THREAD,  // 线程创建失败
    CTPPOOL_ERR_INVALID, // 参数无效或队列满
    CTPPOOL_ERR_SHUTDOWN,// 池已关闭
    CTPPOOL_ERR_TIMEOUT, // 超时（预留）
} ctpool_result_t;
```

### 协程状态

```c
typedef enum {
    CTPPOOL_CORO_NEW,       // 已创建，未启动
    CTPPOOL_CORO_RUNNING,   // 正在执行
    CTPPOOL_CORO_SUSPENDED, // 已 yield
    CTPPOOL_CORO_DONE,      // 执行完毕
    CTPPOOL_CORO_ERROR,     // 错误（如栈溢出）
} ctpool_coro_state_t;
```

## 设计约束

| 要求 | 实现方式 |
|------|----------|
| 纯 C11 | 仅使用 `stdatomic.h`、`_Thread_local`、`_Alignas` |
| 内存安全 | `mmap` + `mprotect` PROT_NONE 保护页，`NULL` 检查 |
| 防伪共享 | 锁/队列使用 `__attribute__((aligned(64)))` 分离 |
| 系统调用健壮 | 所有 `pthread_*`/`malloc`/`mmap`/`mprotect` 检查返回值 |
| 64 位兼容 | `makecontext` 参数强转为 `unsigned long` |

## 注意

- **不支持动态库（`.so`）**：协程使用 `_Thread_local` 存储，链接为共享库时会产生 TPOFF relocation 错误。仅支持静态链接（`libctpool.a`）。
