# ctpool 贡献指南

感谢你对 ctpool 的贡献！本文档说明代码规范和提交流程。

---

## 1. 编码规范

### 1.1 语言标准

- **强制：** 纯 C11（`-std=c11`），禁止使用 C++ 特性
- **强制：** 禁止 C99 扩展语法（如 variadic macros 之外的 `__func__`、`inline` 用法等）
- **允许：** POSIX.1-2008 扩展（`_POSIX_C_SOURCE 200809L`）

### 1.2 编译要求

所有代码必须通过以下编译选项：

```bash
gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread <sources>
clang -std=c11 -Wall -Wextra -Werror -pedantic -pthread <sources>
```

常见编译错误及修复：

| 警告 | 原因 | 修复 |
|------|------|------|
| `unused parameter` | 函数参数未使用 | 使用 `(void)param;` 显式忽略 |
| `implicit fallthrough` | switch case 无 break | 添加 `break` 或 `__attribute__((fallthrough))` |
| `missing field initializer` | 结构体初始化不完整 | 使用 `.field = value` 命名初始化 |
| `cast increases alignment` | 指针转换可能改变对齐 | 使用 `uintptr_t` 中间转换 |

### 1.3 命名规范

| 类型 | 前缀 | 示例 |
|------|------|------|
| 公共函数 | `ctpool_` | `ctpool_pool_create()` |
| 内部函数 | `pool_` / `coro_` | `pool_init()`, `coro_entry()` |
| 类型别名 | `_t` 后缀 | `ctpool_thread_pool_t` |
| 枚举值 | `CTPPOOL_` / `CTPPOOL_CORO_` | `CTPPOOL_OK`, `CTPPOOL_CORO_NEW` |
| 宏常量 | `CTPPOOL_` / `CTPPOOL_CORO_` | `CTPPOOL_DEFAULT_STACK_SIZE` |

### 1.4 错误处理

- 所有系统调用必须检查返回值
- 失败时返回相应的错误码，不 abort / exit
- 资源泄漏必须避免：每个分配路径都有对应的释放路径

```c
// ✅ 正确
if (pthread_mutex_init(&pool->lock, NULL) != 0) {
    free(pool);
    return CTPPOOL_ERR_ALLOC;
}

// ❌ 错误
pthread_mutex_init(&pool->lock, NULL);  // 未检查返回值
```

### 1.5 内存安全

- 所有 `malloc`/`calloc` 必须检查返回值
- 所有 `mmap` 必须检查 `MAP_FAILED`
- 所有 `mprotect` 必须检查返回值
- `destroy` 函数必须释放所有分配的资源

### 1.6 线程安全

- 共享数据必须通过锁或原子操作保护
- `_Thread_local` 变量不需要锁，但需注意信号处理器中的安全性
- 不要跨线程共享 `ucontext_t` 结构

---

## 2. 测试要求

### 2.1 测试覆盖

每个新功能或修复必须包含对应的测试用例：

| 测试文件 | 覆盖范围 |
|----------|----------|
| `tests/test_thread_pool.c` | 线程池所有 API |
| `tests/test_coroutine.c` | 协程所有 API |
| `tests/test_integration.c` | 线程池 + 协程组合使用 |

### 2.2 运行测试

```bash
# CMake 方式
cd build_cmake && ctest --output-on-failure

# 直接编译方式
gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include src/thread_pool.c src/coroutine.c \
    tests/test_coroutine.c -o test_coroutine && ./test_coroutine
```

### 2.3 测试用例要求

- **边界条件：** NULL 参数、空句柄、重复销毁
- **错误路径：** 分配失败、上下文创建失败、保护页触发
- **正常路径：** 完整生命周期（create → resume → yield → destroy）
- **并发测试：** 多线程提交、多协程并发

---

## 3. 提交规范

### 3.1 Commit 消息格式

```
<type>(<scope>): <description>

<body (optional)>

<footer (optional)>
```

**type 类型：**
- `feat`：新功能
- `fix`：bug 修复
- `docs`：文档变更
- `style`：代码格式（不影响功能）
- `refactor`：重构（不改变行为）
- `test`：测试相关
- `chore`：构建/工具链变更

**示例：**
```
fix(coroutine): prevent crash when destroying cross-thread coroutine

The scheduler context was global, causing SIGSEGV when coroutines
were resumed from different threads. Made g_scheduler _Thread_local.

Closes #42
```

### 3.2 PR 要求

1. 所有测试必须通过（`ctest`）
2. 必须通过 `-Wall -Wextra -Werror -pedantic` 编译
3. 变更需包含对应测试用例
4. 公共 API 变更需在 README.md 中更新示例

---

## 4. 代码审查检查清单

提交 PR 前请确认：

- [ ] 编译通过：`gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread`
- [ ] 测试通过：`ctest --output-on-failure`
- [ ] 无内存泄漏：所有分配路径有对应的释放路径
- [ ] 线程安全：共享数据有适当的锁/原子保护
- [ ] 错误处理：所有系统调用返回值已检查
- [ ] 文档更新：公共 API 变更已更新文档
- [ ] Commit 消息符合规范
