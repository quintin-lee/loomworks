# ctpool 设计决策记录

本文档记录关键设计选择及其原因，供后续维护和扩展参考。

---

## 1. 为什么选择 ucontext 而非 makecontext/jmp_buf？

**决策：** 使用 POSIX `ucontext` 族（`getcontext`/`setcontext`/`swapcontext`/`makecontext`）实现协程上下文保存与恢复。

**原因：**
- `ucontext` 是 POSIX.1-2001 标准 API，Linux/glibc 原生支持，无需平台特定代码
- `swapcontext` 比 `setjmp`/`longjmp` 更合适：它同时保存和恢复上下文，天然适合协程切换
- `makecontext` 支持传递参数（1-5 个 `unsigned long`），满足 64 位指针安全传递需求

**替代方案对比：**

| 方案 | 优点 | 缺点 |
|------|------|------|
| `ucontext` (采用) | POSIX 标准，跨平台 | glibc 2.16+ 标注为废弃（但 Linux 仍支持） |
| `setjmp`/`longjmp` | 更轻量 | 无法保存寄存器上下文，不支持"从中间恢复" |
| `makecontext` + `swapcontext` (采用) | 标准 API，完整上下文 | 同上 |
| 手写汇编 | 性能最优 | 平台绑定，维护成本高 |

**结论：** `ucontext` 是最合适折中方案，在 Linux/x86_64 上稳定工作，API 简洁。

---

## 2. 为什么使用 mmap + PROT_NONE 而非 malloc 栈？

**决策：** 协程栈通过 `mmap(PROT_NONE)` 分配，再用 `mprotect` 激活可用区域。

**原因：**
- **栈溢出检测**：`PROT_NONE` 保护页触发 SIGSEGV/SIGBUS，信号处理器可捕获并返回错误码，而非直接崩溃
- **内存可控**：不依赖 `pthread_attr_setstacksize()`，所有栈空间由库完全控制
- **地址空间清晰**：`mmap` 返回的地址可被 `stack_info()` 查询，便于调试

**栈布局设计：**
```
[PROT_NONE] ← 底部保护页（防止栈向下溢出）
[PROT_NONE] ← 第二保护页
[PROT_RW]   ← 可用栈空间
[PROT_NONE] ← 顶部保护页（防止栈向上溢出）
```

**替代方案对比：**

| 方案 | 优点 | 缺点 |
|------|------|------|
| `mmap` + PROT_NONE (采用) | 精确控制，保护页检测 | 需要信号处理，复杂度较高 |
| `pthread_attr_setstacksize` | 简单 | 无保护页，溢出直接崩溃 |
| `malloc` 大缓冲区 | 简单 | 无溢出检测，浪费内存 |
| `sigaltstack` | POSIX 标准 | 只适用于信号处理，不用于协程栈 |

---

## 3. 为什么协程调度器使用 per-thread 而非全局？

**决策：** `g_scheduler`（调度器上下文）和 `g_guard_jmp` 声明为 `_Thread_local`。

**原因：**
- `ucontext_t` 包含线程栈指针和寄存器状态，跨线程使用会导致栈错乱（SIGSEGV）
- 测试中发现：若 `g_scheduler` 为全局变量，在线程池 worker 中调用协程 API 时，`swapcontext` 使用主线程的调度器上下文，导致 glibc malloc 断言失败（`chunk_is_mmapped` 冲突）
- `_Thread_local` 确保每个线程拥有独立的调度器，协程可在线程池 worker 中安全使用

**性能影响：** 每个线程首次调用协程 API 时分配 128KB 调度器栈（`malloc`），之后复用。线程销毁时由操作系统回收，不显式释放（符合常规做法）。

---

## 4. 为什么线程池使用单锁而非分片锁？

**决策：** 整个队列由一个 `pthread_mutex_t` 保护。

**原因：**
- 队列操作本身是 O(1) 的链表插入/删除，锁竞争时间短
- 多锁/分片锁增加代码复杂度和 bug 风险，且需要额外的路由逻辑
- 对于大多数应用场景（任务粒度较大），单锁瓶颈不明显
- 缓存行对齐确保锁与其他热字段不共享 cache line，减少 false sharing

**何时考虑分片锁：** 若任务粒度极小（纳秒级）且并发度极高（数千 worker），可考虑将队列分片为 N 个独立锁。当前设计不适用此场景。

---

## 5. 为什么 `destroy` 不接受 NULL 指针参数？

**决策：** `ctpool_coro_destroy(ctpool_coroutine_t **coro)` 通过检查 `!*coro` 处理 NULL 指针，但不接受 `0xDEAD` 等垃圾指针。

**原因：**
- 接受 `NULL` 指针（`coro` 本身为 NULL）是常见 C API 惯例，简化调用者代码
- 接受任意地址（如 `0xDEAD`）会导致解引用无效内存，引发 SIGSEGV
- 当前实现：`if (!coro || !*coro) return;` 同时处理两种 NULL 情况

**设计权衡：**
- 严格模式：增加指针有效性检查（如检查地址是否在 mmap 范围内），但增加开销且无法检测所有无效指针
- 宽松模式：依赖调用者保证传入有效指针（当前选择）

---

## 6. 为什么不提供动态库（.so）支持？

**决策：** 库仅支持静态链接（`.a`），不支持动态库（`.so`）。

**原因：**
- 协程使用 `_Thread_local` 变量（`g_scheduler`, `g_current`, `g_guard_jmp`），链接为共享库时产生 `R_X86_64_TPOFF32` relocation，glibc 不支持在共享库中使用 TLS 的 `local-exec` 模型
- 编译时错误：`relocation R_X86_64_TPOFF32 against 'g_current' can not be used when making a shared object`
- 解决方案：使用 `-fPIC -ftls-model=initial-exec` 可支持共享库，但增加复杂性且性能略有下降

**当前限制：** 库仅通过静态链接使用（`libctpool.a`）。

---

## 7. 为什么保护页使用 `2 * GUARD_PAGES_EACH` 而非 1？

**决策：** 每端使用 2 个 PROT_NONE 保护页（`CTPPOOL_CORO_GUARD_PAGES_EACH * 2`）。

**原因：**
- 第一个保护页：作为可用栈的边界，与栈数据页相邻
- 第二个保护页：作为"缓冲"，确保即使栈溢出越过第一个保护页，也不会立即访问到 mmap 区域的其他内容
- 2 个保护页提供更大的安全裕度，降低误触发概率

**信号处理器逻辑：** 只有访问到 `base`（第一个页）或 `end-ps`（最后一个页）时才触发 longjmp，中间的 PROT_NONE 页如果也被访问，会触发默认信号处理器（崩溃），这确保了真正的越界访问不会被静默忽略。

---

## 8. 为什么 makecontext 使用 `uintptr_t → unsigned long` 强转？

**决策：** `makecontext(&ctx, entry, 1, (unsigned long)(uintptr_t)coro)`。

**原因：**
- POSIX `makecontext` 要求参数类型为 `unsigned long`，且最多 5 个参数
- 在 x86_64 Linux 上，`unsigned long` 是 64 位，可完整存储指针
- 使用 `(uintptr_t)` 中间转换确保指针到整数的语义正确，而非直接截断
- 此方案在 ILP32（32 位）和 LP64（64 位）模型下均正确工作

**64 位安全验证：**
```c
// 正确：uintptr_t → unsigned long（保证无损转换）
makecontext(&ctx, entry, 1, (unsigned long)(uintptr_t)coro);

// 错误：直接截断 64 位指针为 32 位
makecontext(&ctx, entry, 1, (unsigned long)coro);  // 在 ILP32 上可能截断
```

---

## 9. 为什么不使用 C11 `_Generic` 统一 API？

**决策：** 线程池和协程保持独立的 API 命名空间（`ctpool_pool_*` vs `ctpool_coro_*`）。

**原因：**
- `_Generic` 选择器在 C11 中实现复杂度较高，且 IDE 补全效果差
- 独立命名空间更清晰，易于理解和维护
- 两个子模块功能差异大，统一 API 意义不大

---

## 10. 缓存行对齐策略

**决策：** 所有锁、队列指针使用 `__attribute__((aligned(64)))` 或 padding 字段分离。

**原因：**
- 现代 CPU cache line 大小为 64 字节（x86_64）
- 多线程并发访问同一 cache line 中的不同字段会导致 false sharing，性能下降 10-100 倍
- 队列头尾指针与锁分离到不同 cache line，确保 worker  dequeue 操作不与主线程 lock 操作竞争

**布局验证：**
```
struct ctpool_thread_pool {
    pthread_mutex_t lock              ← cache line 0 (64B)
    pthread_cond_t  cond              ← cache line 1 (64B)
    pthread_cond_t  drain_cond        ← cache line 2 (64B)
    bool            shutdown          ← 与 drain_cond 共享 cache line 2
    bool            draining
    ...
    ctpool_task_t  *queue_head        ← cache line 3 (8B + padding)
    ctpool_task_t  *queue_tail        ← 与 queue_head 同 cache line
    uint32_t        queue_len
};
```
