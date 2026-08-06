# loomworks FAQ

Frequently asked questions about the loomworks library.

---

## Thread Pool

### Q: Can I call `loom_pool_submit()` from multiple threads concurrently?

**A:** Yes. The thread pool uses a single mutex to protect the task queue, making `loom_pool_submit()` and `loom_pool_submit_future()` safe for concurrent calls from any number of threads.

### Q: Does `loom_pool_submit()` block when the queue is full?

**A:** No. When `queue_capacity > 0` and the queue has reached capacity, `loom_pool_submit()` returns `LOOMWORKS_ERR_INVALID` immediately. There is no blocking behavior. If you need back-pressure, consider using a semaphore or condition variable in your application layer.

### Q: What happens to pending tasks when I call `loom_pool_shutdown()`?

**A:** `loom_pool_shutdown()` enters a draining phase: it sets the `shutdown` flag, wakes all idle workers via `pthread_cond_broadcast`, and then joins all worker threads. Each worker processes any remaining tasks in the queue before exiting. After shutdown, no new tasks may be submitted.

### Q: Can I reuse a pool after calling `loom_pool_shutdown()`?

**A:** No. Once `loom_pool_shutdown()` is called, the pool is permanently shut down. Create a new pool for each use case. `loom_pool_destroy()` must be called after shutdown to free resources.

### Q: What is the minimum and maximum number of worker threads?

**A:** 
- Minimum: 1 (even if `hardware_concurrency` reports 0)
- Maximum: 64 (hard limit even if `hardware_concurrency * 2` is larger)
- Default (when `worker_count = 0`): `min(hardware_concurrency * 2, 64)`

### Q: How do I properly free the result returned by a future task?

**A:** The result pointer is allocated by your task function (typically via `malloc`). You are responsible for freeing it after retrieving it with `loom_future_wait()`. Call `loom_future_destroy(future)` after you are done with the result.

```c
void *result = NULL;
loom_result_t rc = loom_future_wait(fut, &result);
if (rc == LOOMWORKS_OK) {
    printf("result = %d\n", *(int *)result);
    free(result);   /* your responsibility */
}
loom_future_destroy(fut);
```

---

## Coroutines

### Q: Can I resume a coroutine from a different thread?

**A:** No. Coroutines are bound to the thread that created them. The scheduler context (`g_scheduler`) is `_Thread_local`, and `swapcontext()` across threads is undefined behavior. If you need to run a coroutine in a thread pool worker, create and destroy the entire coroutine within that same worker thread.

### Q: What happens when a coroutine's stack overflows?

**A:** The library installs SIGSEGV/SIGBUS handlers on first use. When a guard page is accessed:
1. The handler saves the coroutine's state as `LOOMWORKS_CORO_ERROR`
2. It performs a `longjmp` back to the `loom_coro_resume()` call
3. The function returns `LOOMWORKS_CORO_ERR_GUARD`

The process does not crash. You should call `loom_coro_destroy()` immediately after a guard-page error.

### Q: Why does the library use `ucontext` instead of `setjmp`/`longjmp`?

**A:** `setjmp`/`longjmp` only saves/restores the stack pointer and frame pointer — not the full register state. This means you cannot "resume" from the middle of a function. `swapcontext()` from the `ucontext` API saves and restores the complete CPU context, making it suitable for cooperative multitasking with `yield()`.

### Q: How much memory does each coroutine use?

**A:** 
- `loom_coroutine_t` structure: ~160 bytes
- Stack region (mmap): `requested_size + 2 * pagesize * 2` guard pages (typically 64 KiB + 32 KiB)
- Per-thread scheduler stack: 128 KiB (allocated once per thread, shared by all coroutines on that thread)

### Q: Can I nest coroutines (a coroutine that creates and resumes another)?

**A:** Yes. Each thread maintains its own scheduler context, so coroutines can create and manage child coroutines as long as the entire lifecycle stays within the same thread.

### Q: What is the difference between `loom_coro_yield()` and `loom_coro_suspend()`?

**A:** There is no functional difference. `loom_coro_suspend()` is provided as a semantic alias for `loom_coro_yield()` to improve code readability. Both transition the coroutine from `RUNNING` to `SUSPENDED` and switch back to the scheduler.

---

## General

### Q: Is loomworks portable to Windows?

**A:** No. The library depends on POSIX APIs (`ucontext`, `mmap`, `pthread`, `sigaction`). Windows has different concurrency primitives. Porting would require rewriting the coroutine subsystem and thread pool.

### Q: Is the library thread-safe for concurrent use of different pools/coroutines?

**A:** Yes. Multiple independent pools and coroutines can be used concurrently from different threads. The only restriction is that a single coroutine must be created, resumed, and destroyed on the same thread.

### Q: Can I use loomworks with C++?

**A:** Yes. The headers use `extern "C"` guards, so the library can be linked from C++ code without name mangling issues.

### Q: How do I check for memory leaks?

**A:** Compile with AddressSanitizer and run:

```bash
cmake -DCMAKE_C_FLAGS="-fsanitize=address -fno-omit-frame-pointer" -S . -B build_asan
cmake --build build_asan
cd build_asan && ctest --output-on-failure
```

Or run a single test:

```bash
LD_PRELOAD=/usr/lib/libasan.so.8 ./test_integration
```

### Q: Why does the README mention 50,511 integration tests?

**A:** The integration test suite runs a stress test with 10,000 threads each submitting 5 tasks and creating 5 coroutines, yielding 50,000 task submissions and 50,000 coroutine operations. Additional assertions bring the total to 50,511. This validates concurrent correctness under heavy load.
