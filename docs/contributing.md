# ctpool Contributing Guide

Thank you for your interest in contributing to ctpool! This document describes coding standards and the submission process.

---

## 1. Coding Standards

### 1.1 Language Standards

- **Required:** Pure C11 (`-std=c11`), C++ features are strictly prohibited
- **Required:** C99 extension syntax is prohibited (e.g., `__func__` outside variadic macros, non-standard `inline` usage)
- **Allowed:** POSIX.1-2008 extensions (`_POSIX_C_SOURCE 200809L`)

### 1.2 Compilation Requirements

All code must compile cleanly under:

```bash
gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread <sources>
clang -std=c11 -Wall -Wextra -Werror -pedantic -pthread <sources>
```

Common warnings and fixes:

| Warning | Cause | Fix |
|---------|-------|-----|
| `unused parameter` | Function parameter not used | Use `(void)param;` to explicitly suppress |
| `implicit fallthrough` | switch case missing break | Add `break` or `__attribute__((fallthrough))` |
| `missing field initializer` | Incomplete struct initializer | Use `.field = value` designated initialization |
| `cast increases alignment` | Pointer cast may change alignment | Use `uintptr_t` intermediate cast |

### 1.3 Naming Conventions

| Type | Prefix | Example |
|------|--------|---------|
| Public functions | `ctpool_` | `ctpool_pool_create()` |
| Internal functions | `pool_` / `coro_` | `pool_init()`, `coro_entry()` |
| Type aliases | `_t` suffix | `ctpool_thread_pool_t` |
| Enum values | `CTPPOOL_` / `CTPPOOL_CORO_` | `CTPPOOL_OK`, `CTPPOOL_CORO_NEW` |
| Macro constants | `CTPPOOL_` / `CTPPOOL_CORO_` | `CTPPOOL_DEFAULT_STACK_SIZE` |

### 1.4 Error Handling

- All system calls must check their return values
- On failure, return the appropriate error code; never call `abort()` or `exit()`
- Resource leaks must be avoided: every allocation path must have a corresponding free path

```c
// ✅ Correct
if (pthread_mutex_init(&pool->lock, NULL) != 0) {
    free(pool);
    return CTPPOOL_ERR_ALLOC;
}

// ❌ Incorrect
pthread_mutex_init(&pool->lock, NULL);  // return value not checked
```

### 1.5 Memory Safety

- All `malloc`/`calloc` calls must check the return value
- All `mmap` calls must check for `MAP_FAILED`
- All `mprotect` calls must check the return value
- `destroy` functions must free all allocated resources

### 1.6 Thread Safety

- Shared data must be protected by locks or atomic operations
- `_Thread_local` variables do not need locks, but safety in signal handlers must be considered
- Do not share `ucontext_t` structures across threads

---

## 2. Testing Requirements

### 2.1 Test Coverage

Every new feature or fix must include corresponding test cases:

| Test file | Coverage |
|-----------|----------|
| `tests/test_thread_pool.c` | All thread pool APIs |
| `tests/test_coroutine.c` | All coroutine APIs |
| `tests/test_integration.c` | Thread pool + coroutine combined usage |

### 2.2 Running Tests

```bash
# CMake
cd build_cmake && ctest --output-on-failure

# Direct compilation
gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
    -I include src/thread_pool.c src/coroutine.c \
    tests/test_coroutine.c -o test_coroutine && ./test_coroutine
```

### 2.3 Test Case Requirements

- **Boundary conditions:** NULL arguments, null handles, double destroy
- **Error paths:** allocation failure, context creation failure, guard page trigger
- **Normal paths:** full lifecycle (create → resume → yield → destroy)
- **Concurrency tests:** multi-threaded submission, multi-coroutine concurrency

---

## 3. Commit Standards

### 3.1 Commit Message Format

```
<type>(<scope>): <description>

<body (optional)>

<footer (optional)>
```

**Type:**
- `feat` — new feature
- `fix` — bug fix
- `docs` — documentation change
- `style` — code style (no functional change)
- `refactor` — refactoring (no behavior change)
- `test` — test-related
- `chore` — build/toolchain change

**Example:**
```
fix(coroutine): prevent crash when destroying cross-thread coroutine

The scheduler context was global, causing SIGSEGV when coroutines
were resumed from different threads. Made g_scheduler _Thread_local.

Closes #42
```

### 3.2 PR Requirements

1. All tests must pass (`ctest`)
2. Must compile cleanly under `-Wall -Wextra -Werror -pedantic`
3. Changes must include corresponding test cases
4. Public API changes must update README.md examples

---

## 4. Code Review Checklist

Before submitting a PR, confirm:

- [ ] Compiles cleanly: `gcc -std=c11 -Wall -Wextra -Werror -pedantic -pthread`
- [ ] Tests pass: `ctest --output-on-failure`
- [ ] No memory leaks: every allocation path has a corresponding free path
- [ ] Thread-safe: shared data has appropriate lock/atomic protection
- [ ] Error handling: all system call return values are checked
- [ ] Documentation updated: public API changes reflected in docs
- [ ] Commit message follows the format specified above
