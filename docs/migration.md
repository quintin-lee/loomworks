# Migration Guide: ctpool → loomworks

This guide documents the API changes when migrating from `ctpool` to `loomworks`.

---

## Overview

The loomworks project is a direct rename of ctpool. All functionality, behavior, and performance characteristics remain identical. Only naming has changed.

| Category | ctpool | loomworks |
|----------|--------|-----------|
| Project name | `ctpool` | `loomworks` |
| Include directory | `include/ctpool/` | `include/loomworks/` |
| Main header | `ctpool.h` | `loomworks.h` |
| Library name | `libctpool.a` | `libloomworks.a` |
| CMake target | `ctpool_static` | `loomworks_static` |
| Function prefix | `ctpool_` | `loom_` |
| Enum prefix | `CTPPOOL_` | `LOOMWORKS_` |

---

## Header Includes

**Before (ctpool):**
```c
#include "ctpool/ctpool.h"       // or individual headers
#include "ctpool/thread_pool.h"
#include "ctpool/coroutine.h"
```

**After (loomworks):**
```c
#include "loomworks/loomworks.h"    // or individual headers
#include "loomworks/thread_pool.h"
#include "loomworks/coroutine.h"
```

---

## API Renames

### Thread Pool

| ctpool | loomworks |
|--------|-----------|
| `ctpool_thread_pool_t` | `loom_thread_pool_t` |
| `ctpool_future_t` | `loom_future_t` |
| `ctpool_task_fn` | `loom_task_fn` |
| `ctpool_task_fn_result` | `loom_task_fn_result` |
| `ctpool_pool_config_t` | `loom_pool_config_t` |
| `ctpool_result_t` | `loom_result_t` |
| `ctpool_pool_create()` | `loom_pool_create()` |
| `ctpool_pool_submit()` | `loom_pool_submit()` |
| `ctpool_pool_submit_future()` | `loom_pool_submit_future()` |
| `ctpool_future_wait()` | `loom_future_wait()` |
| `ctpool_future_destroy()` | `loom_future_destroy()` |
| `ctpool_pool_shutdown()` | `loom_pool_shutdown()` |
| `ctpool_pool_destroy()` | `loom_pool_destroy()` |
| `ctpool_pool_worker_count()` | `loom_pool_worker_count()` |
| `ctpool_pool_pending_count()` | `loom_pool_pending_count()` |

### Coroutines

| ctpool | loomworks |
|--------|-----------|
| `ctpool_coroutine_t` | `loom_coroutine_t` |
| `ctpool_coro_state_t` | `loom_coro_state_t` |
| `ctpool_coro_result_t` | `loom_coro_result_t` |
| `ctpool_coro_fn` | `loom_coro_fn` |
| `ctpool_coro_create()` | `loom_coro_create()` |
| `ctpool_coro_resume()` | `loom_coro_resume()` |
| `ctpool_coro_yield()` | `loom_coro_yield()` |
| `ctpool_coro_suspend()` | `loom_coro_suspend()` |
| `ctpool_coro_terminate()` | `loom_coro_terminate()` |
| `ctpool_coro_destroy()` | `loom_coro_destroy()` |
| `ctpool_coro_state()` | `loom_coro_state()` |
| `ctpool_coro_stack_info()` | `loom_coro_stack_info()` |
| `ctpool_coro_result_str()` | `loom_coro_result_str()` |

---

## Enum and Macro Renames

### Result Codes

| ctpool | loomworks |
|--------|-----------|
| `CTPPOOL_OK` | `LOOMWORKS_OK` |
| `CTPPOOL_ERR_ALLOC` | `LOOMWORKS_ERR_ALLOC` |
| `CTPPOOL_ERR_THREAD` | `LOOMWORKS_ERR_THREAD` |
| `CTPPOOL_ERR_INVALID` | `LOOMWORKS_ERR_INVALID` |
| `CTPPOOL_ERR_SHUTDOWN` | `LOOMWORKS_ERR_SHUTDOWN` |
| `CTPPOOL_ERR_TIMEOUT` | `LOOMWORKS_ERR_TIMEOUT` |

### Coroutine States

| ctpool | loomworks |
|--------|-----------|
| `CTPPOOL_CORO_NEW` | `LOOMWORKS_CORO_NEW` |
| `CTPPOOL_CORO_RUNNING` | `LOOMWORKS_CORO_RUNNING` |
| `CTPPOOL_CORO_SUSPENDED` | `LOOMWORKS_CORO_SUSPENDED` |
| `CTPPOOL_CORO_DONE` | `LOOMWORKS_CORO_DONE` |
| `CTPPOOL_CORO_ERROR` | `LOOMWORKS_CORO_ERROR` |

### Coroutine Result Codes

| ctpool | loomworks |
|--------|-----------|
| `CTPPOOL_CORO_OK` | `LOOMWORKS_CORO_OK` |
| `CTPPOOL_CORO_ERR_ALLOC` | `LOOMWORKS_CORO_ERR_ALLOC` |
| `CTPPOOL_CORO_ERR_CONTEXT` | `LOOMWORKS_CORO_ERR_CONTEXT` |
| `CTPPOOL_CORO_ERR_MPROTECT` | `LOOMWORKS_CORO_ERR_MPROTECT` |
| `CTPPOOL_CORO_ERR_INVALID` | `LOOMWORKS_CORO_ERR_INVALID` |
| `CTPPOOL_CORO_ERR_GUARD` | `LOOMWORKS_CORO_ERR_GUARD` |
| `CTPPOOL_CORO_ERR_RUNNING` | `LOOMWORKS_CORO_ERR_RUNNING` |

### Constants

| ctpool | loomworks |
|--------|-----------|
| `CTPPOOL_DEFAULT_STACK_SIZE` | `LOOMWORKS_DEFAULT_STACK_SIZE` |
| `CTPPOOL_DEFAULT_WORKER_COUNT` | `LOOMWORKS_DEFAULT_WORKER_COUNT` |
| `CTPPOOL_CORO_DEFAULT_STACK_SIZE` | `LOOMWORKS_CORO_DEFAULT_STACK_SIZE` |
| `CTPPOOL_CORO_GUARD_PAGES_EACH` | `LOOMWORKS_CORO_GUARD_PAGES_EACH` |

---

## CMake Migration

**Before (ctpool):**
```cmake
cmake_minimum_required(VERSION 3.16)
project(ctpool VERSION 1.0.0 LANGUAGES C)

add_library(ctpool_static STATIC src/thread_pool.c src/coroutine.c)
target_include_directories(ctpool_static PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(ctpool_static PUBLIC Threads::Threads)
set_target_properties(ctpool_static PROPERTIES OUTPUT_NAME ctpool)
```

**After (loomworks):**
```cmake
cmake_minimum_required(VERSION 3.16)
project(loomworks VERSION 1.0.0 LANGUAGES C)

add_library(loomworks_static STATIC src/thread_pool.c src/coroutine.c)
target_include_directories(loomworks_static PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(loomworks_static PUBLIC Threads::Threads)
set_target_properties(loomworks_static PROPERTIES OUTPUT_NAME loomworks)
```

---

## Automated Migration

For large codebases, use the following `sed` commands to perform an automated rename:

```bash
# In all source files:
find . -name "*.c" -o -name "*.h" | xargs sed -i \
  -e 's/ctpool_/loom_/g' \
  -e 's/CTPPOOL_/LOOMWORKS_/g' \
  -e 's/"ctpool\//\"loomworks\//g' \
  -e 's/ctpool_thread_pool_t/loom_thread_pool_t/g' \
  -e 's/ctpool_future_t/loom_future_t/g' \
  -e 's/ctpool_coroutine_t/loom_coroutine_t/g' \
  -e 's/ctpool_pool_config_t/loom_pool_config_t/g'

# Update include paths in CMakeLists.txt:
sed -i 's/ctpool/loomworks/g' CMakeLists.txt
```

**Note:** The automated migration assumes no custom identifiers contain `ctpool` outside of library API names. Review the diff carefully after applying.
