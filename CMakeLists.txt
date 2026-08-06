cmake_minimum_required(VERSION 3.16)
project(loomworks VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# clang-tidy: run static analysis as part of every compilation
# ---------------------------------------------------------------------------
option(ENABLE_CLANG_TIDY "Run clang-tidy on every compile" ON)
if(ENABLE_CLANG_TIDY)
  find_program(CLANG_TIDY_BIN clang-tidy)
  if(CLANG_TIDY_BIN)
    message(STATUS "clang-tidy found: ${CLANG_TIDY_BIN}")
    set(CMAKE_C_CLANG_TIDY ${CLANG_TIDY_BIN})
  else()
    message(WARNING "clang-tidy not found; static analysis skipped")
  endif()
endif()

# ---------------------------------------------------------------------------
# clang-format: check formatting before build (lint target)
# ---------------------------------------------------------------------------
option(ENABLE_CLANG_FORMAT "Run clang-format check on build" ON)
if(ENABLE_CLANG_FORMAT)
  find_program(CLANG_FORMAT_BIN clang-format)
  if(CLANG_FORMAT_BIN)
    message(STATUS "clang-format found: ${CLANG_FORMAT_BIN}")

    # Collect all C source and header files
    file(GLOB_RECURSE ALL_C_FILES
      "include/**/*.h"
      "src/**/*.c"
      "src/**/*.h"
      "tests/**/*.c"
    )

  else()
    message(WARNING "clang-format not found; formatting check skipped")
  endif()
endif()

# ---------------------------------------------------------------------------
# Library targets
# ---------------------------------------------------------------------------
add_library(loomworks_static STATIC
    src/thread_pool.c
    src/coroutine.c
    src/task_group.c
    src/metrics.c
)
target_include_directories(loomworks_static PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(loomworks_static PUBLIC Threads::Threads)
set_target_properties(loomworks_static PROPERTIES OUTPUT_NAME loomworks)

add_library(loomworks_shared SHARED
    src/thread_pool.c
    src/coroutine.c
    src/task_group.c
    src/metrics.c
)
target_include_directories(loomworks_shared PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(loomworks_shared PUBLIC Threads::Threads)
set_target_properties(loomworks_shared PROPERTIES
    OUTPUT_NAME loomworks
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
enable_testing()

add_executable(test_thread_pool tests/test_thread_pool.c)
target_link_libraries(test_thread_pool PRIVATE loomworks_static)
add_test(NAME ThreadPoolTests COMMAND test_thread_pool)

add_executable(test_coroutine tests/test_coroutine.c)
target_link_libraries(test_coroutine PRIVATE loomworks_static)
add_test(NAME CoroutineTests COMMAND test_coroutine)

add_executable(test_integration tests/test_integration.c)
target_link_libraries(test_integration PRIVATE loomworks_static)
add_test(NAME IntegrationTests COMMAND test_integration)

add_subdirectory(examples)

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# clang-format: check formatting before build (lint target)
# ---------------------------------------------------------------------------
if(ENABLE_CLANG_FORMAT)
  file(GLOB_RECURSE ALL_C_FILES
    "include/**/*.h"
    "src/**/*.c"
    "src/**/*.h"
    "tests/**/*.c"
  )

  add_custom_target(format-check
    COMMAND ${CLANG_FORMAT_BIN} --dry-run --Werror ${ALL_C_FILES}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Checking code formatting with clang-format ..."
    VERBATIM
  )

  # Run format check before building
  add_dependencies(loomworks_static format-check)
  add_dependencies(loomworks_shared format-check)
  add_dependencies(test_thread_pool format-check)
  add_dependencies(test_coroutine format-check)
  add_dependencies(test_integration format-check)
endif()

include(GNUInstallDirs)
install(TARGETS loomworks_static loomworks_shared
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/loomworks
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
