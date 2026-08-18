cmake_minimum_required(VERSION 3.16)
project(loomworks VERSION 1.0.1 LANGUAGES C ASM)

include(cmake/BuildTypes.cmake)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# Context backend selection: hand-written assembly by default on x86-64 and
# aarch64, POSIX ucontext elsewhere, or forced via LOOMWORKS_CTX_BACKEND.
# ---------------------------------------------------------------------------
set(LOOMWORKS_CTX_BACKEND "asm" CACHE STRING "Context backend: asm|ucontext")
set_property(CACHE LOOMWORKS_CTX_BACKEND PROPERTY STRINGS asm ucontext)

set(LOOMWORKS_SOURCES src/thread_pool.c src/coroutine.c src/task_group.c src/metrics.c src/pipeline.c)
set(LOOMWORKS_CTX_DEFINES "")
if(LOOMWORKS_CTX_BACKEND STREQUAL "asm" AND CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    list(APPEND LOOMWORKS_SOURCES src/ctx_x86_64.S)
    list(APPEND LOOMWORKS_CTX_DEFINES LOOMWORKS_CTX_ASM_X86_64=1)
elseif(LOOMWORKS_CTX_BACKEND STREQUAL "asm" AND CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    list(APPEND LOOMWORKS_SOURCES src/ctx_aarch64.S)
    list(APPEND LOOMWORKS_CTX_DEFINES LOOMWORKS_CTX_ASM_AARCH64=1)
else()
    message(STATUS "Context backend: ucontext (${LOOMWORKS_CTX_BACKEND} on ${CMAKE_SYSTEM_PROCESSOR})")
endif()

# ---------------------------------------------------------------------------
# Portability: force the non-GNU pthread_tryjoin fallback path on any platform
# (simulates macOS/BSD limited pthreads for CI coverage of the shim).
# ---------------------------------------------------------------------------
option(LOOMWORKS_POSIX_FALLBACK "Force the portable pthread_tryjoin fallback (simulates non-GNU platforms)" OFF)
if(LOOMWORKS_POSIX_FALLBACK)
    list(APPEND LOOMWORKS_CTX_DEFINES LOOMWORKS_POSIX_FALLBACK=1)
endif()

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
add_library(loomworks_static STATIC ${LOOMWORKS_SOURCES})
target_compile_definitions(loomworks_static PRIVATE ${LOOMWORKS_CTX_DEFINES})
target_include_directories(loomworks_static PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(loomworks_static PUBLIC Threads::Threads)
set_target_properties(loomworks_static PROPERTIES OUTPUT_NAME loomworks)

add_library(loomworks_shared SHARED ${LOOMWORKS_SOURCES})
target_compile_definitions(loomworks_shared PRIVATE ${LOOMWORKS_CTX_DEFINES})
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
set_tests_properties(ThreadPoolTests PROPERTIES TIMEOUT 600)

add_executable(test_coroutine tests/test_coroutine.c)
target_link_libraries(test_coroutine PRIVATE loomworks_static)
add_test(NAME CoroutineTests COMMAND test_coroutine)
set_tests_properties(CoroutineTests PROPERTIES TIMEOUT 600)

add_executable(test_integration tests/test_integration.c)
target_link_libraries(test_integration PRIVATE loomworks_static)
add_test(NAME IntegrationTests COMMAND test_integration)
set_tests_properties(IntegrationTests PROPERTIES TIMEOUT 600)

add_executable(test_ctx_smoke tests/ctx_smoke.c)
target_compile_definitions(test_ctx_smoke PRIVATE ${LOOMWORKS_CTX_DEFINES})
target_include_directories(test_ctx_smoke PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(test_ctx_smoke PRIVATE loomworks_static m)
add_test(NAME test_ctx_smoke COMMAND test_ctx_smoke TIMEOUT 600)

add_executable(example_pipeline examples/pipeline_demo.c)
target_link_libraries(example_pipeline PRIVATE loomworks_static)

add_subdirectory(examples)

# ---------------------------------------------------------------------------
# clang-format: check formatting before build (lint target)
# ---------------------------------------------------------------------------
# ALL_C_FILES is only defined when clang-format was found above, so owning the
# lint target on the actual program is safe.
if(ENABLE_CLANG_FORMAT AND CLANG_FORMAT_BIN)
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

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
install(TARGETS loomworks_static loomworks_shared
    EXPORT loomworksTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/loomworks
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# ---------------------------------------------------------------------------
# Export config — enables downstream find_package(loomworks)
# ---------------------------------------------------------------------------
install(EXPORT loomworksTargets
    FILE loomworksTargets.cmake
    NAMESPACE loomworks::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/loomworks
)

# Generate package config file
include(CMakePackageConfigHelpers)
configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/loomworksConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/loomworksConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/loomworks
)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/loomworksConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/loomworksConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/loomworksConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/loomworks
)
