cmake_minimum_required(VERSION 3.16)
project(ctpool VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")

find_package(Threads REQUIRED)

add_library(ctpool_static STATIC
    src/thread_pool.c
    src/coroutine.c
)
target_include_directories(ctpool_static PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(ctpool_static PUBLIC Threads::Threads)
set_target_properties(ctpool_static PROPERTIES OUTPUT_NAME ctpool)

add_library(ctpool_shared SHARED
    src/thread_pool.c
    src/coroutine.c
)
target_include_directories(ctpool_shared PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(ctpool_shared PUBLIC Threads::Threads)
set_target_properties(ctpool_shared PROPERTIES
    OUTPUT_NAME ctpool
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

enable_testing()

add_executable(test_thread_pool tests/test_thread_pool.c)
target_link_libraries(test_thread_pool PRIVATE ctpool_static)
add_test(NAME ThreadPoolTests COMMAND test_thread_pool)

add_executable(test_coroutine tests/test_coroutine.c)
target_link_libraries(test_coroutine PRIVATE ctpool_static)
add_test(NAME CoroutineTests COMMAND test_coroutine)

add_executable(test_integration tests/test_integration.c)
target_link_libraries(test_integration PRIVATE ctpool_static)
add_test(NAME IntegrationTests COMMAND test_integration)

include(GNUInstallDirs)
install(TARGETS ctpool_static ctpool_shared
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/ctpool
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
