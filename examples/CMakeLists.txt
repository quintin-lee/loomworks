add_executable(example_basic basic_pool.c)
target_link_libraries(example_basic PRIVATE loomworks_static)

add_executable(example_task_group task_group_demo.c)
target_link_libraries(example_task_group PRIVATE loomworks_static)

add_executable(bench bench.c)
target_link_libraries(bench PRIVATE loomworks_static)
# Report the active context backend from bench output (supports the
# coro_switch scenario); harmless when only the default is configured.
target_compile_definitions(bench PRIVATE
    LOOMWORKS_BENCH_CTX_BACKEND="${LOOMWORKS_CTX_BACKEND}")

add_executable(monitor_demo monitor_demo.c)
target_link_libraries(monitor_demo PRIVATE loomworks_static)

add_executable(example_priority priority_demo.c)
target_link_libraries(example_priority PRIVATE loomworks_static)

add_executable(example_cancel cancel_demo.c)
target_link_libraries(example_cancel PRIVATE loomworks_static)

add_executable(example_coroutine coroutine_demo.c)
target_link_libraries(example_coroutine PRIVATE loomworks_static)

add_executable(example_backpressure backpressure_demo.c)
target_link_libraries(example_backpressure PRIVATE loomworks_static)

add_executable(example_resize resize_demo.c)
target_link_libraries(example_resize PRIVATE loomworks_static)
