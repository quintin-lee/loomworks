add_executable(example_basic basic_pool.c)
target_link_libraries(example_basic PRIVATE loomworks_static)

add_executable(example_task_group task_group_demo.c)
target_link_libraries(example_task_group PRIVATE loomworks_static)

add_executable(bench bench.c)
target_link_libraries(bench PRIVATE loomworks_static)

add_executable(monitor_demo monitor_demo.c)
target_link_libraries(monitor_demo PRIVATE loomworks_static)
