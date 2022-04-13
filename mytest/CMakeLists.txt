add_executable(bench_visit bench_visit.cpp)
target_link_libraries(bench_visit PRIVATE kvs_obj)

add_executable(gc_demo gc_demo.cpp)
target_link_libraries(gc_demo PRIVATE kvs_obj)