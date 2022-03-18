cmake_minimum_required(VERSION 3.10.0)

project (KVS)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# dir to include
set(KVS_INCLUDE_DIRECTORIES include)

# set(ASAN_OPTIONS "fast_unwind_on_fatal=1")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fsanitize-recover=address")

add_compile_options("-Wall" "-W" "-Wextra" "-fPIC" "-g3")

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)
find_package(glog REQUIRED)
find_package(gflags REQUIRED)

# headers to install
file(GLOB_RECURSE KVS_PUBLIC_HEADER include/*)

# source for the lib
file(GLOB_RECURSE KVS_SOURCE src/*)
add_library(kvs_obj STATIC ${KVS_SOURCE})
target_include_directories(kvs_obj PUBLIC ${KVS_INCLUDE_DIRECTORIES})
target_include_directories(kvs_obj INTERFACE ${GLOG_INCLUDE_DIRS})
target_link_libraries(kvs_obj INTERFACE pthread)
target_link_libraries(kvs_obj INTERFACE ${GLOG_LIBRARIES})
target_link_libraries(kvs_obj PUBLIC ${GFLAGS_LIBRARIES})

add_subdirectory(test)
add_subdirectory(unittest)
add_subdirectory(bench)