cmake_minimum_required (VERSION 2.6)

add_executable(run_kvs_benchmark run_benchmark.cpp)
add_executable(benchmark_gossip benchmark_gossip.cpp)
add_executable(benchmark_shared benchmark_shared.cpp)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
   # Mac OS X specific code
   target_link_libraries (run_kvs_benchmark ${PROJECT_SOURCE_DIR}/vendor/gbenchmark/build/src/libbenchmark.dylib)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    # Linux specific code
   target_link_libraries (run_kvs_benchmark ${PROJECT_SOURCE_DIR}/vendor/gbenchmark/build/src/libbenchmark.so)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")