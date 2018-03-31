# Resources:
#   - http://www.kaizou.org/2014/11/gtest-cmake/
#   - http://bit.ly/2cx70Pk
#   - https://github.com/snikulov/google-test-examples

CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

include(ExternalProject)

ExternalProject_Add(googlebenchmark
    GIT_REPOSITORY "https://github.com/google/benchmark"
    GIT_TAG "v1.0.0"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    CMAKE_ARGS "CMAKE_CXX_COMPILER=clang++ -DCMAKE_CXX_FLAGS=-stdlib=libc++"
    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(googlebenchmark SOURCE_DIR)
SET(GBENCH_INCLUDE_DIRS ${SOURCE_DIR}/include PARENT_SCOPE)

ExternalProject_Get_Property(googlebenchmark BINARY_DIR)
SET(GBENCH_LIBS_DIR ${BINARY_DIR}/src PARENT_SCOPE)
