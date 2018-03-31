cmake_minimum_required (VERSION 2.6)

INCLUDE_DIRECTORIES(include)

# Generate protocol buffers.
INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER message.proto)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

# The library dependencies of the key value store clients and servers.
SET(KV_LIBRARY_DEPENDENCIES
    protobuf
    pthread
    zmq
)

# The source dependencies of the key value store clients and servers.
SET(KV_SRC_DEPENDENCIES
    ${PROTO_SRC}
    ${PROTO_HEADER}
)

ADD_EXECUTABLE(kvs_user kvs_user.cpp ${KV_SRC_DEPENDENCIES} include/zmq_util.cc include/zmq_util.h include/socket_cache.cc include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_user ${KV_LIBRARY_DEPENDENCIES})

ADD_EXECUTABLE(kvs_benchmark kvs_benchmark.cpp ${KV_SRC_DEPENDENCIES} include/zmq_util.cc include/zmq_util.h include/socket_cache.cc include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_benchmark ${KV_LIBRARY_DEPENDENCIES})

ADD_EXECUTABLE(kvs_benchmark_trigger kvs_benchmark_trigger.cpp ${KV_SRC_DEPENDENCIES} include/zmq_util.cc include/zmq_util.h include/socket_cache.cc include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_benchmark_trigger ${KV_LIBRARY_DEPENDENCIES})

ADD_EXECUTABLE(kvs_proxy kvs_proxy.cpp ${KV_SRC_DEPENDENCIES} include/zmq_util.cc include/zmq_util.h include/socket_cache.cc include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_proxy ${KV_LIBRARY_DEPENDENCIES} ${TBB_LIBRARIES} ${BOOST_SYSTEM_LIBRARIES} ${BOOST_THREAD_LIBRARIES})

ADD_EXECUTABLE(kvs_server kvs_server.cpp ${KV_SRC_DEPENDENCIES} include/zmq_util.cc include/zmq_util.h include/socket_cache.cc include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_server ${KV_LIBRARY_DEPENDENCIES} ${TBB_LIBRARIES})