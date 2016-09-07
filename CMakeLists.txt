include(CheckCXXCompilerFlag)
set(CMAKE_BUILD_TYPE Debug)

cmake_minimum_required(VERSION 2.8)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -ggdb -std=gnu++11")

project(hpstream)

set(hpstream_VERSION_MAJOR 1)
set(hpstream_VERSION_MINOR 0)
set(CMAKE_BUILD_TYPE Release)
set(SOURCE_FILES src/rdma_buffer.cpp src/rdma_connection.cpp src/rdma_client.cpp src/options.cpp src/utils.cpp
        src/rdma_server.cpp src/rdma_event_loop.cpp src/rdma_fabric.cpp src/rdma_base_connection.cpp
        src/connection.cpp src/packet.cpp src/heron_rdma_client.cpp src/heron_rdma_server.cpp src/ridgen.h src/ridgen.cpp)

link_directories(/home/supun/dev/projects/stream/fabric-build/lib /home/supun/dev/projects/stream/hpstream /home/supun/dev/projects/stream/glog/build/lib)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include /home/supun/dev/projects/stream/fabric-build/include ${CMAKE_CURRENT_SOURCE_DIR}/src /home/supun/dev/projects/stream/glog/build/include)

add_library(hpstream SHARED ${SOURCE_FILES})
target_link_libraries(hpstream fabric rt pthread ibverbs protobuf glog)

add_executable(hps_server test/hps_utils.cpp test/hps_server.cpp)
target_link_libraries(hps_server fabric rt pthread ibverbs libhpstream.so protobuf glog)

add_executable(hps_client test/hps_utils.cpp test/hps_client.cpp)
target_link_libraries(hps_client hpstream fabric rt pthread ibverbs libhpstream.so protobuf glog)
