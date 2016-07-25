include(CheckCXXCompilerFlag)
set(CMAKE_BUILD_TYPE Debug)

cmake_minimum_required(VERSION 2.8)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=gnu++0x")

project(hpstream)

set(hpstream_VERSION_MAJOR 1)
set(hpstream_VERSION_MINOR 0)
set(CMAKE_BUILD_TYPE Release)
set(SOURCE_FILES src/buffer.cpp src/connection.cpp src/client.cpp src/options.cpp src/utils.cpp src/server.cpp)

link_directories(/home/supun/dev/projects/stream/fabric-build/lib /home/supun/dev/projects/stream/hpstream)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include /home/supun/dev/projects/stream/fabric-build/include ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_library(hpstream SHARED ${SOURCE_FILES})
target_link_libraries(hpstream fabric rt pthread ibverbs)

add_executable(hps_server test/hps_utils.cpp test/hps_server.cpp)
target_link_libraries(hps_server fabric rt pthread ibverbs libhpstream.so)

add_executable(hps_client test/hps_utils.cpp test/hps_client.cpp)
target_link_libraries(hps_client hpstream fabric rt pthread ibverbs libhpstream.so)
