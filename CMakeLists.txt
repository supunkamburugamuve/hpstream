include(CheckCXXCompilerFlag)

cmake_minimum_required(VERSION 2.8)

CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)

project (hpstream)

set (hpstream_VERSION_MAJOR 1)
set (hpstream_VERSION_MINOR 0)
set(CMAKE_BUILD_TYPE Release)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include /home/supun/dev/projects/stream/fabric-build/include ${CMAKE_CURRENT_SOURCE_DIR}/src)
add_library(hpstream SHARED src/buffer.cpp src/connection.cpp src/client.cpp src/options.cpp src/utils.cpp src/server.cpp)

add_executable(hps_server test/main.cpp)
target_link_libraries (hps_server hpstream)

add_executable(hps_client test/main.cpp)
target_link_libraries (hps_client hpstream)