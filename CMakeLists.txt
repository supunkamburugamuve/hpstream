include(CheckCXXCompilerFlag)

cmake_minimum_required(VERSION 2.8)

CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)

project (hpstream)
set(CMAKE_BUILD_TYPE Release)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include /home/supun/dev/projects/stream/fabric-build/include)
add_library(hpstream SHARED src/buffer.cpp src/connection.cpp src/client.cpp src/options.cpp src/utils.cpp src/server.cpp)

