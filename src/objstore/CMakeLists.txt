cmake_minimum_required (VERSION 2.8)
project (objstore)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wextra")

add_subdirectory (proto)

set (server-srcs Server.cc System.cc)
add_executable (server ${server-srcs})
target_link_libraries (server jemalloc)

