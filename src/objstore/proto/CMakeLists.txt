cmake_minimum_required (VERSION 2.8)
project (objstore)

list (APPEND srcs ${PROJECT_SOURCE_DIR}/Register.proto)
add_custom_target (proto ALL
    protoc -I${PROJECT_SOURCE_DIR} --cpp_out=${PROJECT_BINARY_DIR} ${srcs}
    SOURCES ${srcs})

