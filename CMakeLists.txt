cmake_minimum_required(VERSION 3.0)
project(wildblocks)

include_directories(ext/libminecraft-file/include ext/hwm.task)
add_definitions(-std=c++17)
add_executable(wildblocks src/main.cpp ext/libminecraft-file/include/minecraft-file.hpp)
target_link_libraries(wildblocks z)
