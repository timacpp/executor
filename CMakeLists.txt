cmake_minimum_required(VERSION 3.1)
project(MIMUW-FORK CXX)

set(CMAKE_CXX_STANDARD "17")
set(CMAKE_C_STANDARD "11")
set(CMAKE_C_FLAGS "-g -Wall -Wextra -Wno-sign-compare")

function(add_example_program target_name)
  add_executable(${target_name} "${target_name}.cpp")
  target_link_libraries(${target_name} pthread)
endfunction()

add_example_program(executor)
target_link_libraries(executor)
