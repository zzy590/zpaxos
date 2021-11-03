cmake_minimum_required(VERSION 3.13)
project(zpaxos)

set(CMAKE_CXX_STANDARD 11)

set(INC_DIR ./include /usr/local/include)
set(LIB_DIR /usr/local/lib)

include_directories(${INC_DIR})
link_directories(${LIB_DIR})
link_libraries(pthread)

aux_source_directory(./test DIR_SRCS)

add_executable(zpaxos_demo ${DIR_SRCS})

target_link_libraries(zpaxos_demo pthread)
