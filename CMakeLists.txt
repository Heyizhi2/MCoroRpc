cmake_minimum_required(VERSION 3.16)
project(MCoroRpc)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
set(CMAKE_BUILD_TYPE Release)  # 或在命令行 cmake -DCMAKE_BUILD_TYPE=Debug ..
find_package(Catch2 REQUIRED)

include_directories(include)

set(SOURCES
    src/event_loop.cc
    src/handle.cc
    src/timer.cc
    src/epoll.cc
    src/tcp_buffer.cc
    src/tcpstream.cc
    src/tinypb_protocol.cc
    src/tinypb_coder.cc
)

add_library(CoroRpc STATIC ${SOURCES})

add_executable(test_task tests/test_task.cc)
target_link_libraries(test_task PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_tinypb_coder tests/test_tinypb_coder.cc)
target_link_libraries(test_tinypb_coder PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_channel tests/test_channel.cc)
target_link_libraries(test_channel PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(benchmark_channel tests/benchmark_channel.cc)
target_link_libraries(benchmark_channel PRIVATE CoroRpc)

add_executable(test_sleep tests/test_sleep.cc)
target_link_libraries(test_sleep PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(echo_server tests/echo_server.cc)
target_link_libraries(echo_server PRIVATE CoroRpc)
add_executable(echo_server_opt tests/echo_server_opt.cc)
target_link_libraries(echo_server_opt PRIVATE CoroRpc)
add_executable(echo_server_coroutine tests/echo_server_coroutine.cc)
target_link_libraries(echo_server_coroutine PRIVATE CoroRpc)
add_executable(udp_echo_server tests/udp_echo_server.cc)
target_link_libraries(udp_echo_server PRIVATE CoroRpc)
add_executable(echo_client tests/echo_client.cc)
target_link_libraries(echo_client PRIVATE CoroRpc)

add_executable(load_test tests/load_test.cc)
target_link_libraries(load_test PRIVATE CoroRpc)
