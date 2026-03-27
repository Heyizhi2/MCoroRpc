cmake_minimum_required(VERSION 3.16)
project(MCoroRpc)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
set(CMAKE_BUILD_TYPE Release)

find_package(Catch2 REQUIRED)
find_package(Protobuf REQUIRED)
find_library(ZOOKEEPER_LIBRARY NAMES zookeeper_mt zookeeper REQUIRED)

# Set protobuf targets if not available
if(TARGET protobuf::libprotobuf)
  set(PROTOBUF_TARGET protobuf::libprotobuf)
elseif(TARGET protobuf)
  set(PROTOBUF_TARGET protobuf)
else()
  set(PROTOBUF_TARGET protobuf-static)
endif()

include_directories(include)

set(SOURCES
    ${CMAKE_SOURCE_DIR}/src/event_loop.cc
    ${CMAKE_SOURCE_DIR}/src/handle.cc
    ${CMAKE_SOURCE_DIR}/src/timer.cc
    ${CMAKE_SOURCE_DIR}/src/epoll.cc
    ${CMAKE_SOURCE_DIR}/src/tcp_buffer.cc
    ${CMAKE_SOURCE_DIR}/src/tcpstream.cc
    ${CMAKE_SOURCE_DIR}/src/tinypb_protocol.cc
    ${CMAKE_SOURCE_DIR}/src/tinypb_coder.cc
    ${CMAKE_SOURCE_DIR}/src/zkclient.cc
    ${CMAKE_SOURCE_DIR}/src/rpc_provider.cc
    ${CMAKE_SOURCE_DIR}/src/net_addr.cc
)

add_library(CoroRpc STATIC ${SOURCES})
target_link_libraries(CoroRpc PUBLIC ${ZOOKEEPER_LIBRARY} ${PROTOBUF_TARGET})

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

add_executable(http_echo_server tests/http_echo_server.cc)
target_link_libraries(http_echo_server PRIVATE CoroRpc)

add_executable(load_test tests/load_test.cc)
target_link_libraries(load_test PRIVATE CoroRpc)

add_executable(test_zkclient tests/test_zkclient.cc)
target_link_libraries(test_zkclient PRIVATE CoroRpc Catch2::Catch2WithMain ${ZOOKEEPER_LIBRARY})

add_executable(test_service_discovery tests/test_service_discovery.cc)
target_link_libraries(test_service_discovery PRIVATE CoroRpc Catch2::Catch2WithMain ${ZOOKEEPER_LIBRARY})

add_executable(test_rpc_provider tests/test_rpc_provider.cc)
target_link_libraries(test_rpc_provider PRIVATE CoroRpc Catch2::Catch2WithMain ${ZOOKEEPER_LIBRARY})

add_executable(test_net_addr tests/test_net_addr.cc)
target_link_libraries(test_net_addr PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_rpc_context tests/test_rpc_context.cc)
target_link_libraries(test_rpc_context PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_rpc_dispatcher tests/test_rpc_dispatcher.cc)
target_link_libraries(test_rpc_dispatcher PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_tcpstream tests/test_tcpstream.cc)
target_link_libraries(test_tcpstream PRIVATE CoroRpc Catch2::Catch2WithMain)
