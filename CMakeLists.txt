cmake_minimum_required(VERSION 3.16)
project(MCoroRpc)
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")

find_package(Catch2 REQUIRED)
find_package(Protobuf REQUIRED)
find_library(ZOOKEEPER_LIBRARY NAMES zookeeper_mt zookeeper REQUIRED)

add_subdirectory(third_part/nanobench)

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
    ${CMAKE_SOURCE_DIR}/src/rpc_server_impl.cc
    ${CMAKE_SOURCE_DIR}/src/rpc_client_impl.cc
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

add_executable(test_service_discovery_dynamic tests/test_service_discovery_dynamic.cc)
target_link_libraries(test_service_discovery_dynamic PRIVATE CoroRpc Catch2::Catch2WithMain ${ZOOKEEPER_LIBRARY})

add_executable(test_rpc_provider tests/test_rpc_provider.cc)
target_link_libraries(test_rpc_provider PRIVATE CoroRpc Catch2::Catch2WithMain ${ZOOKEEPER_LIBRARY})

add_executable(test_watcher tests/test_watcher.cc)
target_link_libraries(test_watcher PRIVATE CoroRpc ${ZOOKEEPER_LIBRARY})

add_executable(test_net_addr tests/test_net_addr.cc)
target_link_libraries(test_net_addr PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_rpc_context tests/test_rpc_context.cc)
target_link_libraries(test_rpc_context PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_rpc_dispatcher tests/test_rpc_dispatcher.cc)
target_link_libraries(test_rpc_dispatcher PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_tcpstream tests/test_tcpstream.cc)
target_link_libraries(test_tcpstream PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_accept_timeout tests/test_accept_timeout.cc)
target_link_libraries(test_accept_timeout PRIVATE CoroRpc Catch2::Catch2WithMain)

add_executable(test_timeout tests/test_timeout.cc)
target_link_libraries(test_timeout PRIVATE CoroRpc)

add_executable(benchmark_sched tests/benchmark_sched.cc)
target_link_libraries(benchmark_sched PRIVATE CoroRpc nanobench)

add_executable(rpc_server_old tests/rpc_test/rpc_server.cc tests/rpc_test/calc.pb.cc)
target_link_libraries(rpc_server_old PRIVATE CoroRpc)

add_executable(rpc_client_old tests/rpc_test/rpc_client.cc tests/rpc_test/calc.pb.cc)
target_link_libraries(rpc_client_old PRIVATE CoroRpc)

add_executable(rpc_server_new tests/rpc_test/rpc_server_new.cc tests/rpc_test/calc.pb.cc tests/rpc_test/calc_service.h)
target_link_libraries(rpc_server_new PRIVATE CoroRpc)

add_executable(rpc_client_new tests/rpc_test/rpc_client_new.cc tests/rpc_test/calc.pb.cc)
target_link_libraries(rpc_client_new PRIVATE CoroRpc)

add_executable(rpc_client_discovery tests/rpc_test/rpc_client_discovery.cc tests/rpc_test/calc.pb.cc)
target_link_libraries(rpc_client_discovery PRIVATE CoroRpc)

add_executable(echo_test_server tests/rpc_test/echo_test_server.cc)
target_link_libraries(echo_test_server PRIVATE CoroRpc)

add_executable(echo_test_client tests/rpc_test/echo_test_client.cc)
target_link_libraries(echo_test_client PRIVATE CoroRpc)

# RPC 性能测试 - 服务端 (支持命令行参数修改 worker 数量)
add_executable(rpc_benchmark_server tests/rpc_test/rpc_benchmark_server.cc tests/rpc_test/calc.pb.cc tests/rpc_test/calc_service.h)
target_link_libraries(rpc_benchmark_server PRIVATE CoroRpc)

# RPC 性能测试 - 客户端 (支持命令行参数修改总请求数、并发数、线程数)
add_executable(rpc_benchmark_client tests/rpc_test/rpc_benchmark_client.cc tests/rpc_test/calc.pb.cc)
target_link_libraries(rpc_benchmark_client PRIVATE CoroRpc)

add_executable(test_eventloop_pool tests/test_eventloop_pool.cc)
target_link_libraries(test_eventloop_pool PRIVATE CoroRpc)

add_executable(test_zk_nodes tests/test_zk_nodes.cc)
target_link_libraries(test_zk_nodes PRIVATE CoroRpc)

add_executable(test_discovery tests/test_discovery.cc)
target_link_libraries(test_discovery PRIVATE CoroRpc)
