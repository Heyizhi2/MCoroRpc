/*
 * @file rpc_benchmark_server.cc
 * @brief RPC 性能测试服务器
 * 
 * 基于 TinyPB 协议的 Echo 服务器
 */

#include "../include/coro.hpp"
#include <iostream>
#include <atomic>

std::atomic<uint64_t> g_total_requests{0};

Coro::Task<void> handleClient(Coro::net::TcpStream stream) {
    while (true) {
        auto header = co_await stream.read(5);
        if (header.empty()) break;
        
        int32_t pk_len = (static_cast<unsigned char>(header[1]) << 24) |
                          (static_cast<unsigned char>(header[2]) << 16) |
                          (static_cast<unsigned char>(header[3]) << 8) |
                          static_cast<unsigned char>(header[4]);
        
        auto body = co_await stream.read(pk_len - 5);
        if (body.empty()) break;
        
        g_total_requests++;
        
        std::vector<char> response(header.begin(), header.end());
        response.insert(response.end(), body.begin(), body.end());
        co_await stream.write(response);
    }
    
    stream.close();
    co_return;
}

Coro::Task<void> benchmarkServer(int port) {
    printf("[RPC Server] Starting on port %d...\n", port);
    fflush(stdout);
    
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", port);
    printf("[RPC Server] Listening on 0.0.0.0:%d\n", port);
    fflush(stdout);
    
    while (true) {
        auto client = co_await service.accept();
        handleClient(std::move(client)).schedule();
    }
}

int main(int argc, char* argv[]) {
    int port = 9000;
    
    if (argc > 1) {
        port = std::stoi(argv[1]);
    }
    
    printf("========================================\n");
    printf("    RPC Benchmark Server\n");
    printf("========================================\n");
    printf("Port: %d\n", port);
    printf("Protocol: TinyPB Echo\n");
    printf("========================================\n\n");
    fflush(stdout);
    
    benchmarkServer(port).schedule();
    Coro::get_event_loop().run_until_complete();
    
    return 0;
}
