/**
 * @file rpc_benchmark_server.cc
 * @brief RPC 性能测试服务端
 * @details 启动 RPC 服务器用于性能测试
 * 
 * 使用方式:
 *   ./rpc_benchmark_server [端口号] [worker数量]
 * 
 * 示例:
 *   ./rpc_benchmark_server              # 默认 port=8001, worker=4
 *   ./rpc_benchmark_server 8002       # port=8002
 *   ./rpc_benchmark_server 8003 16     # port=8003, worker=16
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_server.hpp"
#include "calc.pb.h"
#include "calc_service.h"
#include <iostream>
#include <cstdlib>
#include <csignal>

Coro::RpcServer* g_server = nullptr;

void signal_handler(int sig) {
    printf("\n[Server] Received signal %d, stopping...\n", sig);
    fflush(stdout);
    if (g_server) {
        g_server->stop();
    }
}

void print_usage(const char* prog) {
    printf("Usage: %s [端口号] [worker数量]\n", prog);
    printf("  端口号:   RPC 监听端口 (默认: 8001)\n");
    printf("  worker数: Worker 协程数量 (默认: 4)\n");
    printf("\n示例:\n");
    printf("  %s              # 端口=8001, worker=4\n", prog);
    printf("  %s 8002         # 端口=8002\n", prog);
    printf("  %s 8003 16      # worker=16\n", prog);
}

int main(int argc, char* argv[]) {
    int port = 8001;
    int worker_count = 4;
    
    if (argc >= 2) {
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
        port = atoi(argv[1]);
    }
    if (argc >= 3) {
        worker_count = atoi(argv[2]);
    }
    
    printf("================================================\n");
    printf("         RPC Benchmark Server              \n");
    printf("================================================\n");
    printf("Port:        %d\n", port);
    printf("Workers:     %d\n", worker_count);
    printf("================================================\n\n");
    fflush(stdout);
    
    Coro::RpcServer server(port, "");
    g_server = &server;
    server.setWorkerCount(worker_count);
    
    CalculatorServiceImpl calcService;
    server.registerService(&calcService);
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    auto serverTask = [&server]() -> Coro::Task<void> {
        co_await server.start();
    };
    serverTask().schedule();
    
    printf("[Server] Started on port %d with %d workers\n", port, worker_count);
    printf("[Server] Press Ctrl+C to stop\n\n");
    fflush(stdout);
    
    Coro::get_event_loop().run_until_complete();
    
    printf("[Server] Stopped\n");
    return 0;
}