/**
 * @file rpc_server_new.cc
 * @brief 使用新版 RpcServer 的示例 - 支持命令行参数
 * @usage ./rpc_server_new [端口号] [zk地址]
 * @example ./rpc_server_new 8000           # 启动在 8000 端口
 * @example ./rpc_server_new 8001           # 启动在 8001 端口
 * @example ./rpc_server_new 8001 127.0.0.1:2181
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_server.hpp"
#include "calc.pb.h"
#include "calc_service.h"
#include <iostream>
#include <csignal>
#include <cstdlib>

Coro::RpcServer* g_server = nullptr;

void signal_handler(int sig) {
    printf("\n[Server] Received Ctrl+C, stopping...\n");
    fflush(stdout);
    if (g_server) {
        g_server->stop();
    }
}

void print_usage(const char* prog) {
    printf("Usage: %s [端口号] [zk地址]\n", prog);
    printf("  端口号: RPC 服务监听端口 (默认: 8000)\n");
    printf("  zk地址: ZooKeeper 地址 (默认: 127.0.0.1:2181)\n");
    printf("\nExample:\n");
    printf("  %s 8000              # 启动在 8000 端口\n", prog);
    printf("  %s 8001              # 启动在 8001 端口\n", prog);
    printf("  %s 8002 192.168.1.1:2181  # 使用自定义 ZK\n", prog);
}

int main(int argc, char* argv[]) {
    int port = 8000;
    std::string zkHost = "127.0.0.1:2181";
    std::string ip = "127.0.0.1";
    
    // 解析命令行参数
    if (argc >= 2) {
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
        port = atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Error: Invalid port %d (1-65535)\n", port);
            return 1;
        }
    }
    if (argc >= 3) {
        zkHost = argv[2];
    }
    
    printf("=== RPC Server (Multi-Instance Demo) ===\n");
    printf("Port: %d\n", port);
    printf("ZK:   %s\n", zkHost.c_str());
    printf("================================\n\n");
    fflush(stdout);
    
    Coro::RpcServer server(port, zkHost);
    g_server = &server;
    server.setWorkerCount(4);
    server.setAddress(ip, port);

    CalculatorServiceImpl calcService;
    server.registerService(&calcService);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    auto serverTask = [&server]() -> Coro::Task<void> {
        co_await server.start();
    };

    serverTask().schedule();
    printf("[Server] Started at %s:%d\n", ip.c_str(), port);
    printf("[Server] Registered service: testrpc.Calculator\n");
    printf("[Server] Press Ctrl+C to stop\n\n");
    fflush(stdout);

    Coro::get_event_loop().run_until_complete();

    printf("[Server] Stopped\n");
    return 0;
}