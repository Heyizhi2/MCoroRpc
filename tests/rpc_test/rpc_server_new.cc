/**
 * @file rpc_server_new.cc
 * @brief 使用新版 RpcServer 的示例
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_server.hpp"
#include "calc.pb.h"
#include "calc_service.h"
#include <iostream>

// 全局指针用于信号处理
Coro::RpcServer* g_server = nullptr;

void signal_handler(int sig) {
    printf("\n[RpcServer] Received Ctrl+C, stopping...\n");
    fflush(stdout);
    if (g_server) {
        g_server->stop();
    }
}

int main() {
    Coro::RpcServer server(8000, "127.0.0.1:2181");
    g_server = &server;
    server.setWorkerCount(4);

    CalculatorServiceImpl calcService;
    server.registerService(&calcService);

    signal(SIGINT, signal_handler);

    auto serverTask = [&server]() -> Coro::Task<void> {
        co_await server.start();
    };

    serverTask().schedule();

    Coro::get_event_loop().run_until_complete();

    return 0;
}