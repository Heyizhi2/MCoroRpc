/**
 * @file rpc_server_new.cc
 * @brief 使用新版 RpcServer 的示例
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_server.hpp"
#include "calc.pb.h"
#include "calc_service.h"
#include <iostream>

int main() {
    printf("[RpcServer] Starting...\n");
    fflush(stdout);

    Coro::RpcServer server(8000, "127.0.0.1:2181");
    server.setWorkerCount(4);

    CalculatorServiceImpl calcService;
    server.registerService(&calcService);

    auto serverTask = [&server]() -> Coro::Task<void> {
        printf("[RpcServer] Starting server...\n");
        fflush(stdout);
        co_await server.start();
        printf("[RpcServer] Server stopped\n");
    };

    serverTask().schedule();

    printf("[RpcServer] Running event loop...\n");
    fflush(stdout);

    Coro::get_event_loop().run_until_complete();

    printf("[RpcServer] Done\n");
    return 0;
}
