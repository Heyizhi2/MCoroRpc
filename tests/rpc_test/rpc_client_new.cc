/**
 * @file rpc_client_new.cc
 * @brief 使用新版 RpcClient 的示例
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_client.hpp"
#include "calc.pb.h"
#include <iostream>

int main() {
    printf("[RpcClient] Starting...\n");
    fflush(stdout);

    Coro::RpcClientOptions options;
    options.zkHost = "127.0.0.1:2181";
    options.timeoutMs = 3000;
    options.heartbeatCheckIntervalMs = 2000;
    options.heartbeatTimeoutMs = 15000;

    auto client = std::make_shared<Coro::RpcClient>(options);

    client->setServiceStatusCallback([](const std::string& serviceName, bool isAlive) {
        printf("[RpcClient] Service %s is %s\n", 
               serviceName.c_str(), isAlive ? "UP" : "DOWN");
        fflush(stdout);
    });

    auto clientTask = [client]() -> Coro::Task<void> {
        printf("[RpcClient] Connecting via discovery...\n");
        fflush(stdout);

        co_await client->connectWithDiscovery("testrpc.Calculator", "Add");
        
        printf("[RpcClient] Connected!\n");
        fflush(stdout);

        // 保持运行，观察心跳
        for (int i = 0; i < 60; i++) {
            co_await Coro::sleep_for(std::chrono::seconds(1));
            printf("[RpcClient] tick %d, connected=%d\n", i, client->isConnected());
        }

        client->disconnect();
        printf("[RpcClient] Disconnected\n");
    };
    clientTask().schedule();

    printf("[RpcClient] Running event loop...\n");
    fflush(stdout);

    Coro::get_event_loop().run_until_complete();

    printf("[RpcClient] Done\n");
    return 0;
}