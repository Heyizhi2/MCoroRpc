/**
 * @file rpc_client_discovery.cc
 * @brief 使用服务发现模式的 RpcClient 示例
 * @details 通过 ZooKeeper 服务发现连接到 Calculator 服务
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_client.hpp"
#include "calc.pb.h"
#include <iostream>

int main() {
    printf("[RpcClient] Starting with service discovery...\n");
    fflush(stdout);

    Coro::RpcClientOptions options;
    options.zkHost = "127.0.0.1:2181";
    options.timeoutMs = 3000;
    options.enableLoadBalance = true;

    auto client = std::make_shared<Coro::RpcClient>(options);

    auto clientTask = [client]() -> Coro::Task<void> {
        printf("[RpcClient] Connecting to ZooKeeper for service discovery...\n");
        fflush(stdout);

        co_await client->connectWithDiscovery("testrpc.Calculator");

        printf("[RpcClient] Service discovered and connected\n");
        fflush(stdout);

        auto* serviceDesc = testrpc::Calculator::descriptor();
        auto* methodDesc = serviceDesc->method(0);

        testrpc::AddRequest request;
        testrpc::AddResponse response;

        for (int i = 0; i < 10; ++i) {
            request.set_a(i);
            request.set_b(i * 2);
            
            bool success = client->callMethodSync(methodDesc, &request, &response);
            if (success) {
                printf("[RpcClient] %d + %d = %d\n", i, i*2, response.result());
            } else {
                printf("[RpcClient] Call failed\n");
            }
            fflush(stdout);
        }

        client->disconnect();
    };

    clientTask().schedule();

    printf("[RpcClient] Running event loop...\n");
    fflush(stdout);

    Coro::get_event_loop().run_until_complete();

    printf("[RpcClient] Done\n");
    return 0;
}
