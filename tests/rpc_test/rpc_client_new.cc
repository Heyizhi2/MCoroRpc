/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-28 13:53:35
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-05-18 16:55:11
 * @FilePath: /MCoroRpc/tests/rpc_test/rpc_client_new.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
/**
 * @file rpc_client_new.cc
 * @brief 使用新版 RpcClient 的示例 - 支持多实例负载均衡
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_client.hpp"
#include "calc.pb.h"
#include <iostream>

int main() {
    printf("=== RPC Client New (Multi-Instance) ===\n\n");
    fflush(stdout);

    Coro::RpcClientOptions options;
    options.zkHost = "127.0.0.1:2181";
    options.timeoutMs = 3000;
    options.maxRetries = 3;

    auto client = std::make_shared<Coro::RpcClient>(options);

    client->setServiceStatusCallback([](const std::string& serviceName, bool isAlive) {
        
    });

    auto clientTask = [client]() -> Coro::Task<void> {
        printf("[Client] Connecting via discovery...\n");
        fflush(stdout);

        co_await client->connectWithDiscovery("testrpc.Calculator");
        
        printf("[Client] Connected via discovery!\n");
        printf("[Client] LoadBalancer: %s\n", 
               typeid(*client->getLoadBalancer()).name());
        fflush(stdout);

        // 测试 RPC 调用
        testrpc::AddRequest request;
        

        for (int i = 0; i < 100; i++) {
            request.set_a(10+i);
            request.set_b(20+i);
            testrpc::AddResponse response;
            bool ok = client->callMethodSync(
                testrpc::Calculator::descriptor()->FindMethodByName("Add"),
                &request, &response, 3000);
            
            if (ok) {
                printf("[Client] Call %d: %d + %d = %d\n", 
                       i, request.a(), request.b(), response.result());
            } else {
                printf("[Client] Call %d: FAILED (%s)\n", i, client->getLastErrorText().c_str());
            }
            fflush(stdout);
            
           co_await Coro::sleep_for(std::chrono::milliseconds(500));
        }

        client->disconnect();
        printf("[Client] Done\n");
    };
    clientTask().schedule();

    printf("[Client] Running event loop...\n");
    fflush(stdout);

    Coro::get_event_loop().run_until_complete();

    return 0;
}