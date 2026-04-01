/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-28 13:53:35
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-29 18:15:38
 * @FilePath: /MCoroRpc/tests/rpc_test/rpc_client_new.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
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

    auto client = std::make_shared<Coro::RpcClient>(options);

    auto clientTask = [client]() -> Coro::Task<void> {
        printf("[RpcClient] Connecting to server...\n");
        fflush(stdout);

        co_await client->connect("127.0.0.1", 8000);

        // 等待连接完全建立
        co_await Coro::sleep_for(std::chrono::milliseconds(100));

        printf("[RpcClient] Connected, calling RPC...\n");
        fflush(stdout);

        auto* serviceDesc = testrpc::Calculator::descriptor();
        auto* methodDesc = serviceDesc->method(0);

        // 使用异步调用
        testrpc::AddRequest request;
        request.set_a(10);
        request.set_b(20);
        
        testrpc::AddResponse response;
        
        auto callTask = [client, methodDesc, &request, &response]() -> Coro::Task<void> {
            printf("[RpcClient] callTask started\n");
            fflush(stdout);
            auto channel = client->getChannel();
            if (!channel) {
                printf("[RpcClient] channel is null!\n");
                co_return;
            }
            printf("[RpcClient] channel is valid, calling CallMethodAsync...\n");
            fflush(stdout);
            
            auto controller = std::make_shared<Coro::RpcController>();
            controller->SetTimeout(3000);
            
            co_await channel->CallMethodAsync(methodDesc, controller.get(), &request, &response, nullptr);
            printf("[RpcClient] CallMethodAsync done\n");
            fflush(stdout);
        };
        
        printf("[RpcClient] Before schedule...\n");
        fflush(stdout);
        callTask().schedule();
        printf("[RpcClient] After schedule\n");
        fflush(stdout);
        
        // 等待一下让调用完成
        co_await Coro::sleep_for(std::chrono::milliseconds(500));
        
        printf("[RpcClient] Result: %d + %d = %d\n", request.a(), request.b(), response.result());
        fflush(stdout);

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
