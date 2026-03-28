/**
 * @file simple_client.cc
 * @brief 简化的 RPC 客户端测试
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_channel.hpp"
#include "../../include/coro/sleep.hpp"
#include "calc.pb.h"
#include <iostream>

int main() {
    printf("[Client] Starting...\n");
    fflush(stdout);
    
    auto channel = std::make_shared<Coro::RpcChannel>("127.0.0.1", 8000);
    
    auto runCall = [&channel]() -> Coro::Task<void> {
        printf("[Client] Connecting...\n");
        fflush(stdout);
        
        co_await channel->connect();
        
        printf("[Client] Connected!\n");
        fflush(stdout);
        
        auto controller = std::make_shared<Coro::RpcController>();
        controller->SetTimeout(5000);
        
        testrpc::AddRequest request;
        request.set_a(10);
        request.set_b(20);
        
        testrpc::AddResponse response;
        
        auto* serviceDesc = testrpc::Calculator::descriptor();
        auto* methodDesc = serviceDesc->method(0);
        
        channel->CallMethod(methodDesc, controller.get(), &request, &response, nullptr);
        
        printf("[Client] After CallMethod\n");
        fflush(stdout);
        
        // 等待一下让请求处理完成
        co_await Coro::sleep_for(std::chrono::seconds(1));
        
        if (controller->Failed()) {
            printf("[Client] RPC failed: %s\n", controller->ErrorText().c_str());
        } else {
            printf("[Client] RPC success! Result: %d\n", response.result());
        }
        fflush(stdout);
    };
    
    runCall().schedule();
    
    printf("[Client] Running event loop...\n");
    fflush(stdout);
    
    Coro::get_event_loop().run_until_complete();
    
    printf("[Client] Done\n");
    return 0;
}
