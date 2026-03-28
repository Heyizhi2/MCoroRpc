#include "../include/coro.hpp"
#include "../include/rpc/rpc_channel.hpp"
#include "../include/coro/sleep.hpp"
#include "calc.pb.h"
#include <iostream>
#include <atomic>

int main() {
    std::cout << "[Client] Starting..." << std::endl;
    
    auto channel = std::make_shared<Coro::RpcChannel>("127.0.0.1", 18000);
    std::atomic<int> done{0};
    std::atomic<int> success{0};
    
    auto runCall = [&channel, &done, &success](int a, int b, int id) -> Coro::Task<void> {
        co_await channel->connect();
        
        auto controller = std::make_shared<Coro::RpcController>();
        controller->SetTimeout(5000);
        
        testrpc::AddRequest request;
        request.set_a(a);
        request.set_b(b);
        
        testrpc::AddResponse response;
        
        auto* stub = new testrpc::Calculator::Stub(channel.get());
        
        printf("[Client %d] Calling Add(%d, %d)...\n", id, a, b);
        fflush(stdout);
        
        stub->Add(controller.get(), &request, &response, nullptr);
        
        for (int i = 0; i < 500 && !controller->Finished(); i++) {
            co_await Coro::sleep_for(std::chrono::milliseconds(10));
        }
        
        if (controller->Failed()) {
            printf("[Client %d] RPC failed: %s\n", id, controller->ErrorText().c_str());
        } else {
            printf("[Client %d] result = %d\n", id, response.result());
            success++;
        }
        
        done++;
        
        co_return;
    };
    
    // 启动4个并发调用
    runCall(10, 20, 1).schedule();
    runCall(30, 40, 2).schedule();
    runCall(50, 60, 3).schedule();
    runCall(100, 200, 4).schedule();
    
    Coro::get_event_loop().run_until_complete();
    
    printf("\n[Client] Done: %d/%d calls completed, %d successful\n", 
           done.load(), 4, success.load());
    
    channel->close();
    
    return 0;
}
