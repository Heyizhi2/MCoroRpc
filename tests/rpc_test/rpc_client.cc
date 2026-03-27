#include "../include/coro.hpp"
#include "../include/rpc/rpc_channel.hpp"
#include "../include/coro/sleep.hpp"
#include "calc.pb.h"
#include <iostream>
#include <atomic>

int main() {
    std::cout << "[Client] Starting..." << std::endl;
    
    auto channel = std::make_shared<Coro::RpcChannel>("127.0.0.1", 18000);
    std::atomic<bool> done{false};
    int result = 0;
    
    auto run = [&channel, &done, &result]() -> Coro::Task<void> {
        co_await channel->connect();
        printf("[Client] Connected!\n");
        
        auto controller = std::make_shared<Coro::RpcController>();
        controller->SetTimeout(3000);
        
        testrpc::AddRequest request;
        request.set_a(10);
        request.set_b(20);
        
        testrpc::AddResponse response;
        
        auto* stub = new testrpc::Calculator::Stub(channel.get());
        
        printf("[Client] Calling Add(10, 20)...\n");
        fflush(stdout);
        
        stub->Add(controller.get(), &request, &response, nullptr);
        
        // 等待 controller 完成（通过轮询，最多3秒）
        for (int i = 0; i < 300 && !controller->Finished(); i++) {
            co_await Coro::sleep_for(std::chrono::milliseconds(10));
        }
        
        if (controller->Failed()) {
            printf("[Client] RPC failed: %s (err_code=%d)\n", controller->ErrorText().c_str(), controller->ErrorCode());
        } else {
            printf("[Client] result = %d, finished=%d\n", response.result(), controller->Finished());
        }
        
        done = true;
        
        channel->close();
        co_return;
    };
    
    run().schedule();
    Coro::get_event_loop().run_until_complete();
    
    // 如果3秒内没完成，说明有问题
    if (!done) {
        printf("[Client] ERROR: RPC timed out!\n");
    }
    
    return 0;
}
