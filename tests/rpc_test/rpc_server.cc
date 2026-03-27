#include "../include/coro.hpp"
#include "../include/rpc/rpc_provider.hpp"
#include "../include/coro/sleep.hpp"
#include "calc.pb.h"
#include <iostream>
#include <thread>

class CalculatorServiceImpl : public testrpc::Calculator {
public:
    void Add(::google::protobuf::RpcController* controller,
             const ::testrpc::AddRequest* request,
             ::testrpc::AddResponse* response,
             ::google::protobuf::Closure* done) override {
        response->set_result(request->a() + request->b());
        std::cout << "[Server] Received: " << request->a() << " + " << request->b() << " = " << response->result() << std::endl;
        done->Run();
    }
};

int main() {
    std::cout << "[Server] Starting on port 18000..." << std::endl;
    
    auto provider = std::make_shared<Coro::RpcProvider>();
    provider->setIp("127.0.0.1");
    provider->setPort(18000);
    provider->setZkHost("");  // 不需要 ZooKeeper
    
    auto* service = new CalculatorServiceImpl();
    provider->registerService(service);
    
    auto server_task = [&provider]() -> Coro::Task<void> {
        co_await provider->start();
    };
    
    server_task().schedule();
    
    std::cout << "[Server] Running event loop..." << std::endl;
    
    Coro::get_event_loop().run_until_complete();
    
    return 0;
}
