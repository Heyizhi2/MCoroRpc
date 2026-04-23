/**
 * @file test_discovery.cc
 * @brief 测试服务注册与发现的完整流程
 */
#include "../include/coro.hpp"
#include "../include/rpc/rpc_client.hpp"

int main() {
    printf("=== Service Discovery Integration Test ===\n\n");
    fflush(stdout);

    auto serverTask = []() -> Coro::Task<void> {
        auto provider = std::make_shared<Coro::RpcProvider>();
        provider->setZkHost("127.0.0.1:2181");
        provider->setIp("127.0.0.1");
        provider->setPort(8000);
        provider->setWorkerCount(2);
        
        auto server = co_await Coro::net::start_tcp_service("0.0.0.0", 8000);
        printf("[Server] Started on port 8000\n");
        fflush(stdout);
        
        co_await Coro::sleep_for(std::chrono::seconds(15));
        printf("[Server] Stopping...\n");
        fflush(stdout);
        co_return;
    };

    auto clientTask = []() -> Coro::Task<void> {
        auto client = std::make_shared<Coro::RpcClient>();
        
        client->setServiceStatusCallback([](const std::string& name, bool alive) {
            printf("[Client] Service %s is %s\n", name.c_str(), alive ? "UP" : "DOWN");
            fflush(stdout);
        });
        
        co_await client->connectWithDiscovery("testrpc.Calculator");
        
        printf("[Client] Connected via discovery!\n");
        printf("[Client] Connected: %s\n", client->isConnected() ? "YES" : "NO");
        fflush(stdout);
        
        co_await Coro::sleep_for(std::chrono::seconds(10));
        
        client->disconnect();
        co_return;
    };

    serverTask().schedule();
    clientTask().schedule();
    
    Coro::get_event_loop().run_until_complete();
    
    printf("\n=== Test Complete ===\n");
    return 0;
}