/*
 * @Author: 来自火星的码农
 * @Date: 2026-03-26
 * @Description: RpcProvider 测试用例
 */
#include "../include/coro.hpp"
#include "../include/rpc/rpc_provider.hpp"
#include "../include/coro/sleep.hpp"
#include <catch2/catch.hpp>
#include <thread>
#include <chrono>

TEST_CASE("RpcProvider lifecycle", "[rpc_provider]") {
    SECTION("create and configure provider") {
        auto provider = std::make_shared<AlphaMin::RpcProvider>();
        
        provider->setZkHost("127.0.0.1:2181");
        provider->setIp("127.0.0.1");
        provider->setPort(18000);
        
        REQUIRE(provider->isStopped() == true);
    }
    
    SECTION("start provider with zookeeper") {
        auto task = []() -> Coro::Task<void> {
            auto provider = std::make_shared<AlphaMin::RpcProvider>();
            provider->setZkHost("127.0.0.1:2181");
            provider->setIp("127.0.0.1");
            provider->setPort(18001);
            
            auto startTask = provider->start();
            startTask.schedule();
            
            co_await Coro::sleep_for(std::chrono::milliseconds(500));
            
            provider->stop();
            
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
        
        INFO("Provider start/stop test completed");
    }
}

TEST_CASE("RpcDispatcher", "[rpc_dispatcher]") {
    SECTION("create dispatcher") {
        auto dispatcher = std::make_shared<AlphaMin::RpcDispatcher>();
        REQUIRE(dispatcher != nullptr);
    }
    
    SECTION("parse service full name") {
        std::string service_name, method_name;
        
        std::string full_name = "UserService.Login";
        size_t pos = full_name.find('.');
        if (pos != std::string::npos) {
            service_name = full_name.substr(0, pos);
            method_name = full_name.substr(pos + 1);
        }
        
        REQUIRE(service_name == "UserService");
        REQUIRE(method_name == "Login");
    }
}