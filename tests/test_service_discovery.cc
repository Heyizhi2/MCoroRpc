/*
 * @Author: 来自火星的码农
 * @Date: 2026-03-26
 * @Description: ServiceDiscovery 测试用例
 */
#include "../include/coro.hpp"
#include "../include/rpc/rpc_provider.hpp"
#include "../include/coro/sleep.hpp"
#include <catch2/catch.hpp>

TEST_CASE("ServiceDiscovery basic operations", "[service_discovery]") {
    SECTION("connect to zookeeper") {
        auto task = []() -> Coro::Task<void> {
            auto discovery = std::make_shared<AlphaMin::ServiceDiscovery>();
            discovery->setZkHost("127.0.0.1:2181");
            discovery->setTimeout(30000);
            
            co_await discovery->connect();
            
            // Keep alive to let connection complete
            co_await Coro::sleep_for(std::chrono::milliseconds(50));
            
            REQUIRE(discovery->isConnected() == true);
            
            discovery->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("discover service") {
        auto task = []() -> Coro::Task<void> {
            auto discovery = std::make_shared<AlphaMin::ServiceDiscovery>();
            discovery->setZkHost("127.0.0.1:2181");
            
            co_await discovery->connect();
            
            // 如果没有预先注册的服务，会返回空
            auto addr = co_await discovery->discover("NonExistentService", "Method");
            INFO("Discover result: " << addr);
            
            discovery->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("discover all methods") {
        auto task = []() -> Coro::Task<void> {
            auto discovery = std::make_shared<AlphaMin::ServiceDiscovery>();
            discovery->setZkHost("127.0.0.1:2181");
            
            co_await discovery->connect();
            
            auto methods = co_await discovery->discoverAllMethods("NonExistentService");
            INFO("Methods count: " << methods.size());
            
            discovery->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
}

TEST_CASE("ServiceDiscovery lifecycle", "[service_discovery]") {
    SECTION("reconnect after close") {
        auto task = []() -> Coro::Task<void> {
            auto discovery = std::make_shared<AlphaMin::ServiceDiscovery>();
            discovery->setZkHost("127.0.0.1:2181");
            
            // 第一次连接
            co_await discovery->connect();
            REQUIRE(discovery->isConnected() == true);
            
            // 关闭
            discovery->close();
            REQUIRE(discovery->isConnected() == false);
            
            // 重新连接
            co_await discovery->connect();
            REQUIRE(discovery->isConnected() == true);
            
            discovery->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
}