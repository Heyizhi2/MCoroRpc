/*
 * @Author: 来自火星的码农
 * @Date: 2026-03-26
 * @Description: ZkClient 测试用例
 */
#include "../include/coro.hpp"
#include "../include/rpc/zkclient.hpp"
#include "../include/coro/sleep.hpp"
#include <catch2/catch.hpp>
#include <thread>
#include <chrono>

TEST_CASE("ZkClient basic operations", "[zkclient]") {
    SECTION("connect to zookeeper") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<AlphaMin::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            zk->setTimeout(30000);
            
            auto result = co_await zk->start();
            INFO("Connect result: " << result.ok() << " rc: " << result.rc);
            
            // Wait a bit for connection
            co_await Coro::sleep_for(std::chrono::milliseconds(100));
            
            INFO("Connected: " << zk->isConnected());
            
            REQUIRE(zk->isConnected() == true);
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("create node") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<AlphaMin::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            auto result = co_await zk->start();
            REQUIRE(result.ok() == true);
            
            // 创建测试节点
            auto createResult = co_await zk->create("/test_rpc_node", "test_data", 0);
            REQUIRE(createResult.ok() == true);
            
            // 获取节点数据
            auto getResult = co_await zk->getData("/test_rpc_node");
            REQUIRE(getResult.ok() == true);
            REQUIRE(getResult.data == "test_data");
            
            // 删除节点
            auto delResult = co_await zk->deleteNode("/test_rpc_node");
            REQUIRE(delResult.ok() == true);
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("create ephemeral node") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<AlphaMin::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            // 创建临时节点
            auto result = co_await zk->create("/test_ephemeral", "127.0.0.1:8000", ZOO_EPHEMERAL);
            REQUIRE(result.ok() == true);
            
            // 获取数据
            auto getResult = co_await zk->getData("/test_ephemeral");
            REQUIRE(getResult.ok() == true);
            REQUIRE(getResult.data == "127.0.0.1:8000");
            
            // 删除
            co_await zk->deleteNode("/test_ephemeral");
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
}

TEST_CASE("ZkClient error handling", "[zkclient]") {
    SECTION("connect to invalid host") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<AlphaMin::ZkClient>();
            zk->setHost("127.0.0.1:21999");  // 无效端口
            
            auto result = co_await zk->start();
            INFO("Connect result: " << result.ok() << " error: " << result.error());
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("get non-existent node") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<AlphaMin::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            auto result = co_await zk->getData("/nonexistent_node_12345");
            REQUIRE(result.ok() == false);
            REQUIRE(result.rc == ZNONODE);
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
}