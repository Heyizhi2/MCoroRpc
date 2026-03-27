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
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            zk->setTimeout(30000);
            
            auto result = co_await zk->start();
            INFO("Connect result: " << result.ok() << " rc: " << result.rc);
            
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
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            auto result = co_await zk->start();
            REQUIRE(result.ok() == true);
            
            auto createResult = co_await zk->create("/test_rpc_node", "test_data", 0);
            REQUIRE(createResult.ok() == true);
            
            auto getResult = co_await zk->getData("/test_rpc_node");
            REQUIRE(getResult.ok() == true);
            REQUIRE(getResult.data == "test_data");
            
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
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            auto result = co_await zk->create("/test_ephemeral", "127.0.0.1:8000", ZOO_EPHEMERAL);
            REQUIRE(result.ok() == true);
            
            auto getResult = co_await zk->getData("/test_ephemeral");
            REQUIRE(getResult.ok() == true);
            REQUIRE(getResult.data == "127.0.0.1:8000");
            
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
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:21999");
            
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
            auto zk = std::make_shared<Coro::ZkClient>();
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

TEST_CASE("ZkClient setData operation", "[zkclient]") {
    SECTION("set node data") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            co_await zk->create("/test_setdata", "original_data", 0);
            
            auto setResult = co_await zk->setData("/test_setdata", "updated_data");
            REQUIRE(setResult.ok() == true);
            
            auto getResult = co_await zk->getData("/test_setdata");
            REQUIRE(getResult.ok() == true);
            REQUIRE(getResult.data == "updated_data");
            
            co_await zk->deleteNode("/test_setdata");
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("setData with version") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            co_await zk->create("/test_version", "v0", 0);
            
            auto r1 = co_await zk->setData("/test_version", "v1", 0);
            REQUIRE(r1.ok() == true);
            
            auto r2 = co_await zk->setData("/test_version", "v2", 1);
            REQUIRE(r2.ok() == true);
            
            auto r3 = co_await zk->setData("/test_version", "v3", 0);
            REQUIRE(r3.ok() == false);
            REQUIRE(r3.rc == ZBADVERSION);
            
            co_await zk->deleteNode("/test_version");
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
}

TEST_CASE("ZkClient getChildren operation", "[zkclient]") {
    SECTION("get children of root") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            co_await zk->create("/test_parent", "parent_data", 0);
            co_await zk->create("/test_parent/child1", "data1", 0);
            co_await zk->create("/test_parent/child2", "data2", 0);
            
            auto result = co_await zk->getChildren("/test_parent");
            REQUIRE(result.ok() == true);
            
            INFO("Children: " << result.data);
            
            co_await zk->deleteNode("/test_parent/child1");
            co_await zk->deleteNode("/test_parent/child2");
            co_await zk->deleteNode("/test_parent");
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("get children of non-existent node") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            auto result = co_await zk->getChildren("/nonexistent_parent_12345");
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

TEST_CASE("ZkClient ephemeral node behavior", "[zkclient]") {
    SECTION("ephemeral node deleted on close") {
        auto task = []() -> Coro::Task<void> {
            auto zk1 = std::make_shared<Coro::ZkClient>();
            auto zk2 = std::make_shared<Coro::ZkClient>();
            zk1->setHost("127.0.0.1:2181");
            zk2->setHost("127.0.0.1:2181");
            
            co_await zk1->start();
            co_await zk2->start();
            
            co_await zk1->create("/test_ephemeral_auto", "ephemeral_data", ZOO_EPHEMERAL);
            
            auto r1 = co_await zk2->getData("/test_ephemeral_auto");
            REQUIRE(r1.ok() == true);
            REQUIRE(r1.data == "ephemeral_data");
            
            zk1->close();
            
            co_await Coro::sleep_for(std::chrono::milliseconds(100));
            
            auto r2 = co_await zk2->getData("/test_ephemeral_auto");
            REQUIRE(r2.ok() == false);
            REQUIRE(r2.rc == ZNONODE);
            
            zk2->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("persistent node survives close") {
        auto task = []() -> Coro::Task<void> {
            auto zk1 = std::make_shared<Coro::ZkClient>();
            auto zk2 = std::make_shared<Coro::ZkClient>();
            zk1->setHost("127.0.0.1:2181");
            zk2->setHost("127.0.0.1:2181");
            
            co_await zk1->start();
            co_await zk2->start();
            
            co_await zk1->create("/test_persistent", "persistent_data", 0);
            
            zk1->close();
            
            auto result = co_await zk2->getData("/test_persistent");
            REQUIRE(result.ok() == true);
            REQUIRE(result.data == "persistent_data");
            
            co_await zk2->deleteNode("/test_persistent");
            zk2->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
}

TEST_CASE("ZkClient concurrent operations", "[zkclient]") {
    SECTION("multiple coroutines create nodes") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            auto createTask = [&](const std::string& path, const std::string& data) -> Coro::Task<void> {
                co_await zk->create(path, data, 0);
                co_return;
            };
            
            createTask("/test_concurrent_1", "data1").schedule();
            createTask("/test_concurrent_2", "data2").schedule();
            createTask("/test_concurrent_3", "data3").schedule();
            
            co_await Coro::sleep_for(std::chrono::milliseconds(200));
            
            auto r1 = co_await zk->getData("/test_concurrent_1");
            REQUIRE(r1.ok() == true);
            REQUIRE(r1.data == "data1");
            auto r2 = co_await zk->getData("/test_concurrent_2");
            REQUIRE(r2.ok() == true);
            REQUIRE(r2.data == "data2");
            auto r3 = co_await zk->getData("/test_concurrent_3");
            REQUIRE(r3.ok() == true);
            REQUIRE(r3.data == "data3");
            
            co_await zk->deleteNode("/test_concurrent_1");
            co_await zk->deleteNode("/test_concurrent_2");
            co_await zk->deleteNode("/test_concurrent_3");
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("concurrent setData on same node") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            co_await zk->create("/test_concurrent_set", "initial", 0);
            
            auto updateTask = [&](const std::string& data) -> Coro::Task<void> {
                co_await zk->setData("/test_concurrent_set", data);
                co_return;
            };
            
            updateTask("update1").schedule();
            updateTask("update2").schedule();
            updateTask("update3").schedule();
            
            co_await Coro::sleep_for(std::chrono::milliseconds(200));
            
            auto result = co_await zk->getData("/test_concurrent_set");
            REQUIRE(result.ok() == true);
            INFO("Final value: " << result.data);
            
            co_await zk->deleteNode("/test_concurrent_set");
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
}

TEST_CASE("ZkClient node exists check", "[zkclient]") {
    SECTION("check existing node via getData") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            co_await zk->create("/test_exists", "test", 0);
            
            auto r1 = co_await zk->getData("/test_exists");
            REQUIRE(r1.ok() == true);
            REQUIRE(r1.rc == ZOK);
            
            co_await zk->deleteNode("/test_exists");
            
            auto r2 = co_await zk->getData("/test_exists");
            REQUIRE(r2.ok() == false);
            REQUIRE(r2.rc == ZNONODE);
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("create duplicate node should fail") {
        auto task = []() -> Coro::Task<void> {
            auto zk = std::make_shared<Coro::ZkClient>();
            zk->setHost("127.0.0.1:2181");
            
            co_await zk->start();
            
            auto r1 = co_await zk->create("/test_duplicate", "data", 0);
            REQUIRE(r1.ok() == true);
            
            auto r2 = co_await zk->create("/test_duplicate", "data", 0);
            REQUIRE(r2.ok() == false);
            REQUIRE(r2.rc == ZNODEEXISTS);
            
            co_await zk->deleteNode("/test_duplicate");
            
            zk->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        Coro::get_event_loop().run_until_complete();
    }
}