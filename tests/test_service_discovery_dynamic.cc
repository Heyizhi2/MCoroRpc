/*
 * @Author: yizhi
 * @Date: 2026-04-23
 * @Description: 服务注册与发现动态感知测试
 */
#include "../include/coro.hpp"
#include "../include/rpc/rpc_provider.hpp"
#include "../include/coro/sleep.hpp"
#include <catch2/catch.hpp>
#include <chrono>
#include <atomic>
#include <vector>

using namespace Coro;

TEST_CASE("Service registration ephemeral node", "[service_discovery]") {
    SECTION("provider creates ephemeral child node") {
        auto task = []() -> Task<void> {
            auto provider = std::make_shared<RpcProvider>();
            provider->setZkHost("127.0.0.1:2181");
            provider->setIp("127.0.0.1");
            provider->setPort(19999);
            
            auto startTask = provider->start();
            startTask.schedule();
            
            co_await sleep_for(std::chrono::milliseconds(500));
            
            provider->stop();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        get_event_loop().run_until_complete();
        
        INFO("Ephemeral node test completed");
    }
}

TEST_CASE("ServiceDiscovery getInstances", "[service_discovery]") {
    SECTION("get service instances") {
        auto task = []() -> Task<void> {
            auto discovery = std::make_shared<ServiceDiscovery>();
            discovery->setZkHost("127.0.0.1:2181");
            
            co_await discovery->connect();
            
            // 获取不存在的服务应返回空列表
            auto instances = co_await discovery->getInstances("NonExistentService");
            INFO("NonExistentService instances: " << instances.size());
            REQUIRE(instances.empty());
            
            discovery->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        get_event_loop().run_until_complete();
    }
}

TEST_CASE("Dynamic service change detection", "[service_discovery]") {
    SECTION("detect service instance changes") {
        std::atomic<int> instanceCount{0};
        std::atomic<bool> changeDetected{false};
        std::vector<std::string> lastInstances;
        
        auto serverTask = []() -> Task<void> {
            auto provider = std::make_shared<RpcProvider>();
            provider->setZkHost("127.0.0.1:2181");
            provider->setIp("127.0.0.1");
            provider->setPort(19998);
            
            auto startTask = provider->start();
            startTask.schedule();
            
            // 服务运行一段时间后自动停止
            co_await sleep_for(std::chrono::milliseconds(1000));
            
            provider->stop();
            co_return;
        };
        
        auto clientTask = [&instanceCount, &changeDetected, &lastInstances]() -> Task<void> {
            auto discovery = std::make_shared<ServiceDiscovery>();
            discovery->setZkHost("127.0.0.1:2181");
            
            co_await discovery->connect();
            
            // 设置 watcher 监听服务变化
            discovery->setServiceWatcher("testrpc.Calculator",
                [&instanceCount, &changeDetected, &lastInstances](const std::vector<std::string>& instances) {
                    instanceCount.store(static_cast<int>(instances.size()));
                    changeDetected.store(true);
                    lastInstances = instances;
                    INFO("Service changed! instances=" << instances.size());
                    for (const auto& inst : instances) {
                        INFO("  - " << inst);
                    }
                });
            
            // 等待服务注册和下线
            co_await sleep_for(std::chrono::milliseconds(2000));
            
            discovery->close();
            co_return;
        };
        
        serverTask().schedule();
        clientTask().schedule();
        get_event_loop().run_until_complete();
        
        // 验证 watcher 被触发
        REQUIRE(changeDetected.load() == true);
        INFO("Final instance count: " << instanceCount.load());
    }
}

TEST_CASE("Service instance address format", "[service_discovery]") {
    SECTION("verify ephemeral node address format") {
        auto task = []() -> Task<void> {
            auto discovery = std::make_shared<ServiceDiscovery>();
            discovery->setZkHost("127.0.0.1:2181");
            
            co_await discovery->connect();
            
            // 检查临时子节点的格式应该是 "ip:port" 而非 "ip:port#timestamp"
            auto instances = co_await discovery->getInstances("testrpc.Calculator");
            
            for (const auto& inst : instances) {
                // 验证格式：应该是 ip:port，不包含 #
                INFO("Instance: " << inst);
                REQUIRE(inst.find('#') == std::string::npos);
                
                // 验证包含冒号分隔的端口号
                auto pos = inst.find(':');
                REQUIRE(pos != std::string::npos);
                
                // 端口应该是纯数字
                std::string portStr = inst.substr(pos + 1);
                for (char c : portStr) {
                    REQUIRE(std::isdigit(c));
                }
            }
            
            discovery->close();
            co_return;
        };
        
        auto t = task();
        t.schedule();
        get_event_loop().run_until_complete();
    }
}