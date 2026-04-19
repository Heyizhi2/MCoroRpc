#include "../include/rpc/rpc_provider.hpp"
#include "../include/coro.hpp"
#include "../include/coro/sleep.hpp"
#include <spdlog/fmt/bundled/core.h>
#include <chrono>
#include <memory>

int main() {
    fmt::println("=== ZooKeeper Watcher Test ===\n");
    fmt::println("This test requires ZooKeeper running on 127.0.0.1:2181");
    fmt::println("1. Start this program first");
    fmt::println("2. Start an RPC provider to register service");
    fmt::println("3. Stop the provider, observe callback in this window");
    fmt::println("");

    auto discovery = std::make_shared<Coro::ServiceDiscovery>();
    discovery->setZkHost("127.0.0.1:2181");

    auto task = [&]() -> Coro::Task<void> {
        fmt::println("[Test] Connecting to ZooKeeper...");
        co_await discovery->connect();
        fmt::println("[Test] Connected!");

        // 使用 rpc_server_new 注册的服务名
        std::string serviceName = "testrpc.Calculator";
        std::string path = "/rpc/services/" + serviceName;
        
        // 心跳超时检测（3个心跳周期 = 15秒）
        const int heartbeat_timeout_ms = 15000;
        const int check_interval_ms = 2000;
        int last_timestamp = 0;
        bool last_alive = false;
        
        // 心跳检测函数
        auto checkHeartbeat = [&]() -> Coro::Task<bool> {
            auto result = co_await discovery->getData(path);
            if (!result.ok() || result.data.empty()) {
                co_return false;
            }
            
            // 解析 timestamp
            size_t pos = result.data.find('#');
            if (pos == std::string::npos) {
                co_return false;
            }
            
            int timestamp = 0;
            try {
                timestamp = std::stoi(result.data.substr(pos + 1));
            } catch (...) {
                co_return false;
            }
            
            int now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            
            bool alive = (now - timestamp) < heartbeat_timeout_ms;
            co_return alive;
        };
        
        discovery->setServiceWatcher(serviceName,
            [](const std::vector<std::string>& nodes) {
                fmt::println("\n*** Service changed! ***");
                fmt::println("Current {} nodes:", nodes.size());
                for (size_t i = 0; i < nodes.size(); ++i) {
                    fmt::println("  [{}] {}", i, nodes[i]);
                }
                fmt::println("");
            });

        fmt::println("[Test] Starting heartbeat check every {}ms...\n", check_interval_ms);
        fmt::println("Press Ctrl+C to exit\n");

        while (discovery->isConnected()) {
            bool alive = co_await checkHeartbeat();
            
            if (alive != last_alive) {
                if (alive) {
                    fmt::println("\n*** Service UP! ***\n");
                } else {
                    fmt::println("\n*** Service DOWN! ***\n");
                }
                last_alive = alive;
            }
            
            if (alive) {
                fmt::print(".");  // alive indicator
                fflush(stdout);
            }
            
            co_await Coro::sleep_for(std::chrono::milliseconds(check_interval_ms));
        }
        
        discovery->close();
    };

    auto t = task();
    t.schedule();
    Coro::get_event_loop().run_until_complete();

    return 0;
}