/**
 * @file test_zk_nodes.cc
 * @brief 逐步测试 ZooKeeper 节点创建
 */
#include "../include/coro.hpp"
#include "../include/rpc/rpc_provider.hpp"

int main() {
    printf("=== Step by Step ZK Test ===\n\n");
    fflush(stdout);

    auto task = []() -> Coro::Task<void> {
        auto zk = std::make_shared<Coro::ZkClient>();
        zk->setHost("127.0.0.1:2181");
        
        // 连接
        printf("1. Connecting...\n");
        auto r = co_await zk->start();
        printf("   Result: rc=%d (ZOK=%d)\n", r.rc, ZOK);
        fflush(stdout);
        
        if (!r.ok()) {
            printf("   FAILED\n");
            co_return;
        }
        
        // 检查现有节点
        printf("\n2. Checking existing nodes...\n");
        auto check = co_await zk->getChildren("/rpc", false);
        printf("   /rpc children: '%s' (rc=%d)\n", check.data.c_str(), check.rc);
        fflush(stdout);
        
        // 删除测试节点
        printf("\n3. Cleaning up...\n");
        co_await zk->deleteNode("/rpc/test", -1);
        co_await zk->deleteNode("/rpc", -1);
        co_await Coro::sleep_for(std::chrono::milliseconds(100));
        
        // 创建根节点 - 使用 flags=0 (永久节点)
        printf("\n4. Creating /rpc (permanent)...\n");
        auto r1 = co_await zk->create("/rpc", "", 0);
        printf("   rc=%d (ZOK=%d, ZNODEEXISTS=%d, ZNONODE=%d)\n", r1.rc, ZOK, ZNODEEXISTS, ZNONODE);
        if (r1.rc == ZNONODE) {
            printf("   Parent doesn't exist!\n");
        }
        fflush(stdout);
        
        // 如果成功，创建测试子节点
        if (r1.ok() || r1.rc == ZNODEEXISTS) {
            printf("\n5. Creating /rpc/test (permanent)...\n");
            auto r2 = co_await zk->create("/rpc/test", "", 0);
            printf("   rc=%d\n", r2.rc);
            fflush(stdout);
            
            if (r2.ok() || r2.rc == ZNODEEXISTS) {
                printf("\n6. Creating /rpc/test/instance (EPHEMERAL)...\n");
                auto r3 = co_await zk->create("/rpc/test/instance", "127.0.0.1:8001", ZOO_EPHEMERAL);
                printf("   rc=%d\n", r3.rc);
                if (r3.ok()) {
                    printf("   SUCCESS! path=%s\n", r3.path.c_str());
                } else {
                    printf("   FAILED! error=%s\n", r3.error().c_str());
                }
            }
        }
        
        // 列出最终结果
        printf("\n7. Final listing...\n");
        auto children = co_await zk->getChildren("/rpc/test", false);
        printf("   /rpc/test children: '%s'\n", children.data.c_str());
        fflush(stdout);
        
        // 保持观察
        co_await Coro::sleep_for(std::chrono::seconds(3));
        zk->close();
        co_return;
    };
    
    task().schedule();
    Coro::get_event_loop().run_until_complete();
    
    return 0;
}