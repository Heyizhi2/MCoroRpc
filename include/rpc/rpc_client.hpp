/**
 * @file rpc_client.hpp
 * @brief 多实例 RPC 客户端封装
 * @details 支持：
 *       - 服务发现与动态实例列表
 *       - 负载均衡（轮询/随机）
 *       - 连接池管理
 *       - 故障重试与自动切换
 *       - Watcher 动态感知
 */

#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <set>
#include <shared_mutex>
#include "rpc_channel.hpp"
#include "rpc_provider.hpp"

namespace Coro {

struct RpcClientOptions {
    std::string zkHost = "127.0.0.1:2181";
    int timeoutMs = 3000;
    int maxRetries = 3;                    // 单次调用最大重试次数
    bool enableLoadBalance = true;
    int heartbeatCheckIntervalMs = 2000;  // 健康检查间隔
    int heartbeatTimeoutMs = 15000;     // 健康检查超时
};

class RpcClient : public std::enable_shared_from_this<RpcClient> {
public:
    using ptr = std::shared_ptr<RpcClient>;

    explicit RpcClient(const RpcClientOptions& options = {},
                     LoadBalancer::ptr lb = std::make_shared<RoundRobinLoadBalancer>());
    ~RpcClient();

    void setOptions(const RpcClientOptions& options);

    void setLoadBalancer(LoadBalancer::ptr lb) { m_lb = std::move(lb); }

    void setServiceStatusCallback(std::function<void(const std::string& serviceName, bool isAlive)> callback);

    Coro::Task<void> connectDirect(const std::string& host, int port);

    Coro::Task<void> connectDirect(const net::NetAddr::s_ptr& addr);

    Coro::Task<void> connectWithDiscovery(const std::string& serviceName);

    void disconnect();
    bool isConnected() const;

    bool callMethodSync(const google::protobuf::MethodDescriptor* method,
                      const google::protobuf::Message* request,
                      google::protobuf::Message* response,
                      int timeoutMs = -1);

    Coro::Task<bool> callMethodAsync(const google::protobuf::MethodDescriptor* method,
                                 const google::protobuf::Message* request,
                                 google::protobuf::Message* response,
                                 RpcController* controller);

    net::NetAddr::s_ptr getAddr() const { return m_addr; }
    ServiceDiscovery::ptr getDiscovery() const { return m_discovery; }
    LoadBalancer::ptr getLoadBalancer() const { return m_lb; }

private:
    Coro::Task<std::shared_ptr<RpcChannel>> getOrCreateChannel(const std::string& addr);

    void updateInstances(const std::vector<std::string>& instances);
    void markUnavailable(const std::string& addr);
    void markAvailable(const std::string& addr);

    RpcClientOptions m_options;
    LoadBalancer::ptr m_lb;
    net::NetAddr::s_ptr m_addr;
    std::shared_ptr<ServiceDiscovery> m_discovery;
    std::string m_serviceName;

    mutable std::mutex m_instancesMutex;
    std::vector<std::string> m_instances;
    std::unordered_map<std::string, std::shared_ptr<RpcChannel>> m_channels;

    std::mutex m_failedMutex;
    std::set<std::string> m_failedAddrs;

    std::atomic<bool> m_connected{false};
    std::atomic<bool> m_stop{false};
    std::function<void(const std::string&, bool)> m_statusCallback;
};

class RpcCaller {
public:
    RpcCaller() = default;
    explicit RpcCaller(RpcClient::ptr client);
    void setClient(RpcClient::ptr client);

    template<typename Request, typename Response>
    bool call(const google::protobuf::MethodDescriptor* method,
              const Request& request,
              Response* response,
              int timeoutMs = -1) {
        if (!m_client || !m_client->isConnected()) {
            return false;
        }
        return m_client->callMethodSync(method, &request, response, timeoutMs);
    }

private:
    RpcClient::ptr m_client;
};

}