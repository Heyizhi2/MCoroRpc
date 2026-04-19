/**
 * @file rpc_client.hpp
 * @brief 简化的 RPC 客户端封装
 */

#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include "rpc_channel.hpp"
#include "rpc_provider.hpp"

namespace Coro {

struct RpcClientOptions {
    std::string zkHost = "127.0.0.1:2181";
    int timeoutMs = 3000;
    int maxRetries = 3;
    bool enableLoadBalance = true;
    int heartbeatCheckIntervalMs = 2000;  // 心跳检测间隔
    int heartbeatTimeoutMs = 15000;     // 心跳超时时间
};

class RpcClient : public std::enable_shared_from_this<RpcClient> {
public:
    using ptr = std::shared_ptr<RpcClient>;

    explicit RpcClient(const RpcClientOptions& options = {});
    ~RpcClient();

    void setOptions(const RpcClientOptions& options);

    Task<void> connect(const std::string& host, int port);
    Task<void> connect(const net::NetAddr::s_ptr& addr);
    Task<void> connectWithDiscovery(const std::string& serviceName, 
                                    const std::string& methodName = "");

    void disconnect();
    bool isConnected() const;

    void setServiceStatusCallback(std::function<void(const std::string& serviceName, bool isAlive)> callback);

    void callMethod(const google::protobuf::MethodDescriptor* method,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done = nullptr);

    bool callMethodSync(const google::protobuf::MethodDescriptor* method,
                        const google::protobuf::Message* request,
                        google::protobuf::Message* response,
                        int timeoutMs = -1);

    net::NetAddr::s_ptr getAddr() const { return m_addr; }
    ServiceDiscovery::ptr getDiscovery() const { return m_discovery; }
    RpcChannel::s_ptr getChannel() const { return m_channel; }

private:
    Task<std::string> discoverService(const std::string& serviceName,
                                       const std::string& methodName);

    RpcClientOptions m_options;
    net::NetAddr::s_ptr m_addr;
    RpcChannel::s_ptr m_channel;
    ServiceDiscovery::ptr m_discovery;

    std::atomic<bool> m_connected{false};
    std::atomic<bool> m_usingDiscovery{false};
    std::string m_serviceName;
    std::string m_methodName;
    std::function<void(const std::string&, bool)> m_statusCallback;
    std::atomic<bool> m_stop;
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
