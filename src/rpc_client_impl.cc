/**
 * @file rpc_client.cc
 * @brief RPC 客户端实现
 */

#include "../include/rpc/rpc_client.hpp"
#include <chrono>
#include <thread>

namespace Coro {

RpcClient::RpcClient(const RpcClientOptions& options)
    : m_options(options), m_stop(false) {
    m_discovery = std::make_shared<ServiceDiscovery>();
    m_discovery->setZkHost(options.zkHost);
}

RpcClient::~RpcClient() {
    disconnect();
}

void RpcClient::setOptions(const RpcClientOptions& options) {
    m_options = options;
    m_discovery->setZkHost(options.zkHost);
}

Task<void> RpcClient::connect(const std::string& host, int port) {
    m_addr = net::IPNetAddr::Create(host, port);
    co_await connect(m_addr);
}

Task<void> RpcClient::connect(const net::NetAddr::s_ptr& addr) {
    if (m_connected.load()) {
        co_return;
    }

    m_addr = addr;
    m_channel = std::make_shared<RpcChannel>(addr);
    m_channel->setTimeout(m_options.timeoutMs);

    co_await m_channel->connect();
    m_connected.store(true);
    m_usingDiscovery.store(false);
}

Task<void> RpcClient::connectWithDiscovery(const std::string& serviceName,
                                            const std::string& methodName) {
    if (!m_discovery->isConnected()) {
        co_await m_discovery->connect();
    }

    m_serviceName = serviceName;
    m_methodName = methodName;

    auto addrStr = co_await discoverService(serviceName, methodName);
    if (addrStr.empty()) {
        fprintf(stderr, "[RpcClient] discover service failed: %s\n", serviceName.c_str());
        m_connected.store(false);
        co_return;
    }

    auto colonPos = addrStr.find(':');
    if (colonPos == std::string::npos) {
        throw std::runtime_error("invalid address: " + addrStr);
    }

    std::string host = addrStr.substr(0, colonPos);
    int port = std::stoi(addrStr.substr(colonPos + 1));

    m_addr = net::IPNetAddr::Create(host, port);
    m_channel = std::make_shared<RpcChannel>(m_addr);
m_channel->setTimeout(m_options.timeoutMs);
 
    co_await m_channel->connect();
    m_connected.store(true);
    m_usingDiscovery.store(true);
    
    // 启动心跳检测
    if (m_statusCallback) {
        auto heartbeatTask = [this, serviceName, methodName]() -> Coro::Task<void> {
            std::string path = "/rpc/services/" + serviceName;
            bool lastAlive = false;
            m_statusCallback(serviceName, true);  // 立即触发一次
            
            while (!m_stop.load() && m_usingDiscovery.load()) {
                co_await Coro::sleep_for(std::chrono::milliseconds(m_options.heartbeatCheckIntervalMs));
                
                if (m_stop.load()) break;
                
                auto result = co_await m_discovery->getData(path);
                bool alive = false;
                
                if (result.ok() && !result.data.empty()) {
                    auto pos = result.data.find('#');
                    if (pos != std::string::npos) {
                        try {
                            int timestamp = std::stoi(result.data.substr(pos + 1));
                            int now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now().time_since_epoch()).count();
                            alive = (now - timestamp) < m_options.heartbeatTimeoutMs;
                        } catch (...) {
                            alive = false;
                        }
                    }
                }
                
                if (alive != lastAlive) {
                    m_statusCallback(serviceName, alive);
                    lastAlive = alive;
                }
            }
        };
        heartbeatTask().schedule();
    }
}

void RpcClient::disconnect() {
    m_stop.store(true);
    
    if (!m_connected.load()) {
        return;
    }
    if (m_channel) {
        m_channel->close();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    m_connected.store(false);
    m_usingDiscovery.store(false);
}

bool RpcClient::isConnected() const {
    return m_connected.load() && m_channel && m_channel->isConnected();
}

void RpcClient::setServiceStatusCallback(
    std::function<void(const std::string& serviceName, bool isAlive)> callback) {
    m_statusCallback = callback;
}

void RpcClient::callMethod(const google::protobuf::MethodDescriptor* method,
                           const google::protobuf::Message* request,
                           google::protobuf::Message* response,
                           google::protobuf::Closure* done) {
    if (!m_channel || !m_channel->isConnected()) {
        if (done) done->Run();
        return;
    }

    auto controller = std::make_shared<RpcController>();
    controller->SetTimeout(m_options.timeoutMs);

    m_channel->CallMethod(method, controller.get(), request, response, done);
}

bool RpcClient::callMethodSync(const google::protobuf::MethodDescriptor* method,
                               const google::protobuf::Message* request,
                               google::protobuf::Message* response,
                               int timeoutMs) {
    if (!m_channel || !m_channel->isConnected()) {
        return false;
    }

    auto controller = std::make_shared<RpcController>();
    controller->SetTimeout(timeoutMs > 0 ? timeoutMs : m_options.timeoutMs);

    auto startTime = std::chrono::steady_clock::now();

    m_channel->CallMethod(method, controller.get(), request, response, nullptr);

    auto timeout = std::chrono::milliseconds(controller->GetTimeout());
    while (!controller->Finished()) {
        auto elapsed = std::chrono::steady_clock::now() - startTime;
        if (elapsed >= timeout) {
            controller->SetFailed("timeout");
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return !controller->Failed();
}

Task<std::string> RpcClient::discoverService(const std::string& serviceName,
                                              const std::string& methodName) {
    if (!m_discovery->isConnected()) {
        co_await m_discovery->connect();
    }

    if (!methodName.empty()) {
        auto addr = co_await m_discovery->discover(serviceName, methodName);
        co_return addr;
    } else {
        auto addrs = co_await m_discovery->discoverAllMethods(serviceName);
        co_return addrs.empty() ? "" : addrs[0];
    }
}

RpcCaller::RpcCaller(RpcClient::ptr client) : m_client(client) {}

void RpcCaller::setClient(RpcClient::ptr client) {
    m_client = client;
}

}
