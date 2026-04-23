/**
 * @file rpc_client_impl.cc
 * @brief 多实例 RPC 客户端实现
 */

#include "../include/rpc/rpc_client.hpp"
#include "../include/coro/sleep.hpp"
#include <chrono>
#include <thread>

namespace Coro {

RpcClient::RpcClient(const RpcClientOptions& options, LoadBalancer::ptr lb)
    : m_options(options), m_lb(std::move(lb)), m_stop(false) {
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

Coro::Task<void> RpcClient::connectWithDiscovery(const std::string& serviceName) {
    if (!m_discovery->isConnected()) {
        co_await m_discovery->connect();
    }
    
    m_serviceName = serviceName;
    
    auto instances = co_await m_discovery->getInstances(serviceName);
    updateInstances(instances);
    
    m_discovery->setServiceWatcher(serviceName,
        [this](const std::vector<std::string>& newInstances) {
            updateInstances(newInstances);
            if (m_statusCallback) {
                m_statusCallback(m_serviceName, !newInstances.empty());
            }
        });
    
    m_connected.store(!m_instances.empty());
    co_return;
}

void RpcClient::disconnect() {
    m_stop.store(true);
    m_connected.store(false);
    
    std::unique_lock lock(m_instancesMutex);
    for (auto& [addr, channel] : m_channels) {
        if (channel) {
            channel->close();
        }
    }
    m_channels.clear();
    m_instances.clear();
    lock.unlock();
    
    if (m_discovery) {
        m_discovery->close();
    }
}

bool RpcClient::isConnected() const {
    if (!m_connected.load() || m_instances.empty()) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(m_instancesMutex);
    for (const auto& [addr, channel] : m_channels) {
        if (channel && channel->isConnected()) {
            return true;
        }
    }
    return false;
}

void RpcClient::setServiceStatusCallback(
    std::function<void(const std::string& serviceName, bool isAlive)> callback) {
    m_statusCallback = std::move(callback);
}

Coro::Task<void> RpcClient::connectDirect(const std::string& host, int port) {
    auto addr = net::IPNetAddr::Create(host, port);
    co_await connectDirect(addr);
}

Coro::Task<void> RpcClient::connectDirect(const net::NetAddr::s_ptr& addr) {
    m_addr = addr;
    m_serviceName = "";

    std::vector<std::string> instances = { addr->toString() };
    updateInstances(instances);
    m_connected.store(true);

    co_return;
}

Coro::Task<std::shared_ptr<RpcChannel>> RpcClient::getOrCreateChannel(const std::string& addr) {
    {
        std::lock_guard<std::mutex> lock(m_instancesMutex);
        auto it = m_channels.find(addr);
        if (it != m_channels.end() && it->second && it->second->isConnected()) {
            std::shared_ptr<RpcChannel> result = it->second;
            co_return result;
        }
    }
    
    auto colonPos = addr.find(':');
    if (colonPos == std::string::npos) {
        co_return std::shared_ptr<RpcChannel>(nullptr);
    }
    
    std::string host = addr.substr(0, colonPos);
    int port = std::stoi(addr.substr(colonPos + 1));
    
    auto channel = std::make_shared<RpcChannel>(host, port);
    channel->setTimeout(m_options.timeoutMs);
    
    try {
        co_await channel->connect();
        
        {
            std::lock_guard<std::mutex> lock(m_instancesMutex);
            m_channels[addr] = channel;
        }
        std::shared_ptr<RpcChannel> result = channel;
        co_return result;
    } catch (const std::exception& e) {
        co_return std::shared_ptr<RpcChannel>(nullptr);
    }
}

void RpcClient::updateInstances(const std::vector<std::string>& instances) {
    std::unique_lock lock(m_instancesMutex);
    
    std::set<std::string> newSet(instances.begin(), instances.end());
    std::vector<std::string> oldInstances = m_instances;
    m_instances = instances;
    
    for (auto it = m_channels.begin(); it != m_channels.end(); ) {
        if (newSet.find(it->first) == newSet.end()) {
            if (it->second) {
                it->second->close();
            }
            it = m_channels.erase(it);
        } else {
            ++it;
        }
    }
    
    lock.unlock();
    
    std::unique_lock failedLock(m_failedMutex);
    m_failedAddrs.clear();
    failedLock.unlock();
    
    if (m_statusCallback && instances != oldInstances) {
        m_statusCallback(m_serviceName, !instances.empty());
    }
}

void RpcClient::markUnavailable(const std::string& addr) {
    std::unique_lock lock(m_failedMutex);
    m_failedAddrs.insert(addr);
}

void RpcClient::markAvailable(const std::string& addr) {
    std::unique_lock lock(m_failedMutex);
    m_failedAddrs.erase(addr);
}

Coro::Task<bool> RpcClient::callMethodAsync(
    const google::protobuf::MethodDescriptor* method,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    RpcController* controller) {
    
    std::vector<std::string> availableInstances;
    std::set<std::string> failedAddrsSnapshot;
    
    {
        std::lock_guard<std::mutex> lock(m_instancesMutex);
        if (m_instances.empty()) {
            co_return false;
        }
        
        {
            std::lock_guard<std::mutex> failedLock(m_failedMutex);
            failedAddrsSnapshot = m_failedAddrs;
        }
        
        for (const auto& addr : m_instances) {
            if (failedAddrsSnapshot.find(addr) == failedAddrsSnapshot.end()) {
                availableInstances.push_back(addr);
            }
        }
        
        if (availableInstances.empty()) {
            co_return false;
        }
    }
    
    std::string targetAddr = m_lb->select(availableInstances);
    
    auto channel = co_await getOrCreateChannel(targetAddr);
    if (!channel) {
        markUnavailable(targetAddr);
        co_return false;
    }
    
    auto ctrl = dynamic_cast<RpcController*>(controller);
    if (!ctrl) {
        co_return false;
    }
    
    try {
        co_await channel->CallMethodAsync(method, ctrl, request, response, nullptr);
        
        if (ctrl->Failed()) {
            markUnavailable(targetAddr);
            co_return false;
        }
        
        co_return true;
    } catch (const std::exception& e) {
        markUnavailable(targetAddr);
        co_return false;
    }
}

bool RpcClient::callMethodSync(
    const google::protobuf::MethodDescriptor* method,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    int timeoutMs) {
    
    auto controller = std::make_shared<RpcController>();
    if (timeoutMs > 0) {
        controller->SetTimeout(timeoutMs);
    }
    
    auto task = callMethodAsync(method, request, response, controller.get());
    task.schedule();
    get_event_loop().run_until_complete();
    
    return !controller->Failed();
}

RpcCaller::RpcCaller(RpcClient::ptr client) : m_client(std::move(client)) {}

void RpcCaller::setClient(RpcClient::ptr client) {
    m_client = std::move(client);
}

}