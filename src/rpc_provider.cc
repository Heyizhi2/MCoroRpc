#include "../include/rpc/rpc_provider.hpp"
#include "../include/coro.hpp"
#include <sstream>

namespace AlphaMin {

// ==================== RpcProvider ====================

RpcProvider::RpcProvider() {
    m_zkClient = std::make_shared<ZkClient>();
}

RpcProvider::~RpcProvider() {
    if (m_zkClient) {
        m_zkClient->close();
    }
}

void RpcProvider::setZkHost(const std::string& host) {
    m_zkHost = host;
}

void RpcProvider::setPort(int port) {
    m_port = port;
}

void RpcProvider::setIp(const std::string& ip) {
    m_ip = ip;
}

void RpcProvider::registerService(google::protobuf::Service* service) {
    const auto* desc = service->GetDescriptor();
    std::string service_name = desc->name();
    
    RpcServiceInfo info;
    info.service = service;
    info.service_name = service_name;
    
    for (int i = 0; i < desc->method_count(); ++i) {
        info.methods.push_back(desc->method(i)->name());
    }
    
    m_services[service_name] = std::move(info);
}

Coro::Task<void> RpcProvider::registerToZk() {
    std::string addr = m_ip + ":" + std::to_string(m_port);
    
    for (auto& [service_name, info] : m_services) {
        std::string service_path = "/rpc/" + service_name;
        auto result = co_await m_zkClient->create(service_path, "", 0);
        if (!result.ok() && result.rc != ZNODEEXISTS) {
            continue;
        }
        
        for (const auto& method : info.methods) {
            std::string method_path = service_path + "/" + method;
            co_await m_zkClient->create(method_path, addr, ZOO_EPHEMERAL);
        }
    }
}

std::string RpcProvider::getLocalAddr() {
    return m_ip + ":" + std::to_string(m_port);
}

Coro::Task<void> RpcProvider::start() {
    if (m_started) {
        co_return;
    }
    m_started = true;
    
    m_zkClient->setHost(m_zkHost);
    auto connResult = co_await m_zkClient->start();
    
    if (!connResult.ok()) {
        co_return;
    }
    
    co_await registerToZk();
    co_return;
}

// ==================== ServiceDiscovery ====================

ServiceDiscovery::ServiceDiscovery() {
    m_zkClient = std::make_shared<ZkClient>();
}

ServiceDiscovery::~ServiceDiscovery() {
    close();
}

void ServiceDiscovery::setZkHost(const std::string& host) {
    m_zkHost = host;
}

void ServiceDiscovery::setTimeout(int timeout) {
    m_timeout = timeout;
}

Coro::Task<void> ServiceDiscovery::connect() {
    if (m_connected) {
        co_return;
    }
    
    m_zkClient->setHost(m_zkHost);
    m_zkClient->setTimeout(m_timeout);
    
    auto result = co_await m_zkClient->start();
    if (result.ok()) {
        m_connected = true;
    }
    
    co_return;
}

Coro::Task<std::string> ServiceDiscovery::discover(const std::string& service_name, 
                                                    const std::string& method_name) {
    if (!m_connected) {
        co_return std::string();
    }
    
    std::string path = "/rpc/" + service_name + "/" + method_name;
    auto result = co_await m_zkClient->getData(path);
    
    if (!result.ok()) {
        co_return std::string();
    }
    
    std::string data = std::move(result.data);
    co_return data;
}

Coro::Task<std::vector<std::string>> ServiceDiscovery::discoverAllMethods(const std::string& service_name) {
    std::vector<std::string> methods;
    
    if (!m_connected) {
        co_return std::vector<std::string>();
    }
    
    std::string path = "/rpc/" + service_name;
    auto result = co_await m_zkClient->getChildren(path);
    
    if (!result.ok()) {
        co_return std::vector<std::string>();
    }
    
    std::stringstream ss(result.data);
    std::string method;
    while (std::getline(ss, method, ',')) {
        methods.push_back(method);
    }
    
    co_return std::move(methods);
}

void ServiceDiscovery::close() {
    if (m_zkClient) {
        m_zkClient->close();
        m_zkClient.reset();
    }
    m_connected = false;
}

}