#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include "zkclient.hpp"
#include "../coro/task.hpp"
#include "../coro/channel.hpp"

namespace AlphaMin {

struct RpcServiceInfo {
    google::protobuf::Service* service;
    std::string service_name;
    std::vector<std::string> methods;
};

class RpcProvider {
public:
    using ptr = std::shared_ptr<RpcProvider>;

    RpcProvider();
    ~RpcProvider();

    void setZkHost(const std::string& host);
    void setPort(int port);
    void setIp(const std::string& ip);

    void registerService(google::protobuf::Service* service);
    
    Coro::Task<void> start();

private:
    Coro::Task<void> registerToZk();

    std::string getLocalAddr();

    std::string m_zkHost = "127.0.0.1:2181";
    std::string m_ip = "127.0.0.1";
    int m_port = 8000;

    ZkClient::ptr m_zkClient;
    std::unordered_map<std::string, RpcServiceInfo> m_services;
    bool m_started = false;
};

class ServiceDiscovery {
public:
    using ptr = std::shared_ptr<ServiceDiscovery>;

    ServiceDiscovery();
    ~ServiceDiscovery();

    void setZkHost(const std::string& host);
    void setTimeout(int timeout);

    Coro::Task<void> connect();

    Coro::Task<std::string> discover(const std::string& service_name, const std::string& method_name);

    Coro::Task<std::vector<std::string>> discoverAllMethods(const std::string& service_name);

    void close();

    bool isConnected() const { return m_connected; }

private:
    std::string m_zkHost = "127.0.0.1:2181";
    int m_timeout = 30000;
    ZkClient::ptr m_zkClient;
    bool m_connected = false;
};

}