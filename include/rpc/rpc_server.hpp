/**
 * @file rpc_server.hpp
 * @brief 简化的 RPC 服务器封装
 */

#pragma once
#include <google/protobuf/service.h>
#include <memory>
#include <string>
#include <atomic>
#include "rpc_provider.hpp"

namespace Coro {

class RpcServer {
public:
    using ptr = std::shared_ptr<RpcServer>;

    RpcServer(int port, const std::string& zkHost = "127.0.0.1:2181");
    ~RpcServer();

    void setAddress(const std::string& ip, int port);
    void setZkHost(const std::string& host);
    void setWorkerCount(int count);
    void registerService(google::protobuf::Service* service);

    Task<void> start();
    void stop();
    bool isRunning() const { return m_running.load(); }

    net::NetAddr::s_ptr getAddr() const { return m_addr; }

private:
    net::NetAddr::s_ptr m_addr;
    std::string m_zkHost;
    int m_port;
    int m_workerCount = 4;
    std::atomic<bool> m_running{false};

    RpcProvider::ptr m_provider;
    std::vector<google::protobuf::Service*> m_services;
};

}
