/**
 * @file rpc_server.cc
 * @brief RPC 服务器实现
 */

#include "rpc/rpc_server.hpp"

namespace Coro {

RpcServer::RpcServer(int port, const std::string& zkHost)
    : m_port(port), m_zkHost(zkHost) {
    m_addr = net::IPNetAddr::Create("0.0.0.0", port);
    m_provider = std::make_shared<RpcProvider>();
}

RpcServer::~RpcServer() {
    stop();
}

void RpcServer::setAddress(const std::string& ip, int port) {
    m_port = port;
    m_addr = net::IPNetAddr::Create(ip, port);
}

void RpcServer::setZkHost(const std::string& host) {
    m_zkHost = host;
}

void RpcServer::setWorkerCount(int count) {
    m_workerCount = count;
}

void RpcServer::registerService(google::protobuf::Service* service) {
    m_services.push_back(service);
}

Task<void> RpcServer::start() {
    if (m_running.load()) {
        co_return;
    }

    m_provider->setIp("0.0.0.0");
    m_provider->setPort(m_port);
    m_provider->setZkHost(m_zkHost);
    m_provider->setWorkerCount(m_workerCount);

    for (auto* svc : m_services) {
        m_provider->registerService(svc);
    }

    m_running.store(true);
    co_await m_provider->start();
}

void RpcServer::stop() {
    if (!m_running.load()) {
        return;
    }
    m_provider->stop();
    m_running.store(false);
}

}
