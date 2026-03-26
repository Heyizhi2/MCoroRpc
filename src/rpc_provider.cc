#include "../include/rpc/rpc_provider.hpp"
#include "../include/coro.hpp"
#include "../include/net/tcp/tcp_buffer.h"
#include <google/protobuf/message.h>
#include <sstream>

namespace AlphaMin {

// ==================== RpcDispatcher ====================

void RpcDispatcher::registerService(google::protobuf::Service* service) {
    const auto* desc = service->GetDescriptor();
    std::string service_name = desc->full_name();
    
    RpcServiceInfo info;
    info.service = service;
    info.service_name = service_name;
    
    for (int i = 0; i < desc->method_count(); ++i) {
        info.methods.push_back(desc->method(i)->name());
    }
    
    m_services[service_name] = std::move(info);
}

bool RpcDispatcher::parseServiceFullName(const std::string& full_name, 
                                         std::string& service_name, 
                                         std::string& method_name) {
    if (full_name.empty()) {
        return false;
    }
    
    size_t pos = full_name.find('.');
    if (pos == std::string::npos) {
        return false;
    }
    
    service_name = full_name.substr(0, pos);
    method_name = full_name.substr(pos + 1);
    return true;
}

void RpcDispatcher::dispatch(std::shared_ptr<Coro::TinyPBProtocol> request, 
                             std::shared_ptr<Coro::TinyPBProtocol> response) {
    response->m_msg_id = request->m_msg_id;
    response->m_method_name = request->m_method_name;
    
    std::string service_name, method_name;
    if (!parseServiceFullName(request->m_method_name, service_name, method_name)) {
        response->m_err_code = 1;
        response->m_err_info = "parse service name error";
        return;
    }
    
    auto it = m_services.find(service_name);
    if (it == m_services.end()) {
        response->m_err_code = 2;
        response->m_err_info = "service not found: " + service_name;
        return;
    }
    
    auto& info = it->second;
    const auto* desc = info.service->GetDescriptor();
    const google::protobuf::MethodDescriptor* method = desc->FindMethodByName(method_name);
    
    if (!method) {
        response->m_err_code = 3;
        response->m_err_info = "method not found: " + method_name;
        return;
    }
    
    google::protobuf::Message* req_msg = info.service->GetRequestPrototype(method).New();
    if (!req_msg->ParseFromString(request->m_pb_data)) {
        response->m_err_code = 4;
        response->m_err_info = "parse request error";
        delete req_msg;
        return;
    }
    
    google::protobuf::Message* rsp_msg = info.service->GetResponsePrototype(method).New();
    
    info.service->CallMethod(method, nullptr, req_msg, rsp_msg, nullptr);
    
    if (!rsp_msg->SerializeToString(&response->m_pb_data)) {
        response->m_err_code = 5;
        response->m_err_info = "serialize response error";
    } else {
        response->m_err_code = 0;
    }
    
    delete req_msg;
    delete rsp_msg;
}

// ==================== RpcProvider ====================

RpcProvider::RpcProvider() 
    : m_zkClient(std::make_shared<ZkClient>()),
      m_dispatcher(std::make_unique<RpcDispatcher>()) {
}

RpcProvider::~RpcProvider() {
    stop();
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
    m_dispatcher->registerService(service);
}

Coro::Task<void> RpcProvider::registerToZk() {
    std::string addr = m_ip + ":" + std::to_string(m_port);
    
    for (auto& [service_name, info] : m_dispatcher->getServices()) {
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

Coro::Task<void> RpcProvider::handleClient(Coro::net::TcpStream stream) {
    auto coder = std::make_shared<Coro::TinyPBCoder>();
    
    while (!m_stop.load()) {
        std::vector<Coro::AbstarcPortocol::s_ptr> msgs;
        auto buffer = stream.getReadBuffer();
        
        try {
            co_await stream.readToBuffer();
            
            coder->decode(msgs, buffer);
            
            for (auto& msg : msgs) {
                auto request = std::dynamic_pointer_cast<Coro::TinyPBProtocol>(msg);
                if (!request) continue;
                
                auto response = std::make_shared<Coro::TinyPBProtocol>();
                
                m_dispatcher->dispatch(request, response);
                
                std::vector<Coro::AbstarcPortocol::s_ptr> responses;
                responses.push_back(response);
                
                auto out_buf = std::make_shared<Coro::net::TcpBuffer>(1024);
                coder->encode(responses, out_buf);
                
                std::vector<char> data(out_buf->m_buffer.begin() + out_buf->readIndex(), 
                               out_buf->m_buffer.begin() + out_buf->writeIndex());
                co_await stream.write(data);
            }
            
        } catch (...) {
            break;
        }
    }
}

Coro::Task<void> RpcProvider::start() {
    if (m_started) {
        co_return;
    }
    m_started = true;
    m_stop.store(false);
    
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", m_port);
    m_tcpService = std::make_unique<Coro::net::TcpService>(std::move(service));
    
    m_zkClient->setHost(m_zkHost);
    auto connResult = co_await m_zkClient->start();
    
    if (connResult.ok()) {
        co_await registerToZk();
    }
    
    while (!m_stop.load()) {
        auto stream = co_await m_tcpService->accept();
        handleClient(std::move(stream));
    }
    
    co_return;
}

void RpcProvider::stop() {
    m_stop.store(true);
    if (m_tcpService) {
        m_tcpService.reset();
    }
}

// ==================== ServiceDiscovery ====================

ServiceDiscovery::ServiceDiscovery() 
    : m_zkClient(std::make_shared<ZkClient>()) {
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
    if (!m_connected) {
        co_return std::vector<std::string>();
    }
    
    std::string path = "/rpc/" + service_name;
    auto result = co_await m_zkClient->getChildren(path);
    
    if (!result.ok()) {
        co_return std::vector<std::string>();
    }
    
    std::vector<std::string> methods;
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