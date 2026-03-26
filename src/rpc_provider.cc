#include "../include/rpc/rpc_provider.hpp"
#include "../include/coro.hpp"
#include "../include/net/tcp/tcp_buffer.h"
#include <google/protobuf/message.h>
#include <sstream>

namespace AlphaMin {

// ==================== RpcDispatcher ====================

/**
 * @brief 注册 RPC 服务
 * @param service protobuf 服务指针
 * @details 从服务描述符获取服务名和方法列表并存储
 */
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

/**
 * @brief 解析完整方法名
 * @param full_name 完整方法名(如 "ServiceName.MethodName")
 * @param service_name 输出：服务名
 * @param method_name 输出：方法名
 * @return 解析是否成功
 */
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

/**
 * @brief 分发 RPC 请求
 * @param request TinyPB 协议格式的请求
 * @param response TinyPB 协议格式的响应
 * @details 解析方法名，查找服务，调用对应方法，将结果写入响应
 */
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
    
    // 解析请求数据
    google::protobuf::Message* req_msg = info.service->GetRequestPrototype(method).New();
    if (!req_msg->ParseFromString(request->m_pb_data)) {
        response->m_err_code = 4;
        response->m_err_info = "parse request error";
        delete req_msg;
        return;
    }
    
    // 创建响应消息并调用服务方法
    google::protobuf::Message* rsp_msg = info.service->GetResponsePrototype(method).New();
    
    info.service->CallMethod(method, nullptr, req_msg, rsp_msg, nullptr);
    
    // 序列化响应
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

/**
 * @brief 构造函数
 * @details 初始化 ZooKeeper 客户端和 RPC 分发器
 */
RpcProvider::RpcProvider() 
    : m_zkClient(std::make_shared<ZkClient>()),
      m_dispatcher(std::make_unique<RpcDispatcher>()) {
}

/**
 * @brief 析构函数
 * @details 停止服务并关闭 ZooKeeper 连接
 */
RpcProvider::~RpcProvider() {
    stop();
    if (m_zkClient) {
        m_zkClient->close();
    }
}

/**
 * @brief 设置 ZooKeeper 主机地址
 */
void RpcProvider::setZkHost(const std::string& host) {
    m_zkHost = host;
}

/**
 * @brief 设置监听端口
 */
void RpcProvider::setPort(int port) {
    m_port = port;
}

/**
 * @brief 设置监听 IP
 */
void RpcProvider::setIp(const std::string& ip) {
    m_ip = ip;
}

/**
 * @brief 注册 RPC 服务
 * @param service protobuf 服务指针
 */
void RpcProvider::registerService(google::protobuf::Service* service) {
    m_dispatcher->registerService(service);
}

/**
 * @brief 将服务注册到 ZooKeeper
 * @details 创建 /rpc/service_name 节点，为每个方法创建临时节点存储地址
 * @return 协程Task
 */
Coro::Task<void> RpcProvider::registerToZk() {
    std::string addr = m_ip + ":" + std::to_string(m_port);
    
    for (auto& [service_name, info] : m_dispatcher->getServices()) {
        std::string service_path = "/rpc/" + service_name;
        // 创建服务节点(不存在则创建)
        auto result = co_await m_zkClient->create(service_path, "", 0);
        if (!result.ok() && result.rc != ZNODEEXISTS) {
            continue;
        }
        
        // 为每个方法创建临时节点，存储服务地址
        for (const auto& method : info.methods) {
            std::string method_path = service_path + "/" + method;
            co_await m_zkClient->create(method_path, addr, ZOO_EPHEMERAL);
        }
    }
}

/**
 * @brief 获取本地地址字符串
 */
std::string RpcProvider::getLocalAddr() {
    return m_ip + ":" + std::to_string(m_port);
}

/**
 * @brief 处理客户端连接
 * @param stream TCP 流
 * @details 接收请求，解码，调用分发器处理，编码响应并返回
 */
Coro::Task<void> RpcProvider::handleClient(Coro::net::TcpStream stream) {
    auto coder = std::make_shared<Coro::TinyPBCoder>();
    
    while (!m_stop.load()) {
        std::vector<Coro::AbstarcPortocol::s_ptr> msgs;
        auto buffer = stream.getReadBuffer();
        
        try {
            // 读取数据到缓冲区
            co_await stream.readToBuffer();
            
            // 解码消息
            coder->decode(msgs, buffer);
            
            // 处理每条消息
            for (auto& msg : msgs) {
                auto request = std::dynamic_pointer_cast<Coro::TinyPBProtocol>(msg);
                if (!request) continue;
                
                auto response = std::make_shared<Coro::TinyPBProtocol>();
                
                // 调用分发器处理请求
                m_dispatcher->dispatch(request, response);
                
                // 编码响应并发送
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

/**
 * @brief 启动 RPC 服务
 * @return 协程Task
 * @details 启动 TCP 服务，连接 ZooKeeper，注册服务，接受客户端连接
 */
Coro::Task<void> RpcProvider::start() {
    if (m_started) {
        co_return;
    }
    m_started = true;
    m_stop.store(false);
    
    // 启动 TCP 服务
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", m_port);
    m_tcpService = std::make_unique<Coro::net::TcpService>(std::move(service));
    
    // 连接 ZooKeeper
    m_zkClient->setHost(m_zkHost);
    auto connResult = co_await m_zkClient->start();
    
    // 注册到 ZooKeeper
    if (connResult.ok()) {
        co_await registerToZk();
    }
    
    // 接受客户端连接
    while (!m_stop.load()) {
        auto stream = co_await m_tcpService->accept();
        handleClient(std::move(stream));
    }
    
    co_return;
}

/**
 * @brief 停止 RPC 服务
 */
void RpcProvider::stop() {
    m_stop.store(true);
    if (m_tcpService) {
        m_tcpService.reset();
    }
}

// ==================== ServiceDiscovery ====================

/**
 * @brief 构造函数
 */
ServiceDiscovery::ServiceDiscovery() 
    : m_zkClient(std::make_shared<ZkClient>()) {
}

/**
 * @brief 析构函数
 */
ServiceDiscovery::~ServiceDiscovery() {
    close();
}

/**
 * @brief 设置 ZooKeeper 地址
 */
void ServiceDiscovery::setZkHost(const std::string& host) {
    m_zkHost = host;
}

/**
 * @brief 设置超时时间
 */
void ServiceDiscovery::setTimeout(int timeout) {
    m_timeout = timeout;
}

/**
 * @brief 连接到 ZooKeeper
 */
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

/**
 * @brief 发现指定服务的某个方法
 * @param service_name 服务名称
 * @param method_name 方法名称
 * @return 服务地址
 */
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

/**
 * @brief 发现指定服务的所有方法
 * @param service_name 服务名称
 * @return 所有方法的地址列表
 */
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

/**
 * @brief 关闭连接
 */
void ServiceDiscovery::close() {
    if (m_zkClient) {
        m_zkClient->close();
        m_zkClient.reset();
    }
    m_connected = false;
}

}