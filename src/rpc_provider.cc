/**
 * @file rpc_provider.cc
 * @brief RPC 服务提供者实现
 * @details 实现 RpcDispatcher、RpcProvider 和 ServiceDiscovery 类的所有方法：
 *       - RpcDispatcher：服务注册与请求分发
 *       - RpcProvider：TCP 监听、ZooKeeper 注册、worker 协程处理
 *       - ServiceDiscovery：服务发现、实例列表获取、Watcher 设置
 */

#include "../include/rpc/rpc_provider.hpp"
#include "../include/rpc/rpc_context.h"
#include "../include/coro.hpp"
#include "../include/net/tcp/tcp_buffer.h"
#include "../include/net/tcp/net_addr.h"
#include "../include/utils/object_pool.hpp"
#include <google/protobuf/message.h>
#include <spdlog/fmt/bundled/base.h>
#include <sstream>

namespace Coro {

// ==================== RpcDispatcher ====================

/**
 * @brief 注册 RPC 服务
 * @param service protobuf 服务指针
 * @details 从服务描述符获取服务名和方法列表并存储
 */
void RpcDispatcher::registerService(google::protobuf::Service* service) {
    const auto* desc = service->GetDescriptor();
    std::string service_name = desc->full_name();
    
    // printf("[Dispatcher] registerService: full_name=%s, method_count=%d\n", 
    //        service_name.c_str(), desc->method_count());
    // fflush(stdout);
    
    RpcServiceInfo info;
    info.service = service;
    info.service_name = service_name;
    
    for (int i = 0; i < desc->method_count(); ++i) {
        info.methods.push_back(desc->method(i)->name());
    }
    
    m_services[service_name] = std::move(info);
    
    //printf("[Dispatcher] Registered service, m_services.size=%zu\n", m_services.size());
    // for (auto& kv : m_services) {
    //     printf("  - %s\n", kv.first.c_str());
    // }
    // fflush(stdout);
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
    
    // 从后往前找最后一个 '.'，因为服务名可能包含 '.'
    size_t pos = full_name.rfind('.');
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
 * @param ctx RPC 上下文
 * @details 解析方法名，查找服务，调用对应方法，将结果写入响应
 */
void RpcDispatcher::dispatch(std::shared_ptr<Coro::TinyPBProtocol> request, 
                             std::shared_ptr<Coro::TinyPBProtocol> response,
                             RpcContext::s_ptr ctx) {
    response->m_msg_id = request->m_msg_id;
    response->m_method_name = request->m_method_name;
    
    if (ctx) {
        ctx->setMsgId(request->m_msg_id);
    }
    
    std::string service_name, method_name;
    if (!parseServiceFullName(request->m_method_name, service_name, method_name)) {
        response->m_err_code = 1;
        response->m_err_info = "parse service name error";
        if (ctx) ctx->setFailed(1, "parse service name error");
        return;
    }
    
    auto it = m_services.find(service_name);
    if (it == m_services.end()) {
        response->m_err_code = 2;
        response->m_err_info = "service not found: " + service_name;
        if (ctx) ctx->setFailed(2, "service not found: " + service_name);
        return;
    }
    
    auto& info = it->second;
    const auto* desc = info.service->GetDescriptor();
    const google::protobuf::MethodDescriptor* method = desc->FindMethodByName(method_name);
    
    if (!method) {
        response->m_err_code = 3;
        response->m_err_info = "method not found: " + method_name;
        if (ctx) ctx->setFailed(3, "method not found: " + method_name);
        return;
    }
    
    google::protobuf::Message* req_msg = info.service->GetRequestPrototype(method).New();
    if (!req_msg->ParseFromString(request->m_pb_data)) {
        response->m_err_code = 4;
        response->m_err_info = "parse request error";
        if (ctx) ctx->setFailed(4, "parse request error");
        delete req_msg;
        return;
    }
    
    google::protobuf::Message* rsp_msg = info.service->GetResponsePrototype(method).New();
    
    //printf("[Provider] About to call CallMethod\n");
    //fflush(stdout);
    
    struct NoOpClosure : public google::protobuf::Closure {
        void Run() override {}
    };
    NoOpClosure noop;
    info.service->CallMethod(method, nullptr, req_msg, rsp_msg, &noop);
    // printf("[Provider] CallMethod returned, about to serialize\n");
    // fflush(stdout);
    
    if (!rsp_msg->SerializeToString(&response->m_pb_data)) {
        response->m_err_code = 5;
        response->m_err_info = "serialize response error";
        if (ctx) ctx->setFailed(5, "serialize response error");
    } else {
        response->m_err_code = 0;
        //printf("[Provider] Response pb_data size = %ld\n", response->m_pb_data.size());
        if (ctx) {
            ctx->setErrCode(0);
            ctx->setFinished(true);
        }
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
 * @details 使用临时子节点结构: /rpc/services/{service_name}/{ip:port}
 *        每个服务实例对应一个临时子节点，节点名为 ip:port
 *        临时子节点会在会话断开时自动删除
 * @return 协程Task
 */
Coro::Task<void> RpcProvider::registerToZk() {
    if (!m_zkClient || !m_zkClient->isConnected()) {
        co_return;
    }
    
    std::string addr = m_ip + ":" + std::to_string(m_port);
    
    auto ignoreExists = [](auto result) -> ZkResult {
        if (result.rc == ZNODEEXISTS) {
            return ZkResult{ZOK, "", ""};
        }
        return result;
    };
    
    // 先创建 /rpc 父节点（持久节点）
    ignoreExists(co_await m_zkClient->create("/rpc", "", 0));
    
    // 创建 /rpc/services 根节点（持久节点）
    ignoreExists(co_await m_zkClient->create("/rpc/services", "", 0));
    
    // 为每个服务创建临时子节点
    for (auto& [service_name, info] : m_dispatcher->getServices()) {
        std::string service_path = "/rpc/services/" + service_name;
        
        // 先创建服务节点（持久节点）
        ignoreExists(co_await m_zkClient->create(service_path, "", 0));
        
        // 再创建服务实例节点（临时顺序节点）
        // 节点路径: /rpc/services/{service_name}/{ip:port}
        // 使用 ZOO_EPHEMERAL 标志，服务实例下线时会自动删除
        auto result = co_await m_zkClient->create(service_path + "/" + addr, "", ZOO_EPHEMERAL);
        
        if (result.ok()) {
            m_registered = true;
            fprintf(stderr, "[Provider] Registered service instance: %s -> %s\n", 
                   service_path.c_str(), addr.c_str());
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
 *          长连接：处理完一个请求后继续等待下一个请求，直到连接关闭
 */
Coro::Task<void> RpcProvider::handleClient(Coro::net::TcpStream stream) {
    auto coder = std::make_shared<Coro::TinyPBCoder>();
    
    auto localAddr = std::make_shared<Coro::net::IPNetAddr>(m_ip, m_port);
    
    std::shared_ptr<Coro::TinyPBProtocol> response;
    std::shared_ptr<RpcContext> ctx;
    
    while (!m_stop.load()) {
        std::vector<Coro::AbstarcPortocol::s_ptr> msgs;
        auto buffer = stream.getReadBuffer();
        
        try {
            // printf("[Provider] Before readToBuffer, buffer readable=%ld\n", (long)buffer->readAble());
            // fflush(stdout);
            
            co_await stream.readToBuffer();
            
            // printf("[Provider] After readToBuffer, buffer readable=%ld\n", (long)buffer->readAble());
            // fflush(stdout);
            
            if (buffer->readAble() == 0) {
                // 连接关闭
                break;
            }
            
            coder->decode(msgs, buffer);
            // printf("[Provider] Decoded %ld messages\n", (long)msgs.size());
            // fflush(stdout);
            
            for (auto& msg : msgs) {
                auto request = std::dynamic_pointer_cast<Coro::TinyPBProtocol>(msg);
                if (!request) {
                    // printf("[Provider] Invalid request\n");
                    continue;
                }
                
                // printf("[Provider] Request: method=%s, pb_size=%ld\n", 
                //        request->m_method_name.c_str(), (long)request->m_pb_data.size());
                // fflush(stdout);
                
                auto response = getProtocol();
                auto ctx = getContext();
                ctx->setLocalAddr(localAddr);
                
                auto peerAddr = stream.peerAddr();
                if (peerAddr) {
                    ctx->setPeerAddr(peerAddr);
                }
                
                m_dispatcher->dispatch(request, response, ctx);
                // printf("[Provider] dispatch done, err_code=%d\n", response->m_err_code);
                // fflush(stdout);
                
                std::vector<Coro::AbstarcPortocol::s_ptr> responses;
                responses.push_back(response);
                
                auto out_buf = std::make_shared<Coro::net::TcpBuffer>(1024);
                coder->encode(responses, out_buf);
                // printf("[Provider] encode done\n");
                // fflush(stdout);
                
                std::vector<char> data(out_buf->m_buffer.begin() + out_buf->readIndex(), 
                               out_buf->m_buffer.begin() + out_buf->writeIndex());
                // printf("[Provider] data prepared, size=%ld\n", (long)data.size());
                // fflush(stdout);
                co_await stream.write(data);
                // printf("[Provider] Response written, size=%ld\n", (long)data.size());
                // fflush(stdout);
            }
            
            recycleProtocol(response);
            recycleContext(ctx);
            
            // 清空读缓冲区，准备接受下一个请求
            buffer->moveReadIndex(buffer->readAble());
            
        } catch (const std::exception& e) {
            // printf("[Provider] exception in handleClient: %s\n", e.what());
            // fflush(stdout);
            break;
            break;
        }
    }
    
    stream.close();
}

/**
 * @brief 启动 RPC 服务
 * @return 协程Task
 * @details 启动 TCP 服务，连接 ZooKeeper，注册服务，接受客户端连接
 *         使用 MPMC 模式：accept 作为生产者，多个 worker 作为消费者
 */
Coro::Task<void> RpcProvider::start() {
    if (m_started) {
        co_return;
    }
    m_started = true;
    m_stop.store(false);
    
    // 创建客户端连接 Channel (MPMC 模式)
    m_client_channel = std::make_unique<Channel<Coro::net::TcpStream>>(100);
    
    // 启动 TCP 服务
    // printf("[Provider] Starting TCP service on port %d...\n", m_port);
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", m_port);
    m_tcpService = std::make_unique<Coro::net::TcpService>(std::move(service));
    // printf("[Provider] TCP service started\n");
    
    // 连接 ZooKeeper (可选)
    if (!m_zkHost.empty()) {
        m_zkClient->setHost(m_zkHost);
        auto connResult = co_await m_zkClient->start();
        // fprintf(stderr, "[Provider] ZK connect: rc=%d\n", connResult.rc);
        
        // 注册到 ZooKeeper
        if (connResult.ok()) {
            co_await registerToZk();
            
            // 注意：使用临时子节点后，不再需要心跳机制
            // 临时节点会在会话断开时自动删除
        }
    }
    
    // 启动多个 worker 协程作为消费者
    //printf("[Provider] Starting %d workers...\n", m_worker_count);
    for (int i = 0; i < m_worker_count; ++i) {
        auto worker = [this](int id) -> Coro::Task<void> {
           fmt::println("[Provider] Worker {} started\n", id);
            fflush(stdout);
            while (!m_stop.load()) {
                try {
                    auto stream = co_await this->m_client_channel->recv();
                    // printf("[Provider] Worker %d: got client fd=%d\n", id, stream.fd());
                    // fflush(stdout);
                    co_await this->handleClient(std::move(stream));
                    // printf("[Provider] Worker %d: handleClient finished\n", id);
                    // fflush(stdout);
                } catch (const ChannelClosedException& e) {
                    //printf("[Provider] Worker %d: channel closed\n", id);
                    break;
                } catch (...) {
                   // printf("[Provider] Worker %d: exception\n", id);
                }
            }
            // printf("[Provider] Worker %d: exiting\n", id);
            // fflush(stdout);
        };
        worker(i).schedule();
    }
    
    // accept 循环作为生产者
    //printf("[Provider] Entering accept loop (producer)\n");
    fflush(stdout);
    
    while (!m_stop.load()) {
        Coro::net::TcpStream stream{-1};
        try {
            stream = co_await m_tcpService->accept(std::chrono::milliseconds(24*60*60));
        } catch (const Coro::TimeoutException&) {
            continue;
        } catch (...) {
            break;
        }
        if (m_stop.load()) break;
        
        // 发送到 Channel (生产者) - 使用异步send
        co_await m_client_channel->send(std::move(stream));
    }
    
    // 关闭 Channel
    m_client_channel->close();
    
    co_return;
}

/**
 * @brief 停止 RPC 服务
 */
void RpcProvider::stop() {
    m_stop.store(true);
    m_registered = false;
    // 从 ZK 删除注册的节点（使用临时协程）
    if (m_zkClient && m_zkClient->isConnected()) {
        m_zkClient->close();
    }
    
    // 先关闭 TCP service，让 accept 循环退出
    if (m_tcpService) {
        m_tcpService->close();
    }
    // 再关闭 channel，让 worker 退出 recv
    if (m_client_channel) {
        m_client_channel->close();
        m_client_channel.reset();
    }
    // 最后销毁 TCP service
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
 * @details 从 ZooKeeper 获取服务的所有实例，随机选择一个实现负载均衡
 */
Coro::Task<std::string> ServiceDiscovery::discover(const std::string& service_name, 
                                                    const std::string& method_name) {
    if (!m_connected) {
        co_return std::string();
    }
    
    auto instances = co_await getInstances(service_name);
    if (instances.empty()) {
        co_return std::string();
    }
    
    // 随机选择一个实例，实现简单的负载均衡
    static std::atomic<size_t> roundRobin{0};
    size_t index = roundRobin.fetch_add(1) % instances.size();
    co_return std::string(instances[index]);
}

/**
 * @brief 获取服务的所有实例地址
 * @param service_name 服务名称
 * @return 所有实例的地址列表
 * @details 从 ZooKeeper 获取 /rpc/services/{service_name} 的所有临时子节点
 */
Coro::Task<std::vector<std::string>> ServiceDiscovery::getInstances(const std::string& service_name) {
    if (!m_connected) {
        std::vector<std::string> empty;
        co_return empty;
    }
    
    std::string path = "/rpc/services/" + service_name;
    auto result = co_await m_zkClient->getChildren(path, false);
    
    if (!result.ok()) {
        std::vector<std::string> empty;
        co_return empty;
    }
    
    std::vector<std::string> instances;
    std::stringstream ss(result.data);
    std::string instance;
    while (std::getline(ss, instance, ',')) {
        if (!instance.empty()) {
            instances.push_back(instance);
        }
    }
    
    co_return instances;
}

/**
 * @brief 获取服务节点数据
 */
Coro::Task<ZkResult> ServiceDiscovery::getData(const std::string& path) {
    if (!m_connected) {
        co_return ZkResult{-1, "", "not connected"};
    }
    
    auto result = co_await m_zkClient->getData(path, false);
    co_return std::move(result);
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
    auto result = co_await m_zkClient->getChildren(path, false);
    
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
 * @brief 设置服务变更 watcher
 * @details 监听服务节点下子节点的变化，实现动态感知
 */
void ServiceDiscovery::setServiceWatcher(
    const std::string& service_name,
    std::function<void(const std::vector<std::string>&)> callback) {
    if (!m_zkClient || !m_connected) {
        return;
    }
    
    m_serviceWatcher = std::move(callback);
    std::string path = "/rpc/services/" + service_name;
    
    // 设置子节点变化 watcher
    m_zkClient->setChildrenWatcher(path, 
        [this](const std::vector<std::string>& children) {
            // fprintf(stderr, "[Watcher] children changed, count=%zu\n", children.size());
            if (m_serviceWatcher) {
                m_serviceWatcher(children);
            }
        });
    
    // 立即获取一次，触发 watcher 注册
    m_zkClient->getChildren(path, true);
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