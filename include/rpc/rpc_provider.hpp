/**
 * @file rpc_provider.hpp
 * @brief RPC 服务提供者
 * @details 提供基于 protobuf 的 RPC 服务框架，支持服务注册、ZooKeeper 服务发现、TCP 通信
 */

#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <random>
#include <functional>
#include "zkclient.hpp"
#include "rpc_context.h"
#include "../net/tcp/net_addr.h"
#include "../coro/task.hpp"
#include "../coro/channel.hpp"
#include "../net/tcpservice.hpp"
#include "../net/tcpstream.hpp"
#include "../coder/tinypb_protocol.hpp"
#include "../coder/tinypb_coder.hpp"

namespace Coro {

/**
 * @brief 负载均衡器接口
 * @details 定义选择服务实例的策略
 */
class LoadBalancer {
public:
    using ptr = std::shared_ptr<LoadBalancer>;
    virtual ~LoadBalancer() = default;
    
    /**
     * @brief 从实例列表中选择一个
     * @param instances 可用的实例列表
     * @return 选择的实例地址，空表示无可用实例
     */
    virtual std::string select(const std::vector<std::string>& instances) = 0;
};

/**
 * @brief 轮询负载均衡器
 * @details 按顺序依次选择每个实例
 */
class RoundRobinLoadBalancer : public LoadBalancer {
public:
    using ptr = std::shared_ptr<RoundRobinLoadBalancer>;
    
    std::string select(const std::vector<std::string>& instances) override {
        if (instances.empty()) {
            return "";
        }
        return instances[m_index.fetch_add(1) % instances.size()];
    }
    
private:
    std::atomic<size_t> m_index{0};
};

/**
 * @brief 随机负载均衡器
 * @details 随机选择一个实例
 */
class RandomLoadBalancer : public LoadBalancer {
public:
    using ptr = std::shared_ptr<RandomLoadBalancer>;
    
    std::string select(const std::vector<std::string>& instances) override {
        if (instances.empty()) {
            return "";
        }
        std::uniform_int_distribution<size_t> dist(0, instances.size() - 1);
        std::random_device rd;
        std::mt19937_64 gen(rd());
        return instances[dist(gen)];
    }
};

/**
 * @brief RPC 服务信息结构
 * @details 存储已注册服务的元数据，包括服务实例、名称和方法列表
 */
struct RpcServiceInfo {
    google::protobuf::Service* service;    ///< protobuf 服务实例指针
    std::string service_name;              ///< 服务名称
    std::vector<std::string> methods;      ///< 服务方法列表
};

/**
 * @brief RPC 消息分发器
 * @details 根据请求中的完整方法名路由到对应的 protobuf 服务处理
 */
class RpcDispatcher {
public:
    /// 智能指针类型别名
    using ptr = std::shared_ptr<RpcDispatcher>;

    /**
     * @brief 注册 RPC 服务
     * @param service protobuf 服务指针
     * @note 服务的所有方法会被自动注册
     */
    void registerService(google::protobuf::Service* service);
    
    /**
     * @brief 分发 RPC 请求
     * @param request TinyPB 协议格式的请求消息
     * @param response TinyPB 协议格式的响应消息
     * @param ctx RPC 上下文
     * @details 解析请求中的完整方法名，调用对应的服务方法
     */
    void dispatch(std::shared_ptr<Coro::TinyPBProtocol> request, 
                  std::shared_ptr<Coro::TinyPBProtocol> response,
                  RpcContext::s_ptr ctx = nullptr);

    /**
     * @brief 获取所有已注册服务
     * @return 服务名到服务信息的映射
     */
    std::unordered_map<std::string, RpcServiceInfo>& getServices() { return m_services; }

private:
    /**
     * @brief 解析完整方法名
     * @param full_name 完整方法名 (如 "ServiceName.MethodName")
     * @param service_name 输出：服务名
     * @param method_name 输出：方法名
     * @return 解析是否成功
     */
    bool parseServiceFullName(const std::string& full_name, 
                              std::string& service_name, 
                              std::string& method_name);

    std::unordered_map<std::string, RpcServiceInfo> m_services;  ///< 服务注册表
};

/**
 * @brief RPC 服务提供者
 * @details 作为 RPC 服务器，接受客户端请求，调用注册的 protobuf 服务处理
 * @note 集成 ZooKeeper 用于服务注册，实现服务发现
 */
class RpcProvider {
public:
    /// 智能指针类型别名
    using ptr = std::shared_ptr<RpcProvider>;

    /// 构造函数
    RpcProvider();
    
    /// 析构函数
    ~RpcProvider();

    /**
     * @brief 设置 ZooKeeper 主机地址
     * @param host ZooKeeper 地址，格式如 "127.0.0.1:2181"
     */
    void setZkHost(const std::string& host);
    
    /**
     * @brief 设置监听端口
     * @param port 端口号
     */
    void setPort(int port);
    
    /**
     * @brief 设置监听 IP 地址
     * @param ip IP 地址
     */
    void setIp(const std::string& ip);

    /**
     * @brief 设置 worker 数量
     * @param count worker 协程数量
     */
    void setWorkerCount(int count) { m_worker_count = count; }

    /**
     * @brief 设置心跳间隔
     * @param interval_ms 心跳间隔（毫秒）
     */
    void setHeartbeatInterval(int interval_ms) { m_heartbeatIntervalMs = interval_ms; }

    /**
     * @brief 注册 protobuf 服务
     * @param service protobuf 服务指针
     * @note 服务的所有方法会注册到分发器，并通过 ZooKeeper 暴露给客户端
     */
    void registerService(google::protobuf::Service* service);
    
    /**
     * @brief 启动 RPC 服务器
     * @return 协程Task，启动成功后一直运行直到被停止
     */
    Coro::Task<void> start();

    /**
     * @brief 停止 RPC 服务器
     */
    void stop();

    /**
     * @brief 检查是否已停止
     * @return true 表示已停止
     */
    bool isStopped() const { return m_stop.load(); }

private:
    /**
     * @brief 将服务注册到 ZooKeeper
     * @details 在 ZooKeeper 中创建临时顺序节点，路径格式: /rpc/service_name/method_name/ip:port
     * @return 协程Task
     */
    Coro::Task<void> registerToZk();
    
    /**
     * @brief 处理客户端连接
     * @details 接收客户端请求，调用分发器处理后返回响应
     * @param stream TCP 流对象
     * @return 协程Task
     */
    Coro::Task<void> handleClient(Coro::net::TcpStream stream);

    /**
     * @brief 获取本地地址字符串
     * @return 格式为 "ip:port" 的地址字符串
     */
    std::string getLocalAddr();

    std::string m_zkHost = "127.0.0.1:2181";                ///< ZooKeeper 地址
    std::string m_ip = "127.0.0.1";                         ///< 监听 IP
    int m_port = 8000;                                      ///< 监听端口

    int m_heartbeatIntervalMs = 5000;                       ///< 心跳间隔（毫秒）
    std::atomic<bool> m_registered{false};               ///< 注册状态

    ZkClient::ptr m_zkClient;                               ///< ZooKeeper 客户端
    std::unique_ptr<Coro::net::TcpService> m_tcpService;   ///< TCP 服务
    std::unique_ptr<RpcDispatcher> m_dispatcher;           ///< RPC 分发器
    
    // MPMC 模式：生产者-消费者
    std::unique_ptr<Channel<Coro::net::TcpStream>> m_client_channel;  ///< 客户端连接通道
    int m_worker_count = 4;                                  ///< worker 协程数量
    
    std::atomic<bool> m_stop{true};                         ///< 停止标志
    bool m_started = false;                                ///< 启动状态标志
};

/**
 * @brief 服务发现客户端
 * @details 连接 ZooKeeper 获取可用的 RPC 服务地址列表
 * @note 用于 RPC 客户端实现负载均衡和故障转移
 */
class ServiceDiscovery : public std::enable_shared_from_this<ServiceDiscovery> {
public:
    /// 智能指针类型别名
    using ptr = std::shared_ptr<ServiceDiscovery>;

    /// 构造函数
    ServiceDiscovery();
    
    /// 析构函数
    ~ServiceDiscovery();

    /**
     * @brief 设置 ZooKeeper 地址
     * @param host 地址
     */
    void setZkHost(const std::string& host);
    
    /**
     * @brief 设置超时时间
     * @param timeout 超时时间(毫秒)
     */
    void setTimeout(int timeout);

    /**
     * @brief 连接到 ZooKeeper
     * @return 协程Task
     */
    Coro::Task<void> connect();

    /**
     * @brief 发现指定服务的某个方法
     * @param service_name 服务名称
     * @param method_name 方法名称
     * @return 协程Task，返回可用的服务地址 "ip:port"
     * @details 从 ZooKeeper 获取服务节点，随机选择一个返回实现负载均衡
     */
    Coro::Task<std::string> discover(const std::string& service_name, const std::string& method_name);

    /**
     * @brief 获取服务节点数据
     * @param path 节点路径
     * @return 协程Task，返回节点数据
     */
    Coro::Task<ZkResult> getData(const std::string& path);

    /**
     * @brief 发现指定服务的所有方法
     * @param service_name 服务名称
     * @return 协程Task，返回所有方法的可用地址列表
     */
    Coro::Task<std::vector<std::string>> discoverAllMethods(const std::string& service_name);

    /**
     * @brief 设置服务变更 watcher
     * @param service_name 服务名称
     * @param callback 回调函数，参数为服务地址列表
     */
    void setServiceWatcher(const std::string& service_name,
        std::function<void(const std::vector<std::string>&)> callback);

    /**
     * @brief 获取服务的所有实例地址
     * @param service_name 服务名称（完整服务名）
     * @return 协程Task，返回所有实例的地址列表（如 ["192.168.1.1:8001", "192.168.1.2:8001"]）
     * @details 从 ZooKeeper 获取服务的所有临时子节点，子节点名即为实例地址
     */
    Coro::Task<std::vector<std::string>> getInstances(const std::string& service_name);

    /**
     * @brief 关闭连接
     */
    void close();

    /**
     * @brief 检查是否已连接
     * @return true 表示已连接
     */
    bool isConnected() const { return m_connected; }

private:
    std::string m_zkHost = "127.0.0.1:2181";    ///< ZooKeeper 地址
    int m_timeout = 30000;                       ///< 超时时间
    ZkClient::ptr m_zkClient;                    ///< ZooKeeper 客户端
    bool m_connected = false;                   ///< 连接状态
    std::function<void(const std::vector<std::string>&)> m_serviceWatcher;  ///< 服务变更回调
};

}