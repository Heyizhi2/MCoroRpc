/**
 * @file rpc_provider.hpp
 * @brief RPC 服务提供者
 * @details 提供基于 protobuf 的 RPC 服务框架，支持服务注册、ZooKeeper 服务发现、TCP 通信
 */

#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <atomic>
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

    ZkClient::ptr m_zkClient;                               ///< ZooKeeper 客户端
    std::unique_ptr<Coro::net::TcpService> m_tcpService;   ///< TCP 服务
    std::unique_ptr<RpcDispatcher> m_dispatcher;           ///< RPC 分发器
    
    std::atomic<bool> m_stop{true};                         ///< 停止标志
    bool m_started = false;                                ///< 启动状态标志
};

/**
 * @brief 服务发现客户端
 * @details 连接 ZooKeeper 获取可用的 RPC 服务地址列表
 * @note 用于 RPC 客户端实现负载均衡和故障转移
 */
class ServiceDiscovery {
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
     * @brief 发现指定服务的所有方法
     * @param service_name 服务名称
     * @return 协程Task，返回所有方法的可用地址列表
     */
    Coro::Task<std::vector<std::string>> discoverAllMethods(const std::string& service_name);

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
};

}