/**
 * @file rpc_client.hpp
 * @brief 多实例 RPC 客户端封装
 * @details 支持两种连接模式：
 *       1. 直接连接（connectDirect）：用于固定地址的高性能调用
 *       2. 服务发现连接（connectWithDiscovery）：通过 ZooKeeper 自动发现可用实例
 *       支持负载均衡、连接池、故障重试和动态感知功能
 */

#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <set>
#include <shared_mutex>
#include "rpc_channel.hpp"
#include "rpc_provider.hpp"

namespace Coro {

/**
 * @brief RPC 客户端配置选项
 */
struct RpcClientOptions {
    std::string zkHost = "127.0.0.1:2181";    ///< ZooKeeper 地址（服务发现模式使用）
    int timeoutMs = 3000;                      ///< RPC 调用超时时间（毫秒）
    int maxRetries = 3;                        ///< 单次调用最大重试次数
    bool enableLoadBalance = true;             ///< 是否启用负载均衡
    int heartbeatCheckIntervalMs = 2000;       ///< 健康检查间隔（毫秒）
    int heartbeatTimeoutMs = 15000;              ///< 健康检查超时时间（毫秒）
};

/**
 * @brief 多实例 RPC 客户端
 * @details 封装 RPC 客户端功能，支持：
 *       - 直连模式：直接连接指定地址，适用于固定服务地址场景
 *       - 服务发现模式：通过 ZooKeeper 发现可用实例列表
 *       - 负载均衡：支持轮询、随机等策略
 *       - 连接池：缓存到各实例的连接，支持复用
 *       - 故障重试：调用失败时自动切换到其他实例重试
 *       - 动态感知：通过 Watcher 感知实例变化
 */
class RpcClient : public std::enable_shared_from_this<RpcClient> {
public:
    using ptr = std::shared_ptr<RpcClient>;

    /**
     * @brief 构造函数
     * @param options 配置选项
     * @param lb 负载均衡器（默认为 RoundRobinLoadBalancer）
     */
    explicit RpcClient(const RpcClientOptions& options = {},
                     LoadBalancer::ptr lb = std::make_shared<RoundRobinLoadBalancer>());
    
    ~RpcClient();

    /**
     * @brief 设置配置选项
     * @param options 配置选项
     */
    void setOptions(const RpcClientOptions& options);

    /**
     * @brief 设置负载均衡器
     * @param lb 负载均衡器智能指针
     */
    void setLoadBalancer(LoadBalancer::ptr lb) { m_lb = std::move(lb); }

    /**
     * @brief 设置服务状态变更回调
     * @param callback 回调函数，参数为服务名和服务是否可用
     * @note 回调在服务实例列表变化时被调用
     */
    void setServiceStatusCallback(std::function<void(const std::string& serviceName, bool isAlive)> callback);

    /**
     * @brief 直接连接到指定地址
     * @param host 服务器主机名或 IP
     * @param port 服务器端口
     * @return 协程 Task，连接成功或失败
     * @note 不使用服务发现，适用于固定地址场景，可获得更好的性能
     */
    Coro::Task<void> connectDirect(const std::string& host, int port);

    /**
     * @brief 直接连接到指定地址
     * @param addr 网络地址智能指针
     * @return 协程 Task
     */
    Coro::Task<void> connectDirect(const net::NetAddr::s_ptr& addr);

    /**
     * @brief 通过服务发现连接
     * @param serviceName 服务名称（完整名称，如 "testrpc.Calculator"）
     * @return 协程 Task
     * @note 连接到 ZooKeeper 获取可用实例列表，设置 Watcher 监听变化
     */
    Coro::Task<void> connectWithDiscovery(const std::string& serviceName);

    /**
     * @brief 断开连接
     * @note 关闭所有到实例的连接，清理连接池
     */
    void disconnect();
    
    /**
     * @brief 检查是否已连接
     * @return true 表示至少有一个可用连接
     */
    bool isConnected() const;

    /**
     * @brief 同步调用 RPC 方法
     * @param method 方法描述符
     * @param request 请求消息
     * @param response 响应消息
     * @param timeoutMs 超时时间（毫秒），-1 表示使用默认值
     * @return true 表示调用成功
     */
    bool callMethodSync(const google::protobuf::MethodDescriptor* method,
                      const google::protobuf::Message* request,
                      google::protobuf::Message* response,
                      int timeoutMs = -1);

    /**
     * @brief 异步调用 RPC 方法
     * @param method 方法描述符
     * @param request 请求消息
     * @param response 响应消息
     * @param controller RPC 控制器
     * @return 协程 Task，true 表示调用成功
     */
    Coro::Task<bool> callMethodAsync(const google::protobuf::MethodDescriptor* method,
                                     const google::protobuf::Message* request,
                                     google::protobuf::Message* response,
                                     RpcController* controller);

    /**
     * @brief 获取当前地址
     * @return 网络地址智能指针
     */
    net::NetAddr::s_ptr getAddr() const { return m_addr; }
    
    /**
     * @brief 获取服务发现客户端
     * @return ServiceDiscovery 智能指针
     */
    ServiceDiscovery::ptr getDiscovery() const { return m_discovery; }
    
    /**
     * @brief 获取负载均衡器
     * @return LoadBalancer 智能指针
     */
    LoadBalancer::ptr getLoadBalancer() const { return m_lb; }

    /**
     * @brief 获取最近一次调用的错误信息
     * @return 错误描述字符串，成功时为空字符串
     */
    std::string getLastErrorText() const { return m_last_error; }

private:
    /**
     * @brief 获取或创建到指定地址的连接
     * @param addr 地址字符串（格式 "ip:port"）
     * @return RpcChannel 智能指针
     */
    Coro::Task<std::shared_ptr<RpcChannel>> getOrCreateChannel(const std::string& addr);

    /**
     * @brief 更新实例列表
     * @param instances 新的实例列表
     * @note 自动关闭不再存在的实例连接
     */
    void updateInstances(const std::vector<std::string>& instances);
    
    /**
     * @brief 标记地址不可用
     * @param addr 地址
     * @note 临时标记，用于快速重试时跳过
     */
    void markUnavailable(const std::string& addr);
    
    /**
     * @brief 标记地址可用
     * @param addr 地址
     */
    void markAvailable(const std::string& addr);

    RpcClientOptions m_options;                    ///< 配置选项
    LoadBalancer::ptr m_lb;                      ///< 负载均衡器
    net::NetAddr::s_ptr m_addr;                   ///< 当前连接地址
    std::shared_ptr<ServiceDiscovery> m_discovery; ///< 服务发现客户端
    std::string m_serviceName;                    ///< 服务名称

    mutable std::mutex m_instancesMutex;            ///< 保护实例列表和连接池
    std::vector<std::string> m_instances;           ///< 可用实例列表
    std::unordered_map<std::string, std::shared_ptr<RpcChannel>> m_channels;  ///< 连接池

    std::mutex m_failedMutex;                        ///< 保护失败地址集合
    std::set<std::string> m_failedAddrs;           ///< 临时失败的地址集合

    std::atomic<bool> m_connected{false};         ///< 连接状态
    std::atomic<bool> m_stop{false};             ///< 停止标志
    std::function<void(const std::string&, bool)> m_statusCallback;  ///< 状态回调
    std::string m_last_error;                     ///< 最近一次调用的错误信息
};

/**
 * @brief RPC 调用器封装
 * @details 提供模板化的同步调用接口，简化 RPC 调用
 * @example
 * @code
 * RpcCaller caller(client);
 * MyRequest req;
 * MyResponse rsp;
 * caller.call(method, req, &rsp, 3000);
 * @endcode
 */
class RpcCaller {
public:
    RpcCaller() = default;
    
    /**
     * @brief 构造函数
     * @param client RPC 客户端智能指针
     */
    explicit RpcCaller(RpcClient::ptr client);
    
    /**
     * @brief 设置客户端
     * @param client RPC 客户端智能指针
     */
    void setClient(RpcClient::ptr client);

    /**
     * @brief 模板化同步调用
     * @tparam Request 请求类型
     * @tparam Response 响应类型
     * @param method 方法描述符
     * @param request 请求消息
     * @param response 响应消息
     * @param timeoutMs 超时时间
     * @return true 表示调用成功
     */
    template<typename Request, typename Response>
    bool call(const google::protobuf::MethodDescriptor* method,
              const Request& request,
              Response* response,
              int timeoutMs = -1) {
        if (!m_client || !m_client->isConnected()) {
            return false;
        }
        return m_client->callMethodSync(method, &request, response, timeoutMs);
    }

private:
    RpcClient::ptr m_client;  ///< RPC 客户端
};

}