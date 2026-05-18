/**
 * @file rpc_client_impl.cc
 * @brief 多实例 RPC 客户端实现
 * @details 实现 RpcClient 类的所有方法，包括：
 *       - 直连接口（connectDirect）
 *       - 服务发现接口（connectWithDiscovery）
 *       - 连接池管理（getOrCreateChannel）
 *       - 实例列表管理（updateInstances）
 *       - 异步 RPC 调用（callMethodAsync）
 *       - 同步 RPC 调用（callMethodSync）
 */

#include "../include/rpc/rpc_client.hpp"
#include "../include/coro/sleep.hpp"
#include <chrono>
#include <thread>

namespace Coro {

/**
 * @brief 构造函数
 * @param options 配置选项
 * @param lb 负载均衡器
 */
RpcClient::RpcClient(const RpcClientOptions& options, LoadBalancer::ptr lb)
    : m_options(options), m_lb(std::move(lb)), m_stop(false) {
    m_discovery = std::make_shared<ServiceDiscovery>();
    m_discovery->setZkHost(options.zkHost);
}

/**
 * @brief 析构函数
 * @note 断开所有连接，清理资源
 */
RpcClient::~RpcClient() {
    disconnect();
}

/**
 * @brief 设置配置选项
 * @param options 配置选项
 */
void RpcClient::setOptions(const RpcClientOptions& options) {
    m_options = options;
    m_discovery->setZkHost(options.zkHost);
}

/**
 * @brief 直接连接到指定地址（字符串形式）
 * @param host 服务器主机名或 IP
 * @param port 服务器端口
 * @return 协程 Task
 * @note 复用连接池机制，但不使用服务发现
 */
Coro::Task<void> RpcClient::connectDirect(const std::string& host, int port) {
    auto addr = net::IPNetAddr::Create(host, port);
    co_await connectDirect(addr);
}

/**
 * @brief 直接连接到指定地址
 * @param addr 网络地址智能指针
 * @return 协程 Task
 * @note 将目标地址作为唯一的实例，复用现有的负载均衡和重试逻辑
 */
Coro::Task<void> RpcClient::connectDirect(const net::NetAddr::s_ptr& addr) {
    m_addr = addr;
    m_serviceName = "";

    std::vector<std::string> instances = { addr->toString() };
    updateInstances(instances);
    m_connected.store(true);

    co_return;
}

/**
 * @brief 通过服务发现连接
 * @param serviceName 服务名称
 * @return 协程 Task
 * @note 执行以下操作：
 *       1. 连接到 ZooKeeper
 *       2. 获取服务的所有实例地址列表
 *       3. 设置 Watcher 监听实例变化
 *       4. 更新本地实例列表
 */
Coro::Task<void> RpcClient::connectWithDiscovery(const std::string& serviceName) {
    if (!m_discovery->isConnected()) {
        co_await m_discovery->connect();
    }
    
    m_serviceName = serviceName;
    
    // 设置服务 watcher（用于后续实例变化）
    m_discovery->setServiceWatcher(serviceName,
        [this](const std::vector<std::string>& newInstances) {
            updateInstances(newInstances);
            if (m_statusCallback) {
                m_statusCallback(m_serviceName, !newInstances.empty());
            }
        });
    
    // 尝试获取实例列表
    std::string path = "/rpc/services/" + serviceName;
    auto result = co_await m_discovery->getZkClient()->getChildren(path, true);
    
    //如果节点还不存在（服务端未启动），轮询等待
    while (result.rc == ZNONODE) {
        co_await Coro::sleep_for(std::chrono::milliseconds(500));  // 每 500ms 重试一次
        result = co_await m_discovery->getZkClient()->getChildren(path, true);
    }
    
    // 解析并更新实例列表
    if (result.ok()) {
        std::vector<std::string> instances;
        std::stringstream ss(result.data);
        std::string inst;
        while (std::getline(ss, inst, ',')) {
            if (!inst.empty()) instances.push_back(inst);
        }
        updateInstances(instances);
    }
    
    m_connected.store(!m_instances.empty());
    co_return;
}

/**
 * @brief 断开连接
 * @note 关闭所有到实例的连接，清理连接池
 */
void RpcClient::disconnect() {
    m_stop.store(true);
    m_connected.store(false);
    
    std::unique_lock lock(m_instancesMutex);
    for (auto& [addr, channel] : m_channels) {
        if (channel) {
            channel->close();
        }
    }
    m_channels.clear();
    m_instances.clear();
    lock.unlock();
    
    if (m_discovery) {
        m_discovery->close();
    }
}

/**
 * @brief 检查是否已连接
 * @return true 表示至少有一个可用连接
 */
bool RpcClient::isConnected() const {
    if (!m_connected.load() || m_instances.empty()) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(m_instancesMutex);
    for (const auto& [addr, channel] : m_channels) {
        if (channel && channel->isConnected()) {
            return true;
        }
    }
    return false;
}

/**
 * @brief 设置服务状态变更回调
 * @param callback 回调函数
 */
void RpcClient::setServiceStatusCallback(
    std::function<void(const std::string& serviceName, bool isAlive)> callback) {
    m_statusCallback = std::move(callback);
}

/**
 * @brief 获取或创建到指定地址的连接
 * @param addr 地址字符串（格式 "ip:port"）
 * @return RpcChannel 智能指针
 * @note 如果已存在可用连接则复用，否则创建新连接并加入连接池
 */
Coro::Task<std::shared_ptr<RpcChannel>> RpcClient::getOrCreateChannel(const std::string& addr) {
    {
        std::lock_guard<std::mutex> lock(m_instancesMutex);
        auto it = m_channels.find(addr);
        if (it != m_channels.end() && it->second && it->second->isConnected()) {
            std::shared_ptr<RpcChannel> result = it->second;
            co_return result;
        }
    }
    
    auto colonPos = addr.find(':');
    if (colonPos == std::string::npos) {
        co_return std::shared_ptr<RpcChannel>(nullptr);
    }
    
    std::string host = addr.substr(0, colonPos);
    int port = std::stoi(addr.substr(colonPos + 1));
    
    auto channel = std::make_shared<RpcChannel>(host, port);
    channel->setTimeout(m_options.timeoutMs);
    
    try {
        co_await channel->connect();
        
        {
            std::lock_guard<std::mutex> lock(m_instancesMutex);
            m_channels[addr] = channel;
        }
        std::shared_ptr<RpcChannel> result = channel;
        co_return result;
    } catch (const std::exception& e) {
        co_return std::shared_ptr<RpcChannel>(nullptr);
    }
}

/**
 * @brief 更新实例列表
 * @param instances 新的实例列表
 * @note 自动关闭已不在列表中的实例连接，清空失败地址集合
 */
void RpcClient::updateInstances(const std::vector<std::string>& instances) {
    //加锁
    std::unique_lock lock(m_instancesMutex);
    //获取新的集合
    std::set<std::string> newSet(instances.begin(), instances.end());
    std::vector<std::string> oldInstances = m_instances;
    //更新实例表
    m_instances = instances;
    //更新连接池
    for (auto it = m_channels.begin(); it != m_channels.end(); ) {
        if (newSet.find(it->first) == newSet.end()) {
            if (it->second) {
                it->second->close();
            }
            it = m_channels.erase(it);
        } else {
            ++it;
        }
    }
    
    lock.unlock();
    
    std::unique_lock failedLock(m_failedMutex);
    m_failedAddrs.clear();
    failedLock.unlock();
    
    if (m_statusCallback && instances != oldInstances) {
        m_statusCallback(m_serviceName, !instances.empty());
    }
}

/**
 * @brief 标记地址不可用
 * @param addr 地址
 * @note 临时标记，用于快速重试时跳过该地址
 */
void RpcClient::markUnavailable(const std::string& addr) {
    std::unique_lock lock(m_failedMutex);
    m_failedAddrs.insert(addr);
}

/**
 * @brief 标记地址可用
 * @param addr 地址
 */
void RpcClient::markAvailable(const std::string& addr) {
    std::unique_lock lock(m_failedMutex);
    m_failedAddrs.erase(addr);
}

/**
 * @brief 异步调用 RPC 方法
 * @param method 方法描述符
 * @param request 请求消息
 * @param response 响应消息
 * @param controller RPC 控制器
 * @return 协程 Task，true 表示调用成功
 * @note 使用负载均衡选择实例，调用失败时自动切换到下一个实例重试
 */
Coro::Task<bool> RpcClient::callMethodAsync(
    const google::protobuf::MethodDescriptor* method,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    RpcController* controller) {
    
    if (!controller) {
        co_return false;
    }
    //从zookeeper获取最新的节点列表
    std::vector<std::string> availableInstances;
    //失效地址列表
    std::set<std::string> failedAddrsSnapshot;
    
    {
        std::lock_guard<std::mutex> lock(m_instancesMutex);
        //当前zookeeper无可用节点
        if (m_instances.empty()) {
            controller->SetFailed("no available instances");
            co_return false;
        }
        
        {
            //获取失败节点
            std::lock_guard<std::mutex> failedLock(m_failedMutex);
            failedAddrsSnapshot = m_failedAddrs;
        }
        
        for (const auto& addr : m_instances) {
            //过滤掉失败节点
            if (failedAddrsSnapshot.find(addr) == failedAddrsSnapshot.end()) {
                availableInstances.push_back(addr);
            }
        }
        
        if (availableInstances.empty()) {
            controller->SetFailed("all instances are unavailable");
            co_return false;
        }
    }
    
    //通过负载均衡获取当前访问的节点地址。
    std::string targetAddr = m_lb->select(availableInstances);
    
    auto channel = co_await getOrCreateChannel(targetAddr);
    if (!channel) {
        markUnavailable(targetAddr);
        controller->SetFailed("failed to connect to " + targetAddr);
        co_return false;
    }
    
    auto* ctrl = dynamic_cast<RpcController*>(controller);
    if (!ctrl) {
        co_return false;
    }
    
    try {
        co_await channel->CallMethodAsync(method, ctrl, request, response, nullptr);
        
        if (ctrl->Failed()) {
            markUnavailable(targetAddr);
            co_return false;
        }
        
        co_return true;
    } catch (const std::exception& e) {
        markUnavailable(targetAddr);
        controller->SetFailed(std::string("rpc error: ") + e.what());
        co_return false;
    }
}

/**
 * @brief 同步调用 RPC 方法
 * @param method 方法描述符
 * @param request 请求消息
 * @param response 响应消息
 * @param timeoutMs 超时时间（毫秒）
 * @return true 表示调用成功
 * @note 内部创建协程执行异步调用并阻塞等待结果
 */
bool RpcClient::callMethodSync(
    const google::protobuf::MethodDescriptor* method,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    int timeoutMs) {
    
    auto controller = std::make_shared<RpcController>();
    if (timeoutMs > 0) {
        controller->SetTimeout(timeoutMs);
    }
    
    auto task = callMethodAsync(method, request, response, controller.get());
    task.schedule();
    get_event_loop().run_until_complete();
    
    m_last_error.clear();
    if (controller->Failed()) {
        m_last_error = controller->ErrorText();
    }
    
    return !controller->Failed();
}

/**
 * @brief 构造函数
 * @param client RPC 客户端智能指针
 */
RpcCaller::RpcCaller(RpcClient::ptr client) : m_client(std::move(client)) {}

/**
 * @brief 设置客户端
 * @param client RPC 客户端智能指针
 */
void RpcCaller::setClient(RpcClient::ptr client) {
    m_client = std::move(client);
}

}