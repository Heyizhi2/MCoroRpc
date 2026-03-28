/**
 * @file zkclient.hpp
 * @brief ZooKeeper 协程客户端封装
 * @details 提供异步协程化的ZooKeeper操作接口，支持创建节点、获取/设置数据、删除节点、获取子节点等功能
 */

#pragma once
#include <zookeeper/zookeeper.h>
#include <string>
#include <memory>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "../coro/task.hpp"
#include "../coro/channel.hpp"

namespace Coro {

/**
 * @brief ZooKeeper 操作结果封装
 * @details 封装ZooKeeper操作返回的错误码、数据和路径信息
 */
struct ZkResult {
    int rc = 0;                      ///< ZooKeeper 返回的错误码
    std::string data;                ///< 获取到的节点数据
    std::string path;                ///< 操作的节点路径
    
    /**
     * @brief 判断操作是否成功
     * @return true 表示操作成功(ZOK)
     */
    bool ok() const { return rc == ZOK; }
    
    /**
     * @brief 获取错误码对应的错误描述
     * @return 错误码的可读字符串描述
     */
    std::string error() const {
        switch (rc) {
            case ZOK: return "ok";
            case ZSYSTEMERROR: return "system error";
            case ZRUNTIMEINCONSISTENCY: return "runtime inconsistency";
            case ZDATAINCONSISTENCY: return "data inconsistency";
            case ZCONNECTIONLOSS: return "connection loss";
            case ZMARSHALLINGERROR: return "marshalling error";
            case ZUNIMPLEMENTED: return "unimplemented";
            case ZAUTHFAILED: return "auth failed";
            case ZCLOSING: return "closing";
            case ZNOTHING: return "nothing";
            case ZSESSIONEXPIRED: return "session expired";
            case ZINVALIDSTATE: return "invalid state";
            case ZNOAUTH: return "no auth";
            case ZBADVERSION: return "bad version";
            case ZNOCHILDRENFOREPHEMERALS: return "no children for ephemerals";
            case ZNODEEXISTS: return "node exists";
            case ZNOTEMPTY: return "not empty";
            case ZSESSIONMOVED: return "session moved";
            case ZNONODE: return "node not exist";
            case ZAPIERROR: return "api error";
            default: return "unknown error: " + std::to_string(rc);
        }
    }
};

/**
 * @brief ZooKeeper 协程客户端
 * @details 基于C++协程实现的异步ZooKeeper客户端，将原生回调式API转换为协程异步调用
 * @note 线程安全，可以在多个协程中并发使用
 */
class ZkClient : public std::enable_shared_from_this<ZkClient> {
public:
    /// 智能指针类型别名
    using ptr = std::shared_ptr<ZkClient>;

    /// 默认构造函数
    ZkClient() = default;
    
    /// 析构函数，自动关闭连接
    ~ZkClient() { close(); }

    /**
     * @brief 设置ZooKeeper服务器地址
     * @param host 服务器地址，格式如 "127.0.0.1:2181"
     */
    void setHost(const std::string& host) { m_host = host; }
    
    /**
     * @brief 设置连接超时时间
     * @param timeout 超时时间(毫秒)，默认30000ms
     */
    void setTimeout(int timeout) { m_timeout = timeout; }
    
    /**
     * @brief 检查是否已连接
     * @return true 表示已建立连接
     */
    bool isConnected() const { return m_connected.load(); }

    /**
     * @brief 连接到ZooKeeper服务器
     * @return 协程Task，操作结果
     */
    Coro::Task<ZkResult> start();
    
    /**
     * @brief 创建持久节点
     * @param path 节点路径
     * @param data 节点数据
     * @param flags 节点标志(0=持久节点, ZOO_EPHEMERAL=临时节点, ZOO_SEQUENCE=顺序节点)
     * @return 协程Task，操作结果
     */
    Coro::Task<ZkResult> create(const std::string& path, const std::string& data, int flags = 0);
    
    /**
     * @brief 获取节点数据
     * @param path 节点路径
     * @return 协程Task，操作结果(包含数据)
     */
    Coro::Task<ZkResult> getData(const std::string& path);
    
    /**
     * @brief 设置节点数据
     * @param path 节点路径
     * @param data 要设置的数据
     * @param version 数据版本(-1表示忽略版本检查)
     * @return 协程Task，操作结果
     */
    Coro::Task<ZkResult> setData(const std::string& path, const std::string& data, int version = -1);
    
    /**
     * @brief 删除节点
     * @param path 节点路径
     * @param version 节点版本(-1表示忽略版本检查)
     * @return 协程Task，操作结果
     */
    Coro::Task<ZkResult> deleteNode(const std::string& path, int version = -1);
    
    /**
     * @brief 获取子节点列表
     * @param path 节点路径
     * @return 协程Task，操作结果(子节点列表在data字段中，用逗号分隔)
     */
    Coro::Task<ZkResult> getChildren(const std::string& path);
    
    /**
     * @brief 关闭连接
     */
    void close();

private:
    /**
     * @brief 待处理的异步操作结构
     * @details 用于存储异步操作的相关信息，通过Channel传递结果
     */
    struct PendingOp {
        int opType;                                      ///< 操作类型
        std::string path;                                ///< 节点路径
        std::string data;                                ///< 操作数据
        int flags;                                       ///< 标志位
        int version;                                     ///< 版本号
        Coro::Channel<ZkResult>::s_ptr channel;         ///< 用于传递结果的Channel
    };

    /**
     * @brief 全局 watcher 回调
     * @details ZooKeeper 连接状态变化时触发
     */
    static void globalWatcher(zhandle_t* zh, int type, int state, 
                             const char* path, void* ctx);
    
    /**
     * @brief 创建节点完成回调
     */
    static void createCompletion(int rc, const char* path, const void* data);
    
    /**
     * @brief 获取数据完成回调
     */
    static void getCompletion(int rc, const char* value, int valueLen, 
                              const struct Stat* stat, const void* data);
    
    /**
     * @brief 设置数据完成回调
     */
    static void setCompletion(int rc, const struct Stat* stat, const void* data);
    
    /**
     * @brief 删除节点完成回调
     */
    static void deleteCompletion(int rc, const void* data);
    
    /**
     * @brief 获取子节点完成回调
     */
    static void getChildrenCompletion(int rc, const struct String_vector* strings, 
                                      const struct Stat* stat, const void* data);

    /**
     * @brief 清理待处理操作
     * @details 关闭连接时清理所有pending的操作
     */
    void cleanupPendingOps();

    std::string m_host = "127.0.0.1:2181";              ///< ZooKeeper服务器地址
    int m_timeout = 30000;                              ///< 连接超时时间(毫秒)
    zhandle_t* m_zkHandle = nullptr;                    ///< ZooKeeper句柄
    std::atomic<bool> m_connected{false};               ///< 连接状态标志
    
    std::mutex m_connMutex;                            ///< 连接状态互斥锁
    std::condition_variable m_connCond;                 ///< 连接条件变量
    ZkResult m_connResult;                               ///< 连接结果
    bool m_connNotified{false};                         ///< 是否已通知

    std::mutex m_pendingMutex;                          ///< 保护待操作列表的互斥锁
    std::vector<PendingOp*> m_pendingOps;               ///< 待处理的异步操作列表
};

}  // namespace AlphaMin
