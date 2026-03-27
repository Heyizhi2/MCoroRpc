/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-26 14:26:04
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-26 20:46:59
 * @FilePath: /MCoroRpc/src/zkclient.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/rpc/zkclient.hpp"
#include "../include/coro/wait_for.hpp"
#include <cstring>
#include <algorithm>

namespace Coro {

/**
 * @brief 全局 watcher 回调
 * @details 处理 ZooKeeper 连接状态变化(连接、过期、认证失败)
 */
void ZkClient::globalWatcher(zhandle_t* zh, int type, int state, 
                             const char* path, void* ctx) {
    auto* self = static_cast<ZkClient*>(ctx);
    
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            // 连接成功
            self->m_connected.store(true);
            if (self->m_connectChannel) {
                self->m_connectChannel->send(ZkResult{ZOK, "", ""});
            }
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            // 会话过期
            self->m_connected.store(false);
            self->cleanupPendingOps();
            if (self->m_connectChannel) {
                self->m_connectChannel->send(ZkResult{ZSESSIONEXPIRED, "", ""});
            }
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            // 认证失败
            self->m_connected.store(false);
            if (self->m_connectChannel) {
                self->m_connectChannel->send(ZkResult{ZNOAUTH, "", ""});
            }
        }
    }
}

/**
 * @brief 创建节点完成回调
 */
void ZkClient::createCompletion(int rc, const char* path, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    op->channel->send(ZkResult{rc, "", path ? std::string(path) : ""});
    delete op;
}

/**
 * @brief 获取数据完成回调
 */
void ZkClient::getCompletion(int rc, const char* value, int valueLen, 
                              const struct Stat* stat, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    std::string dataStr;
    if (value && valueLen > 0) {
        dataStr = std::string(value, valueLen);
    }
    op->channel->send(ZkResult{rc, dataStr, ""});
    delete op;
}

/**
 * @brief 设置数据完成回调
 */
void ZkClient::setCompletion(int rc, const struct Stat* stat, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    op->channel->send(ZkResult{rc, "", ""});
    delete op;
}

/**
 * @brief 删除节点完成回调
 */
void ZkClient::deleteCompletion(int rc, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    op->channel->send(ZkResult{rc, "", ""});
    delete op;
}

/**
 * @brief 获取子节点完成回调
 */
void ZkClient::getChildrenCompletion(int rc, const struct String_vector* strings, const struct Stat* stat, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    std::string dataStr;
    // 将子节点用逗号连接
    if (strings && strings->count > 0) {
        for (int i = 0; i < strings->count; ++i) {
            if (i > 0) dataStr += ",";
            dataStr += strings->data[i];
        }
    }
    op->channel->send(ZkResult{rc, dataStr, ""});
    delete op;
}

/**
 * @brief 清理待处理的异步操作
 * @details 关闭连接时调用，将所有待处理操作标记为连接丢失
 */
void ZkClient::cleanupPendingOps() {
    std::lock_guard<std::mutex> lock(m_pendingMutex);
    for (auto* op : m_pendingOps) {
        op->channel->send(ZkResult{ZCONNECTIONLOSS, "", ""});
        delete op;
    }
    m_pendingOps.clear();
}

/**
 * @brief 连接到 ZooKeeper
 * @return 协程Task，连接结果
 */
Coro::Task<ZkResult> ZkClient::start() {
    m_connectChannel = std::make_shared<Coro::Channel<ZkResult>>();
    
    // 初始化 ZooKeeper 连接
    m_zkHandle = zookeeper_init(m_host.c_str(), globalWatcher, 
                                m_timeout, nullptr, this, 0);
    
    if (!m_zkHandle) {
        co_return ZkResult{ZSYSTEMERROR, "", "failed to init zookeeper"};
    }
    
    // 等待连接结果，超时5秒
    auto result = co_await Coro::wait_for(m_connectChannel->recv(), std::chrono::seconds(5));
    
    if (!result.ok || result.is_timeout) {
        if (m_zkHandle) {
            zookeeper_close(m_zkHandle);
            m_zkHandle = nullptr;
        }
        m_connected.store(false);
        co_return ZkResult{ZSYSTEMERROR, "", result.is_timeout ? "connection timeout" : "connection failed"};
    }
    
    co_return std::move(result.value);
}

/**
 * @brief 创建节点
 * @param path 节点路径
 * @param data 节点数据
 * @param flags 节点标志
 * @return 协程Task，操作结果
 */
Coro::Task<ZkResult> ZkClient::create(const std::string& path, 
                                       const std::string& data, int flags) {
    if (!m_connected.load()) {
        co_return ZkResult{ZINVALIDSTATE, "", "not connected"};
    }

    auto channel = std::make_shared<Coro::Channel<ZkResult>>();
    auto* op = new PendingOp{
        .opType = ZOO_CREATE_OP,
        .path = path,
        .data = data,
        .flags = flags,
        .version = -1,
        .channel = channel
    };
    
    // 添加到待处理列表
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.push_back(op);
    }
    
    // 异步创建节点
    int rc = zoo_acreate(m_zkHandle, path.c_str(), data.c_str(), data.size(),
                        &ZOO_OPEN_ACL_UNSAFE, flags, createCompletion, op);
    
    if (rc != ZOK) {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
        delete op;
        co_return ZkResult{rc, "", ""};
    }
    
    // 等待异步结果
    ZkResult result = co_await channel->recv();
    co_return result;
}

/**
 * @brief 获取节点数据
 */
Coro::Task<ZkResult> ZkClient::getData(const std::string& path) {
    if (!m_connected.load()) {
        co_return ZkResult{ZINVALIDSTATE, "", "not connected"};
    }

    auto channel = std::make_shared<Coro::Channel<ZkResult>>();
    auto* op = new PendingOp{
        .opType = ZOO_GETDATA_OP,
        .path = path,
        .data = "",
        .flags = 0,
        .version = -1,
        .channel = channel
    };
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.push_back(op);
    }
    
    int rc = zoo_aget(m_zkHandle, path.c_str(), 0, getCompletion, op);
    
    if (rc != ZOK) {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
        delete op;
        co_return ZkResult{rc, "", ""};
    }
    
    ZkResult result = co_await channel->recv();
    co_return result;
}

/**
 * @brief 设置节点数据
 */
Coro::Task<ZkResult> ZkClient::setData(const std::string& path, 
                                         const std::string& data, int version) {
    if (!m_connected.load()) {
        co_return ZkResult{ZINVALIDSTATE, "", "not connected"};
    }

    auto channel = std::make_shared<Coro::Channel<ZkResult>>();
    auto* op = new PendingOp{
        .opType = ZOO_SETDATA_OP,
        .path = path,
        .data = data,
        .flags = 0,
        .version = version,
        .channel = channel
    };
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.push_back(op);
    }
    
    int rc = zoo_aset(m_zkHandle, path.c_str(), data.c_str(), data.size(),
                      version, setCompletion, op);
    
    if (rc != ZOK) {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
        delete op;
        co_return ZkResult{rc, "", ""};
    }
    
    ZkResult result = co_await channel->recv();
    co_return result;
}

/**
 * @brief 删除节点
 */
Coro::Task<ZkResult> ZkClient::deleteNode(const std::string& path, int version) {
    if (!m_connected.load()) {
        co_return ZkResult{ZINVALIDSTATE, "", "not connected"};
    }

    auto channel = std::make_shared<Coro::Channel<ZkResult>>();
    auto* op = new PendingOp{
        .opType = ZOO_DELETE_OP,
        .path = path,
        .data = "",
        .flags = 0,
        .version = version,
        .channel = channel
    };
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.push_back(op);
    }
    
    int rc = zoo_adelete(m_zkHandle, path.c_str(), version, deleteCompletion, op);
    
    if (rc != ZOK) {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
        delete op;
        co_return ZkResult{rc, "", ""};
    }
    
    ZkResult result = co_await channel->recv();
    co_return result;
}

/**
 * @brief 获取子节点列表
 */
Coro::Task<ZkResult> ZkClient::getChildren(const std::string& path) {
    if (!m_connected.load()) {
        co_return ZkResult{ZINVALIDSTATE, "", "not connected"};
    }

    auto channel = std::make_shared<Coro::Channel<ZkResult>>();
    auto* op = new PendingOp{
        .opType = ZOO_GETCHILDREN_OP,
        .path = path,
        .data = "",
        .flags = 0,
        .version = -1,
        .channel = channel
    };
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.push_back(op);
    }
    
    int rc = zoo_aget_children2(m_zkHandle, path.c_str(), 0, getChildrenCompletion, op);
    
    if (rc != ZOK) {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
        delete op;
        co_return ZkResult{rc, "", ""};
    }
    
    co_return co_await channel->recv();
}

/**
 * @brief 关闭 ZooKeeper 连接
 */
void ZkClient::close() {
    if (m_zkHandle) {
        // 清理待处理操作
        cleanupPendingOps();
        
        // 关闭连接 Channel
        if (m_connectChannel) {
            m_connectChannel->close();
            m_connectChannel.reset();
        }
        
        // 关闭 ZooKeeper
        zookeeper_close(m_zkHandle);
        m_zkHandle = nullptr;
        m_connected.store(false);
    }
}

}
