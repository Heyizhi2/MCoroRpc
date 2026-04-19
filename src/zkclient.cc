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
#include "../include/coro/sleep.hpp"
#include <cstring>
#include <algorithm>

namespace Coro {

/**
 * @brief 全局 watcher 回调
 * @details 处理 ZooKeeper 连接状态变化和 watch 事件
 */
void ZkClient::globalWatcher(zhandle_t* zh, int type, int state, 
                             const char* path, void* ctx) {
    auto* self = static_cast<ZkClient*>(ctx);
    
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            // 连接成功 - 使用条件变量通知
            self->m_connected.store(true);
            {
                std::lock_guard<std::mutex> lock(self->m_connMutex);
                self->m_connResult = ZkResult{ZOK, "", ""};
                self->m_connNotified = true;
            }
            self->m_connCond.notify_one();
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            // 会话过期
            self->m_connected.store(false);
            self->cleanupPendingOps();
            {
                std::lock_guard<std::mutex> lock(self->m_connMutex);
                self->m_connResult = ZkResult{ZSESSIONEXPIRED, "", ""};
                self->m_connNotified = true;
            }
            self->m_connCond.notify_one();
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            // 认证失败
            self->m_connected.store(false);
            {
                std::lock_guard<std::mutex> lock(self->m_connMutex);
                self->m_connResult = ZkResult{ZNOAUTH, "", ""};
                self->m_connNotified = true;
            }
            self->m_connCond.notify_one();
        }
    } else if (type == ZOO_CHANGED_EVENT || type == ZOO_CREATED_EVENT || 
               type == ZOO_DELETED_EVENT || type == ZOO_CHILD_EVENT) {
        // Watch 事件
        // fprintf(stderr, "[globalWatcher] watch event: type=%d, path=%s, state=%d\n", 
        //        type, path ? path : "(null)", state);
        if (path) {
            self->processWatcher(type, state, path);
        }
    } else {
        fprintf(stderr, "[globalWatcher] unknown type=%d\n", type);
    }
}

/**
 * @brief 创建节点完成回调
 */
void ZkClient::createCompletion(int rc, const char* path, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    std::string resultPath;
    if (path) {
        resultPath = std::string(path);
    }
    {
        std::lock_guard<std::mutex> lock(op->mutex);
        op->result = ZkResult{rc, "", resultPath};
        op->ready = true;
    }
    op->cond.notify_one();
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
    {
        std::lock_guard<std::mutex> lock(op->mutex);
        op->result = ZkResult{rc, dataStr, ""};
        op->ready = true;
    }
    op->cond.notify_one();
}

/**
 * @brief 设置数据完成回调
 */
void ZkClient::setCompletion(int rc, const struct Stat* stat, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    {
        std::lock_guard<std::mutex> lock(op->mutex);
        op->result = ZkResult{rc, "", ""};
        op->ready = true;
    }
    op->cond.notify_one();
}

/**
 * @brief 删除节点完成回调
 */
void ZkClient::deleteCompletion(int rc, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    {
        std::lock_guard<std::mutex> lock(op->mutex);
        op->result = ZkResult{rc, "", ""};
        op->ready = true;
    }
    op->cond.notify_one();
}

/**
 * @brief 获取子节点完成回调
 */
void ZkClient::getChildrenCompletion(int rc, const struct String_vector* strings, const struct Stat* stat, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    std::string dataStr;
    if (strings && strings->count > 0) {
        for (int i = 0; i < strings->count; ++i) {
            if (i > 0) dataStr += ",";
            dataStr += strings->data[i];
        }
    }
    {
        std::lock_guard<std::mutex> lock(op->mutex);
        op->result = ZkResult{rc, dataStr, ""};
        op->ready = true;
    }
    op->cond.notify_one();
}

/**
 * @brief 清理待处理的异步操作
 * @details 关闭连接时调用，将所有待处理操作标记为连接丢失
 */
void ZkClient::cleanupPendingOps() {
    std::lock_guard<std::mutex> lock(m_pendingMutex);
    for (auto* op : m_pendingOps) {
        {
            std::lock_guard<std::mutex> opLock(op->mutex);
            op->result = ZkResult{ZCONNECTIONLOSS, "", ""};
            op->ready = true;
        }
        op->cond.notify_one();
        delete op;
    }
    m_pendingOps.clear();
}

/**
 * @brief 连接到 ZooKeeper
 * @return 协程Task，连接结果
 */
Coro::Task<ZkResult> ZkClient::start() {
    if (m_connected.load()) {
        co_return ZkResult{ZOK, "", ""};
    }
    
    // 初始化 ZooKeeper 连接
    m_zkHandle = zookeeper_init(m_host.c_str(), globalWatcher, 
                                m_timeout, nullptr, this, 0);
    
    if (!m_zkHandle) {
        co_return ZkResult{ZSYSTEMERROR, "", "failed to init zookeeper"};
    }
    
    // 使用轮询等待连接结果，超时5秒
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(5);
    
    while (!m_connNotified) {
        auto elapsed = std::chrono::steady_clock::now() - startTime;
        if (elapsed >= timeout) {
            // 超时
            if (m_zkHandle) {
                zookeeper_close(m_zkHandle);
                m_zkHandle = nullptr;
            }
            m_connected.store(false);
            co_return ZkResult{ZSYSTEMERROR, "", "connection timeout"};
        }
        // 短暂休眠让出 CPU
        co_await Coro::sleep_for(std::chrono::milliseconds(10));
    }
    
    ZkResult result;
    {
        std::lock_guard<std::mutex> lock(m_connMutex);
        result = m_connResult;
        m_connNotified = false;  // 重置
    }
    
    if (!result.ok()) {
        if (m_zkHandle) {
            zookeeper_close(m_zkHandle);
            m_zkHandle = nullptr;
        }
        m_connected.store(false);
    }
    
    co_return result;
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

    auto* op = new PendingOp{
        .opType = ZOO_CREATE_OP,
        .path = path,
        .data = data,
        .flags = flags,
        .version = -1
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
    {
        std::unique_lock<std::mutex> lock(op->mutex);
        op->cond.wait(lock, [op] { return op->ready; });
    }
    ZkResult result = op->result;
    
    // 从待处理列表中移除
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
    }
    delete op;
    co_return result;
}

/**
 * @brief 获取节点数据
 * @param path 节点路径
 * @param watch 是否注册 watch
 */
Coro::Task<ZkResult> ZkClient::getData(const std::string& path, bool watch) {
    if (!m_connected.load()) {
        co_return ZkResult{ZINVALIDSTATE, "", "not connected"};
    }

    auto* op = new PendingOp{
        .opType = ZOO_GETDATA_OP,
        .path = path,
        .data = "",
        .flags = 0,
        .version = -1
    };
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.push_back(op);
    }
    
    // 注册 watch：传入 1 表示启用 watch
    int rc = zoo_aget(m_zkHandle, path.c_str(), watch ? 1 : 0, getCompletion, op);
    
    if (rc != ZOK) {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
        delete op;
        co_return ZkResult{rc, "", ""};
    }
    
    {
        std::unique_lock<std::mutex> lock(op->mutex);
        op->cond.wait(lock, [op] { return op->ready; });
    }
    ZkResult result = op->result;
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
    }
    delete op;
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

    auto* op = new PendingOp{
        .opType = ZOO_SETDATA_OP,
        .path = path,
        .data = data,
        .flags = 0,
        .version = version
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
    
    {
        std::unique_lock<std::mutex> lock(op->mutex);
        op->cond.wait(lock, [op] { return op->ready; });
    }
    ZkResult result = op->result;
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
    }
    delete op;
    co_return result;
}

/**
 * @brief 删除节点
 */
Coro::Task<ZkResult> ZkClient::deleteNode(const std::string& path, int version) {
    if (!m_connected.load()) {
        co_return ZkResult{ZINVALIDSTATE, "", "not connected"};
    }

    auto* op = new PendingOp{
        .opType = ZOO_DELETE_OP,
        .path = path,
        .data = "",
        .flags = 0,
        .version = version
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
    
    {
        std::unique_lock<std::mutex> lock(op->mutex);
        op->cond.wait(lock, [op] { return op->ready; });
    }
    ZkResult result = op->result;
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
    }
    delete op;
    co_return result;
}

/**
 * @brief 获取子节点列表
 * @param path 节点路径
 * @param watch 是否注册 watch
 */
Coro::Task<ZkResult> ZkClient::getChildren(const std::string& path, bool watch) {
    if (!m_connected.load()) {
        co_return ZkResult{ZINVALIDSTATE, "", "not connected"};
    }

    auto* op = new PendingOp{
        .opType = ZOO_GETCHILDREN_OP,
        .path = path,
        .data = "",
        .flags = 0,
        .version = -1
    };
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.push_back(op);
    }
    
    // 注册 watch：传入 1 表示启用 watch
    int rc = zoo_aget_children2(m_zkHandle, path.c_str(), watch ? 1 : 0, getChildrenCompletion, op);
    
    if (rc != ZOK) {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
        delete op;
        co_return ZkResult{rc, "", ""};
    }
    
    {
        std::unique_lock<std::mutex> lock(op->mutex);
        op->cond.wait(lock, [op] { return op->ready; });
    }
    ZkResult result = op->result;
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
    }
    delete op;
    co_return result;
}

/**
 * @brief 处理 watch 回调
 * @param type 事件类型
 * @param state 状态
 * @param path 变化的节点路径
 * @note 在 zookeeper 工作线程中执行，不能用 co_await
 */
void ZkClient::processWatcher(int type, int state, const char* path) {
    if (!path || !m_zkHandle) {
        fprintf(stderr, "[ZK Watcher] path=%s or handle=null, type=%d\n", 
                path ? path : "null", type);
        return;
    }
    
    std::string pathStr(path);
    // fprintf(stderr, "[ZK Watcher] processWatcher called: type=%d, path=%s\n", type, pathStr.c_str());
    
    if (type == ZOO_CHANGED_EVENT) {
        std::lock_guard<std::mutex> lock(m_watcherMutex);
        auto it = m_dataWatchers.find(pathStr);
        if (it != m_dataWatchers.end()) {
            char buf[4096];
            int len = sizeof(buf);
            Stat stat;
            int rc = zoo_get(m_zkHandle, pathStr.c_str(), 0, buf, &len, &stat);
            if (rc == ZOK) {
                it->second(std::string(buf, len));
            }
            // 重新注册 watch
            zoo_get(m_zkHandle, pathStr.c_str(), 1, nullptr, nullptr, nullptr);
        }
    } else if (type == ZOO_CHILD_EVENT) {
        std::lock_guard<std::mutex> lock(m_watcherMutex);
        auto it = m_childrenWatchers.find(pathStr);
        if (it != m_childrenWatchers.end()) {
            String_vector children;
            children.data = nullptr;
            children.count = 0;
            int rc = zoo_get_children(m_zkHandle, pathStr.c_str(), 1, &children);
            if (rc == ZOK) {
                std::vector<std::string> childList;
                for (int i = 0; i < children.count; ++i) {
                    childList.push_back(children.data[i]);
                }
                deallocate_String_vector(&children);
                it->second(childList);
            }
        }
    }
}

/**
 * @brief 设置数据变化 watcher
 * @note 这是同步函数，需要用户自己在协程中调用 getData 初始化
 */
void ZkClient::setDataWatcher(const std::string& path, std::function<void(const std::string&)> callback) {
    std::lock_guard<std::mutex> lock(m_watcherMutex);
    m_dataWatchers[path] = callback;
}

/**
 * @brief 设置子节点变化 watcher
 * @note 这是同步函数，需要用户自己在协程中调用 getChildren 初始化
 */
void ZkClient::setChildrenWatcher(const std::string& path, std::function<void(const std::vector<std::string>&)> callback) {
    std::lock_guard<std::mutex> lock(m_watcherMutex);
    m_childrenWatchers[path] = callback;
}

/**
 * @brief 关闭 ZooKeeper 连接
 */
void ZkClient::close() {
    if (m_zkHandle) {
        // 清理待处理操作
        cleanupPendingOps();
        
        // 通知等待的协程
        {
            std::lock_guard<std::mutex> lock(m_connMutex);
            if (!m_connNotified) {
                m_connResult = ZkResult{ZCLOSING, "", "closing"};
                m_connNotified = true;
                m_connCond.notify_one();
            }
        }
        
        // 关闭 ZooKeeper
        zookeeper_close(m_zkHandle);
        m_zkHandle = nullptr;
        m_connected.store(false);
    }
}

}
