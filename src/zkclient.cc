/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-26 14:26:04
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-26 15:54:07
 * @FilePath: /MCoroRpc/src/zkclient.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/rpc/zkclient.hpp"
#include <cstring>
#include <algorithm>

namespace AlphaMin {

void ZkClient::globalWatcher(zhandle_t* zh, int type, int state, 
                             const char* path, void* ctx) {
    auto* self = static_cast<ZkClient*>(ctx);
    
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            self->m_connected.store(true);
            if (self->m_connectChannel) {
                self->m_connectChannel->send(ZkResult{ZOK, "", ""});
            }
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            self->m_connected.store(false);
            if (self->m_connectChannel) {
                self->m_connectChannel->send(ZkResult{ZSESSIONEXPIRED, "", ""});
            }
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            self->m_connected.store(false);
            if (self->m_connectChannel) {
                self->m_connectChannel->send(ZkResult{ZNOAUTH, "", ""});
            }
        }
    }
}

void ZkClient::createCompletion(int rc, const char* path, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    op->channel->send(ZkResult{rc, "", path ? std::string(path) : ""});
    delete op;
}

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

void ZkClient::setCompletion(int rc, const struct Stat* stat, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    op->channel->send(ZkResult{rc, "", ""});
    delete op;
}

void ZkClient::deleteCompletion(int rc, const void* data) {
    auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
    op->channel->send(ZkResult{rc, "", ""});
    delete op;
}

void ZkClient::cleanupPendingOps() {
    std::lock_guard<std::mutex> lock(m_pendingMutex);
    for (auto* op : m_pendingOps) {
        op->channel->send(ZkResult{ZCONNECTIONLOSS, "", ""});
        delete op;
    }
    m_pendingOps.clear();
}

Coro::Task<ZkResult> ZkClient::start() {
    m_connectChannel = std::make_shared<Coro::Channel<ZkResult>>();
    
    m_zkHandle = zookeeper_init(m_host.c_str(), globalWatcher, 
                                m_timeout, nullptr, this, 0);
    
    if (!m_zkHandle) {
        co_return ZkResult{ZSYSTEMERROR, "", "failed to init zookeeper"};
    }
    
    ZkResult result = co_await m_connectChannel->recv();
    co_return result;
}

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
    
    {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.push_back(op);
    }
    
    int rc = zoo_acreate(m_zkHandle, path.c_str(), data.c_str(), data.size(),
                        &ZOO_OPEN_ACL_UNSAFE, flags, createCompletion, op);
    
    if (rc != ZOK) {
        std::lock_guard<std::mutex> lock(m_pendingMutex);
        m_pendingOps.erase(std::remove(m_pendingOps.begin(), m_pendingOps.end(), op), m_pendingOps.end());
        delete op;
        co_return ZkResult{rc, "", ""};
    }
    
    ZkResult result = co_await channel->recv();
    co_return result;
}

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

void ZkClient::close() {
    if (m_zkHandle) {
        cleanupPendingOps();
        
        if (m_connectChannel) {
            m_connectChannel->close();
            m_connectChannel.reset();
        }
        
        zookeeper_close(m_zkHandle);
        m_zkHandle = nullptr;
        m_connected.store(false);
    }
}

}
