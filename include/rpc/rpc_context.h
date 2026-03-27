/**
 * @file rpc_context.h
 * @brief RPC 上下文，用于管理 RPC 调用状态
 */

#pragma once
#include <atomic>
#include <string>
#include <cstdint>
#include "../net/tcp/net_addr.h"

namespace Coro {

class RpcContext {
public:
    using s_ptr = std::shared_ptr<RpcContext>;

    RpcContext() = default;

    void reset() {
        m_err_code = 0;
        m_err_info.clear();
        m_msg_id.clear();
        m_is_failed = false;
        m_is_cancelled = false;
        m_is_finished = false;
        m_local_addr.reset();
        m_peer_addr.reset();
        m_timeout_ms = 3000;
    }

    bool isFailed() const { return m_is_failed.load(); }
    bool isCancelled() const { return m_is_cancelled.load(); }
    bool isFinished() const { return m_is_finished.load(); }

    int32_t getErrCode() const { return m_err_code; }
    std::string getErrInfo() const { return m_err_info; }
    std::string getMsgId() const { return m_msg_id; }
    int getTimeout() const { return m_timeout_ms; }

    net::NetAddr::s_ptr getLocalAddr() const { return m_local_addr; }
    net::NetAddr::s_ptr getPeerAddr() const { return m_peer_addr; }

    void setFailed(int32_t code, const std::string& info) {
        m_err_code = code;
        m_err_info = info;
        m_is_failed = true;
    }

    void setErrCode(int32_t code) { m_err_code = code; }
    void setErrInfo(const std::string& info) { m_err_info = info; }
    void setMsgId(const std::string& id) { m_msg_id = id; }
    void setTimeout(int ms) { m_timeout_ms = ms; }

    void setLocalAddr(net::NetAddr::s_ptr addr) { m_local_addr = addr; }
    void setPeerAddr(net::NetAddr::s_ptr addr) { m_peer_addr = addr; }

    void setFinished(bool value) { m_is_finished = value; }
    void setCancelled(bool value) { 
        m_is_cancelled = value; 
        if (value) m_is_failed = true;
    }

    void startCancel() {
        m_is_cancelled = true;
        m_is_failed = true;
        m_is_finished = true;
    }

private:
    int32_t m_err_code = 0;
    std::string m_err_info;
    std::string m_msg_id;
    std::atomic<bool> m_is_failed{false};
    std::atomic<bool> m_is_cancelled{false};
    std::atomic<bool> m_is_finished{false};
    net::NetAddr::s_ptr m_local_addr;
    net::NetAddr::s_ptr m_peer_addr;
    int m_timeout_ms = 3000;
};

}