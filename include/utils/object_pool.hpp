/**
 * @file object_pool.hpp
 * @brief 对象池，用于减少内存分配开销
 */

#pragma once
#include <memory>
#include <vector>
#include <mutex>
#include "../coder/tinypb_protocol.hpp"
#include "../rpc/rpc_context.h"

namespace Coro {

template<typename T>
class ObjectPool {
public:
    static ObjectPool& instance() {
        static ObjectPool pool;
        return pool;
    }

    std::shared_ptr<T> get() {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_pool.empty()) {
            auto obj = m_pool.back();
            m_pool.pop_back();
            return obj;
        }
        return std::make_shared<T>();
    }

    void recycle(std::shared_ptr<T> obj) {
        if (!obj) return;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_pool.size() < m_maxSize) {
            m_pool.push_back(std::move(obj));
        }
    }

private:
    ObjectPool(size_t maxSize = 1024) : m_maxSize(maxSize) {}
    std::vector<std::shared_ptr<T>> m_pool;
    size_t m_maxSize;
    std::mutex m_mutex;
};

using ProtocolPool = ObjectPool<TinyPBProtocol>;
using ContextPool = ObjectPool<RpcContext>;

inline std::shared_ptr<TinyPBProtocol> getProtocol() {
    return ProtocolPool::instance().get();
}

inline void recycleProtocol(std::shared_ptr<TinyPBProtocol> proto) {
    if (!proto) return;
    proto->m_msg_id.clear();
    proto->m_method_name.clear();
    proto->m_err_code = 0;
    proto->m_err_info.clear();
    proto->m_pb_data.clear();
    proto->m_pk_len = 0;
    ProtocolPool::instance().recycle(proto);
}

inline std::shared_ptr<RpcContext> getContext() {
    return ContextPool::instance().get();
}

inline void recycleContext(std::shared_ptr<RpcContext> ctx) {
    if (!ctx) return;
    ctx->reset();
    ContextPool::instance().recycle(ctx);
}

}