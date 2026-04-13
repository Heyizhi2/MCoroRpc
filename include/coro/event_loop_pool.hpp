/**
 * @file event_loop_pool.hpp
 * @brief 多线程事件循环池
 */

#pragma once
#include "event_loop.hpp"
#include <vector>
#include <thread>
#include <functional>
#include <atomic>
#include <shared_mutex>

namespace Coro {

class EventloopPool {
public:
    static EventloopPool& instance() {
        static EventloopPool pool;
        return pool;
    }

    void init(int threadCount) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_initialized) return;
        
        m_threadCount = threadCount;
        m_eventLoops.resize(threadCount);
        m_threads.resize(threadCount);
        
        for (int i = 0; i < threadCount; ++i) {
            m_eventLoops[i] = std::make_unique<Eventloop>();
        }
        
        m_index.store(0);
        m_initialized = true;
    }

    Eventloop& getNextLoop() {
        int idx = m_index.fetch_add(1) % m_threadCount;
        return *m_eventLoops[idx];
    }

    Eventloop& getLoopByIndex(int index) {
        return *m_eventLoops[index % m_threadCount];
    }

    int threadCount() const { return m_threadCount; }

    void startAll() {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_running) return;
        
        for (int i = 0; i < m_threadCount; ++i) {
            m_threads[i] = std::thread([this, i]() {
                m_eventLoops[i]->run_until_complete();
            });
        }
        m_running = true;
    }

    void stopAll() {
        for (int i = 0; i < m_threadCount; ++i) {
            m_eventLoops[i]->stop();
        }
    }

    void joinAll() {
        for (auto& t : m_threads) {
            if (t.joinable()) {
                t.join();
            }
        }
        m_running = false;
    }

private:
    EventloopPool() : m_initialized(false), m_running(false) {}
    ~EventloopPool() {
        if (m_running) {
            stopAll();
            joinAll();
        }
    }

    bool m_initialized;
    bool m_running;
    int m_threadCount = 0;
    std::vector<std::unique_ptr<Eventloop>> m_eventLoops;
    std::vector<std::thread> m_threads;
    std::atomic<int> m_index{0};
    std::mutex m_mutex;
};

}