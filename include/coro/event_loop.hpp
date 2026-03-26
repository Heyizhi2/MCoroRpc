/**
 * @file event_loop.hpp
 * @brief 事件循环
 * 
 * 事件循环是协程框架的核心组件，负责：
 * 1. 管理协程调度 - 将协程加入就绪队列并执行
 * 2. 定时器管理 - 处理超时任务
 * 3. IO事件处理 - 通过epoll监听文件描述符事件
 * 
 * 工作流程：
 * 1. 从就绪队列(m_ready_queue)取出待执行协程
 * 2. 检查定时器堆(m_scheduled)是否有超时任务
 * 3. 调用epoll等待IO事件
 * 4. 执行到期的回调函数
 */

#pragma once
#include "handle.hpp"
#include "timer.hpp"
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <sys/types.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "../utils/noncopyable.hpp"
#include "../selector/epoll.hpp"
#include <atomic>

namespace Coro {
    /**
     * @brief 事件循环类
     * @details 协程调度中心，管理所有协程的执行和IO事件
     */
    class Eventloop:private Noncopyable{
      
        /** @brief 回调函数类型 */
        using Callback= std::function<void()>;


        public: 
        /**
         * @brief 立即执行回调
         * @param handle 关联的协程句柄
         * @param cb 回调函数
         * @param coro 协程句柄
         * @return 等待ID，用于取消
         * 
         * 将回调加入下一轮事件循环执行
         */
        uint64_t call_soon(Handle& handle,Callback cb, std::coroutine_handle<> coro = nullptr);


        /**
         * @brief 延迟执行回调
         * @tparam Rep 时间值类型
         * @tparam Period 时间单位
         * @param delay 延迟时间
         * @param handle 关联的协程句柄
         * @param cb 回调函数
         * @param coro 协程句柄
         * @return 等待ID，用于取消
         */
        template<typename Rep,typename Period>
        uint64_t call_later(std::chrono::duration<Rep,Period>delay,Handle&handle,Callback cb, std::coroutine_handle<> coro = nullptr){
            auto when=types::Clock::now()+std::chrono::duration_cast<std::chrono::milliseconds>(delay);
            return call_at(when,handle,std::move(cb), coro);
        }
        
        /**
         * @brief 指定时间执行回调
         * @param when 执行时间点
         * @param handle 关联的协程句柄
         * @param cb 回调函数
         * @param coro 协程句柄
         * @return 等待ID，用于取消
         */
        uint64_t call_at(types::TimePoint,Handle&handle,Callback cb, std::coroutine_handle<> coro = nullptr);

        /**
         * @brief 取消协程的所有等待操作
         * @param cancelled_id 协程ID
         */
        void cancel(Handle::ID cancelled_id);
        
        /**
         * @brief 运行事件循环直到停止
         * @details 循环执行run_once()直到is_stop()返回true
         */
        void run_until_complete();
        
        /**
         * @brief 取消特定的等待操作
         * @param wait_id 等待操作ID
         * @return 如果存在则返回被取消的回调，否则返回空
         */
        std::optional<Eventloop::Callback> cancel_wait(uint64_t wait_id);

        /**
         * @brief 注册写事件监听
         * @param fd 文件描述符
         * @param handle 关联的协程句柄
         * @param cb 回调函数
         * @param coro 协程句柄
         * @return 等待ID，用于取消
         */
        uint64_t add_writer(int fd,Handle& handle,Callback cb, std::coroutine_handle<> coro = nullptr);

        /**
         * @brief 注册读事件监听
         * @param fd 文件描述符
         * @param handle 关联的协程句柄
         * @param cb 回调函数
         * @param coro 协程句柄
         * @return 等待ID，用于取消
         */
        uint64_t add_reader(int fd,Handle& handle,Callback cb, std::coroutine_handle<> coro = nullptr);
        
        private:
        /**
         * @brief 检查事件循环是否应停止
         * @return true 表示应停止（无更多任务）
         */
        bool is_stop(){
            return m_ready_queue.empty() && m_scheduled.empty() && m_epoll.is_stop() && m_coro_waits.empty();
        }
        
        bool has_pending_coroutines() const {
            return !m_coro_waits.empty();
        }
        
        /**
         * @brief 执行一次事件循环
         * @details 处理IO事件、定时器和就绪回调
         */
        void run_once();

        /**
         * @brief 处理Epoll事件
         * @param timeout 超时时间（毫秒）
         */
        void process_epoll_event(int timeout);

         /**
          * @brief 处理超时的定时器
          */
        void process_expired_timeout();

         /**
          * @brief 执行就绪队列中的回调
          */
        void execute_ready_callback();

        /** @brief 等待信息结构体 */
        struct WaitCallback {
            Callback callback;
            std::coroutine_handle<> coro_handle;
        };

        /**
         * @brief 创建包装回调
         * @details 在回调执行后清理协程等待记录
         */
        WaitCallback make_warped_callback(uint64_t,Handle::ID,Callback cb, std::coroutine_handle<> coro);

        private:
        /** @brief 下一个等待ID */
        std::atomic<uint64_t> next_wait_id;
        
        /** @brief 就绪队列，存放待执行的等待ID */
        std::queue<uint64_t> m_ready_queue;

        /** @brief 协程ID到等待ID集合的映射 */
        std::unordered_map<Handle::ID,std::unordered_set<uint64_t>> m_coro_waits;
        
        /** @brief 等待ID到回调函数的映射 */
        std::unordered_map<uint64_t, WaitCallback> m_wait_callback;

        /** @brief 定时器堆，按时间排序的等待任务 */
        std::vector<std::pair<uint64_t, types::TimePoint>> m_scheduled;
        
        /** @brief Epoll IO多路复用器 */
        Epoll m_epoll{};
    };
    
    /**
     * @brief 获取全局事件循环单例
     * @return 事件循环引用
     */
    Eventloop &get_event_loop();
}
