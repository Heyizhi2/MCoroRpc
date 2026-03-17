/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 16:13:18
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 17:00:06
 * @FilePath: /MCoroRpc/include/coro/event_loop.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
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
    class Eventloop:private Noncopyable{
     
        using Callback= std::function<void()>;


        public: 
        uint64_t call_soon(Handle& handle,Callback cb);


        template<typename Rep,typename Period>
        uint64_t call_later(std::chrono::duration<Rep,Period>delay,Handle&handle,Callback cb){
            auto when=types::Clock::now()+std::chrono::duration_cast<std::chrono::milliseconds>(delay);
            return call_at(when,handle,std::move(cb));
        }
        
        uint64_t call_at(types::TimePoint,Handle&handle,Callback cb);

        void cancel(Handle::ID cancelled_id);
        
        void run_until_complete();
        
        std::optional<Eventloop::Callback> cancel_wait(uint64_t wait_id);

        uint64_t add_writer(int fd,Handle& handle,Callback cb);

        uint64_t add_reader(int fd,Handle& handle,Callback cb);
        
        private:
        bool is_stop(){
            return m_ready_queue.empty()&&m_scheduled.empty()&&m_epoll.is_stop();
        }
        void run_once();

        void process_epoll_event(int timeout);

        void process_expired_timeout();

        void execute_ready_callback();

        Callback make_warped_callback(uint64_t,Handle::ID,Callback cb);

        private:
        std::atomic<uint64_t> next_wait_id;
        ///@brief 准备唤醒的协程集
        std::queue<uint64_t> m_ready_queue;

        ///@brief 协程所对应的等待操作
        std::unordered_map<Handle::ID,std::unordered_set<uint64_t>> m_coro_waits;
        
        ///@brief 等待id所对应的回调操作
        std::unordered_map<uint64_t,Callback> m_wait_callback;

        ///@brief 定时器堆，存定时器对应的{id,和指针}
        std::vector<std::pair<uint64_t, types::TimePoint>> m_scheduled;
        
        Epoll m_epoll{};
    };
    Eventloop &get_event_loop();
}