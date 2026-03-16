/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 16:13:18
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 21:35:03
 * @FilePath: /MCoroRpc/include/coro/event_loop.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include "handle.hpp"
#include "timer.hpp"
#include <chrono>
#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "../utils/noncopyable.hpp"
#include "../selector/epoll.hpp"
namespace Coro {
    class Eventloop:private Noncopyable{
     
        using Callback= std::function<void()>;


        public: 
        void call_soon(Handle& handle,Callback cb);


        template<typename Rep,typename Period>
        void call_later(std::chrono::duration<Rep,Period>delay,Handle&handle,Callback cb){
            auto when=types::Clock::now()+std::chrono::duration_cast<std::chrono::milliseconds>(delay);
            call_at(when,handle,std::move(cb));
        }
        
        void call_at(types::TimePoint,Handle&handle,Callback cb);

        void cancel(Handle::ID cancelled_id){
            m_callbacks.erase(cancelled_id);
        }

        void resume(Handle::ID id){
            auto it = m_callbacks.find(id);
            if(it != m_callbacks.end()){
                auto func = std::move(it->second);
                if(func){
                    func();
                }
            }
        }

        
        void run_until_complete();
        
        
        private:
        bool is_stop(){
            return m_ready_handle.empty()&&m_scheduled.empty();
        }
        void run_once();

        private:

        ///@brief 准备唤醒的协程集
        std::queue<Handle::ID> m_ready_handle;

        ///@brief 协程所对应的回调
        std::unordered_map<Handle::ID,std::function<void()>> m_callbacks;
        
        ///@brief 定时器堆，存定时器对应的{id,和指针}
        std::vector<std::pair<Handle::ID, types::TimePoint>> m_scheduled;
        
        Epoll m_epoll{};
    };
    Eventloop &get_event_loop();
}