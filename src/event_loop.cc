/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-16 09:50:30
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 16:59:23
 * @FilePath: /MCoroRpc/src/event_loop.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <sys/epoll.h>
#include <coroutine>
namespace Coro {
    uint64_t Eventloop::call_soon(Handle& handle,Callback callback, std::coroutine_handle<> coro){
        auto wait_id=next_wait_id++;
        auto id=handle.id();
        m_wait_callback[wait_id]=make_warped_callback(wait_id,id,std::move(callback), coro);
        m_coro_waits[id].insert(wait_id);
        m_ready_queue.push(wait_id);
        return wait_id;
    }

    Eventloop& get_event_loop(){
        static Eventloop loop;
        return  loop;
    }

    void Eventloop::run_until_complete(){
        while (!is_stop()) {
            run_once();
        }
    }

    void Eventloop::run_once(){
        int timeout_ms=-1;
        if(!m_ready_queue.empty()){
            timeout_ms=0;
        }
        else if(!m_scheduled.empty()){
            auto now=types::Clock::now();
            auto next=m_scheduled[0].second;
            if(next>now){
                timeout_ms=std::chrono::duration_cast<std::chrono::milliseconds>(next-now).count();
            }else {
                timeout_ms=0;
            }
        }

        process_epoll_event(timeout_ms);
        process_expired_timeout();
        execute_ready_callback();
    }

    uint64_t Eventloop::call_at(types::TimePoint when,Handle&handle,Callback cb, std::coroutine_handle<> coro){
        auto wait_id=next_wait_id++;
        auto id=handle.id();
        m_wait_callback[wait_id]=make_warped_callback(wait_id,id,std::move(cb), coro);
        m_coro_waits[id].insert(wait_id);
        m_scheduled.emplace_back(wait_id,when);
        std::push_heap(m_scheduled.begin(),m_scheduled.end(),[](auto& a,auto& b){
            return a.second>b.second;
        });
        return wait_id;
    }

    void Eventloop::process_epoll_event(int timeout){
        epoll_event events[10000];
        int n=m_epoll.wait(events,10000,timeout);
        for(int i=0;i<n;i++){
            auto ev_ptr=static_cast<Event*>(events[i].data.ptr);
            if(ev_ptr->reader!=0&&events[i].events&EPOLLIN){
                m_ready_queue.push(ev_ptr->reader);
            }
            if(ev_ptr->writer!=0&&events[i].events&EPOLLOUT){
                m_ready_queue.push(ev_ptr->writer);
            }
        }
    }

    void Eventloop::process_expired_timeout(){
        auto now=types::Clock::now();
        while(!m_scheduled.empty()&&now>=m_scheduled[0].second){
            auto[wait_id,when]=m_scheduled[0];
            std::pop_heap(m_scheduled.begin(),m_scheduled.end(),[](auto& a,auto& b){
                return a.second>b.second;
            });
            m_scheduled.pop_back();
            m_ready_queue.push(wait_id);
        }
    }

    void Eventloop::execute_ready_callback(){
        while (!m_ready_queue.empty()) {
            uint64_t wait_id=m_ready_queue.front();
            m_ready_queue.pop();
            auto it=m_wait_callback.find(wait_id);
            if(it!=m_wait_callback.end()){
                auto cb=std::move(it->second.callback);
                m_wait_callback.erase(it);
                if(cb) cb();
            }
        }
    }

    Eventloop::WaitCallback Eventloop::make_warped_callback(uint64_t wait_id,Handle::ID coro_id,Callback cb, std::coroutine_handle<> coro){
        auto wrapped = [this,coro_id,wait_id,user_cb=std::move(cb)](){
            user_cb();
            auto it=m_coro_waits.find(coro_id);
            if(it!=m_coro_waits.end()){
                it->second.erase(wait_id);
                if(it->second.empty()){
                    m_coro_waits.erase(it);
                }
            }
        };
        return WaitCallback{std::move(wrapped), coro};
    }

    void Eventloop::cancel(Handle::ID cancelled_id){
        auto it=m_coro_waits.find(cancelled_id);
        if(it==m_coro_waits.end()) return;
        for (auto i:it->second) {
            cancel_wait(i);
        }
        m_coro_waits.erase(it);
    }

    std::optional<Eventloop::Callback> Eventloop::cancel_wait(uint64_t wait_id){
        auto it=m_wait_callback.find(wait_id);
        std::optional<Callback> res;
        
        if(it!=m_wait_callback.end()){
            res=std::move(it->second.callback);
            auto coro = it->second.coro_handle;
            m_wait_callback.erase(it);
            if(coro && !coro.done()){
                coro.destroy();
            }
        }
        
        auto scheduled_it=std::find_if(m_scheduled.begin(),m_scheduled.end(),[wait_id](auto&p){
            return p.first==wait_id;
        });

        //从 epoll 中移除
        m_epoll.cancel_wait(wait_id);

        if(scheduled_it!=m_scheduled.end()){
            m_scheduled.erase(scheduled_it);
            std::make_heap(m_scheduled.begin(),m_scheduled.end(),[](auto& a,auto& b){
            return a.second>b.second;});
        }

        return res;
    }

    uint64_t Eventloop::add_writer(int fd,Handle& handle,Callback cb, std::coroutine_handle<> coro){
        auto wait_id=next_wait_id++;
        auto id=handle.id();
        m_wait_callback[wait_id]=make_warped_callback(wait_id,id,std::move(cb), coro);
        m_coro_waits[id].insert(wait_id);
        if(!m_epoll.add_writer(fd,wait_id)){
            m_wait_callback.erase(wait_id);
            m_coro_waits[id].erase(wait_id);
            if(m_coro_waits[id].empty()){
                m_coro_waits.erase(id);
            }
            return 0;
        }
        return wait_id;
    }
        
    uint64_t Eventloop::add_reader(int fd,Handle& handle,Callback cb, std::coroutine_handle<> coro){
        auto wait_id=next_wait_id++;
        auto id=handle.id();
        m_wait_callback[wait_id]=make_warped_callback(wait_id,id,std::move(cb), coro);
        m_coro_waits[id].insert(wait_id);
        if(!m_epoll.add_reader(fd, wait_id)){
            m_wait_callback.erase(wait_id);
            m_coro_waits[id].erase(wait_id);
            if(m_coro_waits[id].empty()){
                m_coro_waits.erase(id);
            }
            return 0;
        }
        return wait_id;
    }
}