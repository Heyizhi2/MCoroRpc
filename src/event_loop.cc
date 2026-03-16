/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-16 09:50:30
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 15:24:50
 * @FilePath: /MCoroRpc/src/event_loop.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <algorithm>
namespace Coro {
    void Eventloop::call_soon(Handle& handle,Callback callback){
        handle.set_state(Handle::State::SUSPEND);
        auto id=handle.id();
        m_ready_handle.push(id);
        m_callbacks.insert({id,std::move(callback)});    
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
        auto now=types::Clock::now();
        while(!m_scheduled.empty()&&m_scheduled[0].second<=now){
            auto[id,when]=m_scheduled.front();
            std::pop_heap(m_scheduled.begin(),m_scheduled.end(),[](auto &a,auto& b){
                return a.second>b.second;
            });
            m_scheduled.pop_back();
            m_ready_handle.push(id);
        }
        while (!m_ready_handle.empty()) {
            auto id=m_ready_handle.front();
            m_ready_handle.pop();
            auto it=m_callbacks.find(id);
            if(it!=m_callbacks.end()){
                auto func=std::move(it->second);
                m_callbacks.erase(it);
                if(func){
                    func();
                }
            }
        }
    }

    void Eventloop::call_at(types::TimePoint when,Handle&handle,Callback cb){
        auto id=handle.id();
        m_callbacks[id]=std::move(cb);
        m_scheduled.emplace_back(id,when);
        std::push_heap(m_scheduled.begin(),m_scheduled.end(),[](auto& a,auto& b){
            return  a.second>b.second;
        });
        handle.set_state(Handle::State::SCHEDULE);
    }
}