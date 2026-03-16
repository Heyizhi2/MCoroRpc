/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 18:48:30
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 14:55:50
 * @FilePath: /MCoroRpc/src/timer.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <algorithm>
#include <utility>
namespace Coro {
      Timer::Timer(types::TimePoint when,Callback callback)
      :Handle()
      ,m_when(when){
            m_callback=std::move(callback);
       }

      Timer& Timer::operator=(Timer&& timer){
        m_when=timer.m_when;
        m_callback=std::exchange(timer.m_callback,nullptr);
        return *this;
      }

      Timer::Timer(Timer&& other)
      :Handle(std::move(other))
      ,m_when(std::move(other.m_when)){
        m_callback=std::exchange(other.m_callback,nullptr);
      }


     void Timer::cancel()noexcept{
         if(m_cancelled){
          return;
        }
        m_cancelled=true;
        get_event_loop().cancel(id());
        if(m_callback){
          m_callback();
        }
     }

     void Timer::run(){
       if(m_cancelled){
          return;
        }
        m_cancelled=true;
        if(m_callback){
          m_callback();
        }
     }

     void Timer::start(){
        get_event_loop().call_at(m_when,*this,[this](){this->run();});
     }

     void Timer::abort(){
        if(m_cancelled){
          return;
        }
        m_cancelled=true;
        get_event_loop().cancel(id());
     }
     Timer::~Timer(){
        
     }
}       
