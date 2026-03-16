/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 16:13:08
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 14:49:14
 * @FilePath: /MCoroRpc/include/coro/sleep.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include "timer.hpp"
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <memory>
#include "../utils/noncopyable.hpp"
namespace Coro {
    template<typename Rep, typename Period>
    struct SleepAwaiter:public Noncopyable{
        SleepAwaiter() = default;
        SleepAwaiter(std::chrono::duration<Rep, Period> d) : duration(d) {}
        
        bool await_ready()const noexcept{
            return false;
        }
        void await_suspend(std::coroutine_handle<> h){
            auto when=types::Clock::now()+duration;
            m_timer=std::make_shared<Timer>(when,[h](){h.resume();});
            m_timer->start();
        }

        void await_resume()const noexcept{}

        void cancel()noexcept{
            m_timer->cancel();
        }
        
        ~SleepAwaiter(){
            if(m_timer){
                m_timer->abort();
            }
            
        }
        std::chrono::duration<Rep, Period> duration;
        std::shared_ptr<Timer> m_timer{nullptr};
    };

    template<typename Rep, typename Period>
    auto sleep_for(std::chrono::duration<Rep,Period> delay){
         return SleepAwaiter<Rep, Period>{delay};  // 直接返回 awaiter，不加 co_await
    }
   
}