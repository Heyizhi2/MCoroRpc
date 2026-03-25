/**
 * @file sleep.hpp
 * @brief 协程睡眠/延迟执行
 * 
 * 提供协程延迟执行的工具，类似于Go语言的time.Sleep。
 * 使用方式：
 * @code
 * co_await sleep_for(std::chrono::seconds(1));
 * @endcode
 */

#pragma once
#include "timer.hpp"
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <memory>
#include "../utils/noncopyable.hpp"

namespace Coro {
    /**
     * @brief 睡眠等待器
     * @tparam Rep 时间值的表示类型
     * @tparam Period 时间单位
     * 
     * 实现协程的awaitable接口，用于实现延迟执行。
     * 当协程co_await此等待器时，会挂起当前协程直到指定时间到达。
     */
    template<typename Rep, typename Period>
    struct SleepAwaiter:public Noncopyable{
        SleepAwaiter() = default;
        
        /**
         * @brief 构造函数
         * @param d 延迟时间
         */
        SleepAwaiter(std::chrono::duration<Rep, Period> d) : duration(d) {}
        
        /**
         * @brief 检查是否立即完成
         * @return false，总是需要挂起等待
         * 
         * 总是返回false，表示需要挂起协程等待定时器触发
         */
        bool await_ready()const noexcept{
            return false;
        }
        
        /**
         * @brief 协程挂起时的处理
         * @param h 当前协程句柄
         * 
         * 创建定时器，在定时器超时时恢复协程执行
         */
        void await_suspend(std::coroutine_handle<> h){
            auto when=types::Clock::now()+duration;
            m_timer=std::make_shared<Timer>(when,[h](){h.resume();});
            m_timer->run();
        }

        /**
         * @brief 协程恢复时的返回值
         * @return void，无返回值
         */
        void await_resume()const noexcept{}

        /**
         * @brief 取消睡眠
         * @details 中止定时器，协程恢复后立即完成
         */
        void cancel()noexcept{
            m_timer->cancel();
        }
        
        /**
         * @brief 析构函数
         * @details 确保定时器被正确中止
         */
        ~SleepAwaiter(){
            if(m_timer){
                m_timer->abort();
            }
            
        }
        
        /** @brief 延迟时间长度 */
        std::chrono::duration<Rep, Period> duration;
        
        /** @brief 定时器智能指针 */
        std::shared_ptr<Timer> m_timer{nullptr};
    };

    /**
     * @brief 创建睡眠等待器
     * @tparam Rep 时间值的表示类型
     * @tparam Period 时间单位
     * @param delay 延迟时间长度
     * @return SleepAwaiter 等待器对象
     * 
     * 使用示例：
     * @code
     * co_await sleep_for(std::chrono::milliseconds(100));
     * @endcode
     */
    template<typename Rep, typename Period>
    auto sleep_for(std::chrono::duration<Rep,Period> delay){
         return SleepAwaiter<Rep, Period>{delay};  // 直接返回 awaiter，不加 co_await
    }
   
}
