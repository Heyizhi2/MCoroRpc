/**
 * @file timer.hpp
 * @brief 定时器定义
 * 
 * 定时器是协程框架中的重要组件，用于：
 * - 实现协程延迟执行 (sleep_for)
 * - 实现定时任务
 * - 超时处理
 */

#pragma once
#include "handle.hpp"
#include "../comman/types.hpp"
#include <functional>
#include <memory>

namespace Coro {
    /**
     * @brief 定时器句柄
     * @details 继承自Handle，在指定时间点执行回调函数
     */
    struct Timer:public Handle{
        public:
        /** @brief 定时器回调函数类型 */
        using Callback=std::function<void()>;
        
        /**
         * @brief 构造函数
         * @param timeout 超时时间点
         * @param cb 回调函数
         */
        Timer(types::TimePoint timeout,Callback cb);

        Timer(Timer&)=delete;
        Timer& operator=(Timer&)=delete;
        
        /**
         * @brief 移动构造函数
         */
        Timer(Timer&& other);
        
        /**
         * @brief 移动赋值运算符
         */
        Timer& operator=(Timer&&);

        ~Timer() override;

        /**
         * @brief 启动定时器
         * @details 将定时器注册到事件循环中
         */
        void start();

        /**
         * @brief 执行定时器回调
         * @details 由事件循环在定时器超时时调用
         */
        void run()override;

        /**
         * @brief 执行定时器回调
         * @details 由事件循环在定时器超时时调用
         */
        void cancel()noexcept override;

        /**
         * @brief 中止定时器
         * @details 不执行回调，直接从事件循环中移除
         */
        void abort()noexcept;
        
        private:
        /** @brief 是否已被取消 */
        bool m_cancelled{false};
        
        /** @brief 超时时间点 */
        types::TimePoint m_when;
        
        /** @brief 超时回调函数 */
        Callback m_callback;
        
        /** @brief 在事件循环中的等待ID */
        uint64_t m_wait_id = 0;
    };
}
