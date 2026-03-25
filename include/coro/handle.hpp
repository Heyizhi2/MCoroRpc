/**
 * @file handle.hpp
 * @brief 协程句柄定义
 * 
 * 本文件定义了协程框架中的核心句柄类型。
 * - Handle: 基础协程句柄，抽象基类
 * - CoroHandle: 协程专用句柄，继承自Handle，提供协程调度功能
 * 
 * 协程状态：
 * - UNSCHEDULE: 未调度，协程刚创建还未加入调度队列
 * - SCHEDULE: 已调度，协程在就绪队列中等待执行
 * - CANCELLED: 已取消，协程被取消执行
 * - SUSPEND: 已挂起，协程正在等待某个异步操作完成
 */

#pragma once
#include <coroutine>
#include <cstdint>
#include <source_location>
#include <vector>

namespace Coro {
    /**
     * @brief 协程句柄基类
     * @details 所有可调度对象的抽象基类，包括协程、定时器等
     */
    struct Handle {
        /** @brief 句柄唯一标识ID类型 */
        using ID = uint32_t;
       
       /**
        * @brief 协程状态枚举
        */
       enum class State {
            UNSCHEDULE,   /**< 未调度状态 */
            SCHEDULE,     /**< 已调度/就绪状态 */
            CANCELLED,    /**< 已取消状态 */
            SUSPEND,      /**< 挂起/等待状态 */
       };
       
       /**
        * @brief 默认构造函数
        * @details 构造时自动生成唯一ID
        */
       Handle() noexcept : m_id(generator_id()) {}
       
       /**
        * @brief 设置协程状态
        * @param s 新的状态
        */
       void set_state(State s) noexcept { m_state = s; }
       
       /**
        * @brief 获取当前状态
        * @return 当前协程状态
        */
       State state() const noexcept { return m_state; }

       virtual ~Handle() noexcept;
       
       /**
        * @brief 执行协程体
        * @details 由事件循环调用，执行协程的实际工作
        */
       virtual void run() = 0;
       
       /**
        * @brief 获取句柄ID
        * @return 协程的唯一标识
        */
       ID id() const noexcept { return m_id; }

       /**
        * @brief 取消协程执行
        * @details 将协程标记为取消状态，并从事件循环中移除
        */
       virtual void cancel() noexcept;
       
     private:
        /**
         * @brief ID生成器
         * @details 静态ID生成器，确保每个Handle有唯一ID
         * @return 新生成的唯一ID
         */
        inline ID generator_id() noexcept {
             static ID id_generator = 0;
             return ++id_generator == 0 ? ++id_generator : id_generator;
        }

    protected:
       /** @brief 当前协程状态 */
       State m_state{ Handle::State::UNSCHEDULE };
       
    private:
       /** @brief 协程唯一标识 */
       ID m_id;
    };
    
    /**
     * @brief 协程专用句柄
     * @details 继承自Handle，增加了协程特有的功能：
     * - 协程调度(schedule)
     * - 调用栈回溯(traceback)
     * - 协程位置信息(location)
     */
    struct CoroHandle : public Handle {
        /**
         * @brief 输出调用栈回溯信息
         * @param depth 当前回溯深度
         */
        virtual void traceback(int depth) = 0;
        
        /**
         * @brief 调度协程执行
         * @details 将协程加入事件循环的就绪队列，等待执行
         */
        void schedule();
        
        /**
         * @brief 取消协程执行
         */
        void cancel() noexcept override;
        
        /**
         * @brief 获取协程创建位置
         * @details 用于调试，显示协程是在哪里创建的
         * @return 源代码位置信息
         */
        virtual const std::source_location& get_loc() const;
    };
}
