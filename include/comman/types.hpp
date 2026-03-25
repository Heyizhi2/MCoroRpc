/**
 * @file types.hpp
 * @brief 通用类型定义
 * 
 * 本文件定义了框架中使用的通用类型别名和基础数据结构。
 * 包含时间类型、回调函数类型等。
 */

#pragma once
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include "../coro/handle.hpp"
#include <sys/epoll.h>

namespace Coro {
    /**
     * @brief 时间相关类型命名空间
     */
    namespace types {
        /**
         * @brief 稳定时钟类型别名
         * @details 使用std::chrono::steady_clock，
         * 这是一个单调时钟，不会受到系统时间调整的影响，适合用于测量时间间隔
         */
        using Clock =std::chrono::steady_clock;
        
        /**
         * @brief 时间点类型别名
         * @details 表示steady_clock的某个具体时间点
         */
        using TimePoint=std::chrono::time_point<Clock>;
    }

    

    /**
     * @brief 事件循环句柄结构
     * @details 用于在事件循环中注册回调函数，
     * 包含协程ID和对应的回调函数
     */
    struct EventloopHandle{
        /**
         * @brief 回调函数类型
         * @details 无参数无返回值的函数对象
         */
        using CallBack=std::function<void()>;
        
        /** @brief 协程的唯一标识ID */
        Handle::ID m_id;
        
        /** @brief 关联的回调函数 */
        CallBack m_cb;
    };
  
}
