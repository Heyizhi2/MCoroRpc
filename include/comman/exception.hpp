/**
 * @file exception.hpp
 * @brief 协程框架异常定义
 * 
 * 本文件定义了协程框架中使用的各种异常类型，
 * 用于在协程执行过程中报告错误情况。
 */

#pragma once
#include <exception>

namespace Coro {
    /**
     * @brief 无效Future异常
     * @details 当尝试获取一个无效的协程Future的结果时抛出此异常。
     * 通常发生在：
     * - Task对象未被正确初始化
     * - 在协程handle被销毁后尝试获取结果
     * - 违反了协程的使用规则
     */
    struct ExceptionInvalidFuture:public std::exception{
        /**
         * @brief 获取异常描述信息
         * @return 异常描述字符串
         */
        [[nodiscard]] const char* what()const noexcept{
            return  "[Exception] Invalid future";
        } 
    };
}
