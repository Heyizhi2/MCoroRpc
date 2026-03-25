/**
 * @file concept.hpp
 * @brief C++20 协程 Awaiter 概念定义
 * 
 * 本文件定义了C++20协程中Awaiter(等待器)的概念和相关类型萃取。
 * 
 * C++20协程中，一个类型要成为awaiter，需要满足以下三个条件：
 * 1. await_ready() - 返回bool，表示是否立即完成无需挂起
 * 2. await_suspend(std::coroutine_handle) - 接受协程句柄，用于恢复协程
 * 3. await_resume() - 返回await表达式的值
 * 
 * Awaiter可以通过两种方式定义：
 * 1. 类型定义了成员函数 operator co_await()
 * 2. 类型定义了非成员函数 operator co_await()
 */

#pragma once
#include <utility>
#include <coroutine>

namespace Coro {
    namespace detail {
        /**
         * @brief 获取类型的Awaiter类型
         * @details 这是类型萃取的核心，根据不同情况获取正确的awaiter类型：
         * 
         * 默认情况：如果T本身就是awaiter，则T就是AwaiterType
         * 
         * 情况1：如果T有成员函数 operator co_await()，
         *        则通过该函数获取awaiter
         * 
         * 情况2：如果T有非成员函数 operator co_await()（自由函数），
         *        则通过该函数获取awaiter
         * 
         * @tparam T 要获取awaiter的类型
         */
        
        /**
         * @brief 默认情况：T本身就是awaiter
         */
        template<typename T>
        struct GetAwaiter {
            using type = T;  // 默认 T 本身就是 awaiter
        };

        /**
         * @brief 情况1：T有成员函数 operator co_await()
         * @details 使用requires表达式检测T是否有成员co_await()方法
         */
        template<typename T>
        requires requires(T&& t) { std::forward<T>(t).operator co_await(); }
        struct GetAwaiter<T> {
            using type = decltype(std::declval<T>().operator co_await());
        };

        /**
         * @brief 情况2：T有非成员函数 operator co_await()
         * @details 检测是否有自由函数形式的operator co_await，
         *          同时排除已经有成员函数的情况
         */
        template<typename T>
        requires requires(T&& t) { operator co_await(std::forward<T>(t)); } &&
                 (!requires(T&& t) { std::forward<T>(t).operator co_await(); })
        struct GetAwaiter<T> {
            using type = decltype(operator co_await(std::declval<T>()));
        };

        /**
         * @brief Awaiter类型的类型别名便捷定义
         */
        template<typename T>
        using AwaiterType = typename GetAwaiter<T>::type;
        
    }


    namespace concepts {
        /**
         * @brief Awaiter概念定义
         * @details 使用C++20 concept语法定义Awaiter概念
         * 
         * 一个类型A要成为Awaiter，需要满足：
         * 1. a.await_ready() 可调用且返回bool（或可转换为bool）
         * 2. a.await_suspend(h) 合法，其中h是std::coroutine_handle<>
         * 3. a.await_resume() 可调用
         * 
         * @tparam A 待检测的类型
         */
         template<typename A>
            concept Awaiter = requires(detail::AwaiterType<A> a, std::coroutine_handle<> h) {
            { a.await_ready() } -> std::convertible_to<bool>;
            a.await_suspend(h);  // 只要求合法，不检查返回类型（若需精确，可进一步细分）
            a.await_resume();
        };
    }

    /**
     * @brief 静态断言验证概念定义正确性
     * @details 验证标准库的suspend_always和suspend_never满足Awaiter概念
     * - std::suspend_always::await_ready() 总是返回false（总是挂起）
     * - std::suspend_never::await_ready() 总是返回true（从不挂起）
     */
    static_assert(concepts::Awaiter<std::suspend_always>);
    static_assert(concepts::Awaiter<std::suspend_never>);
}
