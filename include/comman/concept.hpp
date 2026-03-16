/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 14:03:51
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-15 17:09:16
 * @FilePath: /MCoroRpc/include/comman/concept.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <utility>
#include <coroutine>
namespace Coro {
    namespace detail {
        
    template<typename T>
    struct GetAwaiter {
        using type = T;  // 默认 T 本身就是 awaiter
    };

    template<typename T>
    requires requires(T&& t) { std::forward<T>(t).operator co_await(); }
    struct GetAwaiter<T> {
        using type = decltype(std::declval<T>().operator co_await());
    };

    template<typename T>
    requires requires(T&& t) { operator co_await(std::forward<T>(t)); } &&
             (!requires(T&& t) { std::forward<T>(t).operator co_await(); })
    struct GetAwaiter<T> {
        using type = decltype(operator co_await(std::declval<T>()));
    };

    template<typename T>
    using AwaiterType = typename GetAwaiter<T>::type;
    
    }


    namespace concepts {
         template<typename T>
            concept Awaiter = requires(detail::AwaiterType<T> a, std::coroutine_handle<> h) {
            { a.await_ready() } -> std::convertible_to<bool>;
            a.await_suspend(h);  // 只要求合法，不检查返回类型（若需精确，可进一步细分）
            a.await_resume();
            };
    }

static_assert(concepts::Awaiter<std::suspend_always>);
static_assert(concepts::Awaiter<std::suspend_never>);
}
