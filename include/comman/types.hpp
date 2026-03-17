/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 14:04:12
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 14:38:46
 * @FilePath: /MCoroRpc/include/comman/types.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include "../coro/handle.hpp"
#include <sys/epoll.h>
namespace Coro {
    namespace types {
        using Clock =std::chrono::steady_clock;
        using TimePoint=std::chrono::time_point<Clock>;
    }

    

    struct EventloopHandle{
        using CallBack=std::function<void()>;
        Handle::ID m_id;
        CallBack m_cb;
    };
  
}