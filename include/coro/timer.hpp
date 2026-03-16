/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 16:12:59
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 13:33:28
 * @FilePath: /MCoroRpc/include/coro/timer.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include "handle.hpp"
#include "../comman/types.hpp"
#include <functional>
#include <memory>
namespace Coro {
    struct Timer:public Handle{
        public:
        using Callback=std::function<void()>;
        
        Timer(types::TimePoint timeout,Callback cb);

        Timer(Timer&)=delete;
        Timer& operator=(Timer&)=delete;
        Timer(Timer&& other);
        Timer& operator=(Timer&&);

        ~Timer() override;


        void start();

        void run()override;

        void cancel()noexcept override;

        void abort();
        private:
        bool m_cancelled{false};
        types::TimePoint m_when;
        Callback m_callback;
    };
}