/*
 * @Author: 来自火星的码农 15122322+heyizhi@user.noreply.gitee.com
 * @Date: 2026-03-15 14:02:37
 * @LastEditors: 来自火星的码农 15122322+heyizhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 19:30:00
 * @FilePath: /MCoroRpc/include/coro/handle.hpp
 * @Description: 协程句柄定义
 */
#pragma once
#include <coroutine>
#include <cstdint>
#include <source_location>
#include <vector>

namespace Coro {
    struct Handle {
        using ID = uint32_t;
       
       enum class State {
            UNSCHEDULE,
            SCHEDULE,
            CANCELLED,
            SUSPEND,
       };
       
       Handle() noexcept : m_id(generator_id()) {}
       
       void set_state(State s) noexcept { m_state = s; }
       State state() const noexcept { return m_state; }

       virtual ~Handle() noexcept;
       
       virtual void run() = 0;
       
       ID id() const noexcept { return m_id; }

       virtual void cancel() noexcept;
       
    private:
       inline ID generator_id() noexcept {
            static ID id_generator = 0;
            return ++id_generator == 0 ? ++id_generator : id_generator;
       }

    protected:
       State m_state{ Handle::State::UNSCHEDULE };
       
    private:
       ID m_id;
    };
    
    struct CoroHandle : public Handle {
        virtual void traceback(int depth) = 0;
        void schedule();
        void cancel() noexcept override;
        
        virtual const std::source_location& get_loc() const;
    };
}
