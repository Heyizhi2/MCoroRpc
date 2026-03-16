/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 14:02:37
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 14:06:17
 * @FilePath: /MCoroRpc/include/coro/handle.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <cstdint>
#include <source_location>
namespace Coro {
    struct Handle{
        using ID=uint32_t;
       
       enum class State{
            UNSCHEDULE,
            SCHEDULE,
            CANCELLED,
            SUSPEND,
       };
       Handle()noexcept:m_id(generator_id()){}
       
       void set_state(State s)noexcept{m_state=s;}

       virtual ~Handle()noexcept;
       
       virtual void run()=0;
       ID id() const noexcept{return  m_id;}

       virtual void cancel() noexcept;
       private:
       inline ID generator_id() noexcept {
            static ID id_generator = 0;
            return ++id_generator == 0 ? ++id_generator : id_generator;
        }

        protected:
        State m_state{Handle::State::UNSCHEDULE};
        
        private:
        ID m_id;
       
    };
    struct CoroHandle:public Handle{
        virtual void traceback(int depth)=0;
        void schedule();
        private:
        virtual const std::source_location &get_loc()const;

    };
}