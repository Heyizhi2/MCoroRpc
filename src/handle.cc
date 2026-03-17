/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 14:28:14
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 16:05:08
 * @FilePath: /MCoroRpc/src/handle.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
namespace Coro {
    void CoroHandle::schedule(){
        if(m_state==Handle::State::UNSCHEDULE){
            get_event_loop().call_soon(*this,[this](){this->run();});
        }
    }

    void CoroHandle::cancel()noexcept{
        for(auto c:child){
            if(auto* p=static_cast<CoroHandle*>(c.address())){
                p->cancel();
            }
        }
        child.clear();
        Handle::cancel();
    }

    void Handle::cancel()noexcept{
       if(m_state==State::CANCELLED){
            return;
       }else {
            get_event_loop().cancel(m_id);
       }
       m_state=State::CANCELLED;
    }

    Handle::~Handle()noexcept{
        cancel();
    }
    const std::source_location& CoroHandle::get_loc()const{
        static const auto loc=std::source_location::current();
        return  loc;
    }
}