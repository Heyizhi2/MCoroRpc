/*
 * @Author: 来自火星的码农 15122322+heyizhi@user.noreply.gitee.com
 * @Date: 2026-03-15 14:28:14
 * @LastEditors: 来自火星的码农 15122322+heyizhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 19:30:00
 * @FilePath: /MCoroRpc/src/handle.cc
 * @Description: 协程句柄实现
 */
#include "../include/coro.hpp"

namespace Coro {
    void CoroHandle::schedule() {
        if (m_state == Handle::State::UNSCHEDULE) {
            get_event_loop().call_soon(*this, [this]() { this->run(); });
        }
    }

    void CoroHandle::scheduleOn(Eventloop& loop) {
        if (m_state == Handle::State::UNSCHEDULE) {
            loop.call_soon(*this, [this]() { this->run(); });
        }
    }

    void CoroHandle::cancel() noexcept {
        Handle::cancel();
    }

    void Handle::cancel() noexcept {
        if (m_state == State::CANCELLED) {
            return;
        }
        get_event_loop().cancel(m_id);
        m_state = State::CANCELLED;
    }

    Handle::~Handle() noexcept {
        cancel();
    }
    
    const std::source_location& CoroHandle::get_loc() const {
        static const auto loc = std::source_location::current();
        return loc;
    }
}
