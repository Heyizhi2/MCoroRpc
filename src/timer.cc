/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 18:48:30
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 17:10:22
 * @FilePath: /MCoroRpc/src/timer.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <algorithm>
#include <utility>
namespace Coro {
          Timer::Timer(types::TimePoint when, Callback callback)
          : Handle(), m_when(when), m_callback(std::move(callback)) {}

      void Timer::start() {
          m_wait_id = get_event_loop().call_at(m_when, *this, [this] { run(); });
      }

      void Timer::run() {
          if (m_cancelled) return;
          m_cancelled = true;
          auto cb = std::move(m_callback);
          m_wait_id = 0;
          if (cb) cb();
      }

      void Timer::cancel() noexcept {
          if (m_cancelled) return;
          m_cancelled = true;
          if (m_wait_id) {
              if (auto cb = get_event_loop().cancel_wait(m_wait_id)) {
                  (*cb)();  // 立即唤醒
              }
          }
      }

      void Timer::abort() noexcept {
          if (m_cancelled) return;
          m_cancelled = true;
          if (m_wait_id) {
              get_event_loop().cancel_wait(m_wait_id);
          }
      }

      Timer::~Timer() {
          abort();
      }
}       
