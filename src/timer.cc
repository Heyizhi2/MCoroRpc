/*
 * @Author: 来自火星的码农 15122322+heyizhi@user.noreply.gitee.com
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
    /**
     * @brief 构造函数
     * @param when 定时器到期时间点
     * @param callback 到期时执行的回调函数
     */
    Timer::Timer(types::TimePoint when, Callback callback)
    : Handle(), m_when(when), m_callback(std::move(callback)) {}

    /**
     * @brief 启动定时器
     * @details 将定时器注册到事件循环，到期时执行回调
     */
    void Timer::start() {
        m_wait_id = get_event_loop().call_at(m_when, *this, [this] { run(); });
    }

    /**
     * @brief 定时器到期执行函数
     * @details 检查取消状态后执行回调，仅执行一次
     */
    void Timer::run() {
        if (m_cancelled) return;
        m_cancelled = true;
        auto cb = std::move(m_callback);
        m_wait_id = 0;
        if (cb) cb();
    }

    /**
     * @brief 取消定时器
     * @note 线程安全，可多次调用
     */
    void Timer::cancel() noexcept {
        if (m_cancelled) return;
        m_cancelled = true;
        if (m_wait_id) {
            get_event_loop().cancel_wait(m_wait_id);
        }
    }

    /**
     * @brief 中止定时器
     * @details 与 cancel 类似，用于析构时清理
     */
    void Timer::abort() noexcept {
        if (m_cancelled) return;
        m_cancelled = true;
        if (m_wait_id) {
            get_event_loop().cancel_wait(m_wait_id);
        }
    }

    /**
     * @brief 析构函数
     * @details 自动中止未执行的定时器
     */
    Timer::~Timer() {
        abort();
    }
}
