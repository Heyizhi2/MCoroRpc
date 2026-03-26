/**
 * @file wait_for.hpp
 * @brief 带超时的等待实现
 */

#pragma once
#include "timer.hpp"
#include "task.hpp"
#include <chrono>
#include <stdexcept>
#include <atomic>
#include <memory>

namespace Coro {

struct TimeoutException : std::exception {
    const char* what() const noexcept override {
        return "Operation timed out";
    }
};

template<typename T>
struct WaitForResult {
    bool ok = false;
    T value;
    bool is_timeout = false;
    
    bool success() const { return ok && !is_timeout; }
    explicit operator bool() const { return success(); }
};

template<>
struct WaitForResult<void> {
    bool ok = false;
    bool is_timeout = false;
    
    bool success() const { return ok && !is_timeout; }
    explicit operator bool() const { return success(); }
};

namespace detail {

struct WaitForState {
    std::atomic<bool> notified{false};
    std::atomic<bool> is_timeout{false};
    std::exception_ptr error;
    CoroHandle* continuation = nullptr;
    std::shared_ptr<Timer> timer;
    std::atomic<bool> task_done{false};
};

inline void timeout_callback(std::shared_ptr<WaitForState> state) {
    if (state->notified.exchange(true)) {
        return;
    }
    state->is_timeout.store(true);
    if (state->timer) {
        state->timer->cancel();
    }
    if (state->continuation) {
        get_event_loop().call_soon(*state->continuation, [state]() {
            state->continuation->run();
        });
    }
}

inline void complete_callback(std::shared_ptr<WaitForState> state) {
    if (state->notified.exchange(true)) {
        return;
    }
    if (state->timer) {
        state->timer->cancel();
    }
    if (state->continuation) {
        get_event_loop().call_soon(*state->continuation, [state]() {
            state->continuation->run();
        });
    }
}

template<typename T>
struct WaitForAwaiter {
    WaitForAwaiter(Task<T> task, std::chrono::milliseconds timeout)
        : task_(std::move(task)), timeout_(timeout) {}
    
    bool await_ready() const noexcept { return false; }
    
    decltype(auto) await_resume() {
        if (state_->is_timeout.load()) {
            throw TimeoutException{};
        }
        if (state_->error) {
            std::rethrow_exception(state_->error);
        }
        if constexpr (std::is_void_v<T>) {
            return;
        } else {
            return std::move(value_);
        }
    }
    
    template<typename P>
    void await_suspend(std::coroutine_handle<P> continuation) {
        state_ = std::make_shared<WaitForState>();
        state_->continuation = static_cast<CoroHandle*>(&continuation.promise());
        state_->continuation->set_state(Handle::State::SUSPEND);
        
        state_->timer = std::make_shared<Timer>(
            types::Clock::now() + timeout_,
            [state = state_]() {
                timeout_callback(state);
            }
        );
        state_->timer->start();
        
        auto wrapper = [this, state = state_]() -> Task<> {
            try {
                if constexpr (std::is_void_v<T>) {
                    co_await task_;
                } else {
                    value_ = co_await task_;
                }
            } catch (...) {
                if (!state->is_timeout.load()) {
                    state->error = std::current_exception();
                }
            }
            
            if (state->task_done.exchange(true)) {
                co_return;
            }
            
            complete_callback(state);
        };
        
        wrapper().schedule();
    }
    
private:
    Task<T> task_;
    std::chrono::milliseconds timeout_;
    std::shared_ptr<WaitForState> state_;
    T value_;
};

}

template<typename T>
auto wait_for(Task<T> task, std::chrono::milliseconds timeout) -> Task<WaitForResult<T>> {
    using Awaiter = detail::WaitForAwaiter<T>;
    auto awaiter = Awaiter{std::move(task), timeout};
    
    WaitForResult<T> result;
    try {
        co_await awaiter;
        result.ok = true;
    } catch (TimeoutException&) {
        result.is_timeout = true;
    }
    
    co_return result;
}

template<typename T, typename Duration>
auto wait_for(Task<T> task, Duration duration) -> Task<WaitForResult<T>> {
    return wait_for(std::move(task), 
        std::chrono::duration_cast<std::chrono::milliseconds>(duration));
}

}