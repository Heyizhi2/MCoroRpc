/**
 * @file wait_for.hpp
 * @brief 带超时的等待实现
 */

#pragma once
#include "timer.hpp"
#include "task.hpp"
#include "sleep.hpp"
#include <chrono>
#include <stdexcept>
#include <atomic>
#include <memory>
#include <type_traits>

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

template<typename T>
auto wait_for(Task<T> task, std::chrono::milliseconds timeout) -> Task<WaitForResult<T>> {
    WaitForResult<T> result;
    bool timeout_fired = false;
    std::atomic<bool> completed{false};
    
    auto timer = std::make_shared<Timer>(
        types::Clock::now() + timeout,
        [&completed, &timeout_fired]() {
            if (!completed.load()) {
                timeout_fired = true;
            }
        }
    );
    timer->start();
    
    try {
        if constexpr (std::is_void_v<T>) {
            co_await task;
        } else {
            result.value = co_await std::move(task);
        }
        result.ok = true;
    } catch (...) {
    }
    
    completed.store(true);
    timer->abort();
    result.is_timeout = timeout_fired;
    
    co_return result;
}

template<typename T, typename Duration>
auto wait_for(Task<T> task, Duration duration) -> Task<WaitForResult<T>> {
    return wait_for(std::move(task), 
        std::chrono::duration_cast<std::chrono::milliseconds>(duration));
}

}
