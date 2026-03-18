/*
 * @Author: 来自火星的码农 15122322+heyizhi@user.noreply.gitee.com
 * @Date: 2026-03-15 10:48:12
 * @LastEditors: 来自火星的码农 15122322+heyizhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 20:55:00
 * @FilePath: /MCoroRpc/include/coro/task.hpp
 * @Description: Task 实现
 */
#pragma once
#include "event_loop.hpp"
#include "handle.hpp"
#include "taskresult.hpp"
#include <cassert>
#include <coroutine>
#include <source_location>
#include <utility>
#include "../comman/utils.hpp"
#include "../comman/concept.hpp"
#include "../comman/exception.hpp"

namespace Coro {

struct TaskCancelledException : std::exception {
    const char* what() const noexcept override {
        return "Task was cancelled.";
    }
};

struct NoAwaitatInitalSuspend{};
inline NoAwaitatInitalSuspend no_wait_at_initial_suspend;

template<typename R>
struct Task;

template<typename ResultType>
struct Promise : public CoroHandle, public TaskResult<ResultType> {
    Promise() = default;
    
    template<typename... Args>
    Promise(NoAwaitatInitalSuspend, Args&&...) : m_wait_at_initial_suspend(false) {}
    
    struct InitialAwaiter {
        constexpr bool await_ready() const noexcept { return !m_wait_at_initial_suspend; }
        template<typename P>
        void await_suspend(std::coroutine_handle<P>) const noexcept {}
        constexpr void await_resume() const noexcept {}
        const bool m_wait_at_initial_suspend{ true };
    };

    struct FinalAwaiter {
        bool await_ready() const noexcept { 
            return false; 
        }
        
        template<typename P>
        void await_suspend(std::coroutine_handle<P> handle) noexcept {
            auto parent = handle.promise().parent();
            if (parent) {
                parent->set_state(Handle::State::SCHEDULE);
                get_event_loop().call_soon(*parent, [parent]() { parent->run(); });
            }
        }
        
        constexpr void await_resume() const noexcept {}
    };

    auto initial_suspend() noexcept {
        return InitialAwaiter{ m_wait_at_initial_suspend };
    }

    auto final_suspend() noexcept {
        return FinalAwaiter{};
    }

    Task<ResultType> get_return_object() {
        return Task<ResultType>{ std::coroutine_handle<Promise<ResultType>>::from_promise(*this) };
    }

    void run() final {
        std::coroutine_handle<Promise<ResultType>>::from_promise(*this).resume();
    }

    const std::source_location& get_loc() const override {
        return m_loc;
    }

    void traceback(int depth) final override {
        utils::print_location(m_loc, depth);
        if (m_parent) {
            m_parent->traceback(depth + 1);
        }
    }

    template<concepts::Awaiter _Awaiter>
    decltype(auto) await_transform(_Awaiter&& awaiter, std::source_location loc = std::source_location::current()) {
        m_loc = loc;
        return std::forward<_Awaiter>(awaiter);
    }

    void set_parent(CoroHandle* p) { m_parent = p; }
    CoroHandle* parent() const { return m_parent; }
    
    void set_cancelled(bool v) { m_cancelled = v; }
    bool cancelled() const { return m_cancelled; }

private:
    const bool m_wait_at_initial_suspend{ true };
    CoroHandle* m_parent{ nullptr };
    std::source_location m_loc{};
    bool m_cancelled{ false };
};

template<typename ResultType = void>
class Task {
public:
    using promise_type = Promise<ResultType>;
    using coro_handle = std::coroutine_handle<promise_type>;
    
    struct AwaiterBase {
        constexpr bool await_ready() const noexcept {
            if (self_handle) [[likely]] {
                return self_handle.done();
            }
            return true;
        }

        template<typename _promise>
        void await_suspend(std::coroutine_handle<_promise> parent) const noexcept {
            assert(!self_handle.promise().parent());
            parent.promise().set_state(Handle::State::SUSPEND);
            self_handle.promise().set_parent(&parent.promise());
            self_handle.promise().schedule();
        }
        
        coro_handle self_handle{};
    };

public:
    explicit Task(coro_handle h) noexcept : m_handle(h) {}
    
    Task(Task&& t) noexcept : m_handle(std::exchange(t.m_handle, {})) {}
    
    ~Task() = default;

    decltype(auto) get_result() & {
        return m_handle.promise().get_result();
    }

    decltype(auto) get_result() && {
        return std::move(m_handle.promise()).get_result();
    }

    auto operator co_await() const & noexcept {
        struct Awaiter : public AwaiterBase {
            decltype(auto) await_resume() {
                if (!AwaiterBase::self_handle) [[unlikely]] {
                    throw ExceptionInvalidFuture();
                }
                return AwaiterBase::self_handle.promise().get_result();
            }
        };
        return Awaiter{ m_handle };
    }

    auto operator co_await() const && noexcept {
        struct Awaiter : public AwaiterBase {
            decltype(auto) await_resume() {
                if (!AwaiterBase::self_handle) [[unlikely]] {
                    throw ExceptionInvalidFuture();
                }
                return std::move(AwaiterBase::self_handle.promise()).get_result();
            }
        };
        return Awaiter{ m_handle };
    }

    bool valid() const { return m_handle != nullptr; }
    bool done() const { return m_handle == nullptr || m_handle.done(); }
    bool cancelled() const { 
        if (m_handle) {
            return m_handle.promise().cancelled();
        }
        return false;
    }
    
    void cancel() {
        if (m_handle) {
            m_handle.promise().set_cancelled(true);
            destroy();
        }
    }
    
    void schedule() {
        if (m_handle) {
            m_handle.promise().schedule();
        }
    }

private:
    void destroy() {
        if (auto handle = std::exchange(m_handle, nullptr)) {
            handle.destroy();
        }
    }

private:
    coro_handle m_handle;
};

template<typename ResultType = void>
class TaskGroup {
public:
    TaskGroup() = default;
    
    template<typename F>
    Task<ResultType> spawn(F&& func) {
        auto task = func();
        m_tasks.push_back(std::move(task));
        return task;
    }
    
    void cancel() {
        for (auto& task : m_tasks) {
            task.cancel();
        }
    }
    
    void clear() {
        m_tasks.clear();
    }

private:
    std::vector<Task<ResultType>> m_tasks;
};

}
