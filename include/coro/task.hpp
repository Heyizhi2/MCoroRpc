/**
 * @file task.hpp
 * @brief Task 协程实现
 * 
 * 提供协程的Promise和Task类型实现。
 * - Task: 协程的返回类型，类似于std::future
 * - Promise: 协程的Promise类型，管理协程状态和结果
 * - TaskGroup: 任务组，管理多个协程
 * 
 * 使用示例：
 * @code
 * Task<int> myCoroutine() {
 *     co_return 42;
 * }
 * 
 * auto task = myCoroutine();
 * int result = co_await task;
 * @endcode
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
    /**
     * @brief 任务取消异常
     * @details 当任务被取消时，如果尝试获取结果会抛出此异常
     */
    struct TaskCancelledException : std::exception {
        const char* what() const noexcept override {
            return "Task was cancelled.";
        }
    };

    /**
     * @brief 标记不使用初始挂起
     * @details 传递给Promise以立即开始执行协程（不挂起在co_await处）
     */
    struct NoAwaitatInitalSuspend{};
    
    /** @brief 默认不使用初始挂起的标记 */
    inline NoAwaitatInitalSuspend no_wait_at_initial_suspend;

    /**
     * @brief Task声明
     * @tparam R 协程返回值类型
     */
    template<typename R>
    struct Task;

    /**
     * @brief Promise实现
     * @tparam ResultType 协程返回值类型
     * 
     * 继承自CoroHandle以支持调度，继承TaskResult以存储结果
     */
    template<typename ResultType>
    struct Promise : public CoroHandle, public TaskResult<ResultType> {
        Promise() = default;
        
        /**
         * @brief 构造函数
         * @tparam Args 构造参数
         * @param 标记，是否等待初始挂起
         */
        template<typename... Args>
        Promise(NoAwaitatInitalSuspend, Args&&...) : m_wait_at_initial_suspend(false) {}
        
        /**
         * @brief 初始挂起等待器
         * @details 协程开始时的挂起点，控制是否立即执行
         */
        struct InitialAwaiter {
            constexpr bool await_ready() const noexcept { return !m_wait_at_initial_suspend; }
            template<typename P>
            void await_suspend(std::coroutine_handle<P>) const noexcept {}
            constexpr void await_resume() const noexcept {}
            const bool m_wait_at_initial_suspend{ true };
        };

        /**
         * @brief 最终挂起等待器
         * @details 协程结束时的挂起点，用于恢复父协程
         */
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

        /**
         * @brief 初始挂起点
         * @return InitialAwaiter
         */
        auto initial_suspend() noexcept {
            return InitialAwaiter{ m_wait_at_initial_suspend };
        }

        /**
         * @brief 最终挂起点
         * @return FinalAwaiter
         */
        auto final_suspend() noexcept {
            return FinalAwaiter{};
        }

        /**
         * @brief 获取协程的Task对象
         * @return Task<R> 协程的返回对象
         */
        Task<ResultType> get_return_object() {
            return Task<ResultType>{ std::coroutine_handle<Promise<ResultType>>::from_promise(*this) };
        }

        /**
         * @brief 执行协程
         */
        void run() final {
            std::coroutine_handle<Promise<ResultType>>::from_promise(*this).resume();
        }

        /**
         * @brief 获取协程位置
         */
        const std::source_location& get_loc() const override {
            return m_loc;
        }

        /**
         * @brief 打印调用栈
         */
        void traceback(int depth) final override {
            utils::print_location(m_loc, depth);
            if (m_parent) {
                m_parent->traceback(depth + 1);
            }
        }

        /**
         * @brief await_transform
         * @details 用于拦截co_await，获取位置信息
         */
        template<concepts::Awaiter _Awaiter>
        decltype(auto) await_transform(_Awaiter&& awaiter, std::source_location loc = std::source_location::current()) {
            m_loc = loc;
            return std::forward<_Awaiter>(awaiter);
        }

        /**
         * @brief 设置父协程
         */
        void set_parent(CoroHandle* p) { m_parent = p; }
        
        /**
         * @brief 获取父协程
         */
        CoroHandle* parent() const { return m_parent; }
        
        /**
         * @brief 设置取消状态
         */
        void set_cancelled(bool v) { m_cancelled = v; }
        
        /**
         * @brief 获取取消状态
         */
        bool cancelled() const { return m_cancelled; }

    private:
        /** @brief 是否等待初始挂起 */
        const bool m_wait_at_initial_suspend{ true };
        
        /** @brief 父协程指针 */
        CoroHandle* m_parent{ nullptr };
        
        /** @brief 协程创建位置 */
        std::source_location m_loc{};
        
        /** @brief 是否已取消 */
        bool m_cancelled{ false };
    };

    /**
     * @brief Task类模板
     * @tparam ResultType 协程返回值类型
     * 
     * Task是协程的返回类型，类似于std::future。
     * 可以co_await获取结果，或调用get_result()同步获取。
     */
    template<typename ResultType = void>
    class Task {
    public:
        /** @brief Promise类型 */
        using promise_type = Promise<ResultType>;
        
        /** @brief 协程句柄类型 */
        using coro_handle = std::coroutine_handle<promise_type>;
        
        /**
         * @brief Task的Awaiter基类
         */
        struct AwaiterBase {
            /**
             * @brief 检查协程是否完成
             */
            constexpr bool await_ready() const noexcept {
                if (self_handle) [[likely]] {
                    return self_handle.done();
                }
                return true;
            }

            /**
             * @brief 挂起当前协程，恢复Task协程
             */
            template<typename _promise>
            void await_suspend(std::coroutine_handle<_promise> parent) const noexcept {
                assert(!self_handle.promise().parent());
                parent.promise().set_state(Handle::State::SUSPEND);
                //记录父协程
                self_handle.promise().set_parent(&parent.promise());
                //将自身协程放入eventloop中
                self_handle.promise().schedule();
            }
            
            /** @brief Task协程句柄 */
            coro_handle self_handle{};
        };

    public:
        /**
         * @brief 构造函数
         * @param h 协程句柄
         */
        explicit Task(coro_handle h) noexcept : m_handle(h) {}
        
        /**
         * @brief 移动构造函数
         */
        Task(Task&& t) noexcept : m_handle(std::exchange(t.m_handle, {})) {}
        
        ~Task() = default;

        /**
         * @brief 获取结果（左值版本）
         */
        decltype(auto) get_result() & {
            return m_handle.promise().get_result();
        }

        /**
         * @brief 获取结果（右值版本）
         */
        decltype(auto) get_result() && {
            return std::move(m_handle.promise()).get_result();
        }

        /**
         * @brief co_await运算符（左值）
         */
        auto operator co_await() const & noexcept {
            struct Awaiter : public AwaiterBase {
                 //执行完成后返回一个结果
                decltype(auto) await_resume() {
                    if (!AwaiterBase::self_handle) [[unlikely]] {
                        throw ExceptionInvalidFuture();
                    }
                    return AwaiterBase::self_handle.promise().get_result();
                }
            };
            return Awaiter{ m_handle };
        }

        /**
         * @brief co_await运算符（右值）
         */
        auto operator co_await() const && noexcept {
            struct Awaiter : public AwaiterBase {
                //执行完成后返回一个结果
                decltype(auto) await_resume() {
                    if (!AwaiterBase::self_handle) [[unlikely]] {
                        throw ExceptionInvalidFuture();
                    }
                    return std::move(AwaiterBase::self_handle.promise()).get_result();
                }
            };
            return Awaiter{ m_handle };
        }

        /**
         * @brief 检查Task是否有效
         */
        bool valid() const { return m_handle != nullptr; }
        
        /**
         * @brief 检查协程是否完成
         */
        bool done() const { return m_handle == nullptr || m_handle.done(); }
        
        /**
         * @brief 检查是否已取消
         */
        bool cancelled() const { 
            if (m_handle) {
                return m_handle.promise().cancelled();
            }
            return false;
        }
        
        /**
         * @brief 取消Task
         */
        void cancel() {
            if (m_handle && !m_handle.done()) {
                m_handle.promise().set_cancelled(true);
                get_event_loop().cancel(m_handle.promise().id());
                destroy();
            }
        }
        
        /**
         * @brief 调度Task执行
         */
        void schedule() {
            if (m_handle) {
                m_handle.promise().schedule();
            }
        }

        /**
         * @brief 调度Task到指定事件循环
         */
        void scheduleOn(Eventloop& loop) {
            if (m_handle) {
                m_handle.promise().scheduleOn(loop);
            }
        }

    private:
        /**
         * @brief 销毁协程句柄
         */
        void destroy() {
            if (auto handle = std::exchange(m_handle, nullptr)) {
                handle.destroy();
            }
        }

    private:
        /** @brief 协程句柄 */
        coro_handle m_handle;
    };


}
