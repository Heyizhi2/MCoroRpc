/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 10:48:12
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 16:44:23
 * @FilePath: /MCoroRpc/include/coro/task.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
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
    struct NoAwaitatInitalSuspend{};
    inline NoAwaitatInitalSuspend no_wait_at_initial_suspend;
    template<typename R>
    struct Task;


    template<typename ResultType>
    struct Promise:public CoroHandle,public TaskResult<ResultType>{
        Promise()=default;
        template<typename  ...Args>

        Promise(NoAwaitatInitalSuspend,Args&& ...):m_wait_at_initial_suspend(false){}
        template<typename Obj,typename ... Args>

        Promise(Obj &&,NoAwaitatInitalSuspend,Args&&...):m_wait_at_initial_suspend(false){}
        struct InitialAwaiter{
            constexpr bool await_ready()const noexcept{return !m_wait_at_initial_suspend;}
            template<typename  P>
            void await_suspend(std::coroutine_handle<P> handle) noexcept{}
            constexpr void await_resume()const noexcept{}
            const bool m_wait_at_initial_suspend{true};
        };

        struct FinalAwaiter{
            constexpr bool await_ready() const noexcept{return false;}
            template<typename  P>
            void await_suspend(std::coroutine_handle<P> handle) noexcept{
                  auto parent = handle.promise().parent();
                    if (parent) {
                        // 直接从父协程的 child 容器中移除当前子协程
                        std::erase(parent->child, handle);
                        parent->set_state(Handle::State::SCHEDULE);
                        get_event_loop().call_soon(*parent, [parent]() { parent->run(); });
                    }
            }
            constexpr void await_resume()const noexcept{}
        };

        auto initial_suspend()noexcept{
            return InitialAwaiter{m_wait_at_initial_suspend};
        }

        auto final_suspend()noexcept{
            return FinalAwaiter{};
        }

        Task<ResultType> get_return_object(){
            return Task{std::coroutine_handle<Promise<ResultType>>::from_promise(*this)};
        }

        void run()final{
            std::coroutine_handle<Promise<ResultType>>::from_promise(*this).resume();
        }

        const std::source_location &get_loc()const final override{
            return  m_loc;
        }

        void traceback(int depth) final override{
            utils::print_location(m_loc,depth);

            if(m_parent){
                m_parent->traceback(depth+1);
            }
        }


        template<concepts::Awaiter _Awaiter>
        decltype(auto) await_transform(_Awaiter &&awaiter,std::source_location loc=std::source_location::current()){
            m_loc=loc;
            return std::forward<_Awaiter>(awaiter);
        }

        public:
         void set_parent(CoroHandle* p) { m_parent = p; }
         CoroHandle* parent() const { return m_parent; }


        private:
        const bool m_wait_at_initial_suspend{true};
        CoroHandle *m_parent{nullptr};
        std::source_location m_loc{};

    };

    template<typename ResultType=void>
    class Task{
        public:
        using promise_type =Promise<ResultType>;
        using coro_handle=std::coroutine_handle<promise_type>;
        
        struct AwaiterBase{
            public:
            constexpr bool await_ready()const noexcept{
                if(self_handle)[[likely]]{
                    return  self_handle.done();
                }
                return true;
            }

            template<typename _promise>
            void await_suspend(std::coroutine_handle<_promise> parent)const noexcept{
                assert(!self_handle.promise().parent());
                parent.promise().set_state(Handle::State::SUSPEND);
                self_handle.promise().set_parent(&parent.promise());
                parent.promise().child.push_back(self_handle);
                self_handle.promise().schedule();
            }
            coro_handle self_handle{};
        };

        public:

        
        explicit Task(coro_handle h)noexcept:m_handle(h){}
        
        Task(Task &&t):m_handle(std::exchange(t.m_handle,{})){}
        
        ~Task()=default;

        decltype(auto) get_result() &{
            return  m_handle.promise().get_result();
        }

        decltype(auto) get_result() &&{
            return  std::move(m_handle.promise()).get_result();
        }

       
        auto operator co_await()const & noexcept{
            struct Awaiter:public AwaiterBase{
                decltype(auto) await_resume(){
                    if(!AwaiterBase::self_handle)[[unlikely]]{
                        throw ExceptionInvalidFuture();
                    }
                    return  AwaiterBase::self_handle.promise().get_result();
                }
            };
            return Awaiter{m_handle};
        }

        auto operator co_await()const && noexcept{
            struct Awaiter:public AwaiterBase{
                decltype(auto) await_resume(){
                    if(!AwaiterBase::self_handle)[[unlikely]]{
                        throw ExceptionInvalidFuture();
                    }
                    return std::move( AwaiterBase::self_handle.promise()).get_result();
                }
            };
            return Awaiter{m_handle};
        }

        bool valid() const{return  m_handle != nullptr;}
        bool done()const{return m_handle == nullptr || m_handle.done();}
        void cancel(){
            if(m_handle){
                destroy();
            }
        }
        void schedule(){
            if(m_handle){
                m_handle.promise().schedule();
            }
        }
        
        
        private:
        coro_handle m_handle;

        private:
        void destroy(){
            if(auto handle=std::exchange(m_handle,nullptr)){
                handle.promise().cancel();
                handle.destroy();
            }
        }
    };
}
