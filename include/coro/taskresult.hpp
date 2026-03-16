/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 14:38:35
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 14:09:18
 * @FilePath: /MCoroRpc/include/coro/taskresult.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <exception>
#include <optional>
#include <utility>
#include <variant>
namespace Coro {
    template<typename R>
    struct TaskResult{
        constexpr bool has_value()const noexcept{return  std::get_if<std::monostate>(&m_result) == nullptr;}

        template<typename ResultType>
        void set_value(ResultType&& value)noexcept{
            m_result=std::forward<ResultType>(std::move(value));
        }
        template<typename  ResultType>
        void return_value(ResultType&& value){
            set_value(std::forward<ResultType>(value));
        }


        constexpr R get_result()&{
            if(auto *exception=std::get_if<std::exception_ptr>(&m_result)){
                std::rethrow_exception(*exception);
            }
            if(auto* res=std::get_if<R>(&m_result)){
                return  *res;
            }
            return R{};
        }
        constexpr R get_result()&&{
            if(auto *exception=std::get_if<std::exception_ptr>(&m_result)){
                std::rethrow_exception(*exception);
            }
            if(auto* res=std::get_if<R>(&m_result)){
                return  std::move(*res);
            }
            return R{};
        }
        void set_exception(std::exception_ptr exception){m_result=exception;}

        void unhandled_exception()noexcept{m_result=std::current_exception();}

        private:
        std::variant<std::monostate,R,std::exception_ptr> m_result;
    };

    template<>
    struct TaskResult<void>{
        constexpr bool has_value()const noexcept{return m_result.has_value();}
        
        void return_void()noexcept{
            m_result.reset();
        }

        void get_result(){
            if(m_result.has_value()&&*m_result!=nullptr){
                std::rethrow_exception(*m_result);
            }
        }

        void set_exception(std::exception_ptr exception){m_result=exception;}

        void unhandled_exception()noexcept{m_result=std::current_exception();}

        private:
        std::optional<std::exception_ptr> m_result;
    };
}