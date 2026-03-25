/**
 * @file taskresult.hpp
 * @brief Task 结果存储
 * 
 * 定义协程Task的结果存储机制。
 * 使用std::variant来存储三种可能的状态：
 * 1. 空（monostate）- 结果尚未设置
 * 2. 值（R）- 协程成功返回的值
 * 3. 异常（exception_ptr）- 协程中抛出的异常
 */

#pragma once
#include <exception>
#include <optional>
#include <stdexcept>
#include <utility>
#include <variant>

namespace Coro {
    /**
     * @brief Task结果存储模板类（非void特化）
     * @tparam R 结果值类型
     * 
     * 使用std::variant存储以下三种状态：
     * - std::monostate: 初始状态，无值
     * - R: 成功返回的值
     * - std::exception_ptr: 抛出的异常
     */
    template<typename R>
    struct TaskResult{
        /**
         * @brief 检查是否有值
         * @return true 表示有值（成功返回或异常）
         */
        constexpr bool has_value()const noexcept{return  std::get_if<std::monostate>(&m_result) == nullptr;}

        /**
         * @brief 设置结果值
         * @tparam ResultType 值类型
         * @param value 要设置的值
         */
        template<typename ResultType>
        void set_value(ResultType&& value)noexcept{
            m_result=std::forward<ResultType>(std::move(value));
        }
        
        /**
         * @brief 设置返回值（协程return调用）
         * @tparam ResultType 值类型
         * @param value 要返回的值
         */
        template<typename  ResultType>
        void return_value(ResultType&& value){
            set_value(std::forward<ResultType>(std::move(value)));
        }

        /**
         * @brief 获取结果（左值引用版本）
         * @return 如果是值则返回值，如果是异常则重新抛出
         * @throw std::runtime_error 如果没有结果
         */
        constexpr R get_result()&{
            if(auto *exception=std::get_if<std::exception_ptr>(&m_result)){
                std::rethrow_exception(*exception);
            }
            if(auto* res=std::get_if<R>(&m_result)){
                return  *res;
            }
            throw std::runtime_error("Task has no result");
        }
        
        /**
         * @brief 获取结果（右值引用版本）
         * @return 移动语义返回值
         * @throw std::runtime_error 如果没有结果
         */
        constexpr R get_result()&&{
            if(auto *exception=std::get_if<std::exception_ptr>(&m_result)){
                std::rethrow_exception(*exception);
            }
            if(auto* res=std::get_if<R>(&m_result)){
                return  std::move(*res);
            }
            throw std::runtime_error("Task has no result");
        }
        
        /**
         * @brief 设置异常
         * @param exception 异常指针
         */
        void set_exception(std::exception_ptr exception){m_result=exception;}

        /**
         * @brief 未处理异常捕获
         * @details 协程中未捕获的异常会调用此函数
         */
        void unhandled_exception()noexcept{m_result=std::current_exception();}

        private:
        /** @brief 存储结果或异常 */
        std::variant<std::monostate,R,std::exception_ptr> m_result;
    };

    /**
     * @brief Task结果存储模板类（void特化）
     * @details 当Task的返回类型为void时使用此特化版本
     */
    template<>
    struct TaskResult<void>{
        /**
         * @brief 检查是否有值
         */
        constexpr bool has_value()const noexcept{return m_result.has_value();}
        
        /**
         * @brief 返回空值
         * @details 用于协程的co_return;语句
         */
        void return_void()noexcept{
            m_result.reset();
        }

        /**
         * @brief 获取结果
         * @throw 如果有异常则重新抛出
         */
        void get_result(){
            if(m_result.has_value()&&*m_result!=nullptr){
                std::rethrow_exception(*m_result);
            }
        }

        /**
         * @brief 设置异常
         */
        void set_exception(std::exception_ptr exception){m_result=exception;}

        /**
         * @brief 未处理异常捕获
         */
        void unhandled_exception()noexcept{m_result=std::current_exception();}

        private:
        /** @brief 存储异常（可选） */
        std::optional<std::exception_ptr> m_result;
    };
}
