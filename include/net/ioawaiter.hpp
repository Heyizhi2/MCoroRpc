/**
 * @file ioawaiter.hpp
 * @brief IO 等待器
 * 
 * 提供协程IO操作的等待器，用于：
 * - ReadAwaiter: 等待文件描述符可读
 * - WriteAwaiter: 等待文件描述符可写
 * 
 * 使用方式：
 * @code
 * co_await ReadAwaiter{fd};   // 等待fd可读
 * co_await WriteAwaiter{fd};  // 等待fd可写
 * @endcode
 */

#pragma once
#include <coroutine>
#include <spdlog/fmt/bundled/base.h>
#include "../coro/event_loop.hpp"

namespace Coro {
    namespace net {
        /**
         * @brief 读等待器
         * @details 协程co_await此等待器时会挂起，直到文件描述符可读
         */
        struct ReadAwaiter{
            /** @brief 文件描述符 */
            int fd;
            
            /** @brief 等待ID */
            int m_wait_id;

            /**
             * @brief 是否立即完成
             * @return false，总是需要等待
             */
            bool await_ready()const noexcept{
                return  false;
            }

            /**
             * @brief 挂起协程并注册读事件
             */
            template<typename Promise>
            void await_suspend(std::coroutine_handle<Promise> handle)noexcept{
                auto& promise=handle.promise();
                m_wait_id=get_event_loop().add_reader(fd,promise,[handle](){handle.resume();});
            }

            /**
             * @brief 恢复时的返回值
             */
            void await_resume(){}

            /**
             * @brief 析构函数
             * @details 确保等待被取消
             */
            ~ReadAwaiter(){
                if (m_wait_id) {
                    get_event_loop().cancel_wait(m_wait_id);
                }
            }
        };

        /**
         * @brief 写等待器
         * @details 协程co_await此等待器时会挂起，直到文件描述符可写
         */
        struct WriteAwaiter{
            /** @brief 文件描述符 */
            int fd;
            
            /** @brief 等待ID */
            int m_wait_id;

            /**
             * @brief 是否立即完成
             * @return false，总是需要等待
             */
            bool await_ready()const noexcept{
                return false;
            }

            /**
             * @brief 挂起协程并注册写事件
             */
            template<typename Promise>
            void await_suspend(std::coroutine_handle<Promise> handle){
                auto& promise=handle.promise();
                m_wait_id=get_event_loop().add_writer(fd,promise,[handle](){handle.resume();});
            }


            /**
             * @brief 恢复时的返回值
             */
            void await_resume() const noexcept{}

            /**
             * @brief 析构函数
             * @details 确保等待被取消
             */
            ~WriteAwaiter(){
                if(m_wait_id){
                    get_event_loop().cancel_wait(m_wait_id);
                }
            }
        };
    }
}
