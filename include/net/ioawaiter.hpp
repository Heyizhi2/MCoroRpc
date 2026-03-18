/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-17 18:53:37
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 21:54:22
 * @FilePath: /MCoroRpc/include/net/ioawaiter.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <coroutine>
#include <spdlog/fmt/bundled/base.h>
#include "../coro/event_loop.hpp"
namespace Coro {
    namespace net {
        struct ReadAwaiter{
            int fd;
            int m_wait_id;

            bool await_ready()const noexcept{
                return  false;
            }

            template<typename Promise>
            void await_suspend(std::coroutine_handle<Promise> handle)noexcept{
                auto& promise=handle.promise();
                m_wait_id=get_event_loop().add_reader(fd,promise,[handle](){handle.resume();});
            }

            void await_resume(){}

            ~ReadAwaiter(){
                if (m_wait_id) {
                    get_event_loop().cancel_wait(m_wait_id);
                }
            }
        };

        struct WriteAwaiter{
            int fd;
            int m_wait_id;

            bool await_ready()const noexcept{
                return false;
            }

            template<typename Promise>
            void await_suspend(std::coroutine_handle<Promise> handle){
                auto& promise=handle.promise();
                m_wait_id=get_event_loop().add_writer(fd,promise,[handle](){handle.resume();});
            }


            void await_resume() const noexcept{}

            ~WriteAwaiter(){
                if(m_wait_id){
                    get_event_loop().cancel_wait(m_wait_id);
                }
            }
        };
    }
}