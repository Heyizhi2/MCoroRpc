/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-16 16:29:13
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 17:12:22
 * @FilePath: /MCoroRpc/include/selector/epoll.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <cstdint>
#include <functional>
#include <unordered_map>
#include "../comman/types.hpp"
#include <sys/epoll.h>
namespace Coro{
      struct Event{
        int fd;
        uint64_t reader=0;
        uint64_t writer=0;
        epoll_event event{};
    };
    struct Epoll{
        
        Epoll();

        bool add_reader(int fd,uint64_t wait_id);

        bool add_writer(int fd,uint64_t wait_id);

        void cancel_wait(uint64_t wait_fd);
        bool is_stop(){return  m_event_map.empty();}

        [[nodiscard]] inline int wait(epoll_event* events,int event_num,int timeout) noexcept{
            return epoll_wait(m_epoll_fd, events, event_num, timeout);
        }
        ~Epoll();

        
        private:
        int m_epoll_fd{-1};
        std::unordered_map<int,Event> m_event_map{};
        std::unordered_map<uint64_t,int> m_wait_to_fd{};
    };
}