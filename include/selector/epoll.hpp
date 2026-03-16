#pragma once
#include <functional>
#include <unordered_map>
#include "../comman/types.hpp"
#include <sys/epoll.h>
namespace Coro{

    struct Epoll{
        using Callback =std::function<void()> ;
        
        Epoll()noexcept=default;
        
        void cancel(Handle::ID);
        
        void clear();

        void add_reader(int fd,Handle& handle,Callback cb);

        void add_writer(int fd,Handle& handle,Callback cb);

        void remove_reader(int fd);
        void remove_writer(int fd);

        void clear_fd(int fd);

        [[nodiscard]] inline int wait(epoll_event* events,int event_num,int timeout) noexcept{
            return epoll_wait(m_epoll_fd, events, event_num, timeout);
        }
        ~Epoll()noexcept;

        
        private:
        int fd();
        int m_epoll_fd{-1};
        std::unordered_map<int,Event> m_event_map{};
        std::unordered_map<Handle::ID,int> m_id_to_fd{};
    };
}