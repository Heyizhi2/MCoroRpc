/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-17 19:30:11
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 21:40:36
 * @FilePath: /MCoroRpc/include/net/tcpservice.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <asm-generic/socket.h>
#include <cerrno>
#include <memory>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <netdb.h>
#include "ioawaiter.hpp"
#include "tcpstream.hpp"
namespace Coro {
    namespace net {
        class TcpService{
            public:

            TcpService(TcpService&)=delete;
            TcpService& operator=(TcpService&)=delete;

            TcpService(TcpService&& other):m_listen_fd(std::exchange(other.m_listen_fd, -1)){}

            TcpService& operator=(TcpService&& other) noexcept{
                if(this!=&other){
                    if(m_listen_fd>=0){
                        ::close(m_listen_fd);
                    }
                    m_listen_fd=std::exchange(other.m_listen_fd,-1);
                }
                return *this;
            }

            TcpService(int fd):m_listen_fd(fd){}

            ~TcpService(){
                if(m_listen_fd>=0){
                    ::close(m_listen_fd);
                }
            }

         Task<TcpStream> accept() {
            while (true) {
                co_await ReadAwaiter{m_listen_fd};
                sockaddr_storage remote_addr;
                socklen_t addrlen = sizeof(remote_addr);
                int client_fd = accept4(m_listen_fd, reinterpret_cast<sockaddr*>(&remote_addr),
                                        &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (client_fd >= 0) {
                    co_return TcpStream{client_fd};
                }
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    throw std::system_error(errno, std::generic_category(), "accept failed");
                }
                // 否则继续循环
            }
        }

            private:
            int m_listen_fd{-1};
        };

        inline Task<TcpService> start_tcp_service(std::string_view host,uint16_t port){
            addrinfo hints{};
            hints.ai_family=AF_UNSPEC;
            hints.ai_socktype=SOCK_STREAM;
            hints.ai_flags=AI_PASSIVE;

            addrinfo* result=nullptr;
            std::string port_str=std::to_string(port);

            int ret=::getaddrinfo(host.data(),port_str.c_str(),&hints, &result);
            if(ret!=0){

            }
            std::unique_ptr<addrinfo,decltype(&freeaddrinfo)> guard(result,freeaddrinfo);
            int listenfd=-1;

            for(addrinfo* p=result;p!=nullptr;p=p->ai_next){
                listenfd=::socket(p->ai_family, p->ai_socktype|SOCK_NONBLOCK|SOCK_CLOEXEC,p->ai_protocol);
                if(listenfd==-1){
                    continue;
                }
                int opt=1;
                ::setsockopt(listenfd, SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
                if(::bind(listenfd,p->ai_addr,p->ai_addrlen)==0){
                    break;
                }
                ::close(listenfd);
                listenfd=-1;

            }
            if(listenfd==-1){

            }
            if(::listen(listenfd,SOMAXCONN)==-1){
                ::close(listenfd);
            }

            co_return TcpService{listenfd};
        }
    }
}