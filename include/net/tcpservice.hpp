/**
 * @file tcpservice.hpp
 * @brief TCP 服务端
 * 
 * 提供TCP服务器功能：
 * - TcpService: TCP监听服务
 * - start_tcp_service(): 创建并启动TCP服务器
 * 
 * 使用示例：
 * @code
 * auto service = co_await start_tcp_service("0.0.0.0", 8080);
 * while (true) {
 *     auto stream = co_await service.accept();
 *     // 处理连接
 * }
 * @endcode
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
        /**
         * @brief TCP服务端
         * @details 监听TCP连接请求，提供accept()方法接受新连接
         */
        class TcpService{
            public:

            TcpService(TcpService&)=delete;
            TcpService& operator=(TcpService&)=delete;

            /**
             * @brief 移动构造函数
             */
            TcpService(TcpService&& other):m_listen_fd(std::exchange(other.m_listen_fd, -1)){}

            /**
             * @brief 移动赋值运算符
             */
            TcpService& operator=(TcpService&& other) noexcept{
                if(this!=&other){
                    if(m_listen_fd>=0){
                        ::close(m_listen_fd);
                    }
                    m_listen_fd=std::exchange(other.m_listen_fd,-1);
                }
                return *this;
            }

            /**
             * @brief 构造函数
             * @param fd 监听socket文件描述符
             */
            TcpService(int fd):m_listen_fd(fd){}

            /**
             * @brief 析构函数
             * @details 关闭监听socket
             */
            ~TcpService(){
                if(m_listen_fd>=0){
                    ::close(m_listen_fd);
                }
            }

            /**
             * @brief 接受新连接（协程）
             * @return Task<TcpStream> 新建立的TCP连接
             * 
             * 等待客户端连接，返回TcpStream用于与客户端通信
             */
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
            /** @brief 监听socket文件描述符 */
            int m_listen_fd{-1};
        };

        /**
         * @brief 启动TCP服务
         * @param host 监听地址（"0.0.0.0"表示所有接口）
         * @param port 监听端口
         * @return Task<TcpService> 创建的TCP服务
         * 
         * 流程：
         * 1. 解析地址
         * 2. 创建监听socket
         * 3. 设置SO_REUSEADDR
         * 4. 绑定地址
         * 5. 开始监听
         */
        inline Task<TcpService> start_tcp_service(std::string_view host,uint16_t port){
            addrinfo hints{};
            hints.ai_family=AF_UNSPEC;
            hints.ai_socktype=SOCK_STREAM;
            hints.ai_flags=AI_PASSIVE;  // 服务器模式

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
