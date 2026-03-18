/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-17 19:02:14
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 21:57:12
 * @FilePath: /MCoroRpc/include/net/tcpstream.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <cerrno>
#include <cstddef>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <vector>
#include <sys/socket.h>
#include "../coro/task.hpp"
#include "ioawaiter.hpp"
namespace Coro {
    namespace net {
        class TcpStream:public Noncopyable{
            public:
            using buffer_type=std::vector<char>;

            explicit TcpStream(int fd)
            :m_fd(fd){
                if(m_fd>=0){
                    socklen_t len=sizeof(m_local_addr);
                    ::getsockname(fd,reinterpret_cast<sockaddr*>(&m_local_addr),&len);
                }
            }

            TcpStream(const TcpStream&)=delete;
            TcpStream& operator=(const TcpStream&)=delete;

            TcpStream(TcpStream && other) noexcept
            :m_fd(std::exchange(other.m_fd,-1)),m_local_addr(other.m_local_addr){}

            TcpStream& operator=(TcpStream&& other) noexcept{
                if(this!=&other){
                    close();
                    m_fd=std::exchange(other.m_fd,-1);
                    m_local_addr=other.m_local_addr;
                }
                return *this;
            }


            void close(){
                if(m_fd>=0){
                    ::close(m_fd);
                    m_fd=-1;
                }
            }

            int fd()const{return  m_fd;}

            const sockaddr_storage& local_addr()const{return m_local_addr;}

            
           Task<buffer_type> read(ssize_t size = -1) {
                    if (size < 0) {
                        co_return co_await read_until_eof();
                    }
                    buffer_type buf(size);
                    size_t total = 0;
                    while (total < buf.size()) {
                        ssize_t n = ::recv(m_fd, buf.data() + total, size - total, 0);
                        if (n > 0) {
                            total += n;
                        } else if (n == 0) {
                            break;
                        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            if (total > 0) break;
                            co_await ReadAwaiter{m_fd};
                        } else {
                            throw std::system_error(errno, std::generic_category(), "read failed");
                        }
                    }
                    buf.resize(total);
                    co_return buf;
        }

        Task<> write(const buffer_type& buffer) {
                size_t total = 0;
                while (total < buffer.size()) {
                    ssize_t n = ::send(m_fd, buffer.data() + total, buffer.size() - total, 0);
                    if (n > 0) {
                        total += n;
                    } else if (n == 0) {
                        throw std::runtime_error("write returned 0 (connection closed)");
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            co_await WriteAwaiter{m_fd};
                            continue;
                        }
                        throw std::system_error(errno, std::generic_category(), "write failed");
                    }
                }
                co_return;
            }
            ~TcpStream(){close();}


            private:
            Task<buffer_type> read_until_eof(){
                buffer_type buf;
                constexpr size_t chunk=4096;
                char tmp[chunk];
                while (true) {
                    ssize_t n=::recv(m_fd,tmp,chunk,0);
                    if(n>0){
                        buf.insert(buf.end(),tmp,tmp+n);
                    }
                    else if (n==0) {
                        break;
                    }
                    else if (errno==EAGAIN||errno==EWOULDBLOCK) {
                        co_await ReadAwaiter{m_fd};
                        continue;
                    }
                    else {
                         throw std::system_error(errno, std::generic_category(), "read failed");
                    }
                }
                co_return buf;
            }
            private:
            int m_fd{-1};
            sockaddr_storage m_local_addr{};
        };
    }
}