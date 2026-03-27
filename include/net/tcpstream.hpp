/**
 * @file tcpstream.hpp
 * @brief TCP 流
 * 
 * 提供TCP连接的读写功能：
 * - 异步读写数据
 * - 读写缓冲区管理
 * - 自动本地地址获取
 * 
 * 使用示例：
 * @code
 * auto stream = co_await connect("127.0.0.1", 8080);
 * 
 * // 读取数据
 * auto data = co_await stream.read(1024);
 * 
 * // 写入数据
 * co_await stream.write(buffer);
 * @endcode
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
#include "tcp/tcp_buffer.h"
#include "tcp/net_addr.h"

namespace Coro {
    namespace net {
        /**
         * @brief TCP流
         * @details 封装TCP socket，提供异步读写功能
         */
        class TcpStream:public Noncopyable{
            public:
            /** @brief 缓冲区类型 */
            using buffer_type=std::vector<char>;

            /**
             * @brief 构造函数
             * @param fd socket文件描述符
             * @param bufferSize 读写缓冲区大小，默认65536
             */
            explicit TcpStream(int fd, int64_t bufferSize = 65536);

            TcpStream(const TcpStream&)=delete;
            TcpStream& operator=(const TcpStream&)=delete;

            /**
             * @brief 移动构造函数
             */
            TcpStream(TcpStream && other) noexcept;

            /**
             * @brief 移动赋值运算符
             */
            TcpStream& operator=(TcpStream&& other) noexcept;


            /**
             * @brief 关闭连接
             */
            void close();

            /**
             * @brief 获取文件描述符
             */
            int fd()const{return  m_fd;}

            /**
             * @brief 获取本地地址
             */
            const sockaddr_storage& local_addr()const{return m_local_addr;}

            /**
             * @brief 获取对端地址
             */
            NetAddr::s_ptr peerAddr() const;

            /**
             * @brief 获取读缓冲区
             */
            TcpBuffer::s_ptr getReadBuffer() const { return m_read_buffer; }
            
            /**
             * @brief 获取写缓冲区
             */
            TcpBuffer::s_ptr getWriteBuffer() const { return m_write_buffer; }

            /**
             * @brief 读取数据（协程）
             * @param size 读取字节数，-1表示读到EOF
             * @return Task<buffer_type> 读取的数据
             */
            Task<buffer_type> read(ssize_t size = -1);

            /**
             * @brief 读取数据到内部缓冲区（协程）
             * @return Task<ssize_t> 读取的字节数
             */
            Task<ssize_t> readToBuffer();

            /**
             * @brief 写入数据（协程）
             * @param buffer 要写入的数据
             * @return Task<> 协程对象
             */
            Task<> write(const buffer_type& buffer);

            /**
             * @brief 从内部缓冲区写入（协程）
             * @return Task<> 协程对象
             */
            Task<> writeFromBuffer();

            /**
             * @brief 析构函数
             */
            ~TcpStream(){close();}


            private:
            /**
             * @brief 读到EOF
             */
            Task<buffer_type> read_until_eof();
            
            private:
            /** @brief socket文件描述符 */
            int m_fd{-1};
            
            /** @brief 本地地址 */
            sockaddr_storage m_local_addr{};
            
            /** @brief 读缓冲区 */
            TcpBuffer::s_ptr m_read_buffer;
            
            /** @brief 写缓冲区 */
            TcpBuffer::s_ptr m_write_buffer;
        };
    }
}
