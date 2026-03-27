/**
 * @file tcpstream.cc
 * @brief TCP 流实现
 * @details 提供协程化的 TCP 读写接口，基于 epoll 实现异步 I/O
 */

#include "../include/coro.hpp"
#include "../include/net/tcp/net_addr.h"

namespace Coro {
namespace net {

/**
 * @brief 构造函数
 * @param fd 文件描述符
 * @param bufferSize 读写缓冲区大小
 */
TcpStream::TcpStream(int fd, int64_t bufferSize)
    : m_fd(fd), 
      m_read_buffer(std::make_shared<TcpBuffer>(bufferSize)),
      m_write_buffer(std::make_shared<TcpBuffer>(bufferSize)) {
    if (m_fd >= 0) {
        socklen_t len = sizeof(m_local_addr);
        ::getsockname(fd, reinterpret_cast<sockaddr*>(&m_local_addr), &len);
    }
}

/**
 * @brief 移动构造函数
 */
TcpStream::TcpStream(TcpStream&& other) noexcept
    : m_fd(std::exchange(other.m_fd, -1)),
      m_local_addr(other.m_local_addr),
      m_read_buffer(std::move(other.m_read_buffer)),
      m_write_buffer(std::move(other.m_write_buffer)) {
}

/**
 * @brief 移动赋值运算符
 */
TcpStream& TcpStream::operator=(TcpStream&& other) noexcept {
    if (this != &other) {
        close();
        m_fd = std::exchange(other.m_fd, -1);
        m_local_addr = other.m_local_addr;
        m_read_buffer = std::move(other.m_read_buffer);
        m_write_buffer = std::move(other.m_write_buffer);
    }
    return *this;
}

/**
 * @brief 关闭连接
 */
void TcpStream::close() {
    if (m_fd >= 0) {
        ::close(m_fd);
        m_fd = -1;
    }
}

/**
 * @brief 读取数据
 * @param size 要读取的字节数，-1 表示读取到 EOF
 * @return 协程Task，返回读取到的数据
 */
Task<TcpStream::buffer_type> TcpStream::read(ssize_t size) {
    if (size < 0) {
        co_return co_await read_until_eof();
    }

    co_await readToBuffer();

    buffer_type result;
    m_read_buffer->readFromBuffer(result, size);
    co_return result;
}

/**
 * @brief 读取数据到内部缓冲区
 * @return 协程Task，返回读取的总字节数
 * @details 使用 epoll 异步等待数据，直到无数据可读
 */
Task<ssize_t> TcpStream::readToBuffer() {
    char tmp[4096];
    ssize_t total = 0;

    while (true) {
        int writable = static_cast<int>(m_read_buffer->writeAble());
        if (writable == 0) {
            m_read_buffer->resizeBuffer(m_read_buffer->writeAble() + 4096);
            writable = static_cast<int>(m_read_buffer->writeAble());
        }

        ssize_t n = ::recv(m_fd, tmp, sizeof(tmp), 0);
        if (n > 0) {
            m_read_buffer->writeToBuffer(tmp, n);
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

    co_return total;
}

/**
 * @brief 写入数据
 * @param buffer 要写入的数据
 * @return 协程Task
 */
Task<> TcpStream::write(const buffer_type& buffer) {
    m_write_buffer->writeToBuffer(buffer.data(), buffer.size());
    co_await writeFromBuffer();
    co_return;
}

/**
 * @brief 从内部缓冲区发送数据
 * @return 协程Task
 * @details 使用 epoll 异步等待可写事件
 */
Task<> TcpStream::writeFromBuffer() {
    while (m_write_buffer->readAble() > 0) {
        ssize_t n = ::send(m_fd, 
            m_write_buffer->m_buffer.data() + m_write_buffer->readIndex(), 
            m_write_buffer->readAble(), 0);
        
        if (n > 0) {
            m_write_buffer->moveReadIndex(n);
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

/**
 * @brief 读取数据直到 EOF
 * @return 协程Task，返回所有读取的数据
 */
Task<TcpStream::buffer_type> TcpStream::read_until_eof() {
    co_await readToBuffer();

    buffer_type buf;
    m_read_buffer->readFromBuffer(buf, m_read_buffer->readAble());
    co_return buf;
}

/**
 * @brief 获取对端地址
 */
NetAddr::s_ptr TcpStream::peerAddr() const {
    if (m_fd < 0) return nullptr;
    sockaddr_in addr;
    socklen_t len = sizeof(addr);
    if (getpeername(m_fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
        return std::make_shared<IPNetAddr>(addr);
    }
    return nullptr;
}

}
}
