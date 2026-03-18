#include "../include/coro.hpp"

namespace Coro {
namespace net {

TcpStream::TcpStream(int fd, int64_t bufferSize)
    : m_fd(fd), 
      m_read_buffer(std::make_shared<TcpBuffer>(bufferSize)),
      m_write_buffer(std::make_shared<TcpBuffer>(bufferSize)) {
    if (m_fd >= 0) {
        socklen_t len = sizeof(m_local_addr);
        ::getsockname(fd, reinterpret_cast<sockaddr*>(&m_local_addr), &len);
    }
}

TcpStream::TcpStream(TcpStream&& other) noexcept
    : m_fd(std::exchange(other.m_fd, -1)),
      m_local_addr(other.m_local_addr),
      m_read_buffer(std::move(other.m_read_buffer)),
      m_write_buffer(std::move(other.m_write_buffer)) {
}

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

void TcpStream::close() {
    if (m_fd >= 0) {
        ::close(m_fd);
        m_fd = -1;
    }
}

Task<TcpStream::buffer_type> TcpStream::read(ssize_t size) {
    if (size < 0) {
        co_return co_await read_until_eof();
    }

    co_await readToBuffer();

    buffer_type result;
    m_read_buffer->readFromBuffer(result, size);
    co_return result;
}

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

Task<> TcpStream::write(const buffer_type& buffer) {
    m_write_buffer->writeToBuffer(buffer.data(), buffer.size());
    co_await writeFromBuffer();
    co_return;
}

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

Task<TcpStream::buffer_type> TcpStream::read_until_eof() {
    co_await readToBuffer();

    buffer_type buf;
    m_read_buffer->readFromBuffer(buf, m_read_buffer->readAble());
    co_return buf;
}

}
}
