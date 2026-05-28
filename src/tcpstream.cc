#include "../include/coro.hpp"
#include "../include/net/tcp/net_addr.h"
#include "../include/utils/utils.hpp"
#include "../include/coder/tinypb_protocol.hpp"

namespace Coro {
namespace net {

TcpStream::TcpStream()
    : m_fd(-1),
      m_read_buffer(std::make_shared<TcpBuffer>(65536)),
      m_write_buffer(std::make_shared<TcpBuffer>(65536)) {
}

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

// ---------- 优化后的 readToBuffer（零拷贝 + 批量读） ----------
Task<ssize_t> TcpStream::readToBuffer() {
    ssize_t total = 0;

    while (true) {
        if (m_read_buffer->writeAble() == 0) {
            m_read_buffer->resizeBuffer(m_read_buffer->writeAble() + 4096);
        }

        // 直接 recv 到 TcpBuffer 内部可写区域，零拷贝
        ssize_t n = ::recv(m_fd, m_read_buffer->writeableHead(),
                           m_read_buffer->writeAble(), 0);
        if (n > 0) {
            m_read_buffer->moveWriteIndex(n);
            total += n;
            continue; // 继续读，不挂起
        } else if (n == 0) {
            break;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (total > 0) break;
                co_await ReadAwaiter{m_fd};
            } else if (errno == ECONNRESET) {
                if (total > 0) break;
                co_return 0;
            } else {
                throw std::system_error(errno, std::generic_category(),
                                        "read failed");
            }
        }
    }
    co_return total;
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

// ---------- 写入优化 ----------
Task<> TcpStream::write(const buffer_type& buffer) {
    m_write_buffer->writeToBuffer(buffer.data(), buffer.size());
    co_await writeFromBuffer();
    co_return;
}

Task<> TcpStream::write(std::string_view data) {
    m_write_buffer->writeToBuffer(data.data(), data.size());
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

Task<TcpStream::buffer_type> TcpStream::readProtocolMessage(int32_t& pk_len) {
    pk_len = 0;
    while (true) {
        int readable = static_cast<int>(m_read_buffer->readAble());
        if (readable < 5) {
            co_await readToBuffer();
            continue;
        }
        char first_byte = m_read_buffer->m_buffer[m_read_buffer->readIndex()];
        if (first_byte != TinyPBProtocol::PB_START) {
            m_read_buffer->moveReadIndex(1);
            continue;
        }
        int32_t pk = getInt32FromNetByte(&m_read_buffer->m_buffer[m_read_buffer->readIndex() + 1]);
        pk_len = pk;
        int total_len = 1 + 4 + pk_len;
        while (m_read_buffer->readAble() < total_len) {
            co_await readToBuffer();
        }
        buffer_type result;
        result.reserve(total_len);
        result.insert(result.end(),
            m_read_buffer->m_buffer.begin() + m_read_buffer->readIndex(),
            m_read_buffer->m_buffer.begin() + m_read_buffer->readIndex() + total_len);
        m_read_buffer->moveReadIndex(total_len);
        co_return result;
    }
}

NetAddr::s_ptr TcpStream::peerAddr() const {
    if (m_fd < 0) return nullptr;
    sockaddr_in addr;
    socklen_t len = sizeof(addr);
    if (getpeername(m_fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
        return std::make_shared<IPNetAddr>(addr);
    }
    return nullptr;
}

} // namespace net
} // namespace Coro