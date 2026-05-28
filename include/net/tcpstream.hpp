/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-05-13 20:44:47
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-05-21 16:19:06
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
#include "tcp/tcp_buffer.h"
#include "tcp/net_addr.h"

namespace Coro {
namespace net {

class TcpStream : public Noncopyable {
public:
    using buffer_type = std::vector<char>;

    TcpStream();
    explicit TcpStream(int fd, int64_t bufferSize = 65536);
    TcpStream(const TcpStream&) = delete;
    TcpStream& operator=(const TcpStream&) = delete;
    TcpStream(TcpStream&& other) noexcept;
    TcpStream& operator=(TcpStream&& other) noexcept;

    void close();
    int fd() const { return m_fd; }
    const sockaddr_storage& local_addr() const { return m_local_addr; }
    NetAddr::s_ptr peerAddr() const;

    TcpBuffer::s_ptr getReadBuffer() const { return m_read_buffer; }
    TcpBuffer::s_ptr getWriteBuffer() const { return m_write_buffer; }

    Task<buffer_type> read(ssize_t size = -1);
    Task<ssize_t> readToBuffer();

    // 原有接口
    Task<> write(const buffer_type& buffer);
    // 新增：直接接受 string_view，避免调用方构造临时 vector
    Task<> write(std::string_view data);

    Task<> writeFromBuffer();
    Task<buffer_type> readProtocolMessage(int32_t& pk_len);
    ~TcpStream() { close(); }

private:
    Task<buffer_type> read_until_eof();

    int m_fd{-1};
    sockaddr_storage m_local_addr{};
    TcpBuffer::s_ptr m_read_buffer;
    TcpBuffer::s_ptr m_write_buffer;
};

} // namespace net
} // namespace Coro