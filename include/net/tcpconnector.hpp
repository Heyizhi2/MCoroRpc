/**
 * @file tcpconnector.hpp
 * @brief TCP 连接器
 * 
 * 提供异步TCP连接功能。
 * - connect(): 异步连接到指定主机和端口
 * - 返回TcpStream用于后续IO操作
 * 
 * 使用示例：
 * @code
 * auto stream = co_await connect("127.0.0.1", 8080);
 * @endcode
 */

#pragma once

#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <memory>
#include <string_view>
#include "../coro/task.hpp"
#include "tcpstream.hpp"
#include "ioawaiter.hpp"

namespace Coro {
namespace net {

namespace detail {
    /**
     * @brief 执行异步非阻塞连接
     * @param fd socket文件描述符
     * @param addr 服务器地址
     * @param len 地址长度
     * @return Task<bool> 连接是否成功
     * 
     * 使用流程：
     * 1. 首先尝试connect()，如果立即成功则返回true
     * 2. 如果返回EINPROGRESS，说明连接正在进行
     * 3. 等待socket变为可写（连接完成或失败）
     * 4. 通过getsockopt检查连接结果
     */
    inline Task<bool> do_connect(int fd, const sockaddr* addr, socklen_t len) {
        int ret = ::connect(fd, addr, len);
        if (ret == 0) {
            co_return true;
        }
        if (ret < 0 && errno != EINPROGRESS) {
            throw std::system_error(errno, std::generic_category(), "connect failed");
        }

        // 等待连接完成（可写事件）
        co_await WriteAwaiter{fd};

        int so_error = 0;
        socklen_t optlen = sizeof(so_error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &optlen) < 0) {
            co_return false;
        }
        co_return (so_error == 0);
    }
} // namespace detail

/**
 * @brief 异步连接到服务器
 * @param host 服务器主机名或IP地址
 * @param port 服务器端口
 * @return Task<TcpStream> 成功后返回TCP流
 * 
 * 使用getaddrinfo解析地址，支持IPv4和IPv6。
 * 尝试所有解析出的地址，直到连接成功。
 * 设置socket为非阻塞模式(SOCK_NONBLOCK)和-close-on-exec(SOCK_CLOEXEC)。
 */
inline Task<TcpStream> connect(std::string_view host, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;    // 支持IPv4和IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP流

    addrinfo* result = nullptr;
    std::string port_str = std::to_string(port);

    int ret = ::getaddrinfo(host.data(), port_str.c_str(), &hints, &result);
    if (ret != 0) {
        throw std::system_error(std::make_error_code(std::errc::address_not_available),
                                gai_strerror(ret));
    }
    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> guard(result, freeaddrinfo);

    int sockfd = -1;
    for (addrinfo* p = result; p != nullptr; p = p->ai_next) {
        sockfd = ::socket(p->ai_family, p->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, p->ai_protocol);
        if (sockfd == -1) {
            continue;
        }

        bool connected = co_await detail::do_connect(sockfd, p->ai_addr, p->ai_addrlen);
        if (connected) {
            break; // 成功
        }

        ::close(sockfd);
        sockfd = -1;
    }

    if (sockfd == -1) {
        throw std::system_error(std::make_error_code(std::errc::address_not_available),
                                "no address succeeded");
    }

    co_return TcpStream{sockfd};
}

} // namespace net
} // namespace Coro
