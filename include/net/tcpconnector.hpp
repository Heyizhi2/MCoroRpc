/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-17 19:30:02
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 19:59:18
 * @FilePath: /MCoroRpc/include/net/tcpconnector.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
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
    // 异步非阻塞连接操作
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

// 异步连接到指定地址和端口
inline Task<TcpStream> connect(std::string_view host, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

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