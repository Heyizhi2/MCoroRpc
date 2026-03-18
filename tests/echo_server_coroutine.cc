/*
 * 协程版 Echo 服务器 - 优化版本
 * 1. 关闭 TCP Nagle 算法 (TCP_NODELAY)
 * 2. 使用框架的协程方法 + MSG_DONTWAIT
 */
#include "../include/coro.hpp"
#include <csignal>
#include <netinet/tcp.h>
#include <sys/socket.h>

constexpr int kPort = 8081;

std::atomic<uint64_t> g_total_requests{0};

inline void set_tcp_nodelay(int fd) {
    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
}

Coro::Task<> handle_client(Coro::net::TcpStream stream) {
    set_tcp_nodelay(stream.fd());
    
    while (true) {
        auto data = co_await stream.read(1024);
        if (data.empty()) {
            break;
        }
        g_total_requests++;
        co_await stream.write(data);
    }
    stream.close();
    co_return;
}

Coro::Task<> echo_server() {
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", kPort);
    
    printf("Optimized coroutine echo server on port %d\n", kPort);
    fflush(stdout);
    
    while (true) {
        auto client = co_await service.accept();
        handle_client(std::move(client)).schedule();
    }
}

int main() {
    signal(SIGPIPE, SIG_IGN);
    
    echo_server().schedule();
    Coro::get_event_loop().run_until_complete();
    
    return 0;
}
