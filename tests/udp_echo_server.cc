/*
 * UDP Echo 服务器 - 配合sockperf测试 (使用协程)
 */
#include "../include/coro.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

Coro::Task<> handle_client(int fd) {
    char buf[4096];
    sockaddr_in client_addr{};
    socklen_t len = sizeof(client_addr);
    
    while (true) {
        ssize_t n = recvfrom(fd, buf, sizeof(buf), 0, 
                             (sockaddr*)&client_addr, &len);
        if (n <= 0) break;
        sendto(fd, buf, n, 0, (sockaddr*)&client_addr, len);
    }
    co_return;
}

Coro::Task<> udp_echo_server() {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(9000);
    
    bind(fd, (sockaddr*)&addr, sizeof(addr));
    
    printf("UDP Echo server listening on port 9000\n");
    fflush(stdout);
    
    while (true) {
        handle_client(fd).schedule();
    }
}

int main() {
    udp_echo_server().schedule();
    Coro::get_event_loop().run_until_complete();
}
