/*
 * 原生 epoll Echo 服务器 - 性能对比基准
 */
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <csignal>
#include <cerrno>

constexpr int kPort = 8081;
constexpr int kMaxEvents = 100000;
constexpr int kBufSize = 1024;

std::atomic<uint64_t> g_total_requests{0};

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void set_tcp_nodelay(int fd) {
    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
}

int main() {
    signal(SIGPIPE, SIG_IGN);
    
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(kPort);
    
    bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, SOMAXCONN);
    set_nonblocking(listen_fd);
    
    int epoll_fd = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);
    
    epoll_event* events = new epoll_event[kMaxEvents];
    char buf[kBufSize];
    
    printf("Native epoll server listening on port %d\n", kPort);
    
    while (true) {
        int n = epoll_wait(epoll_fd, events, kMaxEvents, -1);
        
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            
            if (fd == listen_fd) {
                while (true) {
                    int client_fd = accept(listen_fd, nullptr, nullptr);
                    if (client_fd < 0) break;
                    set_nonblocking(client_fd);
                    set_tcp_nodelay(client_fd);
                    epoll_event ce{};
                    ce.events = EPOLLIN | EPOLLET;
                    ce.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ce);
                }
            } else {
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    close(fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                    continue;
                }
                
                if (events[i].events & EPOLLIN) {
                    while (true) {
                        ssize_t nread = read(fd, buf, kBufSize);
                        if (nread > 0) {
                            g_total_requests++;
                            ssize_t nwrite = write(fd, buf, nread);
                            if (nwrite < 0) break;
                        } else if (nread == 0) {
                            close(fd);
                            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                            break;
                        } else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            close(fd);
                            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                            break;
                        }
                    }
                }
            }
        }
    }
    
    close(listen_fd);
    close(epoll_fd);
    delete[] events;
    return 0;
}
