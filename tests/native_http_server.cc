/*
 * 原生 epoll HTTP Echo 服务器 - 性能对比基准 (HTTP 协议)
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
#include <string>

constexpr int kPort = 8081;
constexpr int kMaxEvents = 100000;
constexpr int kBufSize = 4096;

const char* kResponse = 
    "HTTP/1.1 200 OK\r\n"
    "Content-Length: 12\r\n"
    "Content-Type: text/plain\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "Hello World!";

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
    
    printf("Native epoll HTTP server listening on port %d\n", kPort);
    printf("Test with: wrk -t4 -c100 -d10s http://localhost:%d/\n", kPort);
    
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
                    std::string buffer;
                    buffer.reserve(4096);
                    
                    while (true) {
                        char buf[kBufSize];
                        ssize_t nread = read(fd, buf, sizeof(buf));
                        
                        if (nread > 0) {
                            buffer.append(buf, nread);
                            if (buffer.find("\r\n\r\n") != std::string::npos) {
                                break;
                            }
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
                    
                    if (!buffer.empty()) {
                        g_total_requests++;
                        ssize_t nwrite = write(fd, kResponse, strlen(kResponse));
                        (void)nwrite;
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
