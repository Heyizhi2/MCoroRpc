/*
 * 公平对比: 高性能原生客户端 - epoll + 非阻塞
 */
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <vector>
#include <atomic>
#include <chrono>

constexpr int kPort = 8081;
constexpr int kConnections = 500;
constexpr int kPayloadSize = 1024;
constexpr int kDuration = 10;
constexpr int kMaxEvents = 10000;

std::atomic<uint64_t> g_total_requests{0};
std::atomic<uint64_t> g_connect_success{0};
std::atomic<uint64_t> g_connect_fail{0};

struct Connection {
    int fd;
    int state; // 0=connecting, 1=sending, 2=receiving
    char send_buf[kPayloadSize];
    char recv_buf[kPayloadSize];
    size_t send_pos;
    size_t recv_pos;
};

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main() {
    using namespace std::chrono;
    
    printf("=== Native Epoll Client (公平对比) ===\n");
    printf("目标: 127.0.0.1:%d\n", kPort);
    printf("并发: %d 连接\n", kConnections);
    printf("时长: %d 秒\n\n", kDuration);
    
    int epoll_fd = epoll_create1(0);
    epoll_event events[kMaxEvents];
    std::vector<Connection> conns;
    conns.reserve(kConnections);
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(kPort);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    
    for (int i = 0; i < kConnections; i++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        set_nonblocking(fd);
        
        Connection conn{};
        conn.fd = fd;
        conn.state = 0;
        memset(conn.send_buf, 'A' + (i % 26), kPayloadSize);
        conns.push_back(conn);
        
        int ret = connect(fd, (sockaddr*)&addr, sizeof(addr));
        if (ret < 0 && errno != EINPROGRESS) {
            g_connect_fail++;
            close(fd);
            conns.back().fd = -1;
            continue;
        }
        
        epoll_event ev{};
        ev.events = EPOLLOUT | EPOLLIN;
        ev.data.u32 = i;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
    }
    
    auto start = steady_clock::now();
    bool running = true;
    
    while (running) {
        int n = epoll_wait(epoll_fd, events, kMaxEvents, 100);
        
        for (int i = 0; i < n; i++) {
            int idx = events[i].data.u32;
            if (idx >= conns.size()) continue;
            
            Connection& conn = conns[idx];
            if (conn.fd < 0) continue;
            
            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                close(conn.fd);
                conn.fd = -1;
                continue;
            }
            
            if (conn.state == 0) {
                g_connect_success++;
                conn.state = 1;
            }
            
            if (conn.state == 1 && (events[i].events & EPOLLOUT)) {
                ssize_t n = send(conn.fd, conn.send_buf, kPayloadSize, 0);
                if (n > 0) {
                    conn.state = 2;
                }
            }
            
            if (conn.state == 2 && (events[i].events & EPOLLIN)) {
                ssize_t n = recv(conn.fd, conn.recv_buf, kPayloadSize, 0);
                if (n > 0) {
                    g_total_requests++;
                    conn.state = 1;
                } else if (n == 0) {
                    close(conn.fd);
                    conn.fd = -1;
                }
            }
        }
        
        auto elapsed = duration_cast<seconds>(steady_clock::now() - start).count();
        if (elapsed >= kDuration) {
            running = false;
        }
    }
    
    printf("\n\n");
    
    for (auto& conn : conns) {
        if (conn.fd >= 0) close(conn.fd);
    }
    close(epoll_fd);
    
    auto end = steady_clock::now();
    auto duration = duration_cast<seconds>(end - start).count();
    
    printf("========== 测试结果 ==========\n");
    printf("成功连接: %lu\n", g_connect_success.load());
    printf("失败连接: %lu\n", g_connect_fail.load());
    printf("总请求数: %lu\n", g_total_requests.load());
    printf("QPS: %lu\n", g_total_requests.load() / duration);
    printf("================================\n");
    
    return 0;
}