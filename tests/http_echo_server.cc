#include "../include/coro.hpp"
#include <spdlog/fmt/bundled/base.h>
#include <string>
#include <vector>
#include <thread>
#include <memory>
#include <unordered_map>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <cassert>

// ---------- 统一 HTTP 响应 ----------
static const char kResponse[] =
    "HTTP/1.1 200 OK\r\n"
    "Content-Length: 12\r\n"
    "Content-Type: text/plain\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "Hello World!";

// 静态常量，用于协程版零分配响应
static const std::string kResponseStr = kResponse;

// ========== 1. 优化后的协程版本 ==========
Coro::Task<> handle_client_coro(Coro::net::TcpStream stream) {
    auto rbuf = stream.getReadBuffer();
    const char* const marker = "\r\n\r\n";
    constexpr size_t marker_len = 4;

    while (true) {
        // 循环直到内部缓冲区出现完整请求头
        while (true) {
            const char* start = rbuf->data() + rbuf->readIndex();
            const char* end   = rbuf->data() + rbuf->writeIndex();
            if (end - start >= marker_len &&
                std::search(start, end, marker, marker + marker_len) != end) {
                break;   // 已有完整请求头
            }
            auto n = co_await stream.readToBuffer();
            if (n <= 0) co_return;
        }

        // 找到 \r\n\r\n，跳过请求头
        const char* start = rbuf->data() + rbuf->readIndex();
        const char* end   = rbuf->data() + rbuf->writeIndex();
        const char* pos   = std::search(start, end, marker, marker + marker_len);
        if (pos != end) {
            rbuf->moveReadIndex(pos - start + marker_len);
        }

        // 直接写静态响应，零分配
        co_await stream.write(kResponseStr);
    }
}

Coro::Task<> worker() {
    auto svc = co_await Coro::net::start_tcp_service("0.0.0.0", 8080);
    while (true) {
        auto stream = co_await svc.accept();
        handle_client_coro(std::move(stream)).schedule(); // 在本线程调度
    }
}

void run_worker() {
    worker().schedule();
    Coro::get_event_loop().run_until_complete();
}

// ========== 2. 系统阻塞 IO 版本（未改动） ==========
void handle_blocking(int fd) {
    char buf[4096];
    std::string req;
    req.reserve(4096);

    while (true) {
        req.clear();
        while (true) {
            ssize_t n = recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) { close(fd); return; }
            req.append(buf, n);
            if (req.find("\r\n\r\n") != std::string::npos) break;
        }
        size_t total = strlen(kResponse), sent = 0;
        const char* p = kResponse;
        while (sent < total) {
            ssize_t n = send(fd, p + sent, total - sent, 0);
            if (n <= 0) { close(fd); return; }
            sent += n;
        }
    }
}

void blocking_server() {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    bind(srv, (sockaddr*)&addr, sizeof(addr));
    listen(srv, SOMAXCONN);
    fmt::print("Blocking Echo Server on port 8080\n");
    while (true) {
        sockaddr_in cli;
        socklen_t len = sizeof(cli);
        int cli_fd = accept(srv, (sockaddr*)&cli, &len);
        if (cli_fd < 0) continue;
        std::thread(handle_blocking, cli_fd).detach();
    }
    close(srv);
}

// ========== 3. 非阻塞 epoll 版本（未改动） ==========
struct Connection {
    int fd;
    std::string in, out;
    size_t out_off = 0;
    bool header_done = false;
    Connection(int f) : fd(f) { in.reserve(4096); }
};

int epfd = -1;
std::unordered_map<int, std::unique_ptr<Connection>> conns;

void set_nonblock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

void close_conn(int fd) {
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    conns.erase(fd);
}

void handle_read(int fd) {
    auto& c = conns[fd];
    char tmp[4096];
    while (true) {
        ssize_t n = read(fd, tmp, sizeof(tmp));
        if (n > 0) {
            c->in.append(tmp, n);
            if (c->in.find("\r\n\r\n") != std::string::npos) {
                c->header_done = true;
                c->out = kResponse;
                c->out_off = 0;
                epoll_event ev{};
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                ev.data.fd = fd;
                epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
            }
        } else if (n == 0) {
            close_conn(fd); return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_conn(fd); return;
        }
    }
}

void handle_write(int fd) {
    auto& c = conns[fd];
    if (!c->header_done) return;
    while (c->out_off < c->out.size()) {
        ssize_t n = write(fd, c->out.data() + c->out_off, c->out.size() - c->out_off);
        if (n > 0) c->out_off += n;
        else if (n == 0) { close_conn(fd); return; }
        else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_conn(fd); return;
        }
    }
    if (c->out_off == c->out.size()) {
        c->header_done = false;
        c->in.clear();
        c->out.clear();
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
    }
}

void epoll_server() {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    set_nonblock(srv);
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    bind(srv, (sockaddr*)&addr, sizeof(addr));
    listen(srv, SOMAXCONN);
    epfd = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = srv;
    epoll_ctl(epfd, EPOLL_CTL_ADD, srv, &ev);
    std::vector<epoll_event> events(1024);
    fmt::print("Epoll Echo Server on port 8080\n");
    while (true) {
        int nfds = epoll_wait(epfd, events.data(), events.size(), -1);
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;
            if (fd == srv) {
                while (true) {
                    sockaddr_in cli;
                    socklen_t len = sizeof(cli);
                    int cli_fd = accept(srv, (sockaddr*)&cli, &len);
                    if (cli_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }
                    set_nonblock(cli_fd);
                    epoll_event evc{};
                    evc.events = EPOLLIN | EPOLLET;
                    evc.data.fd = cli_fd;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, cli_fd, &evc);
                    conns[cli_fd] = std::make_unique<Connection>(cli_fd);
                }
            } else {
                uint32_t rev = events[i].events;
                if (rev & (EPOLLERR | EPOLLHUP)) close_conn(fd);
                else {
                    if (rev & EPOLLIN)  handle_read(fd);
                    if (rev & EPOLLOUT) handle_write(fd);
                }
            }
        }
    }
    close(srv);
}

// ========== 主函数 ==========
int main(int argc, char* argv[]) {
    std::string mode = argc > 1 ? argv[1] : "coro";
    if (mode == "blocking") {
        blocking_server();
    } else if (mode == "epoll") {
        epoll_server();
    } else {
         unsigned n =1;
        std::vector<std::thread> threads;
        for (unsigned i = 0; i < n; ++i)
            threads.emplace_back(run_worker);
        for (auto& t : threads) t.join();
    }
    return 0;
}