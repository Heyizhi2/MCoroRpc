#include "../include/coro.hpp"
#include <spdlog/fmt/bundled/base.h>
#include <string_view>
#include <vector>

static const char* kResponse = 
    "HTTP/1.1 200 OK\r\n"
    "Content-Length: 12\r\n"
    "Content-Type: text/plain\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "Hello World!";

Coro::Task<> handle_client(Coro::net::TcpStream stream) {
    std::string buffer;
    buffer.reserve(4096);
    char tmp[4096];
    int client_fd = stream.fd();
    
    try {
        while (client_fd >= 0) {
            buffer.clear();
            
            while (true) {
                auto bytes = co_await stream.read(sizeof(tmp));
                if (bytes.empty()) {
                    co_return;
                }
                buffer.append(bytes.begin(), bytes.end());
                
                if (buffer.find("\r\n\r\n") != std::string::npos) {
                    break;
                }
            }
            
            std::vector<char> response(kResponse, kResponse + strlen(kResponse));
            co_await stream.write(response);
        }
    } catch (const std::exception& e) {
    }
    stream.close();
}

Coro::Task<> echo_server(){
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", 8080);
    fmt::print("HTTP Echo Server (Keep-Alive) listening on port 8080\n");
    fmt::print("Test with: wrk -t4 -c100 -d10s http://localhost:8080/\n");
    
    while (true) {
        auto client = co_await service.accept();
        handle_client(std::move(client)).schedule();
    }
}

int main(){
    echo_server().schedule();
    Coro::get_event_loop().run_until_complete();
}
