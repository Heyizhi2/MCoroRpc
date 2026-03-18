/*
 * 优化版 Echo 服务器 - 减少协程切换
 */
#include "../include/coro.hpp"

Coro::Task<> handle_client(Coro::net::TcpStream stream) {
    try {
        while (true) {
            auto data = co_await stream.read(1024);
            if (data.empty()) {
                break;
            }
            co_await stream.write(data);
        }
    } catch (const std::exception& e) {
    }
    stream.close();
}

Coro::Task<> echo_server(){
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", 8080);
    
    while (true) {
        auto client = co_await service.accept();
        handle_client(std::move(client)).schedule();
    }
}

int main(){
    echo_server().schedule();
    Coro::get_event_loop().run_until_complete();
}
