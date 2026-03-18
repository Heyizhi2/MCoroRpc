/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-17 20:12:26
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 21:47:49
 * @FilePath: /MCoroRpc/tests/echo_server.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <spdlog/fmt/bundled/base.h>
#include <string_view>

Coro::Task<> handle_client(Coro::net::TcpStream stream) {
    fmt::print("Client connected, fd={}\n", stream.fd());
    try {
        while (true) {
            auto data = co_await stream.read(1024);
            if (data.empty()) {
                fmt::print("Client disconnected\n");
                break;
            }
            fmt::print("Received: '{}'\n", std::string(data.begin(), data.end()));
            co_await stream.write(data);
        }
    } catch (const std::exception& e) {
        fmt::print("Error: {}\n", e.what());
    }
    stream.close();
}

Coro::Task<> echo_server(){
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", 8080);
    fmt::print("Server listening on port 8080\n");
    
    while (true) {
        auto client = co_await service.accept();
        handle_client(std::move(client)).schedule();
    }
}

int main(){
    echo_server().schedule();
    Coro::get_event_loop().run_until_complete();
}
