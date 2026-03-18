/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-17 20:12:26
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 21:42:13
 * @FilePath: /MCoroRpc/tests/echo_server.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <chrono>
#include <spdlog/fmt/bundled/base.h>
#include <string_view>

Coro::Task<> echo_server(std::string_view message){
    auto stream = co_await Coro::net::connect("127.0.0.1", 8080);
    fmt::print("send message: {}\n", message);
    co_await stream.write(Coro::net::TcpStream::buffer_type(message.begin(), message.end()));
    co_await Coro::sleep_for(std::chrono::milliseconds(100));
    auto data = co_await stream.read(100);
    fmt::print("Received: '{}'\n", std::string(data.begin(), data.end()));
    stream.close();
}

int main(){
    echo_server("hello world").schedule();
    Coro::get_event_loop().run_until_complete();
}