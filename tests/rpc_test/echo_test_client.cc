#include "../include/coro.hpp"
#include "../include/net/tcpstream.hpp"
#include "../include/coro/sleep.hpp"
#include <iostream>

int main() {
    printf("[Echo Client] Starting...\n");
    fflush(stdout);
    
    auto client_task = []() -> Coro::Task<void> {
        printf("[Echo Client] Connecting...\n");
        fflush(stdout);
        auto stream = co_await Coro::net::connect("127.0.0.1", 18001);
        printf("[Echo Client] Connected\n");
        fflush(stdout);
        
        std::vector<char> msg = {'h', 'e', 'l', 'l', 'o'};
        co_await stream.write(msg);
        
        auto reply = co_await stream.read(1024);
        
        co_await Coro::sleep_for(std::chrono::milliseconds(500));
        
        stream.close();
        co_return;
    };
    
    client_task().schedule();
    Coro::get_event_loop().run_until_complete();
    
    printf("[Echo Client] Done\n");
    return 0;
}
