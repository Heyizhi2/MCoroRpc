#include "../include/coro.hpp"
#include "../include/net/tcpstream.hpp"
#include "../include/coro/sleep.hpp"
#include <iostream>

int main() {
    printf("[Echo Server] Starting...\n");
    fflush(stdout);
    
    auto server_task = []() -> Coro::Task<void> {
        printf("[Echo Server] Creating service...\n");
        fflush(stdout);
        auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 18001);
        printf("[Echo Server] Service started\n");
        fflush(stdout);
        
        while (true) {
            printf("[Echo Server] Waiting for accept...\n");
            fflush(stdout);
            auto stream = co_await service.accept();
            printf("[Echo Server] Client connected! fd=%d\n", stream.fd());
            fflush(stdout);
            
            auto handle_client = [stream = std::move(stream)]() mutable -> Coro::Task<void> {
                auto data = co_await stream.read(1024);
                co_await stream.write(data);
                stream.close();
                co_return;
            };
            handle_client().schedule();
        }
        co_return;
    };
    
    printf("[Echo Server] Scheduling task...\n");
    fflush(stdout);
    
    server_task().schedule();
    
    printf("[Echo Server] Running event loop...\n");
    fflush(stdout);
    
    Coro::get_event_loop().run_until_complete();
    
    printf("[Echo Server] Done\n");
    return 0;
}
