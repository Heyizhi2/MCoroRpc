/*
 * @Author: yizhi
 * @Date: 2026-03-27
 * @Description: TcpStream 测试用例
 */
#include "../include/coro.hpp"
#include "../include/net/tcpstream.hpp"
#include "../include/net/tcpservice.hpp"
#include <catch2/catch.hpp>
#include <chrono>

TEST_CASE("TcpStream basic operations", "[tcpstream]") {
    SECTION("create server and connect") {
        auto serverTask = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 18080);
            
            auto stream = co_await service.accept();
            REQUIRE(stream.fd() >= 0);
            
            stream.close();
            co_return;
        };
        
        auto clientTask = []() -> Coro::Task<void> {
            auto stream = co_await Coro::net::connect("127.0.0.1", 18080);
            REQUIRE(stream.fd() >= 0);
            
            auto peerAddr = stream.peerAddr();
            REQUIRE(peerAddr != nullptr);
            INFO("Connected to: " << peerAddr->toString());
            
            stream.close();
            co_return;
        };
        
        serverTask().schedule();
        clientTask().schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("write and read data") {
        const std::string testMsg = "Hello, Server!";
        
        auto serverTask = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 18081);
            
            auto stream = co_await service.accept();
            
            auto data = co_await stream.read(1024);
            std::string received(data.begin(), data.end());
            INFO("Server received: " << received);
            
            std::vector<char> response = {'O', 'K'};
            co_await stream.write(response);
            
            stream.close();
            co_return;
        };
        
        auto clientTask = [&testMsg]() -> Coro::Task<void> {
            auto stream = co_await Coro::net::connect("127.0.0.1", 18081);
            
            std::vector<char> sendData(testMsg.begin(), testMsg.end());
            co_await stream.write(sendData);
            
            auto response = co_await stream.read(1024);
            std::string respStr(response.begin(), response.end());
            REQUIRE(respStr == "OK");
            
            stream.close();
            co_return;
        };
        
        serverTask().schedule();
        clientTask().schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("get peer address") {
        auto serverTask = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 18082);
            auto stream = co_await service.accept();
            
            auto peer = stream.peerAddr();
            REQUIRE(peer != nullptr);
            REQUIRE(peer->isValid() == true);
            INFO("Server sees peer: " << peer->toString());
            
            stream.close();
            co_return;
        };
        
        auto clientTask = []() -> Coro::Task<void> {
            auto stream = co_await Coro::net::connect("127.0.0.1", 18082);
            
            auto peer = stream.peerAddr();
            REQUIRE(peer != nullptr);
            INFO("Client connected to: " << peer->toString());
            
            stream.close();
            co_return;
        };
        
        serverTask().schedule();
        clientTask().schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("multiple clients connect to same server") {
        auto serverTask = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 18083);
            
            for (int i = 0; i < 3; ++i) {
                auto stream = co_await service.accept();
                INFO("Server accepted connection " << i);
                stream.close();
            }
            
            co_return;
        };
        
        auto clientTask = []() -> Coro::Task<void> {
            for (int i = 0; i < 3; ++i) {
                auto stream = co_await Coro::net::connect("127.0.0.1", 18083);
                REQUIRE(stream.fd() >= 0);
                INFO("Client " << i << " connected");
                stream.close();
                co_await Coro::sleep_for(std::chrono::milliseconds(10));
            }
            co_return;
        };
        
        serverTask().schedule();
        clientTask().schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("server handles multiple connections concurrently") {
        auto serverTask = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 18084);
            
            auto handleClient = [](Coro::net::TcpStream stream) -> Coro::Task<void> {
                auto data = co_await stream.read(1024);
                std::string msg(data.begin(), data.end());
                INFO("Server got: " << msg);
                
                std::vector<char> resp = {'B', 'Y', 'E'};
                co_await stream.write(resp);
                stream.close();
                co_return;
            };
            
            handleClient(co_await service.accept()).schedule();
            handleClient(co_await service.accept()).schedule();
            handleClient(co_await service.accept()).schedule();
            
            co_await Coro::sleep_for(std::chrono::milliseconds(100));
            co_return;
        };
        
        auto clientTask = []() -> Coro::Task<void> {
            for (int i = 0; i < 3; ++i) {
                auto stream = co_await Coro::net::connect("127.0.0.1", 18084);
                
                std::string msg = "client" + std::to_string(i);
                std::vector<char> send(msg.begin(), msg.end());
                co_await stream.write(send);
                
                auto resp = co_await stream.read(1024);
                REQUIRE(resp.size() == 3);
                stream.close();
            }
            co_return;
        };
        
        serverTask().schedule();
        clientTask().schedule();
        Coro::get_event_loop().run_until_complete();
    }
}

TEST_CASE("TcpStream connection lifecycle", "[tcpstream]") {
    SECTION("connection close triggers EOF") {
        auto serverTask = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 18085);
            auto stream = co_await service.accept();
            
            co_await stream.readToBuffer();
            auto data = co_await stream.read(1024);
            REQUIRE(data.size() == 0);
            
            stream.close();
            co_return;
        };
        
        auto clientTask = []() -> Coro::Task<void> {
            auto stream = co_await Coro::net::connect("127.0.0.1", 18085);
            co_await Coro::sleep_for(std::chrono::milliseconds(50));
            stream.close();
            co_return;
        };
        
        serverTask().schedule();
        clientTask().schedule();
        Coro::get_event_loop().run_until_complete();
    }
    
    SECTION("local address") {
        auto task = []() -> Coro::Task<void> {
            auto stream = co_await Coro::net::connect("127.0.0.1", 18080);
            
            const auto& addr = stream.local_addr();
            REQUIRE(addr.ss_family == AF_INET);
            
            stream.close();
            co_return;
        };
        
        task().schedule();
        Coro::get_event_loop().run_until_complete();
    }
}