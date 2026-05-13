#include "../include/coro.hpp"
#include "../include/net/tcpstream.hpp"
#include "../include/net/tcpservice.hpp"
#include "../include/net/tcpserver.hpp"
#include <catch2/catch.hpp>
#include <chrono>

using namespace std::chrono_literals;

TEST_CASE("TcpService::accept with timeout", "[accept][timeout]") {
    SECTION("accept times out when no client connects") {
        auto task = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 19000);
            auto start = std::chrono::steady_clock::now();

            try {
                auto stream = co_await service.accept(50ms);
                FAIL("Should have thrown TimeoutException");
            } catch (const Coro::TimeoutException&) {
                auto elapsed = std::chrono::steady_clock::now() - start;
                REQUIRE(elapsed >= 40ms);
                REQUIRE(elapsed < 500ms);
            }

            service.close();
            co_return;
        };

        task().schedule();
        Coro::get_event_loop().run_until_complete();
    }

    SECTION("accept with timeout succeeds when client connects") {
        auto serverTask = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 19001);

            auto stream = co_await service.accept(5000ms);
            REQUIRE(stream.fd() >= 0);

            stream.close();
            service.close();
            co_return;
        };

        auto clientTask = []() -> Coro::Task<void> {
            co_await Coro::sleep_for(10ms);
            auto stream = co_await Coro::net::connect("127.0.0.1", 19001);
            REQUIRE(stream.fd() >= 0);
            stream.close();
            co_return;
        };

        serverTask().schedule();
        clientTask().schedule();
        Coro::get_event_loop().run_until_complete();
    }
}

TEST_CASE("TcpService::close interrupts accept", "[accept][close]") {
    SECTION("accept after close throws") {
        auto task = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 19002);

            service.close();

            try {
                auto stream = co_await service.accept(100ms);
                FAIL("Should have thrown");
            } catch (const std::system_error& e) {
                REQUIRE(e.code().value() == EBADF);
            }

            co_return;
        };

        task().schedule();
        Coro::get_event_loop().run_until_complete();
    }

    SECTION("close() + accept(timeout) periodic check exits cleanly") {
        // 模拟 TcpServer::run() 的停服流程：
        // accept(timeout) 超时 → 循环检查 m_stop → 退出
        auto task = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 19003);
            std::atomic<bool> stop{false};
            int timeout_count = 0;

            // 在另一个协程中延迟关闭
            auto stopper = [&]() -> Coro::Task<void> {
                co_await Coro::sleep_for(150ms);
                stop.store(true);
                service.close();
                co_return;
            };
            stopper().schedule();

            // 模拟 TcpServer::run() 的 accept 循环
            while (!stop.load()) {
                try {
                    auto stream = co_await service.accept(50ms);
                    stream.close();
                } catch (const Coro::TimeoutException&) {
                    timeout_count++;
                    continue;
                } catch (...) {
                    break;
                }
            }

            // 至少应该有一次超时
            REQUIRE(timeout_count >= 1);
            REQUIRE(stop.load() == true);

            co_return;
        };

        task().schedule();
        Coro::get_event_loop().run_until_complete();
    }
}

TEST_CASE("TcpServer graceful shutdown", "[accept][tcpserver]") {
    SECTION("TcpServer::stop() causes run() to exit within timeout") {
        struct TestServer : Coro::net::TcpServer {
            using TcpServer::TcpServer;
            Coro::Task<void> handle_client(Coro::net::TcpStream) override {
                co_return;
            }
        };

        auto task = []() -> Coro::Task<void> {
            auto service = co_await Coro::net::start_tcp_service("127.0.0.1", 19004);
            auto server = std::make_unique<TestServer>(std::move(service));
            auto server_loop = [&server]() -> Coro::Task<void> {
                co_await server->run();
            };

            server_loop().schedule();

            co_await Coro::sleep_for(50ms);
            server->stop();

            // 给 stop 传播的时间
            co_await Coro::sleep_for(200ms);

            REQUIRE(server->is_stopped() == true);
            co_return;
        };

        task().schedule();
        Coro::get_event_loop().run_until_complete();
    }
}
