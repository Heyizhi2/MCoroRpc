/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-25 20:11:35
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-25 20:15:57
 * @FilePath: /MCoroRpc/include/net/tcpserver.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include "tcpservice.hpp"
#include "tcpstream.hpp"
#include "../coro.hpp"
#include <atomic>
#include <memory>
#include <utility>

namespace Coro {
    namespace net {
        class TcpServer {
        public:
            TcpServer(const TcpServer&) = delete;
            TcpServer& operator=(const TcpServer&) = delete;

            TcpServer(TcpServer&&) = default;
            TcpServer& operator=(TcpServer&&) = default;

            explicit TcpServer(TcpService svc) : m_service(std::move(svc)) {}

            virtual ~TcpServer() = default;

            virtual Task<void> handle_client(TcpStream stream) = 0;

            Task<void> run() {
                m_stop.store(false);
                while (!m_stop.load()) {
                    auto stream = co_await m_service.accept();
                    handle_client(std::move(stream));
                }
            }

            void stop() { m_stop.store(true); }
            bool is_stopped() const { return m_stop.load(); }

            TcpService& service() { return m_service; }
            const TcpService& service() const { return m_service; }

        protected:
            TcpService m_service;
            std::atomic<bool> m_stop{false};
        };

        inline Task<std::unique_ptr<TcpServer>> make_tcp_server(std::string_view host, uint16_t port) {
            auto service = co_await start_tcp_service(host, port);
            struct ConcreteServer : public TcpServer {
                using TcpServer::TcpServer;
                Task<void> handle_client(TcpStream) override {
                    co_return;
                }
            };
            co_return std::make_unique<ConcreteServer>(std::move(service));
        }
    }
}
