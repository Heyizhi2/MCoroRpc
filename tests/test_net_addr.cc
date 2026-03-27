/*
 * @Author: yizhi
 * @Date: 2026-03-27
 * @Description: NetAddr 测试用例
 */
#include "../include/coro.hpp"
#include "../include/net/tcp/net_addr.h"
#include <catch2/catch.hpp>

TEST_CASE("IPNetAddr basic operations", "[net_addr]") {
    SECTION("create IPNetAddr from ip and port") {
        auto addr = std::make_shared<Coro::net::IPNetAddr>("127.0.0.1", 8080);
        REQUIRE(addr->isValid() == true);
        REQUIRE(addr->toString() == "127.0.0.1:8080");
        REQUIRE(addr->getFamily() == AF_INET);
    }
    
    SECTION("create IPNetAddr from string") {
        auto addr = std::make_shared<Coro::net::IPNetAddr>("192.168.1.100:9000");
        REQUIRE(addr->isValid() == true);
        REQUIRE(addr->toString() == "192.168.1.100:9000");
    }
    
    SECTION("create IPNetAddr from sockaddr_in") {
        sockaddr_in sin;
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = htons(8888);
        inet_pton(AF_INET, "10.0.0.1", &sin.sin_addr);
        
        auto addr = std::make_shared<Coro::net::IPNetAddr>(sin);
        REQUIRE(addr->isValid() == true);
        REQUIRE(addr->toString() == "10.0.0.1:8888");
    }
    
    SECTION("check invalid address") {
        auto addr1 = std::make_shared<Coro::net::IPNetAddr>("", 0);
        REQUIRE(addr1->isValid() == false);
        
        auto addr2 = std::make_shared<Coro::net::IPNetAddr>("invalid", 80);
        REQUIRE(addr2->isValid() == false);
    }
    
    SECTION("check valid static method") {
        REQUIRE(Coro::net::IPNetAddr::isValid("127.0.0.1:8080") == true);
        REQUIRE(Coro::net::IPNetAddr::isValid("192.168.1.1:80") == true);
        REQUIRE(Coro::net::IPNetAddr::isValid("invalid") == false);
        REQUIRE(Coro::net::IPNetAddr::isValid("127.0.0.1") == false);
        REQUIRE(Coro::net::IPNetAddr::isValid("127.0.0.1:99999") == false);
    }
    
    SECTION("get sock address") {
        auto addr = std::make_shared<Coro::net::IPNetAddr>("127.0.0.1", 8080);
        sockaddr* sa = addr->getSockAddr();
        REQUIRE(sa != nullptr);
        REQUIRE(addr->getSockLen() == sizeof(sockaddr_in));
    }
}