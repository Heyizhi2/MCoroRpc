/*
 * @Author: yizhi
 * @Date: 2026-03-27
 * @Description: RpcContext 测试用例
 */
#include "../include/coro.hpp"
#include "../include/rpc/rpc_context.h"
#include "../include/net/tcp/net_addr.h"
#include <catch2/catch.hpp>

TEST_CASE("RpcContext basic operations", "[rpc_context]") {
    SECTION("create RpcContext") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        REQUIRE(ctx->isFailed() == false);
        REQUIRE(ctx->isCancelled() == false);
        REQUIRE(ctx->isFinished() == false);
        REQUIRE(ctx->getErrCode() == 0);
        REQUIRE(ctx->getErrInfo() == "");
        REQUIRE(ctx->getTimeout() == 3000);
    }
    
    SECTION("set and get error") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        ctx->setFailed(1001, "Service not found");
        
        REQUIRE(ctx->isFailed() == true);
        REQUIRE(ctx->getErrCode() == 1001);
        REQUIRE(ctx->getErrInfo() == "Service not found");
    }
    
    SECTION("set error code and info separately") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        ctx->setErrCode(500);
        ctx->setErrInfo("Internal error");
        
        REQUIRE(ctx->getErrCode() == 500);
        REQUIRE(ctx->getErrInfo() == "Internal error");
    }
    
    SECTION("set and get msg id") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        ctx->setMsgId("msg_12345");
        
        REQUIRE(ctx->getMsgId() == "msg_12345");
    }
    
    SECTION("set and get timeout") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        ctx->setTimeout(5000);
        
        REQUIRE(ctx->getTimeout() == 5000);
    }
    
    SECTION("set and get addresses") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        auto localAddr = std::make_shared<Coro::net::IPNetAddr>("127.0.0.1", 8080);
        auto peerAddr = std::make_shared<Coro::net::IPNetAddr>("192.168.1.100", 12345);
        
        ctx->setLocalAddr(localAddr);
        ctx->setPeerAddr(peerAddr);
        
        REQUIRE(ctx->getLocalAddr() != nullptr);
        REQUIRE(ctx->getPeerAddr() != nullptr);
        REQUIRE(ctx->getLocalAddr()->toString() == "127.0.0.1:8080");
        REQUIRE(ctx->getPeerAddr()->toString() == "192.168.1.100:12345");
    }
    
    SECTION("set finished and cancelled") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        
        ctx->setFinished(true);
        REQUIRE(ctx->isFinished() == true);
        
        ctx->setCancelled(true);
        REQUIRE(ctx->isCancelled() == true);
        REQUIRE(ctx->isFailed() == true);  // cancelled implies failed
    }
    
    SECTION("startCancel") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        ctx->startCancel();
        
        REQUIRE(ctx->isCancelled() == true);
        REQUIRE(ctx->isFailed() == true);
        REQUIRE(ctx->isFinished() == true);
    }
    
    SECTION("reset") {
        auto ctx = std::make_shared<Coro::RpcContext>();
        ctx->setFailed(100, "error");
        ctx->setMsgId("msg_123");
        ctx->setTimeout(5000);
        
        auto localAddr = std::make_shared<Coro::net::IPNetAddr>("127.0.0.1", 8080);
        ctx->setLocalAddr(localAddr);
        
        ctx->reset();
        
        REQUIRE(ctx->isFailed() == false);
        REQUIRE(ctx->isCancelled() == false);
        REQUIRE(ctx->isFinished() == false);
        REQUIRE(ctx->getErrCode() == 0);
        REQUIRE(ctx->getErrInfo() == "");
        REQUIRE(ctx->getMsgId() == "");
        REQUIRE(ctx->getTimeout() == 3000);
        REQUIRE(ctx->getLocalAddr() == nullptr);
    }
}