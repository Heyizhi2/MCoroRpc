/*
 * @Author: yizhi
 * @Date: 2026-03-27
 * @Description: RpcDispatcher 测试用例
 */
#include "../include/coro.hpp"
#include "../include/rpc/rpc_provider.hpp"
#include <catch2/catch.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

// 简单的测试用 Service Descriptor
TEST_CASE("RpcDispatcher basic operations", "[rpc_dispatcher]") {
    SECTION("create dispatcher") {
        auto dispatcher = std::make_shared<Coro::RpcDispatcher>();
        REQUIRE(dispatcher != nullptr);
    }
    
    SECTION("parse service full name") {
        std::string service_name, method_name;
        
        // Test case 1: normal case
        std::string full_name = "UserService.Login";
        size_t pos = full_name.find('.');
        if (pos != std::string::npos) {
            service_name = full_name.substr(0, pos);
            method_name = full_name.substr(pos + 1);
        }
        REQUIRE(service_name == "UserService");
        REQUIRE(method_name == "Login");
        
        // Test case 2: another service
        full_name = "OrderService.GetOrder";
        pos = full_name.find('.');
        if (pos != std::string::npos) {
            service_name = full_name.substr(0, pos);
            method_name = full_name.substr(pos + 1);
        }
        REQUIRE(service_name == "OrderService");
        REQUIRE(method_name == "GetOrder");
    }
    
    SECTION("parse invalid full name") {
        std::string service_name, method_name;
        
        // No dot separator
        std::string full_name = "NoDotMethod";
        size_t pos = full_name.find('.');
        bool has_dot = (pos != std::string::npos);
        REQUIRE(has_dot == false);
        
        // Empty string
        full_name = "";
        pos = full_name.find('.');
        has_dot = (pos != std::string::npos);
        REQUIRE(has_dot == false);
    }
    
    SECTION("dispatch with invalid method name") {
        auto dispatcher = std::make_shared<Coro::RpcDispatcher>();
        
        auto request = std::make_shared<Coro::TinyPBProtocol>();
        request->m_msg_id = "test_001";
        request->m_method_name = "InvalidService.Method";
        
        auto response = std::make_shared<Coro::TinyPBProtocol>();
        
        // This will fail because no service is registered
        dispatcher->dispatch(request, response);
        
        REQUIRE(response->m_err_code != 0);  // Should have error
    }
    
    SECTION("response inherits msg_id and method_name") {
        auto dispatcher = std::make_shared<Coro::RpcDispatcher>();
        
        auto request = std::make_shared<Coro::TinyPBProtocol>();
        request->m_msg_id = "msg_12345";
        request->m_method_name = "TestService.TestMethod";
        
        auto response = std::make_shared<Coro::TinyPBProtocol>();
        
        dispatcher->dispatch(request, response);
        
        REQUIRE(response->m_msg_id == "msg_12345");
        REQUIRE(response->m_method_name == "TestService.TestMethod");
    }
}