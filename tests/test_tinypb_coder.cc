/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-18 18:04:33
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 18:05:54
 * @FilePath: /MCoroRpc/tests/test_tinypb_coder.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coder/tinypb_coder.hpp"
#include "../include/coder/tinypb_protocol.hpp"
#include "../include/net/tcp/tcp_buffer.h"
#include <catch2/catch.hpp>
#include <cstring>
#include <iostream>

using namespace Coro;

TEST_CASE("TinyPB encode and decode", "[tinypb]") {
    auto message = std::make_shared<TinyPBProtocol>();
    message->m_msg_id = "test_msg_id_123";
    message->m_method_name = "EchoMethod";
    message->m_err_code = 0;
    message->m_err_info = "";
    message->m_pb_data = "Hello PB Data";

    TinyPBCoder coder;
    std::vector<AbstarcPortocol::s_ptr> messages;
    messages.push_back(message);

    auto buffer = std::make_shared<net::TcpBuffer>(4096);
    coder.encode(messages, buffer);

    int encoded_len = buffer->writeIndex();
    REQUIRE(encoded_len > 0);

    std::cout << "Encoded length: " << encoded_len << std::endl;

    std::vector<AbstarcPortocol::s_ptr> out_messages;
    coder.decode(out_messages, buffer);

    REQUIRE(out_messages.size() == 1);

    auto decoded = std::dynamic_pointer_cast<TinyPBProtocol>(out_messages[0]);
    REQUIRE(decoded != nullptr);
    REQUIRE(decoded->parse_sucess == true);

    std::cout << "Decoded msg_id: " << decoded->m_msg_id << std::endl;
    std::cout << "Decoded method_name: " << decoded->m_method_name << std::endl;
    std::cout << "Decoded err_code: " << decoded->m_err_code << std::endl;
    std::cout << "Decoded err_info: " << decoded->m_err_info << std::endl;
    std::cout << "Decoded pb_data: " << decoded->m_pb_data << std::endl;

    REQUIRE(decoded->m_msg_id == message->m_msg_id);
    REQUIRE(decoded->m_method_name == message->m_method_name);
    REQUIRE(decoded->m_err_code == message->m_err_code);
    REQUIRE(decoded->m_err_info == message->m_err_info);
    REQUIRE(decoded->m_pb_data == message->m_pb_data);
}

TEST_CASE("TinyPB decode with error info", "[tinypb]") {
    auto message = std::make_shared<TinyPBProtocol>();
    message->m_msg_id = "error_test";
    message->m_method_name = "ErrorMethod";
    message->m_err_code = -1;
    message->m_err_info = "Some error occurred";
    message->m_pb_data = "";

    TinyPBCoder coder;
    std::vector<AbstarcPortocol::s_ptr> messages;
    messages.push_back(message);

    auto buffer = std::make_shared<net::TcpBuffer>(4096);
    coder.encode(messages, buffer);

    std::vector<AbstarcPortocol::s_ptr> out_messages;
    coder.decode(out_messages, buffer);

    REQUIRE(out_messages.size() == 1);

    auto decoded = std::dynamic_pointer_cast<TinyPBProtocol>(out_messages[0]);
    REQUIRE(decoded->parse_sucess == true);
    REQUIRE(decoded->m_err_code == -1);
    REQUIRE(decoded->m_err_info == "Some error occurred");
}
