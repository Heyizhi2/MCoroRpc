/**
 * @file tinypb_protocol.hpp
 * @brief TinyPB 协议定义
 * 
 * TinyPB 是一个简单的基于TCP的RPC通信协议，采用二进制格式传输数据。
 * 协议结构如下：
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |  START | pk_len |msg_len | msg_id |method_ | err_   | err_   | pb_    |
 * | (1byte)|(4bytes)|(4bytes)|(var)   | name   | code   | info   | data   |
 * |  0x02  |        |        |        | len    |(4bytes)|(var)   | (var)  |
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * | ... pb_data ... | check_num | END |
 * |     (var)        |  (4bytes) |0x03|
 * +--------+--------+--------+--------+
 * 
 * 字段说明：
 * - START: 协议起始标志，固定为 0x02
 * - pk_len: 整个数据包的长度（从msg_id到check_num的所有字段）
 * - msg_id_len: 消息ID的长度
 * - msg_id: 唯一标识请求/响应的ID
 * - method_name_len: 方法名的长度
 * - method_name: 调用的RPC方法名
 * - err_code: 错误码，0表示成功
 * - err_info_len: 错误信息的长度
 * - err_info: 错误信息内容
 * - pb_data: Protobuf序列化后的请求/响应数据
 * - check_num: 校验和（简单实现固定为1）
 * - END: 协议结束标志，固定为 0x03
 */

#pragma once
#include <cstdint>
#include <memory>
#include <string>

namespace Coro {
    /**
     * @brief 抽象协议基类
     * @details 所有协议实现都应继承此类，提供统一的接口
     */
    struct AbstarcPortocol:public std::enable_shared_from_this<AbstarcPortocol>{
        public:
        /** @brief 协议智能指针类型 */
        typedef std::shared_ptr<AbstarcPortocol> s_ptr ;

        virtual ~AbstarcPortocol()=default;

        public:
        /** @brief 请求/响应唯一标识ID，用于追踪请求和响应 */
        std::string m_msg_id;
    };

    /**
     * @brief TinyPB 协议实现
     * @details 继承自抽象协议类，实现了TinyPB二进制序列化格式
     */
    struct TinyPBProtocol:public AbstarcPortocol{
        TinyPBProtocol()=default;
        ~TinyPBProtocol()=default;


        public:
        /** @brief 协议起始标志，值为0x02 */
        static char PB_START;
        /** @brief 协议结束标志，值为0x03 */
        static char PB_END;

        public:
        /** @brief 整个数据包的长度（从msg_id到check_num） */
        int m_pk_len{0};
        /** @brief 消息ID的长度 */
        int m_msg_id_len{0};

        /** @brief 方法名的长度（4字节有符号整数） */
        int32_t m_method_name_len;
        /** @brief 调用的RPC方法名称 */
        std::string m_method_name;
        /** @brief 错误码，0表示成功，非0表示错误 */
        int32_t m_err_code{0};
        /** @brief 错误信息的长度 */
        int32_t m_err_info_len{0};
        /** @brief 错误信息内容 */
        std::string m_err_info;
        /** @brief Protobuf序列化的业务数据 */
        std::string m_pb_data;
        /** @brief 校验和（当前实现固定为1） */
        int32_t m_check_num{0};

        /** @brief 解析是否成功 */
        bool parse_sucess{false};

    };
}
