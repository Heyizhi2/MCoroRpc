/**
 * @file tinypb_coder.hpp
 * @brief TinyPB 编解码器
 * 
 * 提供协议的编码(encode)和解码(decode)功能。
 * - 编码：将协议对象序列化为二进制数据写入TCP缓冲区
 * - 解码：从TCP缓冲区读取数据并反序列化为协议对象
 */

#pragma once
#include "tinypb_protocol.hpp"
#include "../net/tcp/tcp_buffer.h"
#include <memory>
#include <vector>

namespace Coro {
    /**
     * @brief 抽象编解码器接口
     * @details 定义编解码器的通用接口，具体协议实现需继承此类
     */
    class AbstarctCoder{
        public:
        /**
         * @brief 编码多个协议消息
         * @param[in,out] message 待编码的消息向量
         * @param[out] out_buf 输出缓冲区
         * 
         * 将消息序列化为二进制格式并写入输出缓冲区
         */
        virtual void encode(std::vector<AbstarcPortocol::s_ptr> &message,net::TcpBuffer::s_ptr out_buf)=0;

        /**
         * @brief 解码多个协议消息
         * @param[out] out_message 解析出的消息向量
         * @param[in] buf 输入缓冲区
         * 
         * 从输入缓冲区中解析出完整的协议消息
         */
        virtual void decode(std::vector<AbstarcPortocol::s_ptr> &out_message,net::TcpBuffer::s_ptr buf)=0;

        virtual ~AbstarctCoder(){}
    };



    /**
     * @brief TinyPB 协议编解码器实现
     * @details 实现TinyPB协议的序列化和反序列化功能
     * 
     * 编码格式：START + pk_len + msg_id_len + msg_id + method_name_len + 
     *          method_name + err_code + err_info_len + err_info + pb_data + 
     *          check_num + END
     * 
     * 解码时寻找START(0x02)和END(0x03)标志，解析出完整的数据包
     */
    class TinyPBCoder:public AbstarctCoder{
        public:
        TinyPBCoder()=default;
        ~TinyPBCoder()=default;

        /**
         * @brief 编码单个TinyPB消息
         * @param[in,out] message 待编码的消息
         * @param[out] len 编码后的数据长度
         * @return 编码后的二进制数据指针（需要调用方free释放）
         */
        void encode(std::vector<AbstarcPortocol::s_ptr> &message,net::TcpBuffer::s_ptr out_buf)override;

        /**
         * @brief 解码缓冲区中的TinyPB消息
         * @param[out] out_message 解析出的消息列表
         * @param[in] buf 输入缓冲区
         * 
         * 循环解析缓冲区中的所有完整数据包，直到无法解析出完整消息
         */
        void decode(std::vector<AbstarcPortocol::s_ptr> &out_message,net::TcpBuffer::s_ptr buf)override;
        private:
        /**
         * @brief 内部编码方法
         * @param message TinyPB协议消息
         * @param[out] len 编码后数据长度
         * @return 编码后的二进制数据指针
         */
        const char* encoderTinyPB(std::shared_ptr<TinyPBProtocol> message,int &len);
    };
}
