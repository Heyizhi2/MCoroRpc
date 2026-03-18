/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-18 15:26:59
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 18:23:35
 * @FilePath: /MCoroRpc/include/coder/tinypb_coder.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include "tinypb_protocol.hpp"
#include "../net/tcp/tcp_buffer.h"
#include <memory>
#include <vector>
namespace Coro {
    class AbstarctCoder{
        public:
       virtual void encode(std::vector<AbstarcPortocol::s_ptr> &message,net::TcpBuffer::s_ptr out_buf)=0;

       virtual void decode(std::vector<AbstarcPortocol::s_ptr> &out_message,net::TcpBuffer::s_ptr buf)=0;

       virtual ~AbstarctCoder(){}
    };



    class TinyPBCoder:public AbstarctCoder{
        public:
        TinyPBCoder()=default;
        ~TinyPBCoder()=default;

        void encode(std::vector<AbstarcPortocol::s_ptr> &message,net::TcpBuffer::s_ptr out_buf)override;

        void decode(std::vector<AbstarcPortocol::s_ptr> &out_message,net::TcpBuffer::s_ptr buf)override;
        private:
        const char* encoderTinyPB(std::shared_ptr<TinyPBProtocol> message,int &len);
    };
}