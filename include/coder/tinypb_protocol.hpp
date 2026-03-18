/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-18 15:27:27
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 17:28:54
 * @FilePath: /MCoroRpc/include/coder/tinypb_protol.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <cstdint>
#include <memory>
#include <string>
namespace Coro {
    struct AbstarcPortocol:public std::enable_shared_from_this<AbstarcPortocol>{
        public:
        typedef std::shared_ptr<AbstarcPortocol> s_ptr ;

        virtual ~AbstarcPortocol()=default;

        public:
        std::string m_msg_id; //请求号，唯一标识一个请求或者响应
    };

    struct TinyPBProtocol:public AbstarcPortocol{
        TinyPBProtocol()=default;
        ~TinyPBProtocol()=default;


        public:
        static char PB_START;
        static char PB_END;

        public:
        int m_pk_len{0};
        int m_msg_id_len{0};

        int32_t m_method_name_len;
        std::string m_method_name;
        int32_t m_err_code{0};
        int32_t m_err_info_len{0};
        std::string m_err_info;
        std::string m_pb_data;
        int32_t m_check_num{0};

        bool parse_sucess{false};

    };
}