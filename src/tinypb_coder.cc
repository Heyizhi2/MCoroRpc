/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-18 16:22:43
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 18:01:15
 * @FilePath: /MCoroRpc/src/tinypb_coder.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <spdlog/fmt/bundled/base.h>
#include <vector>

namespace Coro {
        /**
         * @brief 编码消息
         * @param message 要编码的消息列表
         * @param out_buf 输出缓冲区
         * @details 将 TinyPBProtocol 消息编码为二进制格式写入缓冲区
         */
        void TinyPBCoder::encode(std::vector<AbstarcPortocol::s_ptr> &message,net::TcpBuffer::s_ptr out_buf){
            
            for(auto &i:message){
                std::shared_ptr<TinyPBProtocol> msg=std::dynamic_pointer_cast<TinyPBProtocol>(i);
                int len=0;
                const char* buf=encoderTinyPB(msg,len);
                if(buf!=NULL&&len!=0){
                    out_buf->writeToBuffer(buf,len);
                }
                if(buf){
                    free((void*)buf);
                    buf=NULL;
                }
            }
        }


        /**
         * @brief 解码消息
         * @param out_message 输出：解析出的消息列表
         * @param buf 输入缓冲区
         * @details 从缓冲区中解析 TinyPB 协议消息，支持粘包处理
         * @note 协议格式: START(1) + pk_len(4) + msg_id_len(4) + msg_id + method_name_len(4) + method_name + err_code(4) + err_info_len(4) + err_info + pb_data + check_sum(4) + END(1)
         */
        void TinyPBCoder::decode(std::vector<AbstarcPortocol::s_ptr> &out_message,net::TcpBuffer::s_ptr buf){
            while (true) {
                 //遍历buf,找到PB_Start标志，捕获msg_len,并判断在这个长度下是否能够找到PD_END
                std::vector<char> tmp=buf->m_buffer;
                int start_index=buf->readIndex();
                int end_index=-1;


                int pk_len=0;
                int parse_success=false;
                int i=0;
                for(i=start_index;i<buf->writeIndex();++i){
                    if(tmp[i]==TinyPBProtocol::PB_START){
                        if(i+1<buf->writeIndex()){
                            pk_len=getInt32FromNetByte(&tmp[i+1]);
                            fmt::println("get pk_lne ={}",pk_len);

                            int j=i+pk_len-1;
                            if(j>buf->writeIndex()){
                                continue;
                            }
                            if(tmp[j]==TinyPBProtocol::PB_END){
                                start_index=i;
                                end_index=j;
                                parse_success=true;
                                break;
                            }
                        }
                    }
                }
                if(i>=buf->writeIndex()){
                    fmt::println("Decode end,read full data");
                    return;
                }

                if(parse_success){
                    buf->moveReadIndex(end_index-start_index+1);
                    std::shared_ptr<TinyPBProtocol> message=std::make_shared<TinyPBProtocol>();
                    message->m_pk_len=pk_len;

                    int mgs_id_len_index=start_index+sizeof(char)+sizeof(message->m_pk_len);
                    if(mgs_id_len_index>=end_index){
                        message->parse_sucess=false;
                        fmt::println("parse error,msg_id_len_index{}>=end_index{}",mgs_id_len_index,end_index);
                        continue;
                    }
                    message->m_msg_id_len=getInt32FromNetByte(&tmp[mgs_id_len_index]);
                    fmt::println("get msg_id_len{}",message->m_msg_id_len);

                    int msg_id_index=mgs_id_len_index+sizeof(message->m_msg_id_len);

                    char msg_id[100]={0};
                    memcpy(&msg_id[0],&tmp[msg_id_index],message->m_msg_id_len);
                    message->m_msg_id=std::string(msg_id);

                    fmt::println("msg id{}",msg_id);


                    int method_name_len_index=msg_id_index+message->m_msg_id_len;
                    if(method_name_len_index>=end_index){
                       message->parse_sucess=false;
                        fmt::println("parse error,method_name_index{}>=end_index{}",method_name_len_index,end_index);
                        continue;
                    }
                    message->m_method_name_len=getInt32FromNetByte(&tmp[method_name_len_index]);


                    int method_name_index=method_name_len_index+sizeof(message->m_method_name_len);
                    char method_name[512] = {0};
                    memcpy(&method_name[0],&tmp[method_name_index],message->m_method_name_len);
                    message->m_method_name=std::string(method_name);
                    fmt::println("get method_name{}",method_name);
                    

                    int error_code_index=method_name_index+message->m_method_name_len;
                    if(error_code_index>=end_index){
                        parse_success=false;
                         fmt::println("parse error,error_code_index{}>=end_index{}",error_code_index,end_index);
                        continue;
                    }
                    message->m_err_code=getInt32FromNetByte(&tmp[error_code_index]);

                    int err_info_len_index=error_code_index+sizeof(message->m_err_code);
                     if(err_info_len_index>=end_index){
                        parse_success=false;
                         fmt::println("parse error,error_info_len_index{}>=end_index{}",err_info_len_index,end_index);
                        continue;
                    }
                    message->m_err_info_len=getInt32FromNetByte(&tmp[err_info_len_index]);


                    int err_info_index = err_info_len_index + sizeof(message->m_err_info_len);
                    char error_info[512] = {0};
                    memcpy(&error_info[0], &tmp[err_info_index], message->m_err_info_len);
                    message->m_err_info = std::string(error_info);
                    fmt::println("get error infd {}",error_info);

                    int pb_data_len=message->m_pk_len-message->m_err_info_len-message->m_msg_id_len-message->m_method_name_len-26;
                    int pd_data_index = err_info_index + message->m_err_info_len;
                    message->m_pb_data = std::string(&tmp[pd_data_index], pb_data_len);


                    message->parse_sucess=true;
                    out_message.push_back(message);
                }
            
            }
          
        
        }
        
        /**
         * @brief 编码单个 TinyPB 消息
         * @param message 要编码的消息
         * @param len 输出：编码后的长度
         * @return 编码后的二进制数据(需手动free)
         */
        const char* TinyPBCoder::encoderTinyPB(std::shared_ptr<TinyPBProtocol> message,int &len){
                if (message->m_msg_id.empty()) {
                        message->m_msg_id = "123456789";
                    }
                    //DEBUGLOG("msg_id = %s", message->m_msg_id.c_str());
                    int pk_len = 26 + message->m_msg_id.length() + message->m_method_name.length() + message->m_err_info.length() + message->m_pb_data.length();
                    //DEBUGLOG("pk_len = %", pk_len);

                    char* buf = reinterpret_cast<char*>(malloc(pk_len));
                    char* tmp = buf;

                    // 写入协议头 START
                    *tmp = TinyPBProtocol::PB_START;
                    tmp++;

                    // 写入数据包长度(网络字节序)
                    int32_t pk_len_net = htonl(pk_len);
                    memcpy(tmp, &pk_len_net, sizeof(pk_len_net));
                    tmp += sizeof(pk_len_net);

                    // 写入消息ID长度和内容
                    int msg_id_len = message->m_msg_id.length();
                    int32_t msg_id_len_net = htonl(msg_id_len);
                    memcpy(tmp, &msg_id_len_net, sizeof(msg_id_len_net));
                    tmp += sizeof(msg_id_len_net);

                    if (!message->m_msg_id.empty()) {
                        memcpy(tmp, &(message->m_msg_id[0]), msg_id_len);
                        tmp += msg_id_len;
                    }

                    // 写入方法名长度和内容
                    int method_name_len = message->m_method_name.length();
                    int32_t method_name_len_net = htonl(method_name_len);
                    memcpy(tmp, &method_name_len_net, sizeof(method_name_len_net));
                    tmp += sizeof(method_name_len_net);

                    if (!message->m_method_name.empty()) {
                        memcpy(tmp, &(message->m_method_name[0]), method_name_len);
                        tmp += method_name_len;
                    }

                    // 写入错误码
                    int32_t err_code_net = htonl(message->m_err_code);
                    memcpy(tmp, &err_code_net, sizeof(err_code_net));
                    tmp += sizeof(err_code_net);

                    // 写入错误信息长度和内容
                    int err_info_len = message->m_err_info.length();
                    int32_t err_info_len_net = htonl(err_info_len);
                    memcpy(tmp, &err_info_len_net, sizeof(err_info_len_net));
                    tmp += sizeof(err_info_len_net);

                    if (!message->m_err_info.empty()) {
                        memcpy(tmp, &(message->m_err_info[0]), err_info_len);
                        tmp += err_info_len;
                    }

                    // 写入 protobuf 数据
                    if (!message->m_pb_data.empty()) {
                        memcpy(tmp, &(message->m_pb_data[0]), message->m_pb_data.length());
                        tmp += message->m_pb_data.length();
                    }

                    // 写入校验和(暂时固定为1)
                    int32_t check_sum_net = htonl(1);
                    memcpy(tmp, &check_sum_net, sizeof(check_sum_net));
                    tmp += sizeof(check_sum_net);

                    // 写入协议尾 END
                    *tmp = TinyPBProtocol::PB_END;

                    // 更新消息的元数据
                    message->m_pk_len = pk_len;
                    message->m_msg_id_len = msg_id_len;
                    message->m_method_name_len = method_name_len;
                    message->m_err_info_len = err_info_len;
                    message->parse_sucess = true;
                    len = pk_len;

                // DEBUGLOG("encode message[%s] success", message->m_msg_id.c_str());

                    return buf;

                            
            }
}