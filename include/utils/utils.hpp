/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-18 17:02:39
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 17:02:54
 * @FilePath: /MCoroRpc/include/utils/utils.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
namespace Coro {
    inline int32_t getInt32FromNetByte(const char* buf) {
        int32_t re;
        memcpy(&re, buf, sizeof(re));
        return ntohl(re);
    }
}