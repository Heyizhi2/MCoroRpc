/**
 * @file utils.hpp
 * @brief 网络字节序转换工具
 * 
 * 提供网络编程中常用的字节序转换函数。
 * 网络字节序是大端序（Big Endian），而大多数主机是小端序（Little Endian）。
 */

#pragma once
#include <cstdint>
#include <cstring>
#include <netinet/in.h>

namespace Coro {
    /**
     * @brief 从网络字节序读取32位整数
     * @param buf 字节数组（4字节）
     * @return 转换后的整数
     * 
     * 流程：
     * 1. memcpy复制4字节到本地变量
     * 2. ntohl转换为本地字节序
     */
    inline int32_t getInt32FromNetByte(const char* buf) {
        int32_t re;
        memcpy(&re, buf, sizeof(re));
        return ntohl(re);
    }
}
