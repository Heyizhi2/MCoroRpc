/**
 * @file tcp_buffer.h
 * @brief TCP 缓冲区
 * 
 * 提供环形缓冲区实现，用于TCP数据缓存。
 * 支持读写索引自动调整，类似于循环缓冲区。
 * 
 * 缓冲区结构：
 * +------------------+------------------+
 * |    readable      |     writable     |
 * +--------+---------+---------+--------+
 * |        ^ read   | write^  |        |
 * +--------+---------+---------+--------+
 * 0        r         w         size
 * 
 * 当read_index和write_index接近buffer末尾时，
 * 会自动调整数据位置到buffer开头，释放空间。
 */

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace Coro {
namespace net {

/**
 * @brief TCP缓冲区
 * @details 环形缓冲区实现，支持动态扩容
 */
class TcpBuffer {
public:
    /** @brief 智能指针类型 */
    using s_ptr = std::shared_ptr<TcpBuffer>;

    /**
     * @brief 构造函数
     * @param size 初始缓冲区大小
     */
    explicit TcpBuffer(int64_t size);

    ~TcpBuffer() = default;

    /**
     * @brief 可读字节数
     */
    int64_t readAble() const;

    /**
     * @brief 可写字节数
     */
    int64_t writeAble() const;

    /**
     * @brief 读索引位置
     */
    int64_t readIndex() const;

    /**
     * @brief 写索引位置
     */
    int64_t writeIndex() const;

    /**
     * @brief 写入数据到缓冲区
     * @param buf 数据指针
     * @param size 数据长度
     * 
     * 如果空间不足会自动扩容
     */
    void writeToBuffer(const char* buf, int64_t size);

    /**
     * @brief 从缓冲区读取数据
     * @param[out] re 接收数据的vector
     * @param size 读取长度，-1表示读所有
     */
    void readFromBuffer(std::vector<char>& re, int64_t size);

    /**
     * @brief 调整缓冲区大小
     * @param newSize 新大小
     */
    void resizeBuffer(int64_t newSize);

    /**
     * @brief 调整缓冲区
     * @details 当读索引超过缓冲区1/3时，
     * 将数据移动到缓冲区开头，释放空间
     */
    void adjustBuffer();

    /**
     * @brief 移动读索引
     * @param size 移动的距离
     */
    void moveReadIndex(int64_t size);

    /**
     * @brief 移动写索引
     * @param size 移动的距离
     */
    void moveWriteIndex(int64_t size);

private:
    /** @brief 读索引 */
    int64_t m_read_index{0};
    
    /** @brief 写索引 */
    int64_t m_write_index{0};
    
    /** @brief 缓冲区大小 */
    int64_t m_size{0};

public:
    /** @brief 实际数据存储 */
    std::vector<char> m_buffer;
};

}
}
