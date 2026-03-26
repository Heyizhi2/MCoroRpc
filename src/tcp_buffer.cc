/**
 * @file tcp_buffer.cc
 * @brief TCP 缓冲区实现
 * @details 提供可动态增长的环形缓冲区，用于 TCP 数据收发
 */

#include <cstring>
#include <algorithm>

#include "../include/coro.hpp"

namespace Coro {
namespace net {

/**
 * @brief 构造函数
 * @param size 初始缓冲区大小
 */
TcpBuffer::TcpBuffer(int64_t size) : m_size(size) {
    m_buffer.resize(size);
}

/**
 * @brief 获取可读数据长度
 * @return 写入位置与读取位置之间的字节数
 */
int64_t TcpBuffer::readAble() const {
    return m_write_index - m_read_index;
}

/**
 * @brief 获取可写空间大小
 * @return 缓冲区总大小减去当前写入位置
 */
int64_t TcpBuffer::writeAble() const {
    return static_cast<int64_t>(m_buffer.size()) - m_write_index;
}

/**
 * @brief 获取当前读索引位置
 */
int64_t TcpBuffer::readIndex() const {
    return m_read_index;
}

/**
 * @brief 获取当前写索引位置
 */
int64_t TcpBuffer::writeIndex() const {
    return m_write_index;
}

/**
 * @brief 向缓冲区写入数据
 * @param buf 数据来源
 * @param size 数据大小
 * @note 如果空间不足会自动扩容
 */
void TcpBuffer::writeToBuffer(const char* buf, int64_t size) {
    if (size > writeAble()) {
        int64_t newSize = m_write_index + size;
        newSize = newSize * 3 / 2;
        resizeBuffer(newSize);
    }
    std::memcpy(&m_buffer[m_write_index], buf, size);
    m_write_index += size;
}

/**
 * @brief 从缓冲区读取数据
 * @param re 输出容器，读取的数据会追加到其中
 * @param size 要读取的字节数
 * @note 读取后会自动调整缓冲区，释放已读空间
 */
void TcpBuffer::readFromBuffer(std::vector<char>& re, int64_t size) {
    if (readAble() == 0) {
        return;
    }

    int64_t readSize = std::min(readAble(), size);

    re.insert(re.end(), &m_buffer[m_read_index], &m_buffer[m_read_index + readSize]);
    m_read_index += readSize;

    adjustBuffer();
}

/**
 * @brief 调整缓冲区大小
 * @param newSize 新的缓冲区大小
 * @note 仅在需要更大空间时扩容，保留已有数据
 */
void TcpBuffer::resizeBuffer(int64_t newSize) {
    if (newSize <= static_cast<int64_t>(m_buffer.size())) {
        return;
    }

    std::vector<char> tmp(newSize);
    int64_t count = readAble();

    if (count > 0) {
        std::memcpy(&tmp[0], &m_buffer[m_read_index], count);
    }

    m_buffer.swap(tmp);
    m_read_index = 0;
    m_write_index = count;
}

/**
 * @brief 调整缓冲区
 * @details 当已读空间超过缓冲区1/3时，移动数据到开头避免空间浪费
 * @note 读取时自动调用
 */
void TcpBuffer::adjustBuffer() {
    if (m_read_index < static_cast<int64_t>(m_buffer.size()) / 3) {
        return;
    }

    if (readAble() == 0) {
        m_read_index = 0;
        m_write_index = 0;
        return;
    }

    int64_t count = readAble();
    std::memmove(m_buffer.data(), &m_buffer[m_read_index], count);
    m_read_index = 0;
    m_write_index = count;
}

/**
 * @brief 移动读索引
 * @param size 要移动的字节数
 * @note 用于跳过不需要的数据
 */
void TcpBuffer::moveReadIndex(int64_t size) {
    int64_t newIndex = m_read_index + size;
    if (newIndex > m_buffer.size()) {
        return;
    }
    m_read_index = newIndex;
    adjustBuffer();
}

/**
 * @brief 移动写索引
 * @param size 要移动的字节数
 * @note 用于预分配空间或标记数据已写入
 */
void TcpBuffer::moveWriteIndex(int64_t size) {
    int64_t newIndex = m_write_index + size;
    if (newIndex > m_buffer.size()) {
        return;
    }
    m_write_index = newIndex;
    adjustBuffer();
}

}
}
