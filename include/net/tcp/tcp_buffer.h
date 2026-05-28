/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-25 14:16:40
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-05-21 16:18:33
 * @FilePath: /MCoroRpc/include/net/tcp/tcp_buffer.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace Coro {
namespace net {

class TcpBuffer {
public:
    using s_ptr = std::shared_ptr<TcpBuffer>;

    explicit TcpBuffer(int64_t size);
    ~TcpBuffer() = default;

    int64_t readAble() const;
    int64_t writeAble() const;
    int64_t readIndex() const;
    int64_t writeIndex() const;

    void writeToBuffer(const char* buf, int64_t size);
    void readFromBuffer(std::vector<char>& re, int64_t size);
    void resizeBuffer(int64_t newSize);
    void adjustBuffer();
    void moveReadIndex(int64_t size);
    void moveWriteIndex(int64_t size);

    // ------------------- 新增方法 -------------------
    /**
     * @brief 获取可写区域的起始指针（用于零拷贝 recv）
     * @warning resizeBuffer 后该指针可能失效
     */
    char* writeableHead() {
        return m_buffer.data() + m_write_index;
    }

    /**
     * @brief 获取底层缓冲区首地址（只读）
     */
    const char* data() const {
        return m_buffer.data();
    }
    // ------------------------------------------------

private:
    int64_t m_read_index{0};
    int64_t m_write_index{0};
    int64_t m_size{0};

public:
    std::vector<char> m_buffer;
};

} // namespace net
} // namespace Coro