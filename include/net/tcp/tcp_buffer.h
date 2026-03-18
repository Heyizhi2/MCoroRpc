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

private:
    int64_t m_read_index{0};
    int64_t m_write_index{0};
    int64_t m_size{0};

public:
    std::vector<char> m_buffer;
};

}
}
