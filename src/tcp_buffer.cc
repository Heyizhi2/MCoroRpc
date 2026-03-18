#include <cstring>
#include <algorithm>

#include "../include/coro.hpp"

namespace Coro {
namespace net {

TcpBuffer::TcpBuffer(int64_t size) : m_size(size) {
    m_buffer.resize(size);
}

int64_t TcpBuffer::readAble() const {
    return m_write_index - m_read_index;
}

int64_t TcpBuffer::writeAble() const {
    return static_cast<int64_t>(m_buffer.size()) - m_write_index;
}

int64_t TcpBuffer::readIndex() const {
    return m_read_index;
}

int64_t TcpBuffer::writeIndex() const {
    return m_write_index;
}

void TcpBuffer::writeToBuffer(const char* buf, int64_t size) {
    if (size > writeAble()) {
        int64_t newSize = m_write_index + size;
        newSize = newSize * 3 / 2;
        resizeBuffer(newSize);
    }
    std::memcpy(&m_buffer[m_write_index], buf, size);
    m_write_index += size;
}

void TcpBuffer::readFromBuffer(std::vector<char>& re, int64_t size) {
    if (readAble() == 0) {
        return;
    }

    int64_t readSize = std::min(readAble(), size);

    re.insert(re.end(), &m_buffer[m_read_index], &m_buffer[m_read_index + readSize]);
    m_read_index += readSize;

    adjustBuffer();
}

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

void TcpBuffer::moveReadIndex(int64_t size) {
    int64_t newIndex = m_read_index + size;
    if (newIndex > m_buffer.size()) {
        return;
    }
    m_read_index = newIndex;
    adjustBuffer();
}

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
