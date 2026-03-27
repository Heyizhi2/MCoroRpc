#include "../include/net/tcp/net_addr.h"
#include <cstring>
#include <cstdlib>

namespace Coro {
namespace net {

bool IPNetAddr::isValid(const std::string& addr) {
    size_t i = addr.find(':');
    if (i == std::string::npos) {
        return false;
    }
    std::string ip = addr.substr(0, i);
    std::string port = addr.substr(i + 1);
    if (ip.empty() || port.empty()) {
        return false;
    }
    int iport = std::atoi(port.c_str());
    if (iport <= 0 || iport > 65535) {
        return false;
    }
    return true;
}

IPNetAddr::IPNetAddr(const std::string& ip, uint16_t port) 
    : m_ip(ip), m_port(port) {
    memset(&m_addr, 0, sizeof(m_addr));
    m_addr.sin_family = AF_INET;
    m_addr.sin_addr.s_addr = inet_addr(m_ip.c_str());
    m_addr.sin_port = htons(m_port);
}

IPNetAddr::IPNetAddr(const std::string& addr) {
    size_t i = addr.find(':');
    if (i == std::string::npos) {
        return;
    }
    m_ip = addr.substr(0, i);
    m_port = static_cast<uint16_t>(std::atoi(addr.substr(i + 1).c_str()));
    memset(&m_addr, 0, sizeof(m_addr));
    m_addr.sin_family = AF_INET;
    m_addr.sin_addr.s_addr = inet_addr(m_ip.c_str());
    m_addr.sin_port = htons(m_port);
}

IPNetAddr::IPNetAddr(sockaddr_in addr) : m_addr(addr) {
    m_ip = std::string(inet_ntoa(m_addr.sin_addr));
    m_port = ntohs(m_addr.sin_port);
}

sockaddr* IPNetAddr::getSockAddr() {
    return reinterpret_cast<sockaddr*>(&m_addr);
}

socklen_t IPNetAddr::getSockLen() const {
    return sizeof(m_addr);
}

int IPNetAddr::getFamily() const {
    return AF_INET;
}

std::string IPNetAddr::toString() const {
    return m_ip + ":" + std::to_string(m_port);
}

bool IPNetAddr::isValid() const {
    if (m_ip.empty()) {
        return false;
    }
    if (m_port == 0 || m_port > 65535) {
        return false;
    }
    if (inet_addr(m_ip.c_str()) == INADDR_NONE) {
        return false;
    }
    return true;
}

}
}