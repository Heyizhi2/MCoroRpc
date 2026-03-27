/**
 * @file net_addr.h
 * @brief Network address classes
 */

#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <string>
#include <memory>

namespace Coro {
namespace net {

class NetAddr {
public:
    using s_ptr = std::shared_ptr<NetAddr>;

    virtual ~NetAddr() = default;

    virtual sockaddr* getSockAddr() = 0;
    virtual socklen_t getSockLen() const = 0;
    virtual int getFamily() const = 0;
    virtual std::string toString() const = 0;
    virtual bool isValid() const = 0;
};

class IPNetAddr : public NetAddr {
public:
    static bool isValid(const std::string& addr);
    static NetAddr::s_ptr Create(const std::string& ip, uint16_t port);

    IPNetAddr() = default;
    IPNetAddr(const std::string& ip, uint16_t port);
    IPNetAddr(const std::string& addr);
    IPNetAddr(sockaddr_in addr);

    sockaddr* getSockAddr() override;
    socklen_t getSockLen() const override;
    int getFamily() const override;
    std::string toString() const override;
    bool isValid() const override;

    std::string ip() const { return m_ip; }
    uint16_t port() const { return m_port; }

private:
    std::string m_ip;
    uint16_t m_port = 0;
    sockaddr_in m_addr{0};
};

}
}