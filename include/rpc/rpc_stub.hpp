/**
 * @file rpc_stub.hpp
 * @brief RPC Stub 封装
 */

#pragma once
#include <google/protobuf/service.h>
#include <memory>
#include <string>
#include "rpc_client.hpp"

namespace Coro {

class RpcStubBase {
public:
    RpcStubBase() = default;
    virtual ~RpcStubBase() = default;

    virtual void callMethod(int methodIndex,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) = 0;

protected:
    RpcClient::ptr m_client;
};

template<typename Stub>
class RpcStub : public RpcStubBase {
public:
    using ServiceType = Stub;

    RpcStub() = default;

    void setClient(RpcClient::ptr client) {
        m_client = client;
    }

    RpcClient::ptr getClient() const { return m_client; }

    static const google::protobuf::ServiceDescriptor* descriptor() {
        return nullptr;
    }

    std::string getServiceName() const {
        auto* desc = static_cast<const Stub*>(this)->descriptor();
        return desc ? desc->name() : "";
    }

protected:
    void setChannel(RpcChannel::s_ptr channel) {
        m_channel = channel;
    }

    RpcChannel::s_ptr m_channel;
};

class RpcSyncCaller {
public:
    RpcSyncCaller() = default;
    explicit RpcSyncCaller(RpcClient::ptr client) : m_client(client) {}

    void setClient(RpcClient::ptr client) { m_client = client; }

    template<typename Request, typename Response>
    bool call(int methodIndex,
              const google::protobuf::MethodDescriptor* method,
              const Request& request,
              Response* response) {
        if (!m_client || !m_client->isConnected()) {
            return false;
        }
        return m_client->callMethodSync(method, &request, response);
    }

private:
    RpcClient::ptr m_client;
};

}

#define RPC_STUB(SERVICE_NAME, SERVICE_CLASS) \
public: \
    using Stub = SERVICE_CLASS; \
    \
    static const google::protobuf::ServiceDescriptor* descriptor() { \
        return SERVICE_CLASS::descriptor(); \
    } \
    \
    virtual void callMethod(const google::protobuf::MethodDescriptor* method, \
                            google::protobuf::RpcController* controller, \
                            const google::protobuf::Message* request, \
                            google::protobuf::Message* response, \
                            google::protobuf::Closure* done) override { \
        if (m_client) { \
            m_client->callMethod(method, request, response, done); \
        } else if (done) { \
            done->Run(); \
        } \
    } \
    \
    template<typename Request, typename Response> \
    bool callMethodSync(const google::protobuf::MethodDescriptor* method, \
                        const Request& request, \
                        Response* response) { \
        if (!m_client) return false; \
        return m_client->callMethodSync(method, &request, response); \
    }

#define RPC_CALLER(client) Coro::RpcSyncCaller(client)
