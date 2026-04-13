/**
 * @file rpc_channel.hpp
 * @brief RPC 客户端通道
 * @details 实现 protobuf RpcChannel 接口，提供异步 RPC 调用能力
 */

#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <memory>
#include <functional>
#include <string>
#include <atomic>
#include <queue>
#include "../coro/task.hpp"
#include "../coro/channel.hpp"
#include "../net/tcpstream.hpp"
#include "../coder/tinypb_protocol.hpp"
#include "../coder/tinypb_coder.hpp"
#include "../utils/object_pool.hpp"
#include "rpc_context.h"

namespace Coro {

class RpcController : public google::protobuf::RpcController {
public:
    RpcController() = default;

    void Reset() override;
    bool Failed() const override;
    std::string ErrorText() const override;
    void SetFailed(const std::string& reason) override;
    int ErrorCode() const;
    void SetErrorCode(int err_code);

    bool IsCanceled() const override;
    void StartCancel() override;
    void NotifyOnCancel(google::protobuf::Closure* callback) override;

    void SetTimeout(int timeout_ms);
    int GetTimeout() const;

    void SetMsgId(const std::string& msg_id);
    std::string GetMsgId() const;

    void SetFinished(bool finished);
    bool Finished() const;

private:
    bool m_failed = false;
    std::string m_error_text;
    int m_error_code = 0;
    bool m_canceled = false;
    int m_timeout_ms = 3000;
    std::string m_msg_id;
    bool m_finished = false;
    google::protobuf::Closure* m_cancel_callback = nullptr;
};

using RpcControllerPtr = std::shared_ptr<RpcController>;

struct RpcRequest {
    std::shared_ptr<TinyPBProtocol> req;
    google::protobuf::Message* response;
    RpcController* controller;
    google::protobuf::Closure* done;
};

class RpcChannel : public google::protobuf::RpcChannel, public std::enable_shared_from_this<RpcChannel> {
public:
    using s_ptr = std::shared_ptr<RpcChannel>;

    RpcChannel(const std::string& host, int port);
    RpcChannel(const net::NetAddr::s_ptr& addr);
    ~RpcChannel();

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

    Task<void> CallMethodAsync(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done = nullptr);

    void setTimeout(int timeout_ms);
    int getTimeout() const { return m_timeout_ms; }

    bool isConnected() const { return m_connected.load(); }

    Task<void> connect();
    void close();

private:
    void initController(google::protobuf::RpcController* controller);
    void doneCallback(google::protobuf::Closure* done, RpcController* ctrl);

    Task<void> workerLoop();
    Task<void> reconnect();

    net::NetAddr::s_ptr m_addr;
    std::unique_ptr<net::TcpStream> m_stream;
    std::atomic<bool> m_connected{false};
    int m_timeout_ms = 3000;

    TinyPBCoder m_coder;
    Channel<RpcRequest>::s_ptr m_request_chan;
    std::atomic<bool> m_worker_running{false};
    std::atomic<bool> m_stopped{false};

    static constexpr int kMaxReconnectRetry = 3;
    static constexpr int kReconnectDelayMs = 1000;
    int m_reconnect_retry{0};
};

}

#include "rpc_channel.inl"