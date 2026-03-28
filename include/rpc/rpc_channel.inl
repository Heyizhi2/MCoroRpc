/**
 * @file rpc_channel.inl
 * @brief RPC Channel 实现
 */

#pragma once
#include "rpc_channel.hpp"
#include "../coder/tinypb_coder.hpp"
#include "../coro/wait_for.hpp"
#include "../coro/sleep.hpp"
#include "../net/tcpconnector.hpp"

namespace Coro {

inline void RpcController::Reset() {
    m_failed = false;
    m_error_text.clear();
    m_error_code = 0;
    m_canceled = false;
    m_finished = false;
}

inline bool RpcController::Failed() const {
    return m_failed;
}

inline std::string RpcController::ErrorText() const {
    return m_error_text;
}

inline void RpcController::SetFailed(const std::string& reason) {
    m_failed = true;
    m_error_text = reason;
}

inline int RpcController::ErrorCode() const {
    return m_error_code;
}

inline void RpcController::SetErrorCode(int err_code) {
    m_error_code = err_code;
}

inline bool RpcController::IsCanceled() const {
    return m_canceled;
}

inline void RpcController::StartCancel() {
    m_canceled = true;
    if (m_cancel_callback) {
        m_cancel_callback->Run();
        m_cancel_callback = nullptr;
    }
}

inline void RpcController::NotifyOnCancel(google::protobuf::Closure* callback) {
    m_cancel_callback = callback;
}

inline void RpcController::SetTimeout(int timeout_ms) {
    m_timeout_ms = timeout_ms;
}

inline int RpcController::GetTimeout() const {
    return m_timeout_ms;
}

inline void RpcController::SetMsgId(const std::string& msg_id) {
    m_msg_id = msg_id;
}

inline std::string RpcController::GetMsgId() const {
    return m_msg_id;
}

inline void RpcController::SetFinished(bool finished) {
    m_finished = finished;
}

inline bool RpcController::Finished() const {
    return m_finished;
}

inline RpcChannel::RpcChannel(const std::string& host, int port)
    : m_addr(net::IPNetAddr::Create(host, port)),
      m_request_chan(std::make_shared<Channel<RpcRequest>>(100)) {
}

inline RpcChannel::RpcChannel(const net::NetAddr::s_ptr& addr)
    : m_addr(addr),
      m_request_chan(std::make_shared<Channel<RpcRequest>>(100)) {
}

inline RpcChannel::~RpcChannel() {
    close();
}

inline Task<void> RpcChannel::connect() {
    if (m_connected.load()) {
        co_return;
    }
    auto ip_addr = std::dynamic_pointer_cast<net::IPNetAddr>(m_addr);
    if (!ip_addr) {
        throw std::runtime_error("invalid address");
    }
    printf("[Channel] Connecting to %s:%d\n", ip_addr->ip().c_str(), ip_addr->port());
    auto stream = co_await net::connect(ip_addr->ip(), ip_addr->port());
    m_stream = std::make_unique<net::TcpStream>(std::move(stream));
    m_connected.store(true);
    m_reconnect_retry = 0;
    
    if (!m_worker_running.exchange(true)) {
        printf("[Channel] Starting workerLoop from connect()\n");
        fflush(stdout);
        // 不等待 workerLoop，让它在后台运行
        auto worker = [this]() -> Task<void> {
            co_await workerLoop();
        };
        worker().schedule();
    }
    
    co_return;
}

inline Task<void> RpcChannel::reconnect() {
    if (m_stopped.load()) {
        co_return;
    }
    
    m_connected.store(false);
    m_stream.reset();
    
    auto ip_addr = std::dynamic_pointer_cast<net::IPNetAddr>(m_addr);
    if (!ip_addr) {
        co_return;
    }
    
    while (m_reconnect_retry < kMaxReconnectRetry && !m_stopped.load()) {
        co_await sleep_for(std::chrono::milliseconds(kReconnectDelayMs * (m_reconnect_retry + 1)));
        
        if (m_stopped.load()) {
            co_return;
        }
        
        try {
            auto stream = co_await net::connect(ip_addr->ip(), ip_addr->port());
            m_stream = std::make_unique<net::TcpStream>(std::move(stream));
            m_connected.store(true);
            m_reconnect_retry = 0;
            co_return;
        } catch (...) {
            m_reconnect_retry++;
            continue;
        }
    }
    
    m_request_chan->cancelAllAwaiters();
}

inline void RpcChannel::close() {
    printf("[Channel] close() called\n");
    fflush(stdout);
    
    // 1. 先停止接收新请求
    m_stopped.store(true);
    
    // 2. 关闭 channel 唤醒 worker（从 recv 中唤醒）
    if (m_request_chan) {
        m_request_chan->close();
    }
    
    // 3. 等待 worker 完成当前操作
    // 这里简单等待一小段时间让 worker 退出
    int wait_count = 0;
    while (m_worker_running.load() && wait_count < 100) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        wait_count++;
    }
    printf("[Channel] close() waited %d iterations\n", wait_count);
    fflush(stdout);
    
    // 4. 最后关闭 stream
    if (m_stream && m_stream->fd() >= 0) {
        printf("[Channel] close stream\n");
        fflush(stdout);
        m_stream->close();
        m_stream.reset();
    }
    m_connected.store(false);
    printf("[Channel] close() done\n");
    fflush(stdout);
}

inline void notifyRequestFailed(RpcRequest& req, const std::string& error) {
    req.controller->SetFailed(error);
    if (req.done) req.done->Run();
}

inline Task<void> RpcChannel::workerLoop() {
    auto channel = shared_from_this();
        printf("[Channel] workerLoop started\n");
        fflush(stdout);
        
        while (!m_stopped.load()) {
        if (!m_connected.load()) {
            printf("[Channel] not connected, attempting reconnect\n");
            co_await reconnect();
            if (!m_connected.load()) {
                break;
            }
        }
        
        RpcRequest req;
        printf("[Channel] waiting for request from channel...\n");
        fflush(stdout);
        try {
            // Direct recv without wait_for to test channel
            printf("[Channel] before recv...\n");
            fflush(stdout);
            req = co_await m_request_chan->recv();
            printf("[Channel] received request: method=%s\n", req.req->m_method_name.c_str());
        } catch (const Coro::ChannelClosedException& e) {
            printf("[Channel] channel closed: %s\n", e.what());
            break;
        } catch (const std::exception& e) {
            printf("[Channel] exception in recv: %s\n", e.what());
            break;
        } catch (...) {
            printf("[Channel] unknown exception in recv\n");
            break;
        }
        
        int timeout_ms = req.controller->GetTimeout();
        
        // 检查 stream 是否有效
        if (!m_stream || m_stream->fd() < 0) {
            printf("[Channel] stream not valid, skipping\n");
            fflush(stdout);
            notifyRequestFailed(req, "connection closed");
            continue;
        }
        
        std::vector<AbstarcPortocol::s_ptr> req_msgs;
        req_msgs.push_back(req.req);
        
        auto out_buf = std::make_shared<net::TcpBuffer>(1024);
        TinyPBCoder coder;
        coder.encode(req_msgs, out_buf);
        
        std::vector<char> data(out_buf->m_buffer.begin() + out_buf->readIndex(),
                               out_buf->m_buffer.begin() + out_buf->writeIndex());
        
            printf("[Channel] About to write %ld bytes, fd=%d\n", (long)data.size(), m_stream->fd());
            fflush(stdout);
            
            try {
                auto write_task = m_stream->write(data);
                printf("[Channel] Before write await...\n");
                fflush(stdout);
                auto write_result = co_await wait_for(std::move(write_task), std::chrono::milliseconds(timeout_ms));
                printf("[Channel] write_result ok=%d, timeout=%d\n", write_result.ok, write_result.is_timeout);
                fflush(stdout);
                
                if (!write_result.ok) {
                    m_connected.store(false);
                    notifyRequestFailed(req, "rpc write error");
                    continue;
                }
            
            if (write_result.is_timeout) {
                m_connected.store(false);
                if (m_stream && m_stream->fd() >= 0) {
                    m_stream->close();
                    m_stream.reset();
                }
                notifyRequestFailed(req, "rpc write timeout, connection closed");
                continue;
            }
            
            printf("[Channel] Write done, waiting for response...\n");
            fflush(stdout);
            
            auto read_task = m_stream->readToBuffer();
            printf("[Channel] before wait_for read...\n");
            fflush(stdout);
            auto read_result = co_await wait_for(std::move(read_task), std::chrono::milliseconds(timeout_ms));
            printf("[Channel] wait_for read returned, ok=%d, timeout=%d, readable=%ld\n", 
                   read_result.ok, read_result.is_timeout, (long)m_stream->getReadBuffer()->readAble());
            fflush(stdout);
            
            if (read_result.ok && !read_result.is_timeout && m_stream->getReadBuffer()->readAble() > 0) {
                auto in_buf = m_stream->getReadBuffer();
                std::vector<AbstarcPortocol::s_ptr> rsp_msgs;
                printf("[Channel] About to decode response...\n");
                fflush(stdout);
                coder.decode(rsp_msgs, in_buf);
                printf("[Channel] Decoded %ld response messages\n", (long)rsp_msgs.size());
                fflush(stdout);
            } else {
                printf("[Channel] Read result not ready: ok=%d, timeout=%d, readable=%ld\n",
                       read_result.ok, read_result.is_timeout, 
                       (long)(m_stream ? m_stream->getReadBuffer()->readAble() : -1));
                fflush(stdout);
            }
            
            if (!read_result.ok) {
                m_connected.store(false);
                notifyRequestFailed(req, "rpc read error");
                continue;
            }
            
            if (read_result.is_timeout) {
                m_connected.store(false);
                if (m_stream && m_stream->fd() >= 0) {
                    m_stream->close();
                    m_stream.reset();
                }
                notifyRequestFailed(req, "rpc read timeout, connection closed");
                continue;
            }
            
            auto in_buf = m_stream->getReadBuffer();
            printf("[Channel] read buffer readable=%ld, writeIndex=%ld\n", 
                   in_buf->readAble(), in_buf->writeIndex());
            
            std::vector<AbstarcPortocol::s_ptr> rsp_msgs;
            coder.decode(rsp_msgs, in_buf);
            
            printf("[Channel] after decode, rsp_msgs size=%ld\n", rsp_msgs.size());
            
            if (rsp_msgs.empty()) {
                printf("[Channel] decode response failed, empty result\n");
                notifyRequestFailed(req, "decode response failed");
                continue;
            }
            
            auto rsp = std::dynamic_pointer_cast<TinyPBProtocol>(rsp_msgs[0]);
            if (!rsp) {
                printf("[Channel] invalid response protocol\n");
                notifyRequestFailed(req, "invalid response protocol");
                continue;
            }

            printf("[Channel] Response: err_code=%d, err_info=%s, pb_data_size=%ld\n", 
                   rsp->m_err_code, rsp->m_err_info.c_str(), rsp->m_pb_data.size());
            printf("[Channel] Response pb_data hex: ");
            for (size_t i = 0; i < std::min((size_t)20, rsp->m_pb_data.size()); i++) {
                printf("%02x ", (unsigned char)rsp->m_pb_data[i]);
            }
            printf("\n");

            if (!req.response->ParseFromString(rsp->m_pb_data)) {
                printf("[Channel] ParseFromString failed\n");
                notifyRequestFailed(req, "parse response failed");
            } else if (rsp->m_err_code != 0) {
                printf("[Channel] Setting error code=%d, info=%s\n", rsp->m_err_code, rsp->m_err_info.c_str());
                req.controller->SetErrorCode(rsp->m_err_code);
                req.controller->SetFailed(rsp->m_err_info);
                if (req.done) req.done->Run();
            } else {
                printf("[Channel] Setting finished=true\n");
                req.controller->SetFinished(true);
                if (req.done) req.done->Run();
            }
            
        } catch (const std::exception& e) {
            m_connected.store(false);
            notifyRequestFailed(req, std::string("rpc error: ") + e.what());
            continue;
        } catch (...) {
            m_connected.store(false);
            notifyRequestFailed(req, "unknown rpc error");
            continue;
        }
    }
    
    m_worker_running.store(false);
}

inline void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
    initController(controller);

    auto* ctrl = dynamic_cast<RpcController*>(controller);
    if (!ctrl) {
        return;
    }

    if (!m_connected.load()) {
        ctrl->SetFailed("not connected");
        if (done) done->Run();
        return;
    }

    auto req = std::make_shared<TinyPBProtocol>();
    req->m_method_name = method->full_name();
    req->m_msg_id = ctrl->GetMsgId();

    if (!request->SerializeToString(&req->m_pb_data)) {
        ctrl->SetFailed("serialize request failed");
        if (done) done->Run();
        return;
    }
    
    printf("[Channel] CallMethod: method=%s, msg_id=%s, pb_data_size=%ld\n", 
           req->m_method_name.c_str(), req->m_msg_id.c_str(), req->m_pb_data.size());

    RpcRequest rpc_req;
    rpc_req.req = req;
    rpc_req.response = response;
    rpc_req.controller = ctrl;
    rpc_req.done = done;

    if (!m_request_chan->sendSync(std::move(rpc_req))) {
        ctrl->SetFailed("channel closed");
        if (done) done->Run();
        return;
    }
}

inline Task<void> RpcChannel::CallMethodAsync(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    google::protobuf::Closure* done) {
    
    initController(controller);
    
    auto* ctrl = dynamic_cast<RpcController*>(controller);
    if (!ctrl) {
        co_return;
    }
    
    if (!m_connected.load() || !m_stream || m_stream->fd() < 0) {
        ctrl->SetFailed("not connected");
        if (done) done->Run();
        co_return;
    }
    
    auto req = std::make_shared<TinyPBProtocol>();
    req->m_method_name = method->full_name();
    req->m_msg_id = ctrl->GetMsgId();
    
    if (!request->SerializeToString(&req->m_pb_data)) {
        ctrl->SetFailed("serialize request failed");
        if (done) done->Run();
        co_return;
    }
    
    // 直接发送和接收，不经过 workerLoop
    printf("[Channel] CallMethodAsync: m_stream=%p, fd=%d\n", m_stream.get(), m_stream ? m_stream->fd() : -1);
    fflush(stdout);
    
    TinyPBCoder coder;
    std::vector<AbstarcPortocol::s_ptr> req_msgs;
    req_msgs.push_back(req);
    
    auto out_buf = std::make_shared<net::TcpBuffer>(1024);
    coder.encode(req_msgs, out_buf);
    
    std::vector<char> data(out_buf->m_buffer.begin() + out_buf->readIndex(),
                           out_buf->m_buffer.begin() + out_buf->writeIndex());
    
    printf("[Channel] About to write %ld bytes, fd=%d\n", (long)data.size(), m_stream->fd());
    fflush(stdout);
    
    // 写入数据
    auto writeTask = m_stream->write(data);
    printf("[Channel] Before co_await write\n");
    fflush(stdout);
    co_await writeTask;
    printf("[Channel] Write done, readable=%ld\n", (long)m_stream->getReadBuffer()->readAble());
    fflush(stdout);
    
    // 读取响应
    printf("[Channel] Before readToBuffer...\n");
    fflush(stdout);
    co_await m_stream->readToBuffer();
    printf("[Channel] After readToBuffer, readable=%ld\n", (long)m_stream->getReadBuffer()->readAble());
    fflush(stdout);
    
    auto in_buf = m_stream->getReadBuffer();
    std::vector<AbstarcPortocol::s_ptr> rsp_msgs;
    printf("[Channel] About to decode...\n");
    fflush(stdout);
    coder.decode(rsp_msgs, in_buf);
    printf("[Channel] Decoded %ld messages\n", (long)rsp_msgs.size());
    
    if (rsp_msgs.empty()) {
        ctrl->SetFailed("decode response failed");
        if (done) done->Run();
        co_return;
    }
    
    auto rsp = std::dynamic_pointer_cast<TinyPBProtocol>(rsp_msgs[0]);
    if (!rsp) {
        ctrl->SetFailed("invalid response");
        if (done) done->Run();
        co_return;
    }
    
    if (rsp->m_err_code != 0) {
        ctrl->SetErrorCode(rsp->m_err_code);
        ctrl->SetFailed(rsp->m_err_info);
    } else {
        response->ParseFromString(rsp->m_pb_data);
    }
    
    ctrl->SetFinished(true);
    if (done) done->Run();
}

inline void RpcChannel::initController(google::protobuf::RpcController* controller) {
    if (!controller) return;
    auto* ctrl = dynamic_cast<RpcController*>(controller);
    if (ctrl) {
        if (ctrl->GetTimeout() <= 0) {
            ctrl->SetTimeout(m_timeout_ms);
        }
    }
}

inline void RpcChannel::doneCallback(google::protobuf::Closure* done, RpcController* ctrl) {
    if (done) done->Run();
}

inline void RpcChannel::setTimeout(int timeout_ms) {
    m_timeout_ms = timeout_ms;
}

}
