/**
 * @file rpc_channel.hpp
 * @brief RPC 客户端通道
 * @details 实现 protobuf RpcChannel 接口，提供异步 RPC 调用能力
 * @note 该通道支持：
 *       - 同步/异步 RPC 调用
 *       - 自动重连机制
 *       - 超时控制
 *       - 协程化调用
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

/**
 * @brief RPC 控制器
 * @details 用于管理 RPC 调用状态，控制调用过程，提供错误信息和超时控制
 * @note 继承自 google::protobuf::RpcController，可用于 protobuf 生成的 RPC 桩
 */
class RpcController : public google::protobuf::RpcController {
public:
    RpcController() = default;

    /**
     * @brief 重置控制器状态
     * @details 将所有状态恢复到初始值，包括错误状态、取消状态、超时时间等
     */
    void Reset() override;

    /**
     * @brief 检查 RPC 调用是否失败
     * @return true 表示调用失败，false 表示成功
     */
    bool Failed() const override;

    /**
     * @brief 获取错误文本信息
     * @return 失败时的错误描述字符串
     */
    std::string ErrorText() const override;

    /**
     * @brief 设置调用失败状态
     * @param reason 失败原因描述
     */
    void SetFailed(const std::string& reason) override;

    /**
     * @brief 获取错误码
     * @return 错误码（0 表示成功，非 0 表示失败）
     */
    int ErrorCode() const;

    /**
     * @brief 设置错误码
     * @param err_code 错误码
     */
    void SetErrorCode(int err_code);

    /**
     * @brief 检查是否已取消
     * @return true 表示已取消，false 表示未取消
     */
    bool IsCanceled() const override;

    /**
     * @brief 启动取消操作
     * @details 标记为已取消，并执行之前注册的取消回调
     */
    void StartCancel() override;

    /**
     * @brief 注册取消回调
     * @param callback 当 RPC 被取消时执行的回调
     */
    void NotifyOnCancel(google::protobuf::Closure* callback) override;

    /**
     * @brief 设置超时时间
     * @param timeout_ms 超时时间（毫秒），默认 3000ms
     */
    void SetTimeout(int timeout_ms);

    /**
     * @brief 获取超时时间
     * @return 超时时间（毫秒）
     */
    int GetTimeout() const;

    /**
     * @brief 设置消息 ID
     * @param msg_id 消息唯一标识
     */
    void SetMsgId(const std::string& msg_id);

    /**
     * @brief 获取消息 ID
     * @return 消息 ID 字符串
     */
    std::string GetMsgId() const;

    /**
     * @brief 设置完成状态
     * @param finished 是否完成
     */
    void SetFinished(bool finished);

    /**
     * @brief 检查是否完成
     * @return true 表示已完成，false 表示未完成
     */
    bool Finished() const;

private:
    bool m_failed = false;                      //< 调用是否失败
    std::string m_error_text;                    //< 错误描述信息
    int m_error_code = 0;                        //< 错误码
    bool m_canceled = false;                     //< 是否已取消
    int m_timeout_ms = 3000;                     //< 超时时间（毫秒）
    std::string m_msg_id;                        //< 消息 ID
    bool m_finished = false;                     //< 是否已完成
    google::protobuf::Closure* m_cancel_callback = nullptr;  //< 取消回调
};

/**
 * @brief RpcController 智能指针类型
 */
using RpcControllerPtr = std::shared_ptr<RpcController>;

/**
 * @brief RPC 请求结构
 * @details 封装 RPC 调用所需的请求信息，包括协议对象、响应、控制器和回调
 */
struct RpcRequest {
    std::shared_ptr<TinyPBProtocol> req;          //< TinyPB 协议请求对象
    google::protobuf::Message* response;          //< 响应消息指针
    RpcController* controller;                    //< RPC 控制器
    google::protobuf::Closure* done;              //< 完成回调
};

/**
 * @brief RPC 通道类
 * @details 实现 protobuf RpcChannel 接口，提供协程化的 RPC 调用能力
 * @note 主要特性：
 *       - 基于 TcpStream 的异步通信
 *       - 支持同步/异步两种调用方式
 *       - 内置重连机制（最多重试 3 次）
 *       - 使用 Channel 进行请求队列管理
 *       - 协程安全的worker loop处理请求
 */
class RpcChannel : public google::protobuf::RpcChannel, public std::enable_shared_from_this<RpcChannel> {
public:
    using s_ptr = std::shared_ptr<RpcChannel>;    //< 智能指针类型别名

    /**
     * @brief 构造函数（使用主机名和端口）
     * @param host 服务器主机名或 IP 地址
     * @param port 服务器端口号
     */
    RpcChannel(const std::string& host, int port);

    /**
     * @brief 构造函数（使用网络地址）
     * @param addr 网络地址智能指针
     */
    RpcChannel(const net::NetAddr::s_ptr& addr);

    /**
     * @brief 析构函数
     * @details 关闭连接并释放资源
     */
    ~RpcChannel();

    /**
     * @brief 调用 RPC 方法（同步方式）
     * @details protobuf 生成的代码会调用此方法，支持阻塞式 RPC 调用
     * @param method 方法描述符
     * @param controller RPC 控制器
     * @param request 请求消息
     * @param response 响应消息
     * @param done 完成回调
     */
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

    /**
     * @brief 异步调用 RPC 方法（协程方式）
     * @details 返回协程 Task，支持非阻塞异步调用
     * @param method 方法描述符
     * @param controller RPC 控制器
     * @param request 请求消息
     * @param response 响应消息
     * @param done 完成回调（可选）
     * @return 协程 Task
     */
    Task<void> CallMethodAsync(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done = nullptr);

    /**
     * @brief 设置超时时间
     * @param timeout_ms 超时时间（毫秒）
     */
    void setTimeout(int timeout_ms);

    /**
     * @brief 获取超时时间
     * @return 超时时间（毫秒）
     */
    int getTimeout() const { return m_timeout_ms; }

    /**
     * @brief 检查是否已连接
     * @return true 表示已连接，false 表示未连接
     */
    bool isConnected() const { return m_connected.load(); }

    /**
     * @brief 异步连接到服务器
     * @return 协程 Task，连接成功后返回
     */
    Task<void> connect();

    /**
     * @brief 关闭连接
     * @details 停止 worker loop，关闭 channel，释放连接资源
     */
    void close();

private:
    /**
     * @brief 初始化控制器
     * @param controller RPC 控制器指针
     * @details 如果控制器未设置超时时间，则使用默认超时
     */
    void initController(google::protobuf::RpcController* controller);

    /**
     * @brief 完成回调处理
     * @param done 完成回调
     * @param ctrl RPC 控制器
     */
    void doneCallback(google::protobuf::Closure* done, RpcController* ctrl);

    /**
     * @brief 工作线程循环
     * @details 从请求 channel 读取请求，发送到服务器并处理响应
     * @note 这是 RPC 通道的核心循环，负责处理所有 RPC 请求
     */
    Task<void> workerLoop();

    /**
     * @brief 重连机制
     * @details 自动尝试重新连接到服务器，最多重试 kMaxReconnectRetry 次
     */
    Task<void> reconnect();

    // 成员变量
    net::NetAddr::s_ptr m_addr;                  //< 服务器地址
    std::unique_ptr<net::TcpStream> m_stream;    //< TCP 流连接
    std::atomic<bool> m_connected{false};         //< 连接状态
    int m_timeout_ms = 3000;                      //< 默认超时时间（毫秒）

    TinyPBCoder m_coder;                          //< TinyPB 编解码器
    Channel<RpcRequest>::s_ptr m_request_chan;    //< 请求队列 channel
    std::atomic<bool> m_worker_running{false};    //< worker loop 运行状态
    std::atomic<bool> m_stopped{false};          //< 停止标志

    static constexpr int kMaxReconnectRetry = 3;  //< 最大重连次数
    static constexpr int kReconnectDelayMs = 1000; //< 重连延迟（毫秒）
    int m_reconnect_retry{0};                     //< 当前重连次数
};

}

#include "rpc_channel.inl"