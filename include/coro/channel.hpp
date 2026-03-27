/**
 * @file channel.hpp
 * @brief 协程 Channel 实现
 * 
 * Channel是协程间通信的通道，类似于Go语言的channel。
 * - 支持阻塞发送/接收
 * - 支持有界和无界容量
 * - 支持多生产者多消费者(MPMC)
 * - 支持关闭和取消
 * 
 * 使用示例：
 * @code
 * auto channel = std::make_shared<Channel<int>>();
 * 
 * // 生产者
 * co_await channel->send(42);
 * 
 * // 消费者
 * int value = co_await channel->recv();
 * @endcode
 */

#pragma once
#include "event_loop.hpp"
#include <coroutine>
#include <list>
#include <memory>
#include <queue>
#include <stdexcept>
#include <atomic>
#include <functional>
#include "task.hpp"

namespace Coro {

/**
 * @brief Channel声明
 * @tparam T 通道传输的数据类型
 */
template<typename T>
class Channel;

/**
 * @brief 取消异常
 * @details 当Channel操作被取消时抛出
 */
struct CancelledException : std::exception {
    const char* what() const noexcept override {
        return "Operation cancelled.";
    }
};

/**
 * @brief Channel关闭异常
 * @details 当从已关闭的Channel接收或发送时抛出
 */
struct ChannelClosedException : std::exception {
    const char* what() const noexcept override {
        return "Channel is closed.";
    }
};

/**
 * @brief 等待器基类
 * @details WriterAwaiter和ReaderAwaiter的基类
 */
struct AwaiterBase {
    /** @brief 关联的协程句柄 */
    std::coroutine_handle<> handle;
    
    /** @brief 是否已取消 */
    std::atomic<bool> cancelled{false};
    
    /** @brief 是否已完成 */
    std::atomic<bool> completed{false};
    
    /**
     * @brief 恢复协程执行
     * @details 如果未取消则恢复协程
     */
    void resume() {
        if (!cancelled.load(std::memory_order_acquire)) {
            completed.store(true, std::memory_order_release);
            handle.resume();
        }
    }
    
    /**
     * @brief 取消等待
     * @details 将协程标记为取消并恢复执行
     */
    void cancel() {
        bool expected = false;
        if (cancelled.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            completed.store(true, std::memory_order_release);
            handle.resume();
        }
    }
    
    /**
     * @brief 检查是否已取消
     */
    bool isCancelled() const {
        return cancelled.load(std::memory_order_acquire);
    }
    
    /**
     * @brief 检查是否已完成
     */
    bool isCompleted() const {
        return completed.load(std::memory_order_acquire);
    }
};

/**
 * @brief 写等待器
 * @tparam T 数据类型
 */
template<typename T>
struct WriterAwaiter : AwaiterBase {
    /** @brief 关联的Channel */
    Channel<T>* chan;
    
    /** @brief 要发送的值 */
    T value;

    /**
     * @brief 构造函数
     */
    WriterAwaiter(Channel<T>* chan, T value) : chan(chan), value(std::move(value)) {}

    /**
     * @brief 是否立即完成
     */
    bool await_ready() { return false; }

    /**
     * @brief 挂起时处理
     */
    void await_suspend(std::coroutine_handle<> h) {
        this->handle = h;
        chan->pushWriter(this);
    }

    /**
     * @brief 恢复时处理
     */
    void await_resume() {
        if (this->isCancelled()) {
            throw CancelledException{};
        }
        chan->checkClosed();
    }
};

/**
 * @brief 读等待器
 * @tparam T 数据类型
 */
template<typename T>
struct ReaderAwaiter : AwaiterBase {
    /** @brief 关联的Channel */
    Channel<T>* chan;
    
    /** @brief 读取的值 */
    T value;
    
    /** @brief 是否有值 */
    bool hasValue = false;

    /**
     * @brief 构造函数
     */
    ReaderAwaiter(Channel<T>* chan) : chan(chan) {}

    /**
     * @brief 是否立即完成
     */
    bool await_ready() { return false; }

    /**
     * @brief 挂起时处理
     */
    void await_suspend(std::coroutine_handle<> h) {
        this->handle = h;
        chan->pushReader(this);
    }

    /**
     * @brief 恢复时处理
     */
    T await_resume() {
        if (this->isCancelled()) {
            throw CancelledException{};
        }
        chan->checkClosed();
        return std::move(value);
    }

    /**
     * @brief 带值恢复
     */
    void resumeWithValue(T val) {
        value = std::move(val);
        hasValue = true;
        AwaiterBase::resume();
    }
};

/**
 * @brief 协程Channel
 * @tparam T 数据类型
 * 
 * 支持有界和无界容量：
 * - capacity=0: 无缓冲Channel
 * - capacity>0: 有界Channel
 * - capacity=无界: 无限缓冲区
 */
template<typename T>
class Channel {
public:
    /** @brief 智能指针类型 */
    using s_ptr = std::shared_ptr<Channel<T>>;

    /**
     * @brief 构造函数
     * @param capacity 缓冲区容量，0表示无缓冲
     */
    explicit Channel(size_t capacity = 0) : capacity_(capacity) {
        isActive_.store(true, std::memory_order_relaxed);
    }

    ~Channel() {
        close();
    }

    Channel(Channel&&) = delete;
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    /**
     * @brief 检查Channel是否活跃
     */
    bool isActive() const {
        return isActive_.load(std::memory_order_relaxed);
    }

    /**
     * @brief 关闭Channel
     * @details 关闭后不能发送，但可以接收剩余数据
     */
    void close() {
        bool expect = true;
        if (isActive_.compare_exchange_strong(expect, false, std::memory_order_relaxed)) {
            cleanup();
        }
    }

    /**
     * @brief 获取等待者数量
     */
    size_t waitingCount() {
        std::lock_guard<std::mutex> lock(mutex_);
        return writers_.size() + readers_.size();
    }

    /**
     * @brief 发送数据（协程）
     * @param value 要发送的值
     * @return Task<> 协程对象
     * 
     * 如果有等待的接收者，直接传递值
     * 如果缓冲区有空间，放入缓冲区
     * 否则挂起等待
     */
    auto send(T value) -> Task<> {
        if (!isActive()) {
            co_return;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!readers_.empty()) {
                auto* reader = readers_.front();
                readers_.pop_front();
                reader->resumeWithValue(std::move(value));
                co_return;
            }
            if (buffer_.size() < capacity_) {
                buffer_.push(std::move(value));
                co_return;
            }
        }

        co_await WriterAwaiter<T>{this, std::move(value)};
    }

    /**
     * @brief 接收数据（协程）
     * @return Task<T> 接收到的值
     * 
     * 如果有等待的发送者，直接获取值
     * 如果缓冲区有数据，从缓冲区取
     * 否则挂起等待
     */
    auto recv() -> Task<T> {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!writers_.empty()) {
                auto* writer = writers_.front();
                writers_.pop_front();
                T val = std::move(writer->value);
                writer->resume();
                co_return val;
            }
            if (!buffer_.empty()) {
                T val = std::move(buffer_.front());
                buffer_.pop();
                if (!writers_.empty()) {
                    auto* writer = writers_.front();
                    writers_.pop_front();
                    buffer_.push(std::move(writer->value));
                    writer->resume();
                }
                co_return val;
            }
        }

        if (!isActive()) {
            throw ChannelClosedException{};
        }

        co_await ReaderAwaiter<T>{this};
        co_return T{};
    }

    /**
     * @brief 尝试接收（非阻塞）
     * @return Task<std::optional<T>> 有值返回，否则返回空
     */
    auto tryRecv() -> Task<std::optional<T>> {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!writers_.empty()) {
                auto* writer = writers_.front();
                writers_.pop_front();
                T val = std::move(writer->value);
                writer->resume();
                co_return val;
            }
            if (!buffer_.empty()) {
                T val = std::move(buffer_.front());
                buffer_.pop();
                if (!writers_.empty()) {
                    auto* writer = writers_.front();
                    writers_.pop_front();
                    buffer_.push(std::move(writer->value));
                    writer->resume();
                }
                co_return val;
            }
        }
        co_return std::nullopt;
    }

    /**
     * @brief 同步发送数据（非协程）
     * @param value 要发送的值
     * @return true 发送成功, false Channel已关闭
     * 
     * 同步版本的send，适用于非协程环境
     */
    bool sendSync(T value) {
        if (!isActive()) {
            return false;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (!readers_.empty()) {
            auto* reader = readers_.front();
            readers_.pop_front();
            reader->resumeWithValue(std::move(value));
            return true;
        }
        if (buffer_.size() < capacity_) {
            buffer_.push(std::move(value));
            return true;
        }
        return false;
    }

    /**
     * @brief 取消所有等待者
     */
    void cancelAllAwaiters() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto* w : writers_) {
            w->cancel();
        }
        writers_.clear();
        for (auto* r : readers_) {
            r->cancel();
        }
        readers_.clear();
    }

private:
    friend struct WriterAwaiter<T>;
    friend struct ReaderAwaiter<T>;

    /**
     * @brief 添加写等待者
     */
    void pushWriter(WriterAwaiter<T>* w) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!readers_.empty()) {
            auto* reader = readers_.front();
            readers_.pop_front();
            reader->resumeWithValue(std::move(w->value));
            w->resume();
        } else if (buffer_.size() < capacity_) {
            buffer_.push(std::move(w->value));
            w->resume();
        } else {
            writers_.push_back(w);
        }
    }

    /**
     * @brief 添加读等待者
     */
    void pushReader(ReaderAwaiter<T>* r) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!writers_.empty()) {
            auto* writer = writers_.front();
            writers_.pop_front();
            T value = std::move(writer->value);
            writer->resume();
            r->resumeWithValue(std::move(value));
        } else if (!buffer_.empty()) {
            r->resumeWithValue(std::move(buffer_.front()));
            buffer_.pop();
            if (!writers_.empty()) {
                auto* writer = writers_.front();
                writers_.pop_front();
                buffer_.push(std::move(writer->value));
                writer->resume();
            }
        } else {
            readers_.push_back(r);
        }
    }

    /**
     * @brief 检查Channel是否关闭
     */
    void checkClosed() {
        if (!isActive()) {
            throw ChannelClosedException{};
        }
    }

    /**
     * @brief 清理资源
     */
    void cleanup() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto* w : writers_) {
            w->cancel();
        }
        writers_.clear();
        for (auto* r : readers_) {
            r->cancel();
        }
        readers_.clear();
        decltype(buffer_) empty;
        std::swap(buffer_, empty);
    }

private:
    /** @brief 缓冲区容量 */
    size_t capacity_;
    
    /** @brief 数据缓冲区 */
    std::queue<T> buffer_;
    
    /** @brief 等待的写者 */
    std::list<WriterAwaiter<T>*> writers_;
    
    /** @brief 等待的读者 */
    std::list<ReaderAwaiter<T>*> readers_;
    
    /** @brief Channel是否活跃 */
    std::atomic<bool> isActive_;
    
    /** @brief 互斥锁 */
    std::mutex mutex_;
};

/**
 * @brief 发送端
 * @tparam T 数据类型
 */
template<typename T>
class Sender {
public:
    explicit Sender(typename Channel<T>::s_ptr chan) : chan_(std::move(chan)) {}
    Sender(Sender&&) noexcept = default;
    Sender& operator=(Sender&&) noexcept = default;

    /**
     * @brief 发送数据
     */
    auto send(T value) -> Task<> {
        return chan_->send(std::move(value));
    }

    /** @brief 关闭发送端 */
    void close() { chan_->close(); }
    
    /** @brief 检查是否活跃 */
    bool isActive() const { return chan_->isActive(); }

private:
    typename Channel<T>::s_ptr chan_;
};

/**
 * @brief 接收端
 * @tparam T 数据类型
 */
template<typename T>
class Receiver {
public:
    explicit Receiver(typename Channel<T>::s_ptr chan) : chan_(std::move(chan)) {}
    Receiver(Receiver&&) noexcept = default;
    Receiver& operator=(Receiver&&) noexcept = default;

    /**
     * @brief 接收数据
     */
    auto recv() -> Task<T> {
        return chan_->recv();
    }

    /**
     * @brief 尝试接收
     */
    auto tryRecv() -> Task<std::optional<T>> {
        return chan_->tryRecv();
    }

    /** @brief 检查是否活跃 */
    bool isActive() const { return chan_->isActive(); }

private:
    typename Channel<T>::s_ptr chan_;
};

/**
 * @brief 多生产者多消费者Channel工厂
 * @tparam T 数据类型
 */
template<typename T>
struct MPMC {
    /** @brief 智能指针类型 */
    using s_ptr = std::shared_ptr<MPMC<T>>;
    
    /**
     * @brief 构造函数
     * @param capacity 缓冲区容量
     */
    MPMC(size_t capacity = 0) : chan_(std::make_shared<Channel<T>>(capacity)) {}

    /** @brief 获取发送端 */
    Sender<T> get_sender() { return Sender<T>(chan_); }
    
    /** @brief 获取接收端 */
    Receiver<T> get_receiver() { return Receiver<T>(chan_); }
    
    /** @brief 底层Channel */
    typename Channel<T>::s_ptr chan_;
};

}
