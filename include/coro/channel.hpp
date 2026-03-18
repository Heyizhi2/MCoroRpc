/*
 * @Author: 来自火星的码农 15122322+heyizhi@user.noreply.gitee.com
 * @Date: 2026-03-18 18:30:00
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 21:26:12
 * @FilePath: /MCoroRpc/include/coro/channel.hpp
 * @Description: 协程 Channel 基础实现
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

template<typename T>
class Channel;

struct CancelledException : std::exception {
    const char* what() const noexcept override {
        return "Operation cancelled.";
    }
};

struct ChannelClosedException : std::exception {
    const char* what() const noexcept override {
        return "Channel is closed.";
    }
};

struct AwaiterBase {
    std::coroutine_handle<> handle;
    std::atomic<bool> cancelled{false};
    std::atomic<bool> completed{false};
    
    void resume() {
        if (!cancelled.load(std::memory_order_acquire)) {
            completed.store(true, std::memory_order_release);
            handle.resume();
        }
    }
    
    void cancel() {
        bool expected = false;
        if (cancelled.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            completed.store(true, std::memory_order_release);
            handle.resume();
        }
    }
    
    bool isCancelled() const {
        return cancelled.load(std::memory_order_acquire);
    }
    
    bool isCompleted() const {
        return completed.load(std::memory_order_acquire);
    }
};

template<typename T>
struct WriterAwaiter : AwaiterBase {
    Channel<T>* chan;
    T value;

    WriterAwaiter(Channel<T>* chan, T value) : chan(chan), value(std::move(value)) {}

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        this->handle = h;
        chan->pushWriter(this);
    }

    void await_resume() {
        if (this->isCancelled()) {
            throw CancelledException{};
        }
        chan->checkClosed();
    }
};

template<typename T>
struct ReaderAwaiter : AwaiterBase {
    Channel<T>* chan;
    T value;
    bool hasValue = false;

    ReaderAwaiter(Channel<T>* chan) : chan(chan) {}

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        this->handle = h;
        chan->pushReader(this);
    }

    T await_resume() {
        if (this->isCancelled()) {
            throw CancelledException{};
        }
        chan->checkClosed();
        return std::move(value);
    }

    void resumeWithValue(T val) {
        value = std::move(val);
        hasValue = true;
        AwaiterBase::resume();
    }
};

template<typename T>
class Channel {
public:
    using s_ptr = std::shared_ptr<Channel<T>>;

    explicit Channel(size_t capacity = 0) : capacity_(capacity) {
        isActive_.store(true, std::memory_order_relaxed);
    }

    ~Channel() {
        close();
    }

    Channel(Channel&&) = delete;
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    bool isActive() const {
        return isActive_.load(std::memory_order_relaxed);
    }

    void close() {
        bool expect = true;
        if (isActive_.compare_exchange_strong(expect, false, std::memory_order_relaxed)) {
            cleanup();
        }
    }

    size_t waitingCount() {
        std::lock_guard<std::mutex> lock(mutex_);
        return writers_.size() + readers_.size();
    }

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

    void checkClosed() {
        if (!isActive()) {
            throw ChannelClosedException{};
        }
    }

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
    size_t capacity_;
    std::queue<T> buffer_;
    std::list<WriterAwaiter<T>*> writers_;
    std::list<ReaderAwaiter<T>*> readers_;
    std::atomic<bool> isActive_;
    std::mutex mutex_;
};

template<typename T>
class Sender {
public:
    explicit Sender(typename Channel<T>::s_ptr chan) : chan_(std::move(chan)) {}
    Sender(Sender&&) noexcept = default;
    Sender& operator=(Sender&&) noexcept = default;

    auto send(T value) -> Task<> {
        return chan_->send(std::move(value));
    }

    void close() { chan_->close(); }
    bool isActive() const { return chan_->isActive(); }

private:
    typename Channel<T>::s_ptr chan_;
};

template<typename T>
class Receiver {
public:
    explicit Receiver(typename Channel<T>::s_ptr chan) : chan_(std::move(chan)) {}
    Receiver(Receiver&&) noexcept = default;
    Receiver& operator=(Receiver&&) noexcept = default;

    auto recv() -> Task<T> {
        return chan_->recv();
    }

    auto tryRecv() -> Task<std::optional<T>> {
        return chan_->tryRecv();
    }

    bool isActive() const { return chan_->isActive(); }

private:
    typename Channel<T>::s_ptr chan_;
};

template<typename T>
struct MPMC {
    using s_ptr = std::shared_ptr<MPMC<T>>;
    MPMC(size_t capacity = 0) : chan_(std::make_shared<Channel<T>>(capacity)) {}

    Sender<T> get_sender() { return Sender<T>(chan_); }
    Receiver<T> get_receiver() { return Receiver<T>(chan_); }
    typename Channel<T>::s_ptr chan_;
};

}
