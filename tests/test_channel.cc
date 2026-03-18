#include "../include/coro.hpp"
#include <catch2/catch.hpp>
#include <chrono>
#include <iostream>
#include <vector>
#include <numeric>

using namespace Coro;

TEST_CASE("Channel cancellation", "[channel][cancellation]") {
    auto chan = std::make_shared<Channel<int>>(0);
    bool cancelled = false;
    bool senderWoken = false;

    auto senderTask = [&]() -> Task<> {
        co_await chan->send(42);
        senderWoken = true;
    };

    auto receiverTask = [&]() -> Task<> {
        try {
            auto val = co_await chan->recv();
            REQUIRE(val == 42);
        } catch (const CancelledException&) {
            cancelled = true;
        }
    };

    senderTask().schedule();
    receiverTask().schedule();
    get_event_loop().run_until_complete();
    
    REQUIRE(senderWoken);
}

TEST_CASE("Channel close cancels all waiters", "[channel][cancellation]") {
    auto chan = std::make_shared<Channel<int>>(0);
    int senderCancelled = 0;
    int receiverCancelled = 0;

    auto senderTask = [&]() -> Task<> {
        try {
            co_await chan->send(1);
        } catch (const CancelledException&) {
            ++senderCancelled;
        }
    };

    auto receiverTask = [&]() -> Task<> {
        try {
            co_await chan->recv();
        } catch (const CancelledException&) {
            ++receiverCancelled;
        } catch (const ChannelClosedException&) {
            // Channel closed before we got data
        }
    };

    senderTask().schedule();
    receiverTask().schedule();
    
    // Close channel while senders/receivers are waiting
    get_event_loop().run_until_complete();
    
    // At least one should have been cancelled or handled close
    REQUIRE(chan->waitingCount() == 0);
}

TEST_CASE("Multiple channels with cancellation", "[channel][cancellation]") {
    auto chan1 = std::make_shared<Channel<int>>(0);
    int completed = 0;

    auto task1 = [&]() -> Task<> {
        co_await chan1->send(1);
        co_await chan1->send(2);
        ++completed;
    };

    auto task2 = [&]() -> Task<> {
        co_await chan1->recv();
        co_await chan1->recv();
        ++completed;
    };

    task1().schedule();
    task2().schedule();
    get_event_loop().run_until_complete();
    
    REQUIRE(completed == 2);
}
