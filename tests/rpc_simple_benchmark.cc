/*
 * @file rpc_simple_benchmark.cc
 * @brief RPC 框架简化性能测试
 */

#include "../include/coro.hpp"
#include <atomic>
#include <chrono>
#include <vector>
#include <iostream>
#include <iomanip>
#include <numeric>
#include <algorithm>
#include <sys/resource.h>

using namespace std::chrono;

constexpr int BENCHMARK_ITERATIONS = 10000;
constexpr int PARALLEL_TASKS = 10;

void testCoroutineScheduling() {
    std::cout << "\n[Test 1] Coroutine Scheduling Performance\n";
    std::cout << std::string(50, '-') << "\n";
    
    auto bench = [&]() -> Coro::Task<> {
        auto start = steady_clock::now();
        for (int i = 0; i < 10000; ++i) {
            auto t = []() -> Coro::Task<int> { co_return 1; };
            co_await t();
        }
        auto end = steady_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start).count();
        
        double ops = 10000 * 1000000000.0 / duration;
        
        std::cout << "  10K sync completions:\n";
        std::cout << "    Total time: " << std::fixed << std::setprecision(2) << duration / 1000000.0 << " ms\n";
        std::cout << "    Avg latency: " << std::fixed << std::setprecision(2) << duration / 10000.0 / 1000.0 << " us\n";
        std::cout << "    Throughput: " << std::fixed << std::setprecision(0) << ops << " ops/s\n";
    };
    bench().schedule();
    Coro::get_event_loop().run_until_complete();
}

void testChannelRendezvous() {
    std::cout << "\n[Test 2a] Channel Rendezvous (0 buffer)\n";
    std::cout << std::string(50, '-') << "\n";
    
    auto chan = std::make_shared<Coro::Channel<int>>(0);
    std::atomic<int> counter{0};
    
    auto sender = [&]() -> Coro::Task<> {
        for (int i = 0; i < BENCHMARK_ITERATIONS; ++i) {
            co_await chan->send(i);
        }
        chan->close();
    };
    
    auto receiver = [&]() -> Coro::Task<> {
        while (true) {
            try {
                co_await chan->recv();
                ++counter;
            } catch (const Coro::ChannelClosedException&) {
                break;
            }
        }
    };
    
    auto start = steady_clock::now();
    sender().schedule();
    receiver().schedule();
    Coro::get_event_loop().run_until_complete();
    auto end = steady_clock::now();
    
    auto duration = duration_cast<nanoseconds>(end - start).count();
    double ops = BENCHMARK_ITERATIONS * 2 * 1000000000.0 / duration;
    
    std::cout << "  Total time: " << std::fixed << std::setprecision(2) << duration / 1000000.0 << " ms\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) << ops << " ops/s\n";
}

void testChannelBuffered() {
    std::cout << "\n[Test 2b] Channel Buffered (1024 buffer)\n";
    std::cout << std::string(50, '-') << "\n";
    
    auto chan = std::make_shared<Coro::Channel<int>>(1024);
    std::atomic<int> counter{0};
    
    auto sender = [&]() -> Coro::Task<> {
        for (int i = 0; i < BENCHMARK_ITERATIONS; ++i) {
            co_await chan->send(i);
        }
        chan->close();
    };
    
    auto receiver = [&]() -> Coro::Task<> {
        while (true) {
            try {
                co_await chan->recv();
                ++counter;
            } catch (const Coro::ChannelClosedException&) {
                break;
            }
        }
    };
    
    auto start = steady_clock::now();
    sender().schedule();
    receiver().schedule();
    Coro::get_event_loop().run_until_complete();
    auto end = steady_clock::now();
    
    auto duration = duration_cast<nanoseconds>(end - start).count();
    double ops = BENCHMARK_ITERATIONS * 2 * 1000000000.0 / duration;
    
    std::cout << "  Total time: " << std::fixed << std::setprecision(2) << duration / 1000000.0 << " ms\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) << ops << " ops/s\n";
}

void testMPMC() {
    std::cout << "\n[Test 2c] MPMC (" << PARALLEL_TASKS << " producers, " << PARALLEL_TASKS << " consumers)\n";
    std::cout << std::string(50, '-') << "\n";
    
    auto chan = std::make_shared<Coro::Channel<int>>(1000);
    std::atomic<int> consumed{0};
    int producers = PARALLEL_TASKS;
    int consumers = PARALLEL_TASKS;
    int itemsPerProducer = BENCHMARK_ITERATIONS / producers;
    
    auto producer = [&]() -> Coro::Task<> {
        for (int i = 0; i < itemsPerProducer; ++i) {
            co_await chan->send(i);
        }
    };
    
    auto consumer = [&]() -> Coro::Task<> {
        while (consumed.load(std::memory_order_relaxed) < BENCHMARK_ITERATIONS) {
            try {
                co_await chan->recv();
                consumed.fetch_add(1, std::memory_order_relaxed);
            } catch (const Coro::ChannelClosedException&) {
                break;
            }
        }
    };
    
    std::vector<Coro::Task<>> producerTasks;
    std::vector<Coro::Task<>> consumerTasks;
    
    for (int i = 0; i < producers; ++i) {
        producerTasks.push_back(producer());
    }
    for (int i = 0; i < consumers; ++i) {
        consumerTasks.push_back(consumer());
    }
    
    auto start = steady_clock::now();
    
    for (auto& t : producerTasks) t.schedule();
    for (auto& t : consumerTasks) t.schedule();
    Coro::get_event_loop().run_until_complete();
    
    chan->close();
    
    auto end = steady_clock::now();
    auto duration = duration_cast<nanoseconds>(end - start).count();
    double ops = consumed.load() * 2 * 1000000000.0 / duration;
    
    std::cout << "  Total items: " << consumed.load() << "\n";
    std::cout << "  Total time: " << std::fixed << std::setprecision(2) << duration / 1000000.0 << " ms\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) << ops << " ops/s\n";
}

void testMemoryUsage() {
    std::cout << "\n[Test 3] Memory Usage\n";
    std::cout << std::string(50, '-') << "\n";
    
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    
    long max_rss_kb = usage.ru_maxrss;
    
    std::cout << "  Max RSS: " << max_rss_kb / 1024 << " MB\n";
    std::cout << "  Minor page faults: " << usage.ru_minflt << "\n";
    std::cout << "  Voluntary context switches: " << usage.ru_nvcsw << "\n";
    std::cout << "  Involuntary context switches: " << usage.ru_nivcsw << "\n";
}

int main() {
    std::cout << "\n";
    std::cout << "########################################\n";
    std::cout << "#    MCoroRpc Performance Benchmark    #\n";
    std::cout << "########################################\n";
    std::cout << "\n";
    std::cout << "C++20 Coroutine-based RPC Framework\n";
    std::cout << "System: Linux\n\n";
    
    testCoroutineScheduling();
    testChannelRendezvous();
    testChannelBuffered();
    testMPMC();
    testMemoryUsage();
    
    std::cout << "\n";
    std::cout << "########################################\n";
    std::cout << "#         BENCHMARK COMPLETE            #\n";
    std::cout << "########################################\n\n";
    
    return 0;
}
