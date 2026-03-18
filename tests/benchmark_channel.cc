#include "../include/coro.hpp"
#include <chrono>
#include <iostream>
#include <vector>
#include <atomic>

using namespace Coro;

constexpr int BENCHMARK_ITERATIONS = 10000;
constexpr int PARALLEL_TASKS = 10;

struct BenchmarkResult {
    std::string name;
    double totalTimeMs;
    double avgTimeNs;
    double throughputOpsPerSec;
};

BenchmarkResult runChannelPingPong() {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto chan = std::make_shared<Channel<int>>(0);
    std::atomic<int> counter{0};
    int maxIterations = BENCHMARK_ITERATIONS;
    
    auto sender = [&]() -> Task<> {
        for (int i = 0; i < maxIterations; ++i) {
            co_await chan->send(i);
        }
        chan->close();
    };
    
    auto receiver = [&]() -> Task<> {
        while (true) {
            try {
                co_await chan->recv();
                ++counter;
            } catch (const ChannelClosedException&) {
                break;
            }
        }
    };
    
    sender().schedule();
    receiver().schedule();
    get_event_loop().run_until_complete();
    
    auto end = std::chrono::high_resolution_clock::now();
    double totalNs = std::chrono::duration<double, std::nano>(end - start).count();
    
    BenchmarkResult result;
    result.name = "Channel Rendezvous (0)";
    result.totalTimeMs = totalNs / 1e6;
    result.avgTimeNs = totalNs / (BENCHMARK_ITERATIONS * 2);
    result.throughputOpsPerSec = (BENCHMARK_ITERATIONS * 2) / (totalNs / 1e9);
    return result;
}

BenchmarkResult runChannelBuffered() {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto chan = std::make_shared<Channel<int>>(1024);
    std::atomic<int> counter{0};
    int maxIterations = BENCHMARK_ITERATIONS;
    
    auto sender = [&]() -> Task<> {
        for (int i = 0; i < maxIterations; ++i) {
            co_await chan->send(i);
        }
        chan->close();
    };
    
    auto receiver = [&]() -> Task<> {
        while (true) {
            try {
                co_await chan->recv();
                ++counter;
            } catch (const ChannelClosedException&) {
                break;
            }
        }
    };
    
    sender().schedule();
    receiver().schedule();
    get_event_loop().run_until_complete();
    
    auto end = std::chrono::high_resolution_clock::now();
    double totalNs = std::chrono::duration<double, std::nano>(end - start).count();
    
    BenchmarkResult result;
    result.name = "Channel Buffered (1024)";
    result.totalTimeMs = totalNs / 1e6;
    result.avgTimeNs = totalNs / (BENCHMARK_ITERATIONS * 2);
    result.throughputOpsPerSec = (BENCHMARK_ITERATIONS * 2) / (totalNs / 1e9);
    return result;
}

BenchmarkResult runChannelBufferedSmall() {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto chan = std::make_shared<Channel<int>>(10);
    std::atomic<int> counter{0};
    int maxIterations = BENCHMARK_ITERATIONS;
    
    auto sender = [&]() -> Task<> {
        for (int i = 0; i < maxIterations; ++i) {
            co_await chan->send(i);
        }
        chan->close();
    };
    
    auto receiver = [&]() -> Task<> {
        while (true) {
            try {
                co_await chan->recv();
                ++counter;
            } catch (const ChannelClosedException&) {
                break;
            }
        }
    };
    
    sender().schedule();
    receiver().schedule();
    get_event_loop().run_until_complete();
    
    auto end = std::chrono::high_resolution_clock::now();
    double totalNs = std::chrono::duration<double, std::nano>(end - start).count();
    
    BenchmarkResult result;
    result.name = "Channel Buffered (10)";
    result.totalTimeMs = totalNs / 1e6;
    result.avgTimeNs = totalNs / (BENCHMARK_ITERATIONS * 2);
    result.throughputOpsPerSec = (BENCHMARK_ITERATIONS * 2) / (totalNs / 1e9);
    return result;
}

BenchmarkResult runMultiProducerConsumer() {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto chan = std::make_shared<Channel<int>>(1000);
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};
    int producers = PARALLEL_TASKS;
    int consumers = PARALLEL_TASKS;
    int itemsPerProducer = BENCHMARK_ITERATIONS / producers;
    
    auto producer = [&]() -> Task<> {
        for (int i = 0; i < itemsPerProducer; ++i) {
            co_await chan->send(produced.fetch_add(1, std::memory_order_relaxed));
        }
    };
    
    auto consumer = [&]() -> Task<> {
        while (consumed.load(std::memory_order_relaxed) < BENCHMARK_ITERATIONS) {
            try {
                co_await chan->recv();
                consumed.fetch_add(1, std::memory_order_relaxed);
            } catch (const ChannelClosedException&) {
                break;
            }
        }
    };
    
    std::vector<Task<>> producerTasks;
    std::vector<Task<>> consumerTasks;
    
    for (int i = 0; i < producers; ++i) {
        producerTasks.push_back(producer());
    }
    for (int i = 0; i < consumers; ++i) {
        consumerTasks.push_back(consumer());
    }
    
    for (auto& t : producerTasks) t.schedule();
    for (auto& t : consumerTasks) t.schedule();
    
    get_event_loop().run_until_complete();
    
    auto end = std::chrono::high_resolution_clock::now();
    double totalNs = std::chrono::duration<double, std::nano>(end - start).count();
    
    BenchmarkResult result;
    result.name = "MPMC (" + std::to_string(producers) + "p/" + std::to_string(consumers) + "c)";
    result.totalTimeMs = totalNs / 1e6;
    result.avgTimeNs = totalNs / (BENCHMARK_ITERATIONS * 2);
    result.throughputOpsPerSec = (BENCHMARK_ITERATIONS * 2) / (totalNs / 1e9);
    return result;
}

BenchmarkResult runCallbackBasedCounter() {
    auto start = std::chrono::high_resolution_clock::now();
    
    int counter = 0;
    int maxIterations = BENCHMARK_ITERATIONS;
    bool senderDone = false;
    
    auto sender = [&]() -> Task<> {
        for (int i = 0; i < maxIterations; ++i) {
            co_await net::WriteAwaiter{1, 0}; // Dummy write to stdout
        }
        senderDone = true;
    };
    
    sender().schedule();
    get_event_loop().run_until_complete();
    
    auto end = std::chrono::high_resolution_clock::now();
    double totalNs = std::chrono::duration<double, std::nano>(end - start).count();
    
    BenchmarkResult result;
    result.name = "Yield Baseline";
    result.totalTimeMs = totalNs / 1e6;
    result.avgTimeNs = totalNs / maxIterations;
    result.throughputOpsPerSec = maxIterations / (totalNs / 1e9);
    return result;
}

void printResult(const BenchmarkResult& r) {
    std::cout << std::format("{:30} | {:>10.2f} ms | {:>8.2f} ns/op | {:>12.0f} ops/s\n",
        r.name, r.totalTimeMs, r.avgTimeNs, r.throughputOpsPerSec);
}

int main() {
    std::cout << "\n========================================\n";
    std::cout << "       Channel Performance Benchmark\n";
    std::cout << "========================================\n";
    std::cout << std::format("{:<30} | {:>10} | {:>8} | {:>12}\n",
        "Test", "Total", "Avg", "Throughput");
    std::cout << "----------------------------------------\n";
    
    auto r1 = runChannelPingPong();
    printResult(r1);
    
    auto r2 = runChannelBuffered();
    printResult(r2);
    
    auto r3 = runChannelBufferedSmall();
    printResult(r3);
    
    auto r4 = runMultiProducerConsumer();
    printResult(r4);
    
    std::cout << "========================================\n";
    std::cout << "Analysis:\n";
    std::cout << "----------------------------------------\n";
    std::cout << "- Channel Rendezvous: sender blocks until receiver takes value\n";
    std::cout << "- Channel Buffered: sender blocks only when buffer full\n";
    std::cout << "- MPMC: Multiple producers/consumers sharing one channel\n";
    std::cout << "- Larger buffer = less context switching = higher throughput\n";
    std::cout << "========================================\n";
    
    return 0;
}
