/*
 * @file rpc_full_benchmark.cc
 * @brief RPC 框架完整性能测试套件
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

// ============================================================================
// 测试 1: 协程调度性能
// ============================================================================
void testCoroutineScheduling() {
    std::cout << "\n[Test 1] Coroutine Scheduling Performance\n";
    std::cout << std::string(50, '-') << "\n";
    
    // 测试 1.1: 空协程调度
    {
        std::vector<uint64_t> samples;
        
        auto bench = [&]() -> Coro::Task<> {
            auto start = steady_clock::now();
            for (int i = 0; i < 10000; ++i) {
                auto t = []() -> Coro::Task<int> { co_return 1; };
                co_await t();
            }
            auto end = steady_clock::now();
            samples.push_back(duration_cast<nanoseconds>(end - start).count());
        };
        bench().schedule();
        Coro::get_event_loop().run_until_complete();
        
        if (!samples.empty()) {
            double ops = 10000 * 1000000000.0 / samples[0];
            std::cout << "  Empty coroutine (10k): " << std::fixed << std::setprecision(0) << ops << " ops/s\n";
            std::cout << "  Avg latency: " << std::fixed << std::setprecision(2) << samples[0] / 10000.0 / 1000.0 << " us\n";
        }
    }
    
    // 测试 1.2: Channel 通信
    {
        std::vector<uint64_t> samples;
        
        auto bench = [&]() -> Coro::Task<> {
            auto chan = std::make_shared<Coro::Channel<int>>(0);
            
            auto sender = [&]() -> Coro::Task<> {
                for (int i = 0; i < 5000; ++i) {
                    auto start = steady_clock::now();
                    co_await chan->send(i);
                    auto end = steady_clock::now();
                    samples.push_back(duration_cast<nanoseconds>(end - start).count());
                }
                chan->close();
            };
            
            auto receiver = [&]() -> Coro::Task<> {
                while (true) {
                    try {
                        co_await chan->recv();
                    } catch (const Coro::ChannelClosedException&) {
                        break;
                    }
                }
            };
            
            sender().schedule();
            receiver().schedule();
        };
        bench().schedule();
        Coro::get_event_loop().run_until_complete();
        
        if (!samples.empty()) {
            std::sort(samples.begin(), samples.end());
            uint64_t sum = std::accumulate(samples.begin(), samples.end(), 0ULL);
            double avg = sum / static_cast<double>(samples.size()) / 1000.0;
            double p99 = samples[samples.size() * 0.99] / 1000.0;
            std::cout << "  Channel send (5k): avg=" << std::fixed << std::setprecision(2) << avg << " us, p99=" << p99 << " us\n";
        }
    }
}

// ============================================================================
// 测试 2: TCP Echo 测试
// ============================================================================
std::atomic<uint64_t> g_tcp_requests{0};
std::atomic<uint64_t> g_tcp_bytes_sent{0};
std::atomic<uint64_t> g_tcp_bytes_recv{0};

Coro::Task<void> tcpEchoServerBenchmark(int port) {
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", port);
    
    while (true) {
        auto client = co_await service.accept();
        
        auto handler = [](Coro::net::TcpStream stream) -> Coro::Task<> {
            while (true) {
                auto data = co_await stream.read(4096);
                if (data.empty()) break;
                g_tcp_bytes_recv += data.size();
                g_tcp_requests++;
                co_await stream.write(data);
                g_tcp_bytes_sent += data.size();
            }
            stream.close();
        };
        
        handler(std::move(client)).schedule();
    }
}

Coro::Task<void> tcpClientBenchmark(const std::string& host, int port, int requests) {
    try {
        auto stream = co_await Coro::net::connect(host, port);
        std::vector<char> buf(1024, 'A');
        
        for (int i = 0; i < requests; ++i) {
            co_await stream.write(buf);
            auto resp = co_await stream.read(1024);
            if (resp.empty()) break;
        }
        
        stream.close();
    } catch (...) {}
}

void testTCPThroughput() {
    std::cout << "\n[Test 2] TCP Echo Throughput\n";
    std::cout << std::string(50, '-') << "\n";
    
    int port = 9001;
    int clients = 10;
    int requests_per_client = 100;
    
    g_tcp_requests = 0;
    g_tcp_bytes_sent = 0;
    g_tcp_bytes_recv = 0;
    
    // 启动服务器
    tcpEchoServerBenchmark(port).schedule();
    
    auto waitTask = []() -> Coro::Task<> {
        co_await Coro::sleep_for(milliseconds(500));
    };
    waitTask().schedule();
    Coro::get_event_loop().run_until_complete();
    
    // 记录开始时间
    auto start = steady_clock::now();
    
    // 启动客户端
    for (int i = 0; i < clients; ++i) {
        tcpClientBenchmark("127.0.0.1", port, requests_per_client).schedule();
    }
    
    // 等待完成
    auto timer = []() -> Coro::Task<> {
        co_await Coro::sleep_for(seconds(5));
    };
    timer().schedule();
    Coro::get_event_loop().run_until_complete();
    
    auto end = steady_clock::now();
    double duration_s = duration_cast<milliseconds>(end - start).count() / 1000.0;
    
    uint64_t total_bytes = g_tcp_bytes_sent.load() + g_tcp_bytes_recv.load();
    uint64_t total_requests = g_tcp_requests.load();
    double qps = total_requests / duration_s;
    double mbps = total_bytes * 8.0 / 1024.0 / 1024.0 / duration_s;
    
    std::cout << "  Clients: " << clients << "\n";
    std::cout << "  Requests: " << total_requests << "\n";
    std::cout << "  Duration: " << std::fixed << std::setprecision(2) << duration_s << "s\n";
    std::cout << "  QPS: " << std::fixed << std::setprecision(0) << qps << "\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << mbps << " Mbps\n";
}

// ============================================================================
// 测试 3: 延迟测试
// ============================================================================
void testLatency() {
    std::cout << "\n[Test 3] Channel Latency Distribution\n";
    std::cout << std::string(50, '-') << "\n";
    
    auto testPing = [&](int count) {
        std::vector<uint64_t> latencies;
        
        auto test = [&]() -> Coro::Task<> {
            auto chan = std::make_shared<Coro::Channel<int>>(0);
            
            auto sender = [&]() -> Coro::Task<> {
                for (int i = 0; i < count; ++i) {
                    auto start = steady_clock::now();
                    co_await chan->send(i);
                    auto end = steady_clock::now();
                    latencies.push_back(duration_cast<nanoseconds>(end - start).count());
                }
                chan->close();
            };
            
            auto receiver = [&]() -> Coro::Task<> {
                for (int i = 0; i < count; ++i) {
                    co_await chan->recv();
                }
            };
            
            sender().schedule();
            receiver().schedule();
        };
        
        test().schedule();
        Coro::get_event_loop().run_until_complete();
        
        if (!latencies.empty()) {
            std::sort(latencies.begin(), latencies.end());
            
            uint64_t sum = std::accumulate(latencies.begin(), latencies.end(), 0ULL);
            double avg = sum / static_cast<double>(latencies.size()) / 1000.0;
            double min = latencies[0] / 1000.0;
            double max = latencies.back() / 1000.0;
            double p50 = latencies[latencies.size() * 0.50] / 1000.0;
            double p90 = latencies[latencies.size() * 0.90] / 1000.0;
            double p99 = latencies[latencies.size() * 0.99] / 1000.0;
            
            std::cout << "  " << count << " iterations:\n";
            std::cout << "    Min: " << std::fixed << std::setprecision(2) << min << " us\n";
            std::cout << "    Avg: " << std::fixed << std::setprecision(2) << avg << " us\n";
            std::cout << "    P50: " << std::fixed << std::setprecision(2) << p50 << " us\n";
            std::cout << "    P90: " << std::fixed << std::setprecision(2) << p90 << " us\n";
            std::cout << "    P99: " << std::fixed << std::setprecision(2) << p99 << " us\n";
            std::cout << "    Max: " << std::fixed << std::setprecision(2) << max << " us\n";
        }
    };
    
    testPing(1000);
    testPing(5000);
    testPing(10000);
}

// ============================================================================
// 测试 4: 并发连接数测试
// ============================================================================
std::atomic<int> g_active_connections{0};
std::atomic<int> g_max_connections{0};

Coro::Task<void> connectionTestServerBenchmark(int port) {
    auto service = co_await Coro::net::start_tcp_service("0.0.0.0", port);
    
    while (true) {
        auto client = co_await service.accept();
        g_active_connections++;
        int current = g_active_connections.load();
        while (g_max_connections.load() < current) {
            g_max_connections.store(current);
        }
        
        auto handler = [](Coro::net::TcpStream stream) -> Coro::Task<> {
            auto data = co_await stream.read(64);
            co_await stream.write(data);
            stream.close();
            g_active_connections--;
        };
        
        handler(std::move(client)).schedule();
    }
}

Coro::Task<void> connectionTestClientBenchmark(int port, int id) {
    try {
        auto stream = co_await Coro::net::connect("127.0.0.1", port);
        std::vector<char> ping(64, 'P');
        co_await stream.write(ping);
        auto pong = co_await stream.read(64);
        stream.close();
    } catch (...) {}
}

void testConcurrentConnections() {
    std::cout << "\n[Test 4] Concurrent Connections\n";
    std::cout << std::string(50, '-') << "\n";
    
    int port = 9003;
    
    g_active_connections = 0;
    g_max_connections = 0;
    
    // 启动服务器
    connectionTestServerBenchmark(port).schedule();
    
    auto waitTask = []() -> Coro::Task<> {
        co_await Coro::sleep_for(milliseconds(500));
    };
    waitTask().schedule();
    Coro::get_event_loop().run_until_complete();
    
    // 测试不同并发数
    for (int n : {10, 50, 100, 200}) {
        g_active_connections = 0;
        
        for (int i = 0; i < n; ++i) {
            connectionTestClientBenchmark(port, i).schedule();
        }
        
        auto waitTask = []() -> Coro::Task<> {
            co_await Coro::sleep_for(seconds(1));
        };
        waitTask().schedule();
        Coro::get_event_loop().run_until_complete();
        
        std::cout << "  Connections: " << n 
                  << " | Active: " << g_active_connections.load()
                  << " | Max seen: " << g_max_connections.load() << "\n";
    }
}

// ============================================================================
// 测试 5: 内存使用测试
// ============================================================================
void testMemoryUsage() {
    std::cout << "\n[Test 5] Memory Usage\n";
    std::cout << std::string(50, '-') << "\n";
    
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    
    long max_rss_kb = usage.ru_maxrss;
    
    std::cout << "  Max RSS: " << max_rss_kb / 1024 << " MB\n";
    std::cout << "  Minor page faults: " << usage.ru_minflt << "\n";
    std::cout << "  Voluntary context switches: " << usage.ru_nvcsw << "\n";
    std::cout << "  Involuntary context switches: " << usage.ru_nivcsw << "\n";
}

// ============================================================================
// 主函数
// ============================================================================
int main() {
    std::cout << "\n";
    std::cout << "########################################\n";
    std::cout << "#    MCoroRpc Performance Benchmark    #\n";
    std::cout << "########################################\n";
    std::cout << "\n";
    std::cout << "C++20 Coroutine-based RPC Framework\n";
    std::cout << "System: Linux\n\n";
    
    // 运行所有测试
    testCoroutineScheduling();
    testTCPThroughput();
    testLatency();
    testConcurrentConnections();
    testMemoryUsage();
    
    std::cout << "\n";
    std::cout << "########################################\n";
    std::cout << "#         BENCHMARK COMPLETE          #\n";
    std::cout << "########################################\n\n";
    
    return 0;
}
