/*
 * @file rpc_benchmark_client.cc
 * @brief RPC QPS 性能测试客户端 - 基于 echo 测试
 */

#include "../include/coro.hpp"
#include <atomic>
#include <chrono>
#include <vector>
#include <iostream>
#include <iomanip>
#include <numeric>
#include <algorithm>

using namespace std::chrono;

std::atomic<uint64_t> g_total_requests{0};
std::atomic<uint64_t> g_success_requests{0};
std::atomic<uint64_t> g_failed_requests{0};
std::atomic<uint64_t> g_total_latency_ns{0};
std::vector<uint64_t> g_latencies;
std::mutex g_latency_mutex;

Coro::Task<void> clientWorker(const std::string& host, int port, int count) {
    try {
        auto stream = co_await Coro::net::connect(host, port);
        std::vector<char> ping(64, 'P');
        
        for (int i = 0; i < count; ++i) {
            auto start = steady_clock::now();
            
            co_await stream.write(ping);
            g_total_requests++;
            
            auto pong = co_await stream.read(64);
            
            auto end = steady_clock::now();
            auto latency = duration_cast<nanoseconds>(end - start).count();
            g_total_latency_ns += latency;
            
            if (pong.empty()) {
                g_failed_requests++;
                break;
            }
            g_success_requests++;
            
            {
                std::lock_guard<std::mutex> lock(g_latency_mutex);
                if (g_latencies.size() < 100000) {
                    g_latencies.push_back(latency);
                }
            }
        }
        
        stream.close();
    } catch (...) {
        g_failed_requests++;
    }
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 8081;
    int connections = 1;
    int requests = 1000;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" && i + 1 < argc) host = argv[++i];
        else if (arg == "-p" && i + 1 < argc) port = std::stoi(argv[++i]);
        else if (arg == "-c" && i + 1 < argc) connections = std::stoi(argv[++i]);
        else if (arg == "-r" && i + 1 < argc) requests = std::stoi(argv[++i]);
    }
    
    std::cout << "\n";
    std::cout << "########################################\n";
    std::cout << "#       RPC QPS Benchmark Client      #\n";
    std::cout << "########################################\n";
    std::cout << "\n";
    std::cout << "Server: " << host << ":" << port << "\n";
    std::cout << "Connections: " << connections << "\n";
    std::cout << "Requests per connection: " << requests << "\n\n";
    
    g_total_requests = 0;
    g_success_requests = 0;
    g_failed_requests = 0;
    g_total_latency_ns = 0;
    g_latencies.clear();
    
    std::vector<Coro::Task<>> tasks;
    
    int requests_per_conn = requests / connections;
    for (int i = 0; i < connections; ++i) {
        tasks.push_back(clientWorker(host, port, requests_per_conn));
    }
    
    for (auto& t : tasks) {
        t.schedule();
    }
    
    auto start = steady_clock::now();
    Coro::get_event_loop().run_until_complete();
    auto end = steady_clock::now();
    
    auto duration_ms = duration_cast<milliseconds>(end - start).count();
    auto duration_s = duration_ms / 1000.0;
    
    std::cout << "========== RESULTS ==========\n";
    std::cout << "Duration: " << std::fixed << std::setprecision(2) << duration_s << "s\n";
    std::cout << "Total requests: " << g_total_requests.load() << "\n";
    std::cout << "Success: " << g_success_requests.load() << "\n";
    std::cout << "Failed: " << g_failed_requests.load() << "\n";
    std::cout << "QPS: " << std::fixed << std::setprecision(0) << g_total_requests.load() / duration_s << "\n";
    
    if (g_success_requests.load() > 0) {
        double avg_latency = g_total_latency_ns.load() / static_cast<double>(g_success_requests.load()) / 1000.0;
        std::cout << "Avg latency: " << std::fixed << std::setprecision(2) << avg_latency << " us\n";
    }
    
    if (!g_latencies.empty()) {
        std::sort(g_latencies.begin(), g_latencies.end());
        size_t n = g_latencies.size();
        std::cout << "\nLatency Distribution:\n";
        std::cout << "  Min: " << g_latencies[0] / 1000.0 << " us\n";
        std::cout << "  P50: " << g_latencies[n * 0.50] / 1000.0 << " us\n";
        std::cout << "  P90: " << g_latencies[n * 0.90] / 1000.0 << " us\n";
        std::cout << "  P99: " << g_latencies[n * 0.99] / 1000.0 << " us\n";
        std::cout << "  Max: " << g_latencies.back() / 1000.0 << " us\n";
    }
    
    std::cout << "================================\n\n";
    
    return 0;
}
