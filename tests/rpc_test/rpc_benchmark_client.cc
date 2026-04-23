/**
 * @file rpc_benchmark_client.cc
 * @brief RPC 性能测试客户端
 * @details 发起并发 RPC 请求进行性能测试
 * 
 * 使用方式:
 *   ./rpc_benchmark_client [服务器地址] [总请求数] [并发客户端数] [线程数]
 * 
 * 示例:
 *   ./rpc_benchmark_client                           # 默认
 *   ./rpc_benchmark_client 127.0.0.1:8001           # 自定义服务器
 *   ./rpc_benchmark_client 127.0.0.1:8001 2000 20   # 2000请求, 20并发
 *   ./rpc_benchmark_client 127.0.0.1:8001 4000 100 16 # 100并发, 16线程
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_channel.hpp"
#include "calc.pb.h"
#include <atomic>
#include <chrono>
#include <vector>
#include <iostream>
#include <iomanip>
#include <thread>
#include <cstring>
#include <cstdlib>

using namespace std::chrono;

struct BenchmarkConfig {
    std::string serverAddr = "127.0.0.1:8001";
    int totalRequests = 4000;
    int concurrentClients = 50;
    int threadCount = 8;
    int requestsPerClient;
    
    BenchmarkConfig() {
        requestsPerClient = totalRequests / concurrentClients;
    }
};

struct BenchmarkResult {
    double totalTimeMs;
    int successCount;
    int failCount;
    double avgLatencyUs;
    double throughputRps;
};

std::atomic<int> g_success_count{0};
std::atomic<int> g_fail_count{0};
std::atomic<int> g_clients_done{0};

void print_usage(const char* prog) {
    printf("Usage: %s [服务器地址] [总请求数] [并发数] [线程数]\n", prog);
    printf("  服务器地址: 格式 host:port (默认: 127.0.0.1:8001)\n");
    printf("  总请求数:   发起请求总数 (默认: 4000)\n");
    printf("  并发数:     并发客户端数量 (默认: 50)\n");
    printf("  线程数:     线程池大小 (默认: 8)\n");
    printf("\n示例:\n");
    printf("  %s                              # 默认配置\n", prog);
    printf("  %s 127.0.0.1:8002                # 自定义服务器\n", prog);
    printf("  %s 127.0.0.1:8001 2000 20      # 2000请求, 20并发\n", prog);
    printf("  %s 127.0.0.1:8001 4000 100 16   # 100并发, 16线程\n", prog);
}

BenchmarkConfig parse_args(int argc, char* argv[]) {
    BenchmarkConfig config;
    
    if (argc >= 2) {
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            print_usage(argv[0]);
            exit(0);
        }
        config.serverAddr = argv[1];
    }
    if (argc >= 3) {
        config.totalRequests = atoi(argv[2]);
    }
    if (argc >= 4) {
        config.concurrentClients = atoi(argv[3]);
    }
    if (argc >= 5) {
        config.threadCount = atoi(argv[4]);
    }
    
    config.requestsPerClient = config.totalRequests / config.concurrentClients;
    return config;
}

void print_config(const BenchmarkConfig& config) {
    printf("========================================\n");
    printf("       Benchmark Configuration        \n");
    printf("========================================\n");
    printf("  Server Address:   %s\n", config.serverAddr.c_str());
    printf("  Total Requests:   %d\n", config.totalRequests);
    printf("  Concurrent:       %d\n", config.concurrentClients);
    printf("  Thread Count:     %d\n", config.threadCount);
    printf("  Requests/Client: %d\n", config.requestsPerClient);
    printf("========================================\n\n");
}

void print_result(const BenchmarkResult& result) {
    printf("\n");
    printf("========================================\n");
    printf("             RESULTS                   \n");
    printf("========================================\n");
    printf("  Total Time:       %.2f ms\n", result.totalTimeMs);
    printf("  Success Count:   %d\n", result.successCount);
    printf("  Fail Count:       %d\n", result.failCount);
    printf("  Avg Latency:      %.2f us\n", result.avgLatencyUs);
    printf("  Throughput:      %.0f req/s\n", result.throughputRps);
    printf("\n");
    printf("########################################\n");
    printf("#       BENCHMARK COMPLETE             #\n");
    printf("########################################\n\n");
}

Coro::Task<void> runClientRequests(int client_id, std::shared_ptr<Coro::RpcChannel> channel, int requests_per_client) {
    auto* serviceDesc = testrpc::Calculator::descriptor();
    auto* methodDesc = serviceDesc->method(0);
    
    if (!channel || !channel->isConnected()) {
        g_fail_count.fetch_add(requests_per_client);
        co_return;
    }
    
    for (int j = 0; j < requests_per_client; ++j) {
        testrpc::AddRequest request;
        request.set_a(client_id);
        request.set_b(j);
        
        testrpc::AddResponse response;
        auto controller = std::make_shared<Coro::RpcController>();
        controller->SetTimeout(30000);
        
        try {
            co_await channel->CallMethodAsync(methodDesc, controller.get(), &request, &response, nullptr);
            if (!controller->Failed()) {
                g_success_count.fetch_add(1);
            } else {
                g_fail_count.fetch_add(1);
            }
        } catch (...) {
            g_fail_count.fetch_add(1);
        }
    }
    
    co_return;
}

Coro::Task<void> runSingleClient(int client_id, const std::string& host, int port, int requests_per_client) {
    auto channel = std::make_shared<Coro::RpcChannel>(host, port);
    channel->setTimeout(30000);
    
    try {
        co_await channel->connect();
        co_await runClientRequests(client_id, channel, requests_per_client);
        channel->close();
    } catch (...) {
        g_fail_count.fetch_add(requests_per_client);
    }
    
    g_clients_done.fetch_add(1);
    co_return;
}

void runClientsOnThread(int threadId, int clientStart, int clientCount, 
                     const std::string& host, int port, int requests_per_client) {
    std::vector<Coro::Task<>> clientTasks;
    for (int i = 0; i < clientCount; ++i) {
        auto task = runSingleClient(clientStart + i, host, port, requests_per_client);
        task.schedule();
    }
    
    Coro::get_event_loop().run_until_complete();
}

BenchmarkResult runBenchmark(const BenchmarkConfig& config) {
    std::string host;
    int port;
    
    auto colonPos = config.serverAddr.find(':');
    if (colonPos != std::string::npos) {
        host = config.serverAddr.substr(0, colonPos);
        port = std::stoi(config.serverAddr.substr(colonPos + 1));
    } else {
        host = config.serverAddr;
        port = 8001;
    }
    
    print_config(config);
    
    printf("Starting benchmark...\n");
    fflush(stdout);
    
    Coro::EventloopPool::instance().init(config.threadCount);
    
    int clientsPerThread = config.concurrentClients / config.threadCount;
    
    std::vector<std::thread> threads;
    for (int t = 0; t < config.threadCount; ++t) {
        int start = t * clientsPerThread;
        int count = (t == config.threadCount - 1) 
                  ? (config.concurrentClients - start) 
                  : clientsPerThread;
        threads.emplace_back(runClientsOnThread, t, start, count, host, port, config.requestsPerClient);
    }
    
    auto start = steady_clock::now();
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = steady_clock::now();
    auto duration = duration_cast<nanoseconds>(end - start).count();
    double total_time_ms = duration / 1000000.0;
    
    int total = g_success_count.load() + g_fail_count.load();
    double avg_latency_us = total > 0 ? (duration / 1000.0 / total) : 0;
    double throughput_rps = total > 0 ? (total * 1000.0 / total_time_ms) : 0;
    
    BenchmarkResult result;
    result.totalTimeMs = total_time_ms;
    result.successCount = g_success_count.load();
    result.failCount = g_fail_count.load();
    result.avgLatencyUs = avg_latency_us;
    result.throughputRps = throughput_rps;
    
    return result;
}

int main(int argc, char* argv[]) {
    auto config = parse_args(argc, argv);
    
    auto result = runBenchmark(config);
    
    print_result(result);
    
    return 0;
}