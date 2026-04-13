/**
 * @file rpc_concurrency_benchmark.cc
 * @brief 高并发 RPC 框架吞吐量测试
 * 
 * 使用方式:
 * 1. 先启动 server: ./rpc_concurrency_benchmark --server
 * 2. 后启动 client: ./rpc_concurrency_benchmark --client
 */

#include "../../include/coro.hpp"
#include "../../include/rpc/rpc_server.hpp"
#include "../../include/rpc/rpc_client.hpp"
#include "calc.pb.h"
#include "calc_service.h"
#include <atomic>
#include <chrono>
#include <vector>
#include <iostream>
#include <iomanip>
#include <thread>
#include <cstring>

using namespace std::chrono;

constexpr int TOTAL_REQUESTS = 40;
constexpr int CONCURRENT_CLIENTS = 40;
constexpr int REQUESTS_PER_CLIENT = TOTAL_REQUESTS / CONCURRENT_CLIENTS;
constexpr int SERVER_PORT = 8001;
constexpr int THREAD_COUNT = 8;

std::atomic<int> g_success_count{0};
std::atomic<int> g_fail_count{0};
std::atomic<int> g_clients_done{0};

Coro::Task<void> runClientRequests(int client_id, std::shared_ptr<Coro::RpcClient> client) {
    auto* serviceDesc = testrpc::Calculator::descriptor();
    auto* methodDesc = serviceDesc->method(0);
    
    auto channel = client->getChannel();
    if (!channel) {
        g_fail_count.fetch_add(REQUESTS_PER_CLIENT);
        co_return;
    }
    
    for (int j = 0; j < REQUESTS_PER_CLIENT; ++j) {
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

Coro::Task<void> runSingleClient(int client_id) {
    Coro::RpcClientOptions options;
    options.zkHost = "";
    options.timeoutMs = 30000;
    
    auto client = std::make_shared<Coro::RpcClient>(options);
    
    co_await client->connect("127.0.0.1", SERVER_PORT);
    
    co_await runClientRequests(client_id, client);
    
    client->disconnect();
    g_clients_done.fetch_add(1);
    
    co_return;
}

void runClientOnThread(int threadId, int clientStart, int clientCount) {
    std::vector<Coro::Task<>> clientTasks;
    for (int i = 0; i < clientCount; ++i) {
        auto task = runSingleClient(clientStart + i);
        task.schedule();
    }
    
    Coro::get_event_loop().run_until_complete();
}

void runServer() {
    std::cout << "Starting RPC server on port " << SERVER_PORT << "...\n";
    fflush(stdout);
    
    Coro::RpcServer server(SERVER_PORT, "");
    server.setWorkerCount(8);
    
    CalculatorServiceImpl calcService;
    server.registerService(&calcService);
    
    auto serverTask = [&server]() -> Coro::Task<void> {
        co_await server.start();
    };
    serverTask().schedule();
    
    Coro::get_event_loop().run_until_complete();
}

void runClient() {
    std::cout << "Starting client benchmark...\n";
    std::cout << "Configuration:\n";
    std::cout << "  Total Requests:    " << TOTAL_REQUESTS << "\n";
    std::cout << "  Concurrent Clients: " << CONCURRENT_CLIENTS << "\n";
    std::cout << "  Requests/Client:  " << REQUESTS_PER_CLIENT << "\n";
    std::cout << "  Server Port:      " << SERVER_PORT << "\n";
    std::cout << "  Thread Count:     " << THREAD_COUNT << "\n";
    fflush(stdout);
    
    Coro::EventloopPool::instance().init(THREAD_COUNT);
    
    int clientsPerThread = CONCURRENT_CLIENTS / THREAD_COUNT;
    
    std::vector<std::thread> threads;
    for (int t = 0; t < THREAD_COUNT; ++t) {
        int start = t * clientsPerThread;
        int count = (t == THREAD_COUNT - 1) ? (CONCURRENT_CLIENTS - start) : clientsPerThread;
        threads.emplace_back(runClientOnThread, t, start, count);
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
    
    std::cout << "\n";
    std::cout << "========================================\n";
    std::cout << "            RESULTS                     \n";
    std::cout << "========================================\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  Total Time:        " << total_time_ms << " ms\n";
    std::cout << "  Success Count:    " << g_success_count.load() << "\n";
    std::cout << "  Fail Count:       " << g_fail_count.load() << "\n";
    std::cout << "  Avg Latency:      " << avg_latency_us << " us\n";
    std::cout << "  Throughput:       " << std::setprecision(0) << throughput_rps << " req/s\n";
    std::cout << "\n";
    std::cout << "########################################\n";
    std::cout << "#         BENCHMARK COMPLETE           #\n";
    std::cout << "########################################\n\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " --server | --client\n";
        return 1;
    }
    
    if (strcmp(argv[1], "--server") == 0) {
        runServer();
    } else if (strcmp(argv[1], "--client") == 0) {
        runClient();
    } else {
        std::cerr << "Unknown option: " << argv[1] << "\n";
        std::cerr << "Usage: " << argv[0] << " --server | --client\n";
        return 1;
    }
    
    return 0;
}