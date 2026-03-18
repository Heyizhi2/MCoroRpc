/*
 * 压力测试客户端 - 测试最大并发连接数 + IO吞吐
 */
#include "../include/coro.hpp"
#include <atomic>
#include <chrono>
#include <vector>
#include <iostream>

using namespace std::chrono;

struct Stats {
    std::atomic<uint64_t> connect_success{0};
    std::atomic<uint64_t> connect_fail{0};
    std::atomic<uint64_t> request_total{0};
    std::atomic<uint64_t> request_success{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> bytes_recv{0};
    std::atomic<uint64_t> latency_sum{0};
    
    void print(int duration_sec) {
        auto total = request_total.load();
        auto success = request_success.load();
        auto sent = bytes_sent.load();
        auto recv = bytes_recv.load();
        auto lat_sum = latency_sum.load();
        
        std::cout << "\n========== 压力测试结果 ==========\n";
        std::cout << "测试时长: " << duration_sec << "s\n";
        std::cout << "成功连接: " << connect_success.load() << "\n";
        std::cout << "失败连接: " << connect_fail.load() << "\n";
        std::cout << "总请求数: " << total << "\n";
        std::cout << "成功响应: " << success << "\n";
        std::cout << "发送字节: " << sent << " (" << sent/1024/1024 << " MB)\n";
        std::cout << "接收字节: " << recv << " (" << recv/1024/1024 << " MB)\n";
        std::cout << "QPS: " << total / duration_sec << "\n";
        std::cout << "带宽: " << (sent + recv) / 1024 / 1024 / duration_sec << " MB/s\n";
        if (success > 0) {
            std::cout << "平均延迟: " << lat_sum / success << " us\n";
        }
        std::cout << "==================================\n";
    }
} stats;

constexpr int kPayloadSize = 1024;  // 1KB payload

int kPort = 8080;

Coro::Task<> client_task(const std::string& host, int id) {
    auto stream = co_await Coro::net::connect(host, kPort);
    if (stream.fd() < 0) {
        stats.connect_fail++;
        co_return;
    }
    stats.connect_success++;
    
    std::vector<char> send_buf(kPayloadSize, 'A' + (id % 26));
    std::vector<char> recv_buf(kPayloadSize);
    
    while (true) {
        auto start = steady_clock::now();
        
        co_await stream.write(Coro::net::TcpStream::buffer_type(
            send_buf.begin(), send_buf.end()));
        stats.bytes_sent += kPayloadSize;
        stats.request_total++;
        
        auto data = co_await stream.read(kPayloadSize);
        if (data.empty()) {
            break;
        }
        
        auto end = steady_clock::now();
        auto latency = duration_cast<microseconds>(end - start).count();
        
        stats.bytes_recv += data.size();
        stats.request_success++;
        stats.latency_sum += latency;
    }
    
    stream.close();
}

Coro::Task<> load_test(const std::string& host, int connections, int duration_sec) {
    std::vector<Coro::Task<>> tasks;
    tasks.reserve(connections);
    
    auto start = steady_clock::now();
    
    for (int i = 0; i < connections; i++) {
        tasks.push_back(client_task(host, i));
        if (i % 100 == 0) {
            co_await Coro::sleep_for(milliseconds(10));
        }
    }
    
    for (auto& t : tasks) {
        t.schedule();
    }
    
    while (true) {
        auto elapsed = duration_cast<seconds>(steady_clock::now() - start).count();
        if (elapsed >= duration_sec) {
            break;
        }
        co_await Coro::sleep_for(milliseconds(100));
        
        auto total = stats.request_total.load();
        std::cout << "\rElapsed: " << elapsed << "s | "
                  << "Conn: " << stats.connect_success.load() << " | "
                  << "Requests: " << total << " | "
                  << "QPS: " << total / (elapsed + 1) << "   " << std::flush;
    }
    
    stats.print(duration_sec);
}

void print_usage(const char* prog) {
    std::cout << "用法: " << prog << " <host> <port> <并发连接数> <测试时长(秒)>\n";
    std::cout << "示例: " << prog << " 127.0.0.1 8080 1000 10\n";
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        print_usage(argv[0]);
        return 1;
    }
    
    std::string host = argv[1];
    kPort = std::stoi(argv[2]);
    int connections = std::stoi(argv[3]);
    int duration = std::stoi(argv[4]);
    
    std::cout << "开始压力测试\n";
    std::cout << "目标: " << host << ":" << kPort << "\n";
    std::cout << "并发: " << connections << " 连接\n";
    std::cout << "时长: " << duration << " 秒\n";
    
    load_test(host, connections, duration).schedule();
    Coro::get_event_loop().run_until_complete();
    
    return 0;
}
