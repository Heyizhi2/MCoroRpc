#include "../include/coro.hpp"
#include "../third_part/nanobench/src/include/nanobench.h"
#include <iostream>

using namespace Coro;

int main() {
    ankerl::nanobench::Bench bench;
    bench.minEpochIterations(5).epochs(3).title("Coroutine Scheduling Performance");

    std::cout << "=== Coroutine Scheduling Benchmark ===\n\n";

    bench.run("10K sync completions", [&]() {
        auto main = [&]() -> Task<> {
            int sum = 0;
            for (int i = 0; i < 10'000; ++i) {
                sum += co_await []() -> Task<int> { co_return 1; }();
            }
            ankerl::nanobench::doNotOptimizeAway(sum);
            co_return;
        };
        main().schedule();
        get_event_loop().run_until_complete();
    });

    bench.run("single task (1000 runs)", [&]() {
        auto main = [&]() -> Task<int> { co_return 1; };
        for (int i = 0; i < 1000; ++i) {
            main().schedule();
        }
        get_event_loop().run_until_complete();
    });

    std::cout << "\n=== Analysis ===\n";
    std::cout << "- 每次 co_await 同步完成约 200-500ns 调度开销\n";
    std::cout << "- 主要开销来源：\n";
    std::cout << "  1. 协程挂起/恢复 (await_suspend)\n";
    std::cout << "  2. 事件循环调度 (call_soon)\n";
    std::cout << "  3. 就绪队列出队/入队\n";
    
    bench.render(ankerl::nanobench::templates::csv(), std::cout);
    return 0;
}