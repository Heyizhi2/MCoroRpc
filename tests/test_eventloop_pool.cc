#include "../include/coro.hpp"
#include <iostream>
#include <atomic>
#include <vector>
#include <thread>

std::atomic<int> g_counter{0};
std::vector<int> g_thread_ids;

Coro::Task<void> incrementTask(int id) {
    g_counter.fetch_add(1);
    g_thread_ids.push_back(Coro::get_event_loop().getThreadId());
    co_return;
}

int main() {
    std::cout << "=== EventloopPool Test ===\n\n";
    
    const int THREAD_COUNT = 4;
    const int TASKS_PER_LOOP = 100;
    
    std::cout << "1. Initializing EventloopPool with " << THREAD_COUNT << " threads...\n";
    Coro::EventloopPool::instance().init(THREAD_COUNT);
    
    std::cout << "2. Starting all event loops...\n";
    Coro::EventloopPool::instance().startAll();
    
    std::cout << "3. Scheduling " << (THREAD_COUNT * TASKS_PER_LOOP) << " tasks...\n";
    
    for (int i = 0; i < THREAD_COUNT * TASKS_PER_LOOP; ++i) {
        auto task = incrementTask(i);
        auto& loop = Coro::EventloopPool::instance().getNextLoop();
        loop.schedule(task);
    }
    
    std::cout << "4. Waiting for tasks to complete...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    std::cout << "\n=== RESULTS ===\n";
    std::cout << "Total tasks executed: " << g_counter.load() << "\n";
    std::cout << "Expected: " << (THREAD_COUNT * TASKS_PER_LOOP) << "\n";
    
    std::cout << "\nUnique thread IDs that executed tasks: ";
    std::vector<int> unique_ids;
    for (auto id : g_thread_ids) {
        bool found = false;
        for (auto u : unique_ids) {
            if (u == id) { found = true; break; }
        }
        if (!found) unique_ids.push_back(id);
    }
    for (size_t i = 0; i < unique_ids.size(); ++i) {
        std::cout << unique_ids[i] << (i < unique_ids.size() - 1 ? ", " : "");
    }
    std::cout << "\n";
    
    bool success = (g_counter.load() == THREAD_COUNT * TASKS_PER_LOOP) && (unique_ids.size() > 1);
    
    std::cout << "\n=== " << (success ? "PASS" : "FAIL") << " ===\n";
    std::cout << "Tasks executed: " << (success ? "OK" : "FAIL") << "\n";
    std::cout << "Multiple threads used: " << (unique_ids.size() > 1 ? "YES" : "NO") << "\n";
    
    Coro::EventloopPool::instance().stopAll();
    Coro::EventloopPool::instance().joinAll();
    
    return success ? 0 : 1;
}
