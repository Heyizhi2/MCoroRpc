/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-16 13:54:38
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 14:14:47
 * @FilePath: /MCoroRpc/tests/test_sleep.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <catch2/catch.hpp>
#include <chrono>

TEST_CASE("Sleep for short duration", "[sleep]") {
    auto start = std::chrono::steady_clock::now();
    
    auto task = []() -> Coro::Task<void> {
        co_await Coro::sleep_for(std::chrono::milliseconds(50));
    }();
    
    task.schedule();
    Coro::get_event_loop().run_until_complete();
    
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    REQUIRE(elapsed.count() >= 40);
    REQUIRE(task.done() == true);
}

TEST_CASE("Sleep returns correct value", "[sleep]") {
    auto task = []() -> Coro::Task<int> {
        co_await Coro::sleep_for(std::chrono::milliseconds(10));
        co_return 100;
    }();
    
    task.schedule();
    Coro::get_event_loop().run_until_complete();
    
    REQUIRE(task.done() == true);
    REQUIRE(task.get_result() == 100);
}

TEST_CASE("Multiple sleep tasks", "[sleep]") {
    auto task1 = []() -> Coro::Task<int> {
        co_await Coro::sleep_for(std::chrono::milliseconds(30));
        co_return 1;
    }();
    
    auto task2 = []() -> Coro::Task<int> {
        co_await Coro::sleep_for(std::chrono::milliseconds(10));
        co_return 2;
    }();
    
    task1.schedule();
    task2.schedule();
    
    auto start = std::chrono::steady_clock::now();
    Coro::get_event_loop().run_until_complete();
    auto end = std::chrono::steady_clock::now();
    
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    REQUIRE(task1.done() == true);
    REQUIRE(task2.done() == true);
    REQUIRE(task1.get_result() == 1);
    REQUIRE(task2.get_result() == 2);
    REQUIRE(elapsed.count() >= 25);
}
