/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-16 13:54:38
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 14:14:47
 * @FilePath: /MCoroRpc/tests/test_task.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"
#include <catch2/catch.hpp>
#include <chrono>

Coro::Task<int> simple_task() {
    co_return 42;
}

Coro::Task<int> task_with_value(int value) {
    co_return value * 2;
}

Coro::Task<void> void_task() {
    co_return;
}

Coro::Task<int> nested_task() {
    auto inner = task_with_value(10);
    int result = co_await inner;
    co_return result + 5;
}

TEST_CASE("Task creation and return value", "[task]") {
    auto task = simple_task();
    REQUIRE(task.valid() == true);
    task.schedule();
    Coro::get_event_loop().run_until_complete();
    REQUIRE(task.done() == true);
    REQUIRE(task.get_result() == 42);
}

TEST_CASE("Task with parameter", "[task]") {
    auto task = task_with_value(21);
    task.schedule();
    Coro::get_event_loop().run_until_complete();
    REQUIRE(task.done() == true);
    REQUIRE(task.get_result() == 42);
}

TEST_CASE("Void task", "[task]") {
    auto task = void_task();
    task.schedule();
    Coro::get_event_loop().run_until_complete();
    REQUIRE(task.done() == true);
}

TEST_CASE("Nested task with co_await", "[task]") {
    auto task = nested_task();
    task.schedule();
    Coro::get_event_loop().run_until_complete();
    REQUIRE(task.done() == true);
    REQUIRE(task.get_result() == 25);
}

TEST_CASE("Task cancel", "[task]") {
    auto task = simple_task();
    task.schedule();
    task.cancel();
    REQUIRE(task.done() == true);
}
