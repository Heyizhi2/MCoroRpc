/**
 * @file singleton.hpp
 * @brief 单例模式模板
 * 
 * 线程安全的单例模式实现模板。
 * 使用C++11静态局部变量特性，确保线程安全。
 */

#pragma once
#include <type_traits>

namespace Coro {
    /**
     * @brief 单例模板类
     * @tparam T 要创建单例的类型
     * 
     * 使用方法：
     * @code
     * class MyClass : public Singleton<MyClass> {
     *     friend class Singleton<MyClass>;
     * private:
     *     MyClass() = default;
     * public:
     *     void doSomething() {}
     * };
     * 
     * // 获取实例
     * MyClass::GetInst().doSomething();
     * @endcode
     */
    template <typename T>
    class Singleton {
    protected:
        /** @brief 构造函数（protected供派生类调用） */
        Singleton() = default;
        
        /** @brief 析构函数 */
        ~Singleton() = default;

    public:
        /** @brief 禁用拷贝构造 */
        Singleton(const Singleton&) = delete;
        
        /** @brief 禁用拷贝赋值 */
        Singleton& operator=(const Singleton&) = delete;

        /**
         * @brief 获取单例实例
         * @return T& 单例引用
         * 
         * 使用Magic Static特性（C++11线程安全）：
         * 局部静态变量初始化时，编译器会自动添加同步机制
         */
        static T& GetInst() noexcept(std::is_nothrow_default_constructible_v<T>) {
            static T instance;
            return instance;
        }
    };
}
