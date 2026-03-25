/**
 * @file noncopyable.hpp
 * @brief 不可拷贝基类
 * 
 * 继承此类将禁用拷贝构造和拷贝赋值。
 * 用于那些不能或不应该被拷贝的对象。
 */

#pragma once

namespace Coro {
    /**
     * @brief 禁止拷贝的基类
     * @details 公有继承后，派生类将不可拷贝
     * 
     * 使用示例：
     * @code
     * class MyClass : private Noncopyable {
     *     // 禁止拷贝
     * };
     * @endcode
     */
    class Noncopyable{
        protected:
        /** @brief 默认构造函数 */
        Noncopyable()=default;
        
        /** @brief 析构函数 */
        ~Noncopyable()=default;

        public:
        /** @brief 禁用拷贝赋值 */
        Noncopyable& operator=(const Noncopyable&) =delete;
        
        /** @brief 移动赋值（默认启用） */
        Noncopyable& operator=(Noncopyable&&)=default;
        
        /** @brief 禁用拷贝构造 */
        Noncopyable (const Noncopyable&)=delete;
        
        /** @brief 移动构造（默认启用） */
        Noncopyable(Noncopyable&&)=default;
    };
}
