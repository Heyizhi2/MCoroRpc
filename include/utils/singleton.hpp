#pragma once
#include <type_traits>
namespace Coro {
    template <typename T>
class Singleton {
protected:
    Singleton() = default;
    ~Singleton() = default;

public:
    Singleton(const Singleton&) = delete;
    Singleton& operator=(const Singleton&) = delete;

    static T& GetInst() noexcept(std::is_nothrow_default_constructible_v<T>) {
        static T instance;
        return instance;
    }
};
}