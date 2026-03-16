#pragma once
namespace Coro {
    class Noncopyable{
        protected:
        Noncopyable()=default;
        ~Noncopyable()=default;

        public:
        Noncopyable& operator=(const Noncopyable&) =delete;
        Noncopyable& operator=(Noncopyable&&)=delete;
        Noncopyable (const Noncopyable&)=delete;
        Noncopyable(Noncopyable&&)=delete;
    };
}