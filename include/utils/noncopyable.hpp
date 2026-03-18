/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-14 18:30:56
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 20:26:15
 * @FilePath: /MCoroRpc/include/utils/noncopyable.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
namespace Coro {
    class Noncopyable{
        protected:
        Noncopyable()=default;
        ~Noncopyable()=default;

        public:
        Noncopyable& operator=(const Noncopyable&) =delete;
        Noncopyable& operator=(Noncopyable&&)=default;
        Noncopyable (const Noncopyable&)=delete;
        Noncopyable(Noncopyable&&)=default;
    };
}