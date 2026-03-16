/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-16 11:26:24
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 14:07:32
 * @FilePath: /MCoroRpc/include/comman/exception.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <exception>
namespace Coro {
    struct ExceptionInvalidFuture:public std::exception{
        [[nodiscard]] const char* what()const noexcept{
            return  "[Exception] Invalid future";
        } 
    };
}