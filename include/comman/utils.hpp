/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 16:18:51
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-15 16:46:28
 * @FilePath: /MCoroRpc/include/comman/utils.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include <source_location>
#include <spdlog/fmt/bundled/base.h>
#include <spdlog/spdlog.h>

namespace Coro {
    namespace utils {
        inline void print_location(std::source_location& loc,int depth){
            if(depth==0){
               fmt::println("traceback below");
            }

            fmt::println(
                "[{}] {} at {}:{}",
                depth,
                loc.function_name(),
                loc.file_name(),
                loc.line()
            );
        }
    }
}