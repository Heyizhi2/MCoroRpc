/**
 * @file utils.hpp
 * @brief 调试工具函数
 * 
 * 本文件提供协程调试相关的工具函数，
 * 主要用于输出调用栈跟踪信息。
 */

#pragma once
#include <source_location>
#include <spdlog/fmt/bundled/base.h>
#include <spdlog/spdlog.h>

namespace Coro {
    /**
     * @brief 调试工具命名空间
     */
    namespace utils {
        /**
         * @brief 打印源代码位置信息
         * @details 用于调试时的调用栈打印，显示函数名、文件名和行号
         * 
         * @param loc 源代码位置信息（通过std::source_location::current()获取）
         * @param depth 调用栈深度（用于缩进显示）
         * 
         * 输出格式：[depth] function_name at filename:line
         */
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
