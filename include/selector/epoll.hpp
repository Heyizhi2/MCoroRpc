/**
 * @file epoll.hpp
 * @brief Epoll IO多路复用封装
 * 
 * 封装Linux epoll系统调用，提供：
 * - 添加/移除文件描述符监听
 * - 等待IO事件
 * - 取消等待
 * 
 * 使用边缘触发模式(EPOLLET)提高效率。
 */

#pragma once
#include <cstdint>
#include <functional>
#include <unordered_map>
#include "../comman/types.hpp"
#include <sys/epoll.h>

namespace Coro{
    /**
     * @brief 事件结构
     * @details 关联文件描述符和对应的等待ID
     */
    struct Event{
        /** @brief 文件描述符 */
        int fd;
        
        /** @brief 读事件等待ID */
        uint64_t reader=0;
        
        /** @brief 写事件等待ID */
        uint64_t writer=0;
        
        /** @brief epoll事件 */
        epoll_event event{};
    };
    
    /**
     * @brief Epoll封装类
     * @details 对Linux epoll的封装，支持边缘触发模式
     */
    struct Epoll{
        
        /**
         * @brief 构造函数
         * @details 创建epoll实例
         */
        Epoll();

        /**
         * @brief 添加读事件监听
         * @param fd 文件描述符
         * @param wait_id 等待ID
         * @return 是否成功
         */
        bool add_reader(int fd,uint64_t wait_id);

        /**
         * @brief 添加写事件监听
         * @param fd 文件描述符
         * @param wait_id 等待ID
         * @return 是否成功
         */
        bool add_writer(int fd,uint64_t wait_id);

        /**
         * @brief 取消等待
         * @param wait_id 等待ID
         */
        void cancel_wait(uint64_t wait_fd);
        
        /**
         * @brief 检查是否无监听
         */
        bool is_stop(){return  m_event_map.empty();}

        /**
         * @brief 等待IO事件
         * @param events 事件数组
         * @param event_num 最大事件数
         * @param timeout 超时时间（毫秒）
         * @return 事件数量
         */
        [[nodiscard]] inline int wait(epoll_event* events,int event_num,int timeout) noexcept{
            return epoll_wait(m_epoll_fd, events, event_num, timeout);
        }
        
        /**
         * @brief 析构函数
         */
        ~Epoll();

        
        private:
        /** @brief epoll文件描述符 */
        int m_epoll_fd{-1};
        
        /** @brief fd到事件的映射 */
        std::unordered_map<int,Event> m_event_map{};
        
        /** @brief 等待ID到fd的映射 */
        std::unordered_map<uint64_t,int> m_wait_to_fd{};
    };
}
