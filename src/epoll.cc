/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-16 20:39:34
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-17 22:35:24
 * @FilePath: /MCoroRpc/src/epoll.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */

#include "../include/coro.hpp"
#include <cstdlib>
#include <spdlog/spdlog.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <utility>
namespace Coro{
    /**
     * @brief 构造函数
     * @details 创建 epoll 文件描述符
     */
   Epoll::Epoll(){
        m_epoll_fd=epoll_create1(0);
        if(m_epoll_fd==-1){
            std::abort();
        }
   }

    /**
     * @brief 析构函数
     * @details 关闭 epoll 文件描述符
     */
   Epoll::~Epoll(){
        if (m_epoll_fd!=-1) {
            close(m_epoll_fd);
        }
   }

    /**
     * @brief 添加读事件监听
     * @param fd 文件描述符
     * @param wait_id 等待ID，用于标识和取消
     * @return 是否添加成功
     */
    bool Epoll::add_reader(int fd,uint64_t wait_id){
        auto &ev=m_event_map[fd];
        if(ev.reader!=0){
            m_wait_to_fd.erase(ev.reader);
        }
        int op=(ev.event.events==0)?EPOLL_CTL_ADD:EPOLL_CTL_MOD;
        ev.event.events|=EPOLLIN|EPOLLET;
        ev.event.data.ptr=&ev;
        ev.reader=wait_id;
        m_wait_to_fd[wait_id]=fd;
        if(epoll_ctl(m_epoll_fd, op,fd, &ev.event)==-1){
            m_event_map.erase(fd);
            m_wait_to_fd.erase(wait_id);
            return false;
        }
        return true;
    }

    /**
     * @brief 添加写事件监听
     * @param fd 文件描述符
     * @param wait_id 等待ID，用于标识和取消
     * @return 是否添加成功
     */
    bool Epoll::add_writer(int fd,uint64_t wait_id){
        auto &ev=m_event_map[fd];
        if(ev.writer!=0){
            m_wait_to_fd.erase(ev.writer);
        }
        int op=(ev.event.events==0)?EPOLL_CTL_ADD:EPOLL_CTL_MOD;
        ev.event.events|=EPOLLOUT|EPOLLET;
        ev.event.data.ptr=&ev;
        ev.writer=wait_id;
        m_wait_to_fd[wait_id]=fd;
        if(epoll_ctl(m_epoll_fd, op,fd, &ev.event)==-1){
            m_event_map.erase(fd);
            m_wait_to_fd.erase(wait_id);
            return false;
        }
        return true;
    }

    /**
     * @brief 取消等待
     * @param wait_id 要取消的等待ID
     * @details 从 epoll 中移除对应的事件监听
     */
   void Epoll::cancel_wait(uint64_t wait_id) {
        auto it = m_wait_to_fd.find(wait_id);
        if (it == m_wait_to_fd.end()) return;
        int fd = it->second;

        auto& ev = m_event_map[fd];
        bool modified = false;

        if (ev.reader == wait_id) {
            ev.reader = 0;
            ev.event.events &= ~EPOLLIN;
            modified = true;
        }
        if (ev.writer == wait_id) {
            ev.writer = 0;
            ev.event.events &= ~EPOLLOUT;
            modified = true;
        }

        if (!modified) {
            m_wait_to_fd.erase(it);
            return;
        }

        // 检查是否还有任何事件类型（忽略 EPOLLET 等标志）
        if ((ev.event.events & (EPOLLIN | EPOLLOUT)) == 0) {
            // 无任何事件，从 epoll 删除 fd
            m_event_map.erase(fd);
            epoll_ctl(m_epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        } else {
            // 仍有事件，更新事件掩码
            epoll_ctl(m_epoll_fd, EPOLL_CTL_MOD, fd, &ev.event);
        }

        m_wait_to_fd.erase(it);
    }

   
}