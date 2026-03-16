/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-16 20:39:34
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 21:31:39
 * @FilePath: /MCoroRpc/src/epoll.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once
#include "../include/coro.hpp"
#include <spdlog/spdlog.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <utility>
namespace Coro{
  

    void Epoll::cancel(Handle::ID id){
          auto it = m_id_to_fd.find(id);
        if (it == m_id_to_fd.end()) return;  // 未找到该 ID

        int fd = it->second;
        auto event_it = m_event_map.find(fd);
        if (event_it ==m_event_map.end()) {
            // 不一致状态，直接删除反向映射
            m_id_to_fd.erase(it);
            return;
        }

        auto& event = event_it->second;
        bool modified = false;

        if (event.reader && event.reader->m_id == id) {
            event.reader.reset();
            event.event.events &= ~EPOLLIN;
            modified = true;
        }
        if (event.writer && event.writer->m_id == id) {
            event.writer.reset();
            event.event.events &= ~EPOLLOUT;
            modified = true;
        }

        if (!modified) {
            // ID 存在但对应的等待已不在，清理映射
            m_id_to_fd.erase(it);
            return;
        }

        // 根据剩余事件更新 epoll
        if (event.event.events == 0) {
            // 无事件，移除 fd
            m_event_map.erase(event_it);
            if (epoll_ctl(this->fd(), EPOLL_CTL_DEL, fd, nullptr) == -1) {
               
            }
            m_id_to_fd.erase(id);  // 删除当前 ID，注意可能还有其他 ID 指向同一 fd？但这里我们已经删除了该 ID，且 fd 已移除，所以其他 ID 也会在后续取消时找不到。
        } else {
            // 仍有事件，更新
            if (epoll_ctl(this->fd(), EPOLL_CTL_MOD, fd, &event.event) == -1) {
              
            }
            // 当前 ID 已从 event 中移除，所以从 _id_to_fd 删除该 ID
            m_id_to_fd.erase(id);
        }
        SPDLOG_DEBUG("successfully cancel ID {}", id);
    }
        
    void Epoll::clear(){
        if(auto fd=std::exchange(m_epoll_fd,-1);fd!=-1){
            close(fd);
        }
        m_event_map.clear();
        m_id_to_fd.clear();
    }

    void Epoll::add_reader(int fd,Handle& handle,Callback cb){
        Event* event;
        int op;
        if(m_event_map.contains(fd)){
           event=&m_event_map[fd];
           event->event.events|=EPOLLIN;
           op=EPOLL_CTL_MOD;
        }

        else {
            event=&(m_event_map[fd]={});
            event->event.data.ptr=event;
            event->event.events|=EPOLLIN|EPOLLET;
            op=EPOLL_CTL_ADD;
        }
        
        if(event->reader.has_value()){
            
        }
        event->reader=EventloopHandle{handle.id(),std::move(cb)};

        m_id_to_fd[handle.id()]=fd;
        if(epoll_ctl(this->fd(),op,fd, &event->event)==-1){
            
        }
        SPDLOG_DEBUG("successfully add reader for fd {}", fd);

    }
    void Epoll::add_writer(int fd,Handle& handle,Callback cb){
        Event* event;
        int op;
        if(m_event_map.contains(fd)){
           event=&m_event_map[fd];
           event->event.events|=EPOLLOUT;
           op=EPOLL_CTL_MOD;
        }

        else {
            event=&(m_event_map[fd]={});
            event->event.data.ptr=event;
            event->event.events|=EPOLLOUT|EPOLLET;
            op=EPOLL_CTL_ADD;
        }
        
        if(event->writer.has_value()){
            
        }
        event->writer=EventloopHandle{handle.id(),std::move(cb)};

        m_id_to_fd[handle.id()]=fd;
        if(epoll_ctl(this->fd(),op,fd, &event->event)==-1){
            
        }
        SPDLOG_DEBUG("successfully add write for fd {}", fd);
    }

    void Epoll::remove_reader(int fd){
        auto it=m_event_map.find(fd);
        if(it==m_event_map.end()){
            return;
        }
        auto& ev=it->second;
        if(!ev.reader){
            return;
        }

        auto id=ev.reader->m_id;
        ev.reader.reset();
        ev.event.events&=~EPOLLIN;

        if(ev.writer){
             if(epoll_ctl(this->fd(),EPOLL_CTL_MOD,fd, &ev.event)==-1){
                
            }
            m_id_to_fd.erase(id);
        }
        else {
            m_event_map.erase(id);
            if(epoll_ctl(this->fd(),EPOLL_CTL_DEL,fd,nullptr)==-1){
                
            }
            m_id_to_fd.erase(id);
        }
    }
    void Epoll::remove_writer(int fd){
        auto it=m_event_map.find(fd);
        if(it==m_event_map.end()){
            return;
        }
        auto& ev=it->second;
        if(!ev.writer){
            return;
        }

        auto id=ev.reader->m_id;
        ev.reader.reset();
        ev.event.events&=~EPOLLOUT;

        if(ev.reader){
             if(epoll_ctl(this->fd(),EPOLL_CTL_MOD,fd, &ev.event)==-1){
                
            }
            m_id_to_fd.erase(id);
        }
        else {
            m_event_map.erase(id);
            if(epoll_ctl(this->fd(),EPOLL_CTL_DEL,fd,nullptr)==-1){
                
            }
            m_id_to_fd.erase(id);
        }
    }

    void Epoll::clear_fd(int fd){
        auto it =m_event_map.find(fd);
        if (it == m_event_map.end()) return;

        // 清除反向映射中与该 fd 关联的所有 ID
        if (it->second.reader) m_id_to_fd.erase(it->second.reader->m_id);
        if (it->second.writer) m_id_to_fd.erase(it->second.writer->m_id);

        m_event_map.erase(it);
        if (epoll_ctl(this->fd(), EPOLL_CTL_DEL, fd, nullptr) == -1) {
        }
    }

    int Epoll::fd(){
        if(m_epoll_fd==-1){
            m_epoll_fd=epoll_create1(0);
            SPDLOG_DEBUG("successfully ceate epoll fd{} in Epoll",m_epoll_fd);
        }
        return m_epoll_fd;
        
    }

    Epoll::~Epoll()noexcept{
        clear();
    }
}