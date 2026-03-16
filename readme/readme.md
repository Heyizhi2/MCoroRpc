<!--
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-15 10:39:56
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-16 10:49:03
 * @FilePath: /MCoroRpc/readme/readme.md
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
-->
### 设计思路
#### 仿造python的asyncio库的单线程事件循环模式，实现一个基于cpp20的异步I/O模型作为rpc的网络框架，同时通过协程对rpc的请求进行管理，协议基于grpc的protobuf,网络底层基于llbc库

##### 协程的实现
####### 一、协程的返回类型，通用的Task模板
        - task用来封装任何返回结果的


依赖图
Handle / HandleId (基础)
    │
    ▼
CoroHandle
    │
    ▼
Task<_Tp> (实现 SchedulableTask 概念)
    │
    ├──┐
    │  ▼
    │ Selector (I/O 多路复用)
    │
    ▼
EventLoop (事件循环)
    │
    ▼
ScheduledTask (调度封装)

协程调度关系图
[父协程] ──co_await child_task──► [父协程 SUSPENDED]
                                        │
                                        │ continuation_ 保存
                                        ▼
                                   [子协程 SCHEDULED]
                                        │
                                        │ 子协程执行
                                        ▼
                                   [子协程 FINAL_SUSPEND]
                                        │
                    call_soon(*continuation_)
                                        │
                                        ▼
[父协程 RESUMED] ◄────────────────── [EventLoop ready_]

