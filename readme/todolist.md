<!--
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-26 15:57:16
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-26 15:57:21
 * @FilePath: /MCoroRpc/readme/todolist.md
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
-->
潜在问题与改进建议
⚠️ 1. 回调与 close 的竞态条件
问题：close() 中调用 cleanupPendingOps()，它会遍历 m_pendingOps，对每个操作发送错误并删除 PendingOp。但是，ZooKeeper 的回调可能晚于 close() 被调用（例如网络延迟），此时回调会访问已经 delete 的 PendingOp，导致未定义行为（use-after-free）。

建议：

在 PendingOp 中增加一个 std::atomic<bool> cancelled 标志，并在 close() 时设置该标志。回调中先检查该标志，若已取消则直接返回（不发送 channel）。

或者使用 shared_ptr 配合弱引用，但这样会增加复杂度。

⚠️ 2. cleanupPendingOps() 中的迭代删除
cpp
for (auto* op : m_pendingOps) {
    op->channel->send(ZkResult{ZCONNECTIONLOSS, "", ""});
    delete op;
}
发送 ZCONNECTIONLOSS 给等待的协程，这是合理的。

但 delete op 后，m_pendingOps 中的指针成为悬空指针。虽然容器随后被清空，但如果有其他线程仍在遍历该容器（例如在添加新操作时），可能引发问题。当前在锁内操作，且没有并发添加，所以安全。

⚠️ 3. Channel 关闭语义
m_connectChannel->close() 在 close() 中调用。如果 start() 协程仍在等待 recv()，close() 后 send 可能失败（取决于 Channel 实现）。目前 cleanupPendingOps 发送了 ZCONNECTIONLOSS，但 m_connectChannel 没有被发送任何结果就被关闭了，这会导致 start() 协程永远无法恢复（如果它还没有收到结果）。实际上 start() 在收到连接结果后才会退出，如果连接尚未建立就调用 close()，m_connectChannel 可能处于等待状态，关闭后 recv() 应返回一个错误状态（如通道关闭）。需要确保 Channel 的 close() 能唤醒等待者并返回一个特定错误。

建议：在 close() 中，如果 m_connectChannel 存在且未收到结果，也发送一个错误（如 ZCONNECTIONLOSS）而不是仅仅 close()。

⚠️ 4. 回调中的 const_cast 和内存释放
cpp
auto* op = static_cast<PendingOp*>(const_cast<void*>(data));
ZooKeeper API 的回调参数是 const void*，这里使用 const_cast 去除 const 修饰。虽然目的是允许删除对象，但最好在定义 PendingOp* 时就直接使用 void* 而不是 const void*，或者在回调中重新解释为 const PendingOp* 并禁止修改。如果不需要修改，可以只读取 op 的成员。但当前代码确实需要修改（发送 channel），因此 const_cast 可以接受，但要确保 PendingOp 的生命周期正确。

⚠️ 5. 错误码字符串映射
ZkResult::error() 中手动列举了常见错误码，但未覆盖所有 ZooKeeper 定义的可能值（如 ZBADARGUMENTS 等）。如果遇到未列出的错误码，会返回 "unknown error: xxx"，仍然可用。

⚠️ 6. 未处理的 Watcher 事件
globalWatcher 只处理了会话事件（ZOO_SESSION_EVENT），没有处理节点变化事件（ZOO_CHANGED_EVENT、ZOO_CHILD_EVENT 等）。如果用户需要监听节点变化，当前版本无法支持。可以扩展接口允许注册自定义 watcher，或提供更高级的观察者模式。