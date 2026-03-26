#pragma once
#include <zookeeper/zookeeper.h>
#include <string>
#include <memory>
#include <vector>
#include <atomic>
#include <mutex>
#include "../coro/task.hpp"
#include "../coro/channel.hpp"

namespace AlphaMin {

struct ZkResult {
    int rc = 0;
    std::string data;
    std::string path;
    
    bool ok() const { return rc == ZOK; }
    std::string error() const {
        switch (rc) {
            case ZOK: return "ok";
            case ZSYSTEMERROR: return "system error";
            case ZRUNTIMEINCONSISTENCY: return "runtime inconsistency";
            case ZDATAINCONSISTENCY: return "data inconsistency";
            case ZCONNECTIONLOSS: return "connection loss";
            case ZMARSHALLINGERROR: return "marshalling error";
            case ZUNIMPLEMENTED: return "unimplemented";
            case ZAUTHFAILED: return "auth failed";
            case ZCLOSING: return "closing";
            case ZNOTHING: return "nothing";
            case ZSESSIONEXPIRED: return "session expired";
            case ZINVALIDSTATE: return "invalid state";
            case ZNOAUTH: return "no auth";
            case ZBADVERSION: return "bad version";
            case ZNOCHILDRENFOREPHEMERALS: return "no children for ephemerals";
            case ZNODEEXISTS: return "node exists";
            case ZNOTEMPTY: return "not empty";
            case ZSESSIONMOVED: return "session moved";
            case ZNONODE: return "node not exist";
            case ZAPIERROR: return "api error";
            default: return "unknown error: " + std::to_string(rc);
        }
    }
};

class ZkClient : public std::enable_shared_from_this<ZkClient> {
public:
    using ptr = std::shared_ptr<ZkClient>;

    ZkClient() = default;
    ~ZkClient() { close(); }

    void setHost(const std::string& host) { m_host = host; }
    void setTimeout(int timeout) { m_timeout = timeout; }
    bool isConnected() const { return m_connected.load(); }

    Coro::Task<ZkResult> start();
    Coro::Task<ZkResult> create(const std::string& path, const std::string& data, int flags = 0);
    Coro::Task<ZkResult> getData(const std::string& path);
    Coro::Task<ZkResult> setData(const std::string& path, const std::string& data, int version = -1);
    Coro::Task<ZkResult> deleteNode(const std::string& path, int version = -1);
    Coro::Task<ZkResult> getChildren(const std::string& path);
    void close();

private:
    struct PendingOp {
        int opType;
        std::string path;
        std::string data;
        int flags;
        int version;
        Coro::Channel<ZkResult>::s_ptr channel;
    };

    static void globalWatcher(zhandle_t* zh, int type, int state, 
                             const char* path, void* ctx);
    static void createCompletion(int rc, const char* path, const void* data);
    static void getCompletion(int rc, const char* value, int valueLen, 
                              const struct Stat* stat, const void* data);
    static void setCompletion(int rc, const struct Stat* stat, const void* data);
    static void deleteCompletion(int rc, const void* data);
    static void getChildrenCompletion(int rc, const struct String_vector* strings, 
                                      const struct Stat* stat, const void* data);

    void cleanupPendingOps();

    std::string m_host = "127.0.0.1:2181";
    int m_timeout = 30000;
    zhandle_t* m_zkHandle = nullptr;
    std::atomic<bool> m_connected{false};
    
    Coro::Channel<ZkResult>::s_ptr m_connectChannel;
    std::mutex m_pendingMutex;
    std::vector<PendingOp*> m_pendingOps;
};

}  // namespace AlphaMin
