#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "EventLoop.h"

class Connection;
class RedisClient;
class ThreadPool;

struct HttpRequest {
    std::string method;
    std::string path;
    std::string headers;  // 原始 header 块（不含首行，不含结尾空行）
    std::string body;
    bool keepAlive = false;

    // 大小写不敏感的 header 查找
    std::string header(const std::string &name) const;
};

// 主从 Reactor 的“主”这一侧 + 业务层。
//
//   mainLoop_ ：只做 accept，把新 fd 交给某个从 Reactor
//   ioPool_   ：N 个从 Reactor，每个线程一个 epoll，负责自己名下连接的全部读写
//
// 连接表按 Reactor 分片（connsByLoop_），只在所属线程访问，因此不加锁；
// 只有跨线程共享的「用户名 -> 连接」索引才需要 userMtx_。
class ChatServer {
public:
    using ConnectionPtr = std::shared_ptr<Connection>;

    ChatServer(int port, size_t ioThreads);
    ~ChatServer();

    ChatServer(const ChatServer &) = delete;
    ChatServer &operator=(const ChatServer &) = delete;

    void run();
    void stop();

    // 以下回调都在连接所属的 Reactor 线程内被调用
    void onHttpRequest(const ConnectionPtr &conn, const HttpRequest &req);
    void onWsText(const ConnectionPtr &conn, const std::string &text);
    void onDisconnected(const ConnectionPtr &conn, int fd);

private:
    void acceptAll();
    void sweepIdle(size_t loopIdx);
    void loadIndexHtml();

    void doLogin(const ConnectionPtr &conn, const std::string &name);
    void finishLogin(const ConnectionPtr &conn, const std::string &name);
    void handleChat(const ConnectionPtr &conn, const std::string &text);
    void sendHistory(const ConnectionPtr &conn);
    void appendHistory(const std::string &from, const std::string &text);

    void broadcast(const std::string &payload);
    std::string buildMessageJson(const std::string &from, const std::string &text, bool pm) const;
    size_t onlineCount() const;

    int port_;
    int listenFd_ = -1;
    std::string indexHtml_;

    std::unique_ptr<EventLoop> mainLoop_;
    std::unique_ptr<EventLoopThreadPool> ioPool_;
    std::unique_ptr<RedisClient> redis_;
    std::unique_ptr<ThreadPool> diskPool_;

    // 每个从 Reactor 一份连接表，只在所属线程读写
    std::vector<std::unordered_map<int, ConnectionPtr>> connsByLoop_;

    // 跨线程共享：登录用户索引
    mutable std::mutex userMtx_;
    std::unordered_map<std::string, std::weak_ptr<Connection>> users_;

    // 近期群聊历史（已序列化成 JSON 片段），带锁
    mutable std::mutex historyMtx_;
    std::deque<std::string> history_;
};

// 信号处理器里调用，线程安全
void requestStop();
