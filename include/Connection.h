#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "Buffer.h"

class EventLoop;
class ChatServer;

// 一条 TCP 连接。
//
// 线程亲和是这里最重要的不变量：
//   **Connection 的所有成员函数只允许在它所属的 EventLoop 线程里执行。**
//   因为一条连接只注册在唯一一个 Reactor 上，所以连接内部的
//   read_/out_/user_/state_ 全都不需要加锁 —— 数据竞争这一类 bug
//   在结构上被消灭了，而不是靠加锁掩盖。
//
//   其他线程想发数据，只能走 postSend()，内部转成 runInLoop 投递回 Reactor。
class Connection : public std::enable_shared_from_this<Connection> {
public:
    enum class State { Http, WebSocket, Closed };

    Connection(EventLoop *loop, int fd, std::string ip, int port, size_t loopIdx,
               ChatServer *server);
    ~Connection();

    Connection(const Connection &) = delete;
    Connection &operator=(const Connection &) = delete;

    // 注册到 epoll，开始收数据（只能在 loop 线程调用）
    void established();
    void onEvent(uint32_t events);

    // 只能在 loop 线程调用。
    // send() 发送“业务载荷”：HTTP 阶段原样写出，WebSocket 阶段自动包成文本帧。
    // sendRaw() 发送“原始字节”，调用方自己负责成帧（ping / pong / close 用）。
    void send(const std::string &payload);
    void sendRaw(const std::string &bytes);
    void sendPing();
    void close();
    void finishHttp();  // HTTP 响应写完后关闭连接

    // 任意线程可调用：把数据投递回本连接所属的 Reactor 再发送
    void postSend(std::string data);

    EventLoop *loop() const { return loop_; }
    size_t loopIndex() const { return loopIdx_; }
    int fd() const { return fd_; }
    State state() const { return state_; }
    bool loggedIn() const { return !user_.empty(); }
    const std::string &user() const { return user_; }
    void setUser(std::string u) { user_ = std::move(u); }

    int64_t lastActive() const { return lastActive_; }
    int64_t lastPing() const { return lastPing_; }
    void setLastPing(int64_t t) { lastPing_ = t; }

    // 简单的令牌桶限流：每秒最多 30 条消息
    bool allowMessage(int64_t now);

    void switchToWebSocket() { state_ = State::WebSocket; }

private:
    void handleRead();
    void flush();
    void enableWrite(bool on);
    void processHttp();
    void processWebSocket();
    void handleFrame(bool fin, uint8_t opcode, const std::string &payload);
    void sendCloseFrame(uint16_t code);

    EventLoop *loop_;
    int fd_;
    std::string ip_;
    int port_;
    size_t loopIdx_;
    ChatServer *server_;

    State state_ = State::Http;
    Buffer in_;
    Buffer out_;

    std::string user_;
    int64_t lastActive_ = 0;
    int64_t lastPing_ = 0;

    bool writeEnabled_ = false;
    bool closeAfterFlush_ = false;

    // 分片消息重组
    std::string fragment_;
    bool inFragment_ = false;

    // 限流窗口
    int64_t windowStart_ = 0;
    int windowCount_ = 0;
};
