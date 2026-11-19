#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class EventLoop;

struct RedisReply {
    bool ok = false;      // 命令是否执行成功（网络错误 / Redis 报错都是 false）
    bool nil = false;     // 空回复，例如 SET NX 没抢到
    long long integer = 0;
    std::string str;
};

// 异步 Redis 客户端。
//
// 旧版本的问题：在 Reactor 线程里做同步 redisConnect + redisCommand，
// 一次 RTT 就把整个事件循环卡住，而且每次请求都新建一条 TCP 连接。
//
// 现在的模型：
//   * 一个专职线程持有一条长连接，所有命令进队列；
//   * 业务线程只负责把命令塞进队列（几乎零成本），立刻返回；
//   * 命令执行完，回调通过 EventLoop::runInLoop 投回“发起命令的那个 Reactor”，
//     所以回调里可以安全地访问 Connection，不需要再加锁；
//   * 连接断了自动重连，失败的命令明确返回 ok=false，由业务层决定降级策略。
class RedisClient {
public:
    using Callback = std::function<void(const RedisReply &)>;

    RedisClient(std::string host, int port);
    ~RedisClient();

    RedisClient(const RedisClient &) = delete;
    RedisClient &operator=(const RedisClient &) = delete;

    bool start();
    void stop();

    // cbLoop 为 nullptr 表示不需要回调（例如断线时的 DEL）
    void command(std::vector<std::string> args, EventLoop *cbLoop, Callback cb);

private:
    struct Request {
        std::vector<std::string> args;
        EventLoop *loop = nullptr;
        Callback cb;
    };

    void run();
    bool ensureConnected();
    void execute(const std::vector<std::string> &args, RedisReply &out);
    void closeCtx();

    std::string host_;
    int port_;
    std::thread thread_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<Request> queue_;
    bool stop_ = false;
    bool started_ = false;

    void *ctx_ = nullptr;  // redisContext*，避免在头文件里引入 hiredis
    int64_t lastConnectAttemptMs_ = 0;
};
