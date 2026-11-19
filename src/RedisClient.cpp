#include "RedisClient.h"

#include <hiredis/hiredis.h>

#include <iostream>
#include <utility>

#include "EventLoop.h"
#include "Util.h"

namespace {
constexpr int kConnectTimeoutSec = 2;
constexpr int64_t kReconnectIntervalMs = 1000;
}  // namespace

RedisClient::RedisClient(std::string host, int port)
    : host_(std::move(host)), port_(port) {}

RedisClient::~RedisClient() { stop(); }

bool RedisClient::start() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (started_) return ctx_ != nullptr;
    started_ = true;

    const bool ok = ensureConnected();
    // 无论首次连接是否成功都起线程：运行期间会自动重连，业务侧拿到 ok=false 后自行降级
    thread_ = std::thread([this] { run(); });
    return ok;
}

void RedisClient::stop() {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        stop_ = true;
    }
    cv_.notify_all();

    if (thread_.joinable()) thread_.join();

    closeCtx();
}

void RedisClient::command(std::vector<std::string> args, EventLoop *cbLoop, Callback cb) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        queue_.push_back(Request{std::move(args), cbLoop, std::move(cb)});
    }
    cv_.notify_one();
}

void RedisClient::run() {
    while (true) {
        std::deque<Request> batch;
        {
            std::unique_lock<std::mutex> lock(mtx_);
            cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
            if (queue_.empty()) {
                // 只有 stop_ 为真才会走到这里
                if (stop_) break;
                continue;
            }
            batch.swap(queue_);
        }

        for (auto &req : batch) {
            RedisReply reply;
            if (!ensureConnected()) {
                reply.ok = false;
                reply.str = "redis unavailable";
            } else {
                execute(req.args, reply);
            }

            if (!req.cb) continue;
            if (req.loop) {
                // 投回发起命令的那个 Reactor，回调里可以安全访问 Connection
                auto cb = std::move(req.cb);
                req.loop->runInLoop([cb = std::move(cb), reply] { cb(reply); });
            } else {
                req.cb(reply);
            }
        }
    }
}

bool RedisClient::ensureConnected() {
    auto *cur = static_cast<redisContext *>(ctx_);
    if (cur) {
        if (cur->err == 0) return true;
        closeCtx();
    }

    // 重连退避：1s 内不重复尝试
    const int64_t now = nowMs();
    if (now - lastConnectAttemptMs_ < kReconnectIntervalMs) return false;
    lastConnectAttemptMs_ = now;

    timeval tv{kConnectTimeoutSec, 0};
    redisContext *ctx = redisConnectWithTimeout(host_.c_str(), port_, tv);
    if (ctx == nullptr || ctx->err) {
        std::cerr << "[Redis] 连接失败: " << (ctx ? ctx->errstr : "分配 context 失败")
                  << std::endl;
        if (ctx) redisFree(ctx);
        ctx_ = nullptr;
        return false;
    }

    redisSetTimeout(ctx, tv);
    redisEnableKeepAlive(ctx);
    ctx_ = ctx;
    std::cout << "[Redis] 已连接 " << host_ << ":" << port_ << std::endl;
    return true;
}

void RedisClient::execute(const std::vector<std::string> &args, RedisReply &out) {
    auto *ctx = static_cast<redisContext *>(ctx_);
    if (ctx == nullptr) {
        out.ok = false;
        out.str = "no connection";
        return;
    }
    if (args.empty()) {
        out.ok = false;
        out.str = "empty command";
        return;
    }

    std::vector<const char *> argv;
    std::vector<size_t> argvLen;
    argv.reserve(args.size());
    argvLen.reserve(args.size());
    for (const auto &a : args) {
        argv.push_back(a.c_str());
        argvLen.push_back(a.size());
    }

    // 用 redisCommandArgv 而不是 redisCommand(format)：
    // 后者是 printf 变参风格，用户输入里的 %s 会被当成格式符，直接 UB。
    auto *reply = static_cast<redisReply *>(
        redisCommandArgv(ctx, static_cast<int>(argv.size()), argv.data(), argvLen.data()));

    if (reply == nullptr) {
        out.ok = false;
        out.str = ctx->errstr;
        closeCtx();
        return;
    }

    switch (reply->type) {
        case REDIS_REPLY_STATUS:
            out.ok = true;
            out.str = reply->str ? reply->str : "";
            break;
        case REDIS_REPLY_STRING:
            out.ok = true;
            out.str.assign(reply->str ? reply->str : "", static_cast<size_t>(reply->len));
            break;
        case REDIS_REPLY_INTEGER:
            out.ok = true;
            out.integer = reply->integer;
            out.str = std::to_string(reply->integer);
            break;
        case REDIS_REPLY_NIL:
            out.ok = true;
            out.nil = true;
            break;
        case REDIS_REPLY_ERROR:
            out.ok = false;
            out.str = reply->str ? reply->str : "redis error";
            break;
        default:
            out.ok = true;
            break;
    }

    freeReplyObject(reply);

    if (ctx->err) {
        // 连接层出错，下一轮自动重连
        closeCtx();
    }
}

void RedisClient::closeCtx() {
    if (ctx_) {
        redisFree(static_cast<redisContext *>(ctx_));
        ctx_ = nullptr;
    }
}
