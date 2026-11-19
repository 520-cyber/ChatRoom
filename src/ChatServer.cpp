#include "ChatServer.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

#include "Connection.h"
#include "Json.h"
#include "RedisClient.h"
#include "ThreadPool.h"
#include "Util.h"
#include "WebSocket.h"

namespace {

std::atomic<bool> g_stop{false};

constexpr int64_t kIdleTimeoutMs = 120 * 1000;   // 120s 没有任何数据就断开
constexpr int64_t kPingIntervalMs = 30 * 1000;   // 30s 发一次 WS ping
constexpr size_t kMaxHistory = 200;              // 内存里保留的最近消息条数
constexpr size_t kMaxNameBytes = 48;             // UTF-8 下约 16 个汉字
constexpr size_t kMaxWsMessage = 8 * 1024;

std::string toLower(std::string s) {
    for (auto &c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

std::string onlineKey(const std::string &name) { return "chat:online:" + name; }

bool validName(const std::string &n) {
    if (n.empty() || n.size() > kMaxNameBytes) return false;
    if (n.front() == ' ' || n.back() == ' ') return false;
    for (unsigned char c : n) {
        if (c < 0x20 || c == 0x7F) return false;  // 控制字符
        if (c == '"' || c == '\\' || c == '@' || c == '|') return false;
    }
    return true;
}

std::string httpResponse(int code, const char *reason, const std::string &type,
                         const std::string &body) {
    std::string s = "HTTP/1.1 " + std::to_string(code) + " " + reason + "\r\n";
    s += "Content-Type: " + type + "\r\n";
    s += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    s += "Connection: close\r\n\r\n";
    s += body;
    return s;
}

std::string errorJson(const std::string &text) {
    return "{\"type\":\"error\",\"text\":\"" + json::escape(text) + "\"}";
}

std::string sysJson(const std::string &text) {
    return "{\"type\":\"sys\",\"text\":\"" + json::escape(text) + "\"}";
}

size_t loopIndexOf(EventLoopThreadPool &pool, EventLoop *loop) {
    for (size_t i = 0; i < pool.size(); ++i) {
        if (pool.get(i) == loop) return i;
    }
    return 0;
}

}  // namespace

void requestStop() { g_stop.store(true, std::memory_order_relaxed); }

std::string HttpRequest::header(const std::string &name) const {
    const std::string needle = toLower(name) + ":";
    const std::string hay = toLower(headers);

    size_t pos = 0;
    while (pos < hay.size()) {
        size_t eol = hay.find("\r\n", pos);
        if (eol == std::string::npos) eol = hay.size();

        if (hay.compare(pos, needle.size(), needle) == 0) {
            size_t v = pos + needle.size();
            while (v < eol && (headers[v] == ' ' || headers[v] == '\t')) ++v;
            return headers.substr(v, eol - v);
        }
        pos = eol + 2;
    }
    return "";
}

ChatServer::ChatServer(int port, size_t ioThreads) : port_(port) {
    // 限流阈值可配置：默认 30 条/秒，压测时可用 CHAT_RATE_LIMIT=0 关闭
    if (const char *env = std::getenv("CHAT_RATE_LIMIT")) {
        const int v = std::atoi(env);
        if (v >= 0) g_rateLimitPerSec.store(v);  // 0 = 关闭限流
    }

    listenFd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listenFd_ < 0) {
        std::cerr << "[启动] socket 失败: " << std::strerror(errno) << std::endl;
        std::abort();
    }

    int opt = 1;
    ::setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (::bind(listenFd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::cerr << "[启动] bind " << port_ << " 失败: " << std::strerror(errno) << std::endl;
        std::abort();
    }
    if (::listen(listenFd_, 1024) < 0) {
        std::cerr << "[启动] listen 失败: " << std::strerror(errno) << std::endl;
        std::abort();
    }

    loadIndexHtml();

    if (ioThreads == 0) ioThreads = 1;
    ioPool_ = std::make_unique<EventLoopThreadPool>(ioThreads);
    ioPool_->start();
    connsByLoop_.resize(ioPool_->size());

    // 主 Reactor：只负责 accept
    mainLoop_ = std::make_unique<EventLoop>();
    mainLoop_->addFd(listenFd_, EPOLLIN, [this](uint32_t) { acceptAll(); });

    redis_ = std::make_unique<RedisClient>("127.0.0.1", 6379);
    if (!redis_->start()) {
        std::cerr << "[启动] Redis 不可用，登录请求会被拒绝（fail-closed）" << std::endl;
    }

    // 只有一个消费者，专门做磁盘这种阻塞操作，绝不占用 Reactor 线程
    diskPool_ = std::make_unique<ThreadPool>(1);

    // 给每个从 Reactor 挂上定时任务（必须在它自己的线程里设置）
    for (size_t i = 0; i < ioPool_->size(); ++i) {
        EventLoop *loop = ioPool_->get(i);
        loop->runInLoop([this, loop, i] {
            loop->setTickCallback([this, i] { sweepIdle(i); });
        });
    }

    std::cout << "==============================================\n"
              << " 服务已启动\n"
              << "   地址      : http://127.0.0.1:" << port_ << "\n"
              << "   IO 线程数 : " << ioPool_->size() << "（主从 Reactor）\n"
              << "==============================================\n"
              << std::endl;
}

ChatServer::~ChatServer() {
    if (mainLoop_) mainLoop_->stop();
    if (ioPool_) ioPool_->stop();
    if (redis_) redis_->stop();
    diskPool_.reset();
    if (listenFd_ >= 0) ::close(listenFd_);
}

void ChatServer::run() {
    mainLoop_->setTickCallback([this] {
        if (g_stop.load(std::memory_order_relaxed)) mainLoop_->stop();
    });

    mainLoop_->loop();

    std::cout << "[主线程] 事件循环退出，正在关闭 IO 线程…" << std::endl;
    ioPool_->stop();
    std::cout << "[主线程] 已全部关闭" << std::endl;
}

void ChatServer::stop() {
    if (mainLoop_) mainLoop_->stop();
}

void ChatServer::loadIndexHtml() {
    std::vector<std::string> candidates;
#ifdef SERVER_WEB_ROOT
    candidates.emplace_back(std::string(SERVER_WEB_ROOT) + "/index.html");
#endif
    candidates.emplace_back("web/index.html");
    candidates.emplace_back("../web/index.html");

    for (const auto &path : candidates) {
        std::ifstream file(path, std::ios::binary);
        if (!file) continue;
        std::string content((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
        if (content.empty()) continue;
        indexHtml_ = std::move(content);
        std::cout << "[静态] 已加载 " << path << " (" << indexHtml_.size() << " bytes)\n";
        return;
    }
    std::cerr << "[静态] 警告：未找到 web/index.html\n";
}

// ---------------- accept（主 Reactor） ----------------

void ChatServer::acceptAll() {
    for (;;) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        const int fd = ::accept4(listenFd_, reinterpret_cast<sockaddr *>(&addr), &len,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EMFILE || errno == ENFILE) {
                // fd 耗尽：accept 会一直就绪，必须退让一下，否则 CPU 空转 100%
                std::cerr << "[Accept] 文件描述符耗尽，暂停 50ms" << std::endl;
                ::usleep(50 * 1000);
                break;
            }
            break;
        }

        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));

        char ip[INET_ADDRSTRLEN] = {0};
        ::inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
        const int port = ntohs(addr.sin_port);

        EventLoop *sub = ioPool_->next();
        if (sub == nullptr) {
            ::close(fd);
            continue;
        }
        const size_t idx = loopIndexOf(*ioPool_, sub);

        // 连接对象的构造、注册、epoll 添加，全部放到目标 Reactor 线程里做。
        // 这样从 Connection 诞生的第一行起，就满足“只被一个线程访问”。
        sub->runInLoop([this, sub, idx, fd, ipStr = std::string(ip), port]() {
            auto conn = std::make_shared<Connection>(sub, fd, ipStr, port, idx, this);
            connsByLoop_[idx][fd] = conn;
            conn->established();
        });
    }
}

// ---------------- 定时任务（在各自 Reactor 线程内执行） ----------------

void ChatServer::sweepIdle(size_t loopIdx) {
    if (loopIdx >= connsByLoop_.size()) return;

    auto &table = connsByLoop_[loopIdx];
    const int64_t now = nowMs();

    std::vector<ConnectionPtr> dying;
    for (auto &kv : table) {
        const ConnectionPtr &conn = kv.second;
        if (!conn || conn->state() == Connection::State::Closed) continue;

        if (now - conn->lastActive() > kIdleTimeoutMs) {
            dying.push_back(conn);
            continue;
        }
        if (conn->state() == Connection::State::WebSocket &&
            now - conn->lastPing() > kPingIntervalMs) {
            conn->sendPing();
            conn->setLastPing(now);
        }
    }
    for (auto &conn : dying) conn->close();
}

// ---------------- HTTP ----------------

void ChatServer::onHttpRequest(const ConnectionPtr &conn, const HttpRequest &req) {
    if (req.method == "GET" && (req.path == "/" || req.path == "/index.html")) {
        if (indexHtml_.empty()) {
            conn->send(httpResponse(503, "Service Unavailable", "text/plain; charset=utf-8",
                                    "web/index.html 未找到"));
            return;
        }
        conn->send(httpResponse(200, "OK", "text/html; charset=utf-8", indexHtml_));
        return;
    }

    if (req.method == "GET" && req.path == "/ws") {
        const std::string key = req.header("sec-websocket-key");
        const std::string upgrade = req.header("upgrade");
        if (key.empty() || toLower(upgrade) != "websocket") {
            conn->send(httpResponse(400, "Bad Request", "text/plain",
                                    "not a websocket handshake"));
            return;
        }

        std::string resp = "HTTP/1.1 101 Switching Protocols\r\n";
        resp += "Upgrade: websocket\r\n";
        resp += "Connection: Upgrade\r\n";
        resp += "Sec-WebSocket-Accept: " + ws::acceptKey(key) + "\r\n\r\n";
        conn->send(resp);
        conn->switchToWebSocket();
        return;
    }

    if (req.path == "/favicon.ico") {
        conn->send("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n");
        return;
    }

    conn->send(httpResponse(404, "Not Found", "text/plain; charset=utf-8", "not found"));
}

// ---------------- WebSocket 业务 ----------------

void ChatServer::onWsText(const ConnectionPtr &conn, const std::string &text) {
    if (text.size() > kMaxWsMessage) {
        conn->send(errorJson("消息太长"));
        return;
    }
    if (!conn->allowMessage(nowMs())) {
        conn->send(errorJson("发送太快了，休息一下"));
        return;
    }

    std::string type;
    if (!json::getString(text, "type", type)) {
        conn->send(errorJson("非法报文"));
        return;
    }

    if (type == "login") {
        if (conn->loggedIn()) {
            conn->send(errorJson("你已经登录了"));
            return;
        }
        std::string name;
        if (!json::getString(text, "name", name) || !validName(name)) {
            conn->send(errorJson("昵称不合法（1-48 字节，不能含控制字符、@ | \\ \"）"));
            return;
        }
        doLogin(conn, name);
        return;
    }

    if (type == "msg") {
        if (!conn->loggedIn()) {
            conn->send(errorJson("请先登录"));
            return;
        }
        std::string body;
        if (!json::getString(text, "text", body)) {
            conn->send(errorJson("缺少 text 字段"));
            return;
        }
        if (body.empty()) return;
        handleChat(conn, body);
        return;
    }

    if (type == "ping") {
        conn->send("{\"type\":\"pong\"}");
        return;
    }

    conn->send(errorJson("未知报文类型"));
}

void ChatServer::doLogin(const ConnectionPtr &conn, const std::string &name) {
    // 用 SET NX EX 抢占昵称：
    //   +OK   -> 抢到
    //   nil   -> 已有人在线
    //   !ok   -> Redis 不可用，拒绝登录（fail-closed，绝不放过未鉴权用户）
    redis_->command({"SET", onlineKey(name), "1", "NX", "EX", "3600"}, conn->loop(),
                    [this, conn, name](const RedisReply &reply) {
                        if (conn->state() == Connection::State::Closed) return;

                        if (!reply.ok) {
                            conn->send(errorJson("鉴权服务暂不可用，请稍后重试"));
                            return;
                        }
                        if (reply.nil) {
                            conn->send(errorJson("昵称已被占用，换一个吧"));
                            return;
                        }
                        finishLogin(conn, name);
                    });
}

void ChatServer::finishLogin(const ConnectionPtr &conn, const std::string &name) {
    conn->setUser(name);

    size_t count = 0;
    {
        std::lock_guard<std::mutex> lock(userMtx_);
        users_[name] = conn;
        count = users_.size();
    }

    std::string ok = "{\"type\":\"login_ok\",\"name\":\"" + json::escape(name) +
                     "\",\"online\":" + std::to_string(count) + "}";
    conn->send(ok);
    sendHistory(conn);
    broadcast(sysJson(name + " 加入了聊天室"));

    std::cout << "[登录] " << name << " fd=" << conn->fd() << " 在线=" << count << std::endl;
}

void ChatServer::handleChat(const ConnectionPtr &conn, const std::string &text) {
    const std::string from = conn->user();

    // 私聊：@昵称 内容
    if (text.front() == '@') {
        const size_t sp = text.find(' ');
        if (sp == std::string::npos || sp <= 1) {
            conn->send(errorJson("私聊格式：@昵称 内容"));
            return;
        }
        const std::string to = text.substr(1, sp - 1);
        std::string content = text.substr(sp + 1);
        while (!content.empty() && (content.front() == ' ' || content.front() == '\t')) {
            content.erase(content.begin());
        }
        if (content.empty()) {
            conn->send(errorJson("私聊内容不能为空"));
            return;
        }
        if (to == from) {
            conn->send(errorJson("不能私聊自己"));
            return;
        }

        ConnectionPtr target;
        {
            std::lock_guard<std::mutex> lock(userMtx_);
            auto it = users_.find(to);
            if (it != users_.end()) target = it->second.lock();
        }
        if (!target || target->state() == Connection::State::Closed) {
            conn->send(errorJson("对方不在线"));
            return;
        }

        const std::string payload = buildMessageJson(from, content, true);
        target->postSend(payload);
        conn->postSend(payload);  // 回显给发送方，刷新后也能从服务端恢复
        return;
    }

    // 群聊
    const std::string payload = buildMessageJson(from, text, false);
    appendHistory(from, text);
    broadcast(payload);
}

std::string ChatServer::buildMessageJson(const std::string &from, const std::string &text,
                                         bool pm) const {
    std::string s = "{\"type\":\"msg\",\"from\":\"";
    s += json::escape(from);
    s += "\",\"text\":\"";
    s += json::escape(text);
    s += "\",\"ts\":";
    s += std::to_string(nowMs());
    if (pm) s += ",\"pm\":true";
    s += "}";
    return s;
}

void ChatServer::appendHistory(const std::string &from, const std::string &text) {
    const std::string fragment = buildMessageJson(from, text, false);

    {
        std::lock_guard<std::mutex> lock(historyMtx_);
        history_.push_back(fragment);
        while (history_.size() > kMaxHistory) history_.pop_front();
    }

    // 落盘是阻塞 IO，交给线程池，Reactor 线程一步都不等
    diskPool_->enqueue([fragment]() {
        static thread_local std::ofstream ofs("chat_history.log", std::ios::app);
        if (ofs) ofs << fragment << '\n';
    });
}

void ChatServer::sendHistory(const ConnectionPtr &conn) {
    std::string out = "{\"type\":\"history\",\"msgs\":[";
    {
        std::lock_guard<std::mutex> lock(historyMtx_);
        bool first = true;
        for (const auto &item : history_) {
            if (!first) out += ',';
            out += item;
            first = false;
        }
    }
    out += "]}";
    conn->send(out);
}

void ChatServer::broadcast(const std::string &payload) {
    // 关键优化：不要「每条连接投递一次」。
    // 那样一条消息会产生 N 次跨线程任务 + N 次 eventfd 唤醒，
    // 广播扇出到几百人时，跨 Reactor 的投递开销本身就成了瓶颈。
    //
    // 现在按 Reactor 分桶：一条消息对每个从 Reactor 只投递一个任务，
    // 任务里再顺序发给该 Reactor 名下的所有连接。
    std::vector<std::vector<ConnectionPtr>> byLoop(ioPool_->size());
    {
        std::lock_guard<std::mutex> lock(userMtx_);
        for (auto &kv : users_) {
            if (auto conn = kv.second.lock()) {
                const size_t idx = conn->loopIndex();
                if (idx < byLoop.size()) byLoop[idx].push_back(std::move(conn));
            }
        }
    }

    for (size_t i = 0; i < byLoop.size(); ++i) {
        if (byLoop[i].empty()) continue;
        ioPool_->get(i)->runInLoop(
            [conns = std::move(byLoop[i]), payload]() {
                for (auto &conn : conns) conn->send(payload);
            });
    }
}

size_t ChatServer::onlineCount() const {
    std::lock_guard<std::mutex> lock(userMtx_);
    return users_.size();
}

void ChatServer::onDisconnected(const ConnectionPtr &conn, int fd) {
    const size_t idx = conn->loopIndex();
    if (idx < connsByLoop_.size()) {
        connsByLoop_[idx].erase(fd);
    }

    if (!conn->loggedIn()) return;

    const std::string name = conn->user();
    bool removed = false;
    {
        std::lock_guard<std::mutex> lock(userMtx_);
        auto it = users_.find(name);
        if (it != users_.end()) {
            auto current = it->second.lock();
            if (!current || current.get() == conn.get()) {
                users_.erase(it);
                removed = true;
            }
        }
    }
    if (!removed) return;

    // 释放昵称占用（fire-and-forget，不需要回调）
    redis_->command({"DEL", onlineKey(name)}, nullptr, nullptr);
    broadcast(sysJson(name + " 离开了聊天室"));
    std::cout << "[下线] " << name << " 在线=" << onlineCount() << std::endl;
}
