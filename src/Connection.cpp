#include "Connection.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cctype>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string_view>

#include "ChatServer.h"
#include "EventLoop.h"
#include "Util.h"
#include "WebSocket.h"

namespace {

constexpr size_t kMaxHttpHeader = 8 * 1024;         // 请求头上限
constexpr size_t kMaxBody = 1 * 1024 * 1024;        // 请求体上限
constexpr size_t kMaxInput = 2 * 1024 * 1024;       // 读缓冲硬上限
constexpr size_t kMaxOutput = 16 * 1024 * 1024;     // 写缓冲硬上限（背压）
constexpr size_t kMaxFragment = 256 * 1024;         // 分片消息重组上限
constexpr size_t kReadChunk = 64 * 1024;

std::string toLower(std::string s) {
    for (auto &c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

std::string httpError(int code, const std::string &reason, const std::string &body) {
    std::string s = "HTTP/1.1 " + std::to_string(code) + " " + reason + "\r\n";
    s += "Content-Type: text/plain; charset=utf-8\r\n";
    s += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    s += "Connection: close\r\n\r\n";
    s += body;
    return s;
}

}  // namespace

Connection::Connection(EventLoop *loop, int fd, std::string ip, int port, size_t loopIdx,
                       ChatServer *server)
    : loop_(loop),
      fd_(fd),
      ip_(std::move(ip)),
      port_(port),
      loopIdx_(loopIdx),
      server_(server),
      lastActive_(nowMs()),
      lastPing_(nowMs()),
      windowStart_(nowMs()) {}

Connection::~Connection() {
    // 兜底：正常情况下 close() 已经把 fd_ 置 -1
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void Connection::established() {
    std::weak_ptr<Connection> weak = shared_from_this();
    loop_->addFd(fd_, EPOLLIN, [weak](uint32_t events) {
        if (auto conn = weak.lock()) conn->onEvent(events);
    });
}

void Connection::onEvent(uint32_t events) {
    if (state_ == State::Closed) return;

    if (events & EPOLLIN) {
        handleRead();
    }
    if (state_ == State::Closed) return;

    if (events & EPOLLOUT) {
        flush();
    }
    if (state_ == State::Closed) return;

    if (events & (EPOLLERR | EPOLLHUP)) {
        close();
    }
}

void Connection::handleRead() {
    char buf[kReadChunk];

    for (;;) {
        const ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
        if (n > 0) {
            in_.append(buf, static_cast<size_t>(n));
            if (in_.readable() > kMaxInput) {
                std::cerr << "[Conn] fd=" << fd_ << " 读缓冲超限，主动断开" << std::endl;
                close();
                return;
            }
            continue;
        }
        if (n == 0) {  // 对端关闭
            close();
            return;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        close();
        return;
    }

    lastActive_ = nowMs();

    if (state_ == State::Http) {
        processHttp();
    } else if (state_ == State::WebSocket) {
        processWebSocket();
    }
}

void Connection::flush() {
    if (state_ == State::Closed || fd_ < 0) return;

    while (out_.readable() > 0) {
        const ssize_t n = ::send(fd_, out_.peek(), out_.readable(), MSG_NOSIGNAL);
        if (n > 0) {
            out_.retrieve(static_cast<size_t>(n));
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            enableWrite(true);  // 内核缓冲区满了，等 EPOLLOUT 再续发
            return;
        }
        close();  // 0 或真实错误，链路已不可用
        return;
    }

    enableWrite(false);
    if (closeAfterFlush_) close();
}

void Connection::send(const std::string &payload) {
    if (state_ == State::Closed || fd_ < 0) return;
    if (state_ == State::WebSocket) {
        sendRaw(ws::encodeFrame(ws::kOpText, payload));
    } else {
        sendRaw(payload);  // HTTP 阶段：payload 已经是完整报文
    }
}

void Connection::sendRaw(const std::string &bytes) {
    if (state_ == State::Closed || fd_ < 0) return;

    out_.append(bytes);
    if (out_.readable() > kMaxOutput) {
        // 背压：对端长期不读，直接断开，不让内存无限增长
        std::cerr << "[Conn] fd=" << fd_ << " 写缓冲超限，判定为慢客户端并断开" << std::endl;
        close();
        return;
    }
    flush();
}

void Connection::sendPing() {
    sendRaw(ws::encodeFrame(ws::kOpPing, std::string()));
}

void Connection::postSend(std::string data) {
    auto self = shared_from_this();
    loop_->runInLoop([self, data = std::move(data)]() mutable { self->send(data); });
}

void Connection::enableWrite(bool on) {
    if (writeEnabled_ == on) return;
    writeEnabled_ = on;
    const uint32_t events = EPOLLIN | (on ? static_cast<uint32_t>(EPOLLOUT) : 0u);
    loop_->modFd(fd_, events);
}

void Connection::finishHttp() {
    closeAfterFlush_ = true;
    if (out_.readable() == 0) {
        close();
    } else {
        enableWrite(true);
    }
}

void Connection::close() {
    if (state_ == State::Closed) return;
    state_ = State::Closed;

    const int fd = fd_;
    fd_ = -1;

    // 延迟到本轮事件处理结束再真正摘除 fd：
    // 否则会在“正在执行的 std::function 内部”销毁它自己，属于 UB。
    auto self = shared_from_this();
    loop_->queueInLoop([this, self, fd]() {
        loop_->delFd(fd);
        ::close(fd);
        server_->onDisconnected(self, fd);
    });
}

// ---------------- HTTP ----------------

void Connection::processHttp() {
    while (state_ == State::Http) {
        const std::string_view data(in_.peek(), in_.readable());
        const size_t headEnd = data.find("\r\n\r\n");
        if (headEnd == std::string_view::npos) {
            if (data.size() > kMaxHttpHeader) {
                send(httpError(431, "Request Header Fields Too Large", "header too large"));
                finishHttp();
            }
            return;  // 半包，等下一批数据
        }

        std::string head(data.substr(0, headEnd));

        size_t contentLen = 0;
        const std::string lowered = toLower(head);
        size_t clPos = lowered.find("content-length:");
        if (clPos != std::string::npos) {
            size_t v = clPos + std::strlen("content-length:");
            while (v < head.size() && (head[v] == ' ' || head[v] == '\t')) ++v;
            size_t e = head.find("\r\n", v);
            if (e == std::string::npos) e = head.size();
            try {
                contentLen = static_cast<size_t>(std::stoul(head.substr(v, e - v)));
            } catch (...) {
                contentLen = 0;
            }
        }

        if (contentLen > kMaxBody) {
            send(httpError(413, "Payload Too Large", "body too large"));
            finishHttp();
            return;
        }

        const size_t total = headEnd + 4 + contentLen;
        if (data.size() < total) return;  // body 还没收全

        // ---- 首行 ----
        const size_t lineEnd = head.find("\r\n");
        const std::string requestLine =
            head.substr(0, lineEnd == std::string::npos ? head.size() : lineEnd);

        const size_t sp1 = requestLine.find(' ');
        if (sp1 == std::string::npos) {
            send(httpError(400, "Bad Request", "bad request line"));
            finishHttp();
            return;
        }
        const size_t sp2 = requestLine.find(' ', sp1 + 1);

        HttpRequest req;
        req.method = requestLine.substr(0, sp1);
        req.path = (sp2 == std::string::npos) ? requestLine.substr(sp1 + 1)
                                              : requestLine.substr(sp1 + 1, sp2 - sp1 - 1);
        const size_t q = req.path.find('?');
        if (q != std::string::npos) req.path = req.path.substr(0, q);
        req.headers = (lineEnd == std::string::npos) ? "" : head.substr(lineEnd + 2);
        req.body.assign(data.data() + headEnd + 4, contentLen);

        // 到这里这一条请求已经完整，从缓冲里摘掉（后面的字节可能是 WS 帧）
        in_.retrieve(total);
        lastActive_ = nowMs();

        server_->onHttpRequest(shared_from_this(), req);

        if (state_ == State::WebSocket) {
            // 握手完成：紧跟在请求后面的字节就是 WS 帧，必须在这里接着解析，
            // 否则客户端“握手后立刻发第一帧”会一直等到下一次 EPOLLIN 才被处理。
            processWebSocket();
            return;
        }
        if (state_ == State::Closed) return;

        finishHttp();
        return;  // 本项目 HTTP 一律短连接
    }
}

// ---------------- WebSocket ----------------

void Connection::processWebSocket() {
    while (state_ == State::WebSocket) {
        ws::Frame frame;
        const ws::ParseResult r = ws::parseFrame(in_, frame);

        if (r == ws::ParseResult::Incomplete) return;
        if (r == ws::ParseResult::Error) {
            sendCloseFrame(1002);  // protocol error
            return;
        }

        handleFrame(frame.fin, frame.opcode, frame.payload);
        if (state_ == State::Closed) return;
    }
}

void Connection::handleFrame(bool fin, uint8_t opcode, const std::string &payload) {
    lastActive_ = nowMs();

    switch (opcode) {
        case ws::kOpPing:
            sendRaw(ws::encodeFrame(ws::kOpPong, payload));
            return;

        case ws::kOpPong:
            return;

        case ws::kOpClose:
            sendCloseFrame(1000);
            return;

        case ws::kOpContinuation: {
            if (!inFragment_) {
                sendCloseFrame(1002);
                return;
            }
            fragment_ += payload;
            if (fragment_.size() > kMaxFragment) {
                sendCloseFrame(1009);  // message too big
                return;
            }
            if (fin) {
                std::string msg = std::move(fragment_);
                fragment_.clear();
                inFragment_ = false;
                server_->onWsText(shared_from_this(), msg);
            }
            return;
        }

        case ws::kOpText:
        case ws::kOpBinary: {
            if (inFragment_) {  // 上一条分片还没结束，不允许插入新消息
                sendCloseFrame(1002);
                return;
            }
            if (!fin) {
                inFragment_ = true;
                fragment_ = payload;
                return;
            }
            if (opcode == ws::kOpBinary) return;  // 本项目只处理文本
            server_->onWsText(shared_from_this(), payload);
            return;
        }

        default:
            sendCloseFrame(1002);
            return;
    }
}

void Connection::sendCloseFrame(uint16_t code) {
    if (state_ == State::Closed) return;

    std::string payload;
    payload.push_back(static_cast<char>((code >> 8) & 0xFF));
    payload.push_back(static_cast<char>(code & 0xFF));

    out_.append(ws::encodeFrame(ws::kOpClose, payload));
    closeAfterFlush_ = true;
    flush();  // 发完自然关闭
}

bool Connection::allowMessage(int64_t now) {
    if (now - windowStart_ >= 1000) {
        windowStart_ = now;
        windowCount_ = 0;
    }
    const int limit = g_rateLimitPerSec.load(std::memory_order_relaxed);
    return limit <= 0 || ++windowCount_ <= limit;
}
