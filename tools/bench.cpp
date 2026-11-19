// WebSocket 压测客户端（epoll 多路复用版）。
//
// 为什么不用「一条连接一个线程」：100 条连接就是 100 个线程，在 2 核机器上
// 客户端自己就会因为线程调度把 RTT 抬高一个数量级，压出来的数据是客户端的
// 调度延迟，不是服务端的能力。这里改成少量线程 + epoll 管理全部连接，
// 客户端开销可以忽略，测到的才是服务端。
//
//   ./bench <port> <conns> <seconds> [per_conn_rate] [client_threads]
//
//   per_conn_rate = 0 表示不限速（压到饱和为止）
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "Util.h"

namespace {

constexpr int kWindow = 32;  // 每条连接最多同时在途的消息数

int64_t nowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct Buf {
    std::string data;
    size_t off = 0;
    size_t size() const { return data.size() - off; }
    const char *peek() const { return data.data() + off; }
    void append(const char *p, size_t n) { data.append(p, n); }
    void consume(size_t n) {
        off += n;
        if (off >= data.size()) {
            data.clear();
            off = 0;
        } else if (off > 64 * 1024) {
            data.erase(0, off);
            off = 0;
        }
    }
};

std::string base64(const uint8_t *d, size_t n) {
    static const char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = static_cast<uint32_t>(d[i]) << 16;
        if (i + 1 < n) v |= static_cast<uint32_t>(d[i + 1]) << 8;
        if (i + 2 < n) v |= static_cast<uint32_t>(d[i + 2]);
        out.push_back(tbl[(v >> 18) & 63]);
        out.push_back(tbl[(v >> 12) & 63]);
        out.push_back(i + 1 < n ? tbl[(v >> 6) & 63] : '=');
        out.push_back(i + 2 < n ? tbl[v & 63] : '=');
    }
    return out;
}

std::string encodeMasked(uint8_t opcode, const std::string &payload, uint32_t seed) {
    std::string f;
    const size_t n = payload.size();
    f.push_back(static_cast<char>(0x80 | opcode));
    if (n < 126) {
        f.push_back(static_cast<char>(0x80 | n));
    } else if (n < 65536) {
        f.push_back(static_cast<char>(0x80 | 126));
        f.push_back(static_cast<char>((n >> 8) & 0xFF));
        f.push_back(static_cast<char>(n & 0xFF));
    } else {
        f.push_back(static_cast<char>(0x80 | 127));
        for (int i = 7; i >= 0; --i) {
            f.push_back(static_cast<char>((static_cast<uint64_t>(n) >> (i * 8)) & 0xFF));
        }
    }
    uint8_t mask[4] = {static_cast<uint8_t>(seed >> 24), static_cast<uint8_t>(seed >> 16),
                       static_cast<uint8_t>(seed >> 8), static_cast<uint8_t>(seed)};
    for (int i = 0; i < 4; ++i) f.push_back(static_cast<char>(mask[i]));
    for (size_t i = 0; i < n; ++i) f.push_back(static_cast<char>(payload[i] ^ mask[i % 4]));
    return f;
}

int parseFrame(Buf &b, uint8_t &opcode, std::string &payload) {
    if (b.size() < 2) return 0;
    const uint8_t *p = reinterpret_cast<const uint8_t *>(b.peek());
    opcode = p[0] & 0x0F;
    const bool masked = (p[1] & 0x80) != 0;
    uint64_t len = p[1] & 0x7F;
    size_t off = 2;
    if (len == 126) {
        if (b.size() < 4) return 0;
        len = (static_cast<uint64_t>(p[2]) << 8) | p[3];
        off = 4;
    } else if (len == 127) {
        if (b.size() < 10) return 0;
        len = 0;
        for (int i = 0; i < 8; ++i) len = (len << 8) | p[2 + i];
        off = 10;
    }
    if (masked) off += 4;
    if (b.size() < off + len) return 0;
    payload.assign(reinterpret_cast<const char *>(p + off), static_cast<size_t>(len));
    if (masked) {
        const uint8_t *m = p + off - 4;
        for (size_t i = 0; i < payload.size(); ++i) {
            payload[i] = static_cast<char>(static_cast<uint8_t>(payload[i]) ^ m[i % 4]);
        }
    }
    b.consume(off + static_cast<size_t>(len));
    return 1;
}

bool sendAll(int fd, const std::string &s) {
    size_t sent = 0;
    int spins = 0;
    while (sent < s.size()) {
        const ssize_t n = ::send(fd, s.data() + sent, s.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<size_t>(n);
            spins = 0;
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (++spins > 3) return false;
            pollfd p{fd, POLLOUT, 0};
            ::poll(&p, 1, 2);
            continue;
        }
        return false;
    }
    return true;
}

// 返回 false 表示对端关闭或出错
bool drain(int fd, Buf &b) {
    char tmp[65536];
    for (;;) {
        const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n > 0) {
            b.append(tmp, static_cast<size_t>(n));
            continue;
        }
        if (n == 0) return false;
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
        return false;
    }
}

bool doHandshake(int fd, Buf &buf) {
    uint8_t key[16];
    for (auto &k : key) k = static_cast<uint8_t>(rand() & 0xFF);

    std::string req = "GET /ws HTTP/1.1\r\nHost: 127.0.0.1\r\n";
    req += "Upgrade: websocket\r\nConnection: Upgrade\r\n";
    req += "Sec-WebSocket-Key: " + base64(key, sizeof(key)) + "\r\n";
    req += "Sec-WebSocket-Version: 13\r\n\r\n";
    if (!sendAll(fd, req)) return false;

    std::string resp;
    char tmp[4096];
    while (resp.find("\r\n\r\n") == std::string::npos) {
        pollfd pfd{fd, POLLIN, 0};
        if (::poll(&pfd, 1, 5000) <= 0) return false;
        const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        resp.append(tmp, static_cast<size_t>(n));
    }
    const size_t pos = resp.find("\r\n\r\n");
    if (resp.substr(0, pos).find("101") == std::string::npos) return false;
    buf.data = resp.substr(pos + 4);
    buf.off = 0;
    return true;
}

int connectTo(int port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
    return fd;
}

struct Conn {
    int fd = -1;
    int id = 0;
    Buf buf;
    long long seq = 0;
    int inFlight = 0;
    int64_t ts[kWindow];
    int64_t nextSend = 0;
    uint32_t maskSeed = 0;
};

struct ThreadResult {
    long long sent = 0;
    long long frames = 0;
    int failed = 0;
    std::string err;
    std::vector<uint32_t> latUs;
};

std::atomic<int> g_ready{0};
std::atomic<bool> g_go{false};
std::atomic<int64_t> g_deadline{0};

// 单条连接登录（阻塞态完成，不进入计时）
bool loginConn(Conn &c, int port, int globalIdx) {
    c.fd = connectTo(port);
    if (c.fd < 0) return false;
    if (!doHandshake(c.fd, c.buf)) return false;

    const std::string payload =
        "{\"type\":\"login\",\"name\":\"bench" + std::to_string(globalIdx) + "\"}";
    if (!sendAll(c.fd, encodeMasked(1, payload, static_cast<uint32_t>(globalIdx)))) return false;

    const int64_t deadline = nowUs() + 10 * 1000000;
    while (nowUs() < deadline) {
        uint8_t op;
        std::string p;
        const int rc = parseFrame(c.buf, op, p);
        if (rc == 0) {
            pollfd pfd{c.fd, POLLIN, 0};
            if (::poll(&pfd, 1, 200) <= 0) continue;
            if (!drain(c.fd, c.buf)) return false;
            continue;
        }
        if (rc < 0) return false;
        if (op == 1) {
            if (p.find("\"type\":\"login_ok\"") != std::string::npos) return true;
            if (p.find("\"type\":\"error\"") != std::string::npos) {
                std::fprintf(stderr, "[登录失败 id=%d] %s\n", globalIdx, p.c_str());
                return false;
            }
        }
        if (op == 8) return false;
    }
    return false;
}

void worker(int tid, int port, int baseId, int count, int perConnRate, ThreadResult *out) {
    (void)tid;
    std::vector<Conn> conns(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        const int gid = baseId + i;
        conns[static_cast<size_t>(i)].id = gid;
        conns[static_cast<size_t>(i)].maskSeed = static_cast<uint32_t>(gid * 2654435761u + 7);
        if (!loginConn(conns[static_cast<size_t>(i)], port, gid)) {
            ++out->failed;
            if (conns[static_cast<size_t>(i)].fd >= 0) {
                ::close(conns[static_cast<size_t>(i)].fd);
                conns[static_cast<size_t>(i)].fd = -1;
            }
        }
    }

    const int ep = ::epoll_create1(EPOLL_CLOEXEC);
    std::vector<epoll_event> events(static_cast<size_t>(count) + 8);
    int alive = 0;
    for (auto &c : conns) {
        if (c.fd < 0) continue;
        ::fcntl(c.fd, F_SETFL, ::fcntl(c.fd, F_GETFL, 0) | O_NONBLOCK);
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.ptr = &c;
        ::epoll_ctl(ep, EPOLL_CTL_ADD, c.fd, &ev);
        ++alive;
    }

    g_ready.fetch_add(1);
    while (!g_go.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    const int64_t deadline = g_deadline.load();
    const int64_t interval = perConnRate > 0 ? (1000000LL / perConnRate) : 0;
    const int64_t start = nowUs();
    for (auto &c : conns) c.nextSend = start;
    out->latUs.reserve(50000);

    while (nowUs() < deadline) {
        const int n = ::epoll_wait(ep, events.data(), static_cast<int>(events.size()), 1);
        for (int i = 0; i < n; ++i) {
            Conn *c = static_cast<Conn *>(events[i].data.ptr);
            if (!drain(c->fd, c->buf)) {
                if (c->fd >= 0) {
                    ::epoll_ctl(ep, EPOLL_CTL_DEL, c->fd, nullptr);
                    ::close(c->fd);
                    c->fd = -1;
                    ++out->failed;
                    --alive;
                }
                continue;
            }

            for (;;) {
                uint8_t op;
                std::string payload;
                const int rc = parseFrame(c->buf, op, payload);
                if (rc == 0) break;
                if (rc < 0) {
                    ::epoll_ctl(ep, EPOLL_CTL_DEL, c->fd, nullptr);
                    ::close(c->fd);
                    c->fd = -1;
                    ++out->failed;
                    --alive;
                    break;
                }
                ++out->frames;

                if (op == 9) {
                    sendAll(c->fd, encodeMasked(0xA, payload, static_cast<uint32_t>(nowUs())));
                    continue;
                }
                if (op == 8) {
                    ::epoll_ctl(ep, EPOLL_CTL_DEL, c->fd, nullptr);
                    ::close(c->fd);
                    c->fd = -1;
                    ++out->failed;
                    --alive;
                    break;
                }
                if (op != 1) continue;

                // 找到属于自己的那条消息，计算 RTT
                const std::string prefix = "\"text\":\"b" + std::to_string(c->id) + "_";
                const size_t p = payload.find(prefix);
                if (p == std::string::npos) continue;
                size_t q = p + prefix.size();
                long long got = 0;
                while (q < payload.size() && payload[q] >= '0' && payload[q] <= '9') {
                    got = got * 10 + (payload[q] - '0');
                    ++q;
                }
                const int slot = static_cast<int>(got % kWindow);
                if (c->ts[slot] <= 0) continue;
                out->latUs.push_back(static_cast<uint32_t>(nowUs() - c->ts[slot]));
                c->ts[slot] = 0;
                ++out->sent;
                if (c->inFlight > 0) --c->inFlight;
            }
        }

        // 发送：窗口内允许就发
        const int64_t now = nowUs();
        for (auto &c : conns) {
            if (c.fd < 0) continue;
            while (c.inFlight < kWindow && (interval == 0 || now >= c.nextSend)) {
                const std::string text =
                    "b" + std::to_string(c.id) + "_" + std::to_string(c.seq);
                const std::string frame = encodeMasked(
                    1, "{\"type\":\"msg\",\"text\":\"" + text + "\"}",
                    c.maskSeed ^ static_cast<uint32_t>(c.seq));
                const int64_t t0 = nowUs();
                if (!sendAll(c.fd, frame)) {
                    ::epoll_ctl(ep, EPOLL_CTL_DEL, c.fd, nullptr);
                    ::close(c.fd);
                    c.fd = -1;
                    ++out->failed;
                    --alive;
                    break;
                }
                c.ts[c.seq % kWindow] = t0;
                ++c.seq;
                ++c.inFlight;
                if (interval > 0) {
                    c.nextSend += interval;
                } else {
                    c.nextSend = t0;
                }
                if (interval > 0 && nowUs() < c.nextSend) break;
            }
        }
    }

    for (auto &c : conns) {
        if (c.fd >= 0) ::close(c.fd);
    }
    ::close(ep);
    g_ready.fetch_add(-1);
}

double percentile(std::vector<uint32_t> &v, double p) {
    if (v.empty()) return 0;
    const size_t k = static_cast<size_t>(p * static_cast<double>(v.size() - 1));
    std::nth_element(v.begin(), v.begin() + static_cast<std::ptrdiff_t>(k), v.end());
    return static_cast<double>(v[k]) / 1000.0;
}

}  // namespace

int main(int argc, char **argv) {
    if (argc < 4) {
        std::printf("用法: %s <port> <conns> <seconds> [per_conn_rate] [client_threads]\n", argv[0]);
        return 1;
    }
    const int port = std::atoi(argv[1]);
    const int conns = std::atoi(argv[2]);
    const int seconds = std::atoi(argv[3]);
    const int rate = argc > 4 ? std::atoi(argv[4]) : 0;
    int threads = argc > 5 ? std::atoi(argv[5]) : 2;
    if (threads < 1) threads = 1;
    if (threads > conns) threads = conns;

    std::srand(12345);

    std::vector<ThreadResult> results(static_cast<size_t>(threads));
    std::vector<std::thread> pool;
    pool.reserve(static_cast<size_t>(threads));

    const int64_t t0 = nowUs();
    int assigned = 0;
    for (int t = 0; t < threads; ++t) {
        const int cnt = conns / threads + (t < conns % threads ? 1 : 0);
        pool.emplace_back(worker, t, port, assigned, cnt, rate, &results[static_cast<size_t>(t)]);
        assigned += cnt;
    }

    while (g_ready.load() < threads && nowUs() - t0 < 20 * 1000000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const int64_t loginDone = nowUs();
    std::printf("登录完成: %d 条连接 / %d 个客户端线程，耗时 %.2fs\n", conns, threads,
                static_cast<double>(loginDone - t0) / 1e6);

    g_deadline.store(nowUs() + static_cast<int64_t>(seconds) * 1000000);
    g_go.store(true, std::memory_order_release);

    const auto wallStart = std::chrono::steady_clock::now();
    for (auto &t : pool) t.join();
    const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - wallStart).count();

    long long sent = 0, frames = 0;
    int failed = 0;
    std::vector<uint32_t> all;
    for (auto &r : results) {
        sent += r.sent;
        frames += r.frames;
        failed += r.failed;
        all.insert(all.end(), r.latUs.begin(), r.latUs.end());
    }

    rusage ru{};
    getrusage(RUSAGE_SELF, &ru);
    const double cpuUser = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6;
    const double cpuSys = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;

    std::printf("\n================ 结果 ================\n");
    std::printf("连接数        : %d\n", conns);
    std::printf("压测时长      : %.2fs\n", elapsed);
    std::printf("成功往返      : %lld 条消息 (%.0f msg/s)\n", sent, sent / elapsed);
    std::printf("收到广播帧    : %lld 个 (%.0f frames/s)\n", frames, frames / elapsed);
    std::printf("失败连接      : %d\n", failed);
    if (!all.empty()) {
        std::printf("RTT  p50      : %.2f ms\n", percentile(all, 0.50));
        std::printf("RTT  p90      : %.2f ms\n", percentile(all, 0.90));
        std::printf("RTT  p99      : %.2f ms\n", percentile(all, 0.99));
        std::printf("RTT  max      : %.2f ms\n",
                    static_cast<double>(*std::max_element(all.begin(), all.end())) / 1000.0);
    }
    const long nCpu = ::sysconf(_SC_NPROCESSORS_ONLN);
    if (nCpu > 0) {
        std::printf("客户端 CPU    : user %.1fs / sys %.1fs（占 %ld 核 → %.0f%%）\n", cpuUser,
                    cpuSys, nCpu, (cpuUser + cpuSys) / elapsed * 100.0 / static_cast<double>(nCpu));
    }
    std::printf("======================================\n");
    return 0;
}
