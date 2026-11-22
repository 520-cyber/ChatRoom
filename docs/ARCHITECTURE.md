# 架构说明

> 面向两类读者：面试前的自己，以及点开仓库想看「这人到底写了什么」的工程师。

## 1. 一句话定位

纯 C++20 手写的 WebSocket 聊天服务器：主从 Reactor（epoll）负责网络，业务层做登录 / 群聊 / 私聊 / 历史，
Redis 做昵称抢占，不依赖任何网络框架。

## 2. 分层

```
┌──────────────────────────────────────────────────────┐
│  客户端  web/index.html(WebSocket)  tools/bench.cpp   │
├──────────────────────────────────────────────────────┤
│  业务层  ChatServer · main                            │
├──────────────────────────────────────────────────────┤
│  协议层  Connection · WebSocket · Json                │
├──────────────────────────────────────────────────────┤
│  网络层  EventLoop · Buffer · ThreadPool              │
├──────────────────────────────────────────────────────┤
│  支撑    RedisClient · Util                           │
└──────────────────────────────────────────────────────┘
```

| 文件 | 行数 | 职责 |
| --- | ---: | --- |
| `include/Util.h` | 15 | 单调时钟、全局限流阈值 |
| `include/Buffer.h` | 64 | 应用层缓冲区（读写共用） |
| `include/EventLoop.h` / `src/EventLoop.cpp` | 108 / 200 | 一个 Reactor：epoll 循环 + 线程池 |
| `include/ThreadPool.h` / `src/ThreadPool.cpp` | 64 / 49 | 通用线程池，仅用于落盘 |
| `include/RedisClient.h` / `src/RedisClient.cpp` | 71 / 190 | 异步 Redis：专职线程 + 回调投回 |
| `include/Json.h` / `src/Json.cpp` | 20 / 125 | JSON 转义与取值 |
| `include/WebSocket.h` / `src/WebSocket.cpp` | 47 / 180 | 握手 + 帧编解码 |
| `include/Connection.h` / `src/Connection.cpp` | 99 / 380 | 单连接：HTTP → WebSocket 状态机 |
| `include/ChatServer.h` / `src/ChatServer.cpp` | 91 / 567 | 主从 Reactor + 全部业务 |
| `src/main.cpp` | 44 | 入口、信号、参数 |
| `web/index.html` | 488 | WebSocket 前端 |
| `tools/bench.cpp` | 502 | 压测客户端 |

## 3. 线程模型（整个项目的地基）

```
                ┌──────────────────────────────┐
                │        主 Reactor 线程        │
                │  epoll_wait 只关心 listen_fd  │
                │  accept4 → round-robin 分发   │
                └───────────┬──────────────────┘
                            │ runInLoop(fd)
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ 从 Reactor #0 │   │ 从 Reactor #1 │   │ 从 Reactor #N │
│  独立 epoll   │   │  独立 epoll   │   │  独立 epoll   │
│  独立线程     │   │  独立线程     │   │  独立线程     │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        └───────────────────┼───────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
      ┌───────────────┐           ┌───────────────┐
      │  Redis 线程   │           │  落盘线程池    │
      │ 长连接 + 重连 │           │ 聊天记录异步写 │
      └───────────────┘           └───────────────┘
```

**核心不变量**

> 一条 `Connection` 只注册在唯一一个 `EventLoop` 上，因此它的所有成员只会在那一个线程里被访问。

推论：

- `Connection` 内部（读缓冲、写缓冲、状态、用户名）**一把锁都不需要**；
- 连接表按 Reactor 分片（`connsByLoop_`），只在所属线程读写；
- 只有跨线程共享的「用户名 → 连接」索引需要 `userMtx_`。

跨线程发送数据统一走 `Connection::postSend()`，内部转成 `EventLoop::runInLoop()` 投递回所属 Reactor。

## 4. 网络层

### 4.1 Buffer

内部是 `std::vector<char> + readIdx_`：

```cpp
std::vector<char> buf_;
size_t readIdx_ = 0;
```

- `retrieve(n)` 只移动读偏移，不 `memmove`；
- 仅当「已读 > 64KB 且占容量一半以上」才 `compact()` 一次，摊还 O(1)；
- 只暴露 `peek / readable / retrieve / append`，强制调用方先判长度。

读缓冲与写缓冲共用这一个类。

### 4.2 EventLoop

一个 EventLoop = 一个 epoll fd + 一个 eventfd + 一张 `fd → 回调` 表。

```cpp
while (!quit_) {
    int n = epoll_wait(epFd_, events, 1024, 1000);   // 1s 超时
    for (i < n) {
        EventHandler h = it->second;   // 先拷贝再调用
        h(events[i].events);
    }
    doPendingFns();                    // 跨线程投递的任务
    if (tick_) tick_();                // 定时任务
}
```

两个关键细节：

1. **回调先拷贝再执行**。回调里可能 `delFd`，那会销毁 `handlers_` 里的 `std::function`；如果正在执行的就是它，属于在函数执行期间销毁自身（UB）。拷贝一份副本即可。
2. **epoll_wait 用 1s 超时而不是 -1**。让 `tick_` 能定期跑（心跳、空闲清理），同时保证 `stop()` 后最多 1 秒退出。

跨线程投递：

```cpp
void runInLoop(Fn fn) {
    if (isInLoopThread()) fn();
    else queueInLoop(std::move(fn));
}

void queueInLoop(Fn fn) {
    {
        std::lock_guard lock(mtx_);
        const bool needWake = pending_.empty();
        pending_.push_back(std::move(fn));
        if (!needWake) return;      // 队列非空就不用重复唤醒
    }
    wakeup();                        // 写 eventfd
}
```

`EventLoopThread` 在**子线程内**构造 `EventLoop`——因为 `tid_` 在构造时记录，`isInLoopThread()` 依赖它。

### 4.3 ThreadPool

三行决定正确性：

```cpp
cv_.wait(lock, [this]{ return stop_ || !tasks_.empty(); });   // 谓词等待，免疫虚假唤醒
if (stop_ && tasks_.empty()) return;                          // 队列跑完再退
try { task(); } catch (...) { /* 记录 */ }                     // 异常不 terminate
```

队列有上限，满了丢弃并计数，避免生产者过快打爆内存。项目中只用于聊天记录落盘。

## 5. 协议层

### 5.1 WebSocket

- 握手：`Sec-WebSocket-Accept = base64(sha1(clientKey + GUID))`，SHA-1 与 Base64 均为手写实现；
- 帧编码：服务端帧不加掩码，长度按 7 / 16 / 64 位三档编码；
- 帧解析返回三态：`Incomplete`（不消费任何字节）/ `Ok` / `Error`（RSV 非 0、未加掩码、超过 1MB）；
- 支持分片消息重组与控制帧（ping / pong / close）。

### 5.2 Connection

状态机：

```
HTTP ──(Upgrade 并回 101)──> WebSocket ──(close)──> Closed
  └──(普通 HTTP 响应发完)────┘
```

- **读**：循环 `recv` 到 `EAGAIN`，避免 LT 模式空转；
- **HTTP**：找 `\r\n\r\n` → 解析首行与 `Content-Length` → 先 `retrieve` 再回调（握手后剩余字节属于 WS 帧）；
- **WS**：`parseFrame` 循环 → `handleFrame` 分发；
- **写**：写缓冲 + `send` 循环 + `EAGAIN` 时等 `EPOLLOUT` 续发，修复「只调一次 send 导致响应截断」；
- **背压**：写缓冲超过 16MB 判定慢客户端并断开；
- **关闭**：延迟到本轮事件处理结束再 `delFd + close`，避免回调内自毁；
- **限流**：每连接每秒 30 条（可被 `CHAT_RATE_LIMIT` 覆盖，0 表示关闭）。

## 6. 业务层

### 6.1 ChatServer

- 主 Reactor 只挂 `listen_fd`；`acceptAll()` 循环 accept 到 `EAGAIN`，`EMFILE` 时退让 50ms；
- 新连接通过 `runInLoop` 交给某个从 Reactor，**在目标线程内构造**；
- 连接表按 Reactor 分片，无锁；
- 定时任务：120s 无数据断开、30s 发一次 WS ping。

### 6.2 登录

```cpp
redis_->command({"SET", "chat:online:" + name, "1", "NX", "EX", "3600"},
                conn->loop(), callback);
```

- `NX` 保证昵称互斥，`EX` 防止进程崩溃后昵称永久占用；
- Redis 不可用时**拒绝登录（fail-closed）**，不放行未鉴权用户；
- 回调通过 `runInLoop` 投回该连接所属的 Reactor 再执行。

### 6.3 广播（性能优化点）

```cpp
// 优化前：一条消息 N 次跨线程投递 + N 次 eventfd 唤醒
for (auto &c : targets) c->postSend(payload);

// 优化后：按 Reactor 分桶，每个 Reactor 只投递一次
std::vector<std::vector<ConnectionPtr>> byLoop(io_pool_->size());
for (auto &c : targets) byLoop[c->loopIndex()].push_back(c);
for (size_t i = 0; i < byLoop.size(); ++i) {
    if (byLoop[i].empty()) continue;
    io_pool_->get(i)->runInLoop([conns = std::move(byLoop[i]), payload] {
        for (auto &c : conns) c->send(payload);
    });
}
```

扇出到 200 人时，投递次数从 200 降到 8。压测显示 4 / 8 个从 Reactor 吞吐分别提升 57% / 66%，服务端 CPU 几乎不变。

### 6.4 历史

- 内存：`std::deque<std::string>`，上限 200 条，存**已序列化的 JSON 片段**，发送时直接拼接；
- 磁盘：`diskPool_->enqueue(...)` 异步写文件，Reactor 线程不参与 IO。

## 7. 关键调用链

### 7.1 一条消息

```
客户端 ws.send('{"type":"msg","text":"hi"}')
  └─ EventLoop::loop() 收到 EPOLLIN
      └─ Connection::onEvent → handleRead → processWebSocket
          └─ ws::parseFrame → handleFrame
              └─ ChatServer::onWsText
                  ├─ json::getString
                  ├─ Connection::allowMessage（限流）
                  └─ ChatServer::handleChat
                      ├─ buildMessageJson
                      ├─ appendHistory（内存 + 线程池落盘）
                      └─ broadcast → 每个 Reactor 一次 runInLoop
                          └─ Connection::send → ws::encodeFrame → flush
```

### 7.2 登录

```
101 握手完成 → switchToWebSocket
  └─ onWsText(type=login) → doLogin
      └─ RedisClient::command(SET NX EX)
          └─ [Redis 线程] redisCommandArgv
              └─ runInLoop 投回该 Reactor
                  └─ finishLogin：绑定用户名 / 写 users_ / 回 login_ok / 下发历史 / 广播上线
```

## 8. 设计取舍

| 选择 | 理由 | 代价 |
| --- | --- | --- |
| 主从 Reactor 而非单 Reactor | accept 与 IO 分离，扩展性好 | 线程多，跨 Reactor 投递有开销 |
| 连接绑定唯一线程（无锁） | 消除数据竞争，结构上不可能并发访问 | 单连接吞吐受限于单线程 |
| LT 而非 ET | 不会漏事件，逻辑简单 | 必须一次读到 EAGAIN，否则空转 |
| 异步 Redis | 不在事件循环里做网络 IO | 一次投递延迟，逻辑变复杂 |
| 先广播后落盘 | 消息延迟低 | 进程崩溃会丢最后几条 |
| 写缓冲 16MB 上限 | 防止慢客户端吃光内存 | 极端场景会主动断开客户端 |

## 9. 面试提纲

按问题顺序讲，不要按文件顺序讲：

1. **怎么处理并发连接？** → 主从 Reactor（`ChatServer` + `EventLoop`）
2. **多线程怎么保证不出数据竞争？** → 连接绑定唯一 Reactor，连接内部零锁（`Connection`）
3. **粘包怎么办？** → `Buffer` + 长度定界（`Buffer` + `Connection`）
4. **Redis 访问会不会阻塞？** → 专职线程 + 回调投回（`RedisClient`）
5. **慢客户端怎么办？** → 写缓冲 + `EPOLLOUT` + 16MB 背压（`Connection`）
6. **瓶颈在哪、怎么优化的？** → 广播投递，按 Reactor 批量（`ChatServer::broadcast`）

压测数据与复现方式见 [`BENCHMARK.md`](BENCHMARK.md) 与 [`bench-raw/`](bench-raw/)。
