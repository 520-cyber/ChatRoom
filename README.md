# 高并发聊天室 · Epoll 主从 Reactor + WebSocket

纯 C++20 手写，不依赖任何网络框架（Drogon / muduo 都没有）。前后端分离，前端是单文件
HTML，后端是 Linux 下的 epoll 服务器。

---

## 1. 架构

```
                        ┌──────────────────────────────┐
                        │        主 Reactor 线程        │
                        │  epoll_wait 只关心 listen_fd  │
                        │  accept4 → round-robin 分发   │
                        └───────────┬──────────────────┘
                                    │ runInLoop(fd)
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
      ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
      │ 从 Reactor #0 │     │ 从 Reactor #1 │     │ 从 Reactor #N │
      │  独立 epoll   │     │  独立 epoll   │     │  独立 epoll   │
      │  独立线程     │     │  独立线程     │     │  独立线程     │
      └───────┬───────┘     └───────┬───────┘     └───────┬───────┘
              │  一条连接只属于一个 Reactor，                              │
              │  读写/状态变更全在这一个线程里 → 无需加锁                    │
              └─────────────────────┬─────────────────────┘
                                    │
                     ┌──────────────┴──────────────┐
                     ▼                             ▼
            ┌─────────────────┐          ┌─────────────────┐
            │  Redis 线程     │          │  落盘线程池      │
            │  长连接 + 重连  │          │  聊天记录异步写  │
            │  回调投回 Reactor│          │  绝不阻塞 Reactor│
            └─────────────────┘          └─────────────────┘
```

**线程不变量（整个项目最重要的一条）**

> 一条 `Connection` 只注册在唯一一个 `EventLoop` 上，因此它的所有成员
> 只会在那一个线程里被访问。跨线程发送数据必须走 `postSend()`，
> 内部转成 `runInLoop()` 投递回所属 Reactor。

这条不变量成立之后，`Connection` 内部一个锁都不需要，数据竞争和
"两个线程同时 recv 同一条连接" 这类问题在结构上就不存在了。

---

## 2. 目录

```
include/
  Buffer.h          读写缓冲区（vector<char> + 读偏移，避免 memmove）
  EventLoop.h       一个 Reactor：epoll + 任务队列 + eventfd 唤醒
  Connection.h      一条连接：HTTP 解析 / WebSocket / 写缓冲 / 背压
  WebSocket.h       RFC6455 握手、帧编解码（SHA1 + Base64 + 掩码 + 分片）
  ChatServer.h      主 Reactor + 业务（登录/群聊/私聊/历史/限流/超时）
  RedisClient.h     异步 Redis：专职线程 + 长连接 + 回调投回 Reactor
  ThreadPool.h      通用线程池（仅用于落盘这类阻塞任务）
  Json.h            极简 JSON：转义 / 单层对象取值
  Util.h            nowMs()
src/
  main.cpp          入口、信号处理
  EventLoop.cpp     事件循环、EventLoopThread(Pool)
  Connection.cpp    HTTP 解析、WebSocket 帧处理、发送与关闭
  ChatServer.cpp    accept 分发、业务路由、广播、定时清理
  RedisClient.cpp   hiredis 封装（redisCommandArgv，杜绝格式串注入）
  WebSocket.cpp     SHA1 / Base64 / 握手 / 帧解析
  Json.cpp          JSON 转义与解析
  ThreadPool.cpp    线程池实现
web/
  index.html        WebSocket 前端（玻璃拟态 UI）
```

---

## 3. 构建与运行

```bash
# 依赖：cmake >= 3.16、g++ >= 10、hiredis 开发包、redis-server
sudo apt install build-essential cmake libhiredis-dev redis-server

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# 先确保 Redis 在跑
redis-server --daemonize yes
redis-cli ping            # 期望 PONG

./build/server 8888 2     # 端口 8888，2 个从 Reactor 线程
```

浏览器打开 <http://127.0.0.1:8888>。

### Sanitizer 构建

```bash
cmake -S . -B build-asan -DENABLE_ASAN=ON -DCMAKE_BUILD_TYPE=Debug && cmake --build build-asan -j
cmake -S . -B build-tsan -DENABLE_TSAN=ON -DCMAKE_BUILD_TYPE=Debug && cmake --build build-tsan -j
# WSL 下 TSan 需要用 setarch 关闭 ASLR：
setarch $(uname -m) -R ./build-tsan/server 8888 2
```

---

## 4. 通信协议

握手走标准 HTTP Upgrade：`GET /ws`，校验 `Sec-WebSocket-Key`，
返回 `101` + `Sec-WebSocket-Accept`。之后全部是 WebSocket 文本帧，
每帧一个 JSON 对象。

客户端 → 服务端：

```json
{"type":"login","name":"张三"}
{"type":"msg","text":"大家好"}
{"type":"msg","text":"@李四 这是私聊"}
{"type":"ping"}
```

服务端 → 客户端：

```json
{"type":"login_ok","name":"张三","online":3}
{"type":"history","msgs":[{"type":"msg","from":"李四","text":"hi","ts":1700000000000}]}
{"type":"msg","from":"张三","text":"大家好","ts":1700000000000}
{"type":"msg","from":"张三","text":"这是私聊","ts":1700000000000,"pm":true}
{"type":"sys","text":"李四 加入了聊天室"}
{"type":"error","text":"昵称已被占用，换一个吧"}
```

**身份不来自报文**。登录之后，服务端把这个昵称绑定在 `Connection` 上，
后续所有消息的 `from` 都取自服务端状态，客户端无法冒充别人。

---

## 5. 关键设计

| 问题 | 做法 |
| --- | --- |
| 粘包/半包 | `Buffer` + 按 `\r\n\r\n` / `Content-Length` 切分；WS 按帧头长度切分，不完整就攒着 |
| 部分发送 | 写缓冲 + `send` 循环 + `EPOLLOUT` 续发，发完才关 |
| 慢客户端 | 写缓冲超过 16MB 直接断开（背压） |
| 空闲连接 | 120s 无数据断开；WS 每 30s 发 ping |
| 大包攻击 | 请求头 8KB / body 1MB / WS 帧 1MB / 单条消息 8KB 上限 |
| 刷屏 | 每连接每秒 30 条令牌桶限流 |
| Redis 阻塞 | 专职线程 + 长连接，回调 `runInLoop` 投回 Reactor |
| Redis 故障 | 登录 fail-closed：拿不到结果就拒绝，不放行未鉴权用户 |
| 昵称抢占 | `SET chat:online:<name> 1 NX EX 3600`，原子操作；断线 `DEL` |
| 优雅退出 | SIGINT/SIGTERM → 原子标志 → 主循环退出 → 各 Reactor stop → join |

---

## 6. 验证

| 项 | 结果 |
| --- | --- |
| 功能用例（登录/重名/未登录/非法昵称/群聊/私聊/历史） | 7 组全部通过 |
| 100 条长连接 × 3 条消息，广播 300 条 | 抽样客户端 300/300，零丢失 |
| ThreadSanitizer（含上述全部用例） | 0 warnings |
| AddressSanitizer + LeakSanitizer | 0 errors |
| 优雅退出（SIGTERM） | 1s 内退出，无残留 |

---

## 7. 与旧版（单 Reactor + 线程池 + 轮询）的差异

| 旧实现 | 问题 | 现在 |
| --- | --- | --- |
| `epoll_wait` 后直接把 fd 丢进线程池 | LT 模式下同一 fd 被重复投递，多线程同时 `recv` 同一条连接，`std::string` 并发写 → 堆破坏 | 连接绑定唯一 Reactor，I/O 只在一个线程 |
| `connections.find()` 不加锁 | `unordered_map` 并发读写 UB，随机 crash | 连接表按 Reactor 分片，无跨线程访问 |
| `send` 只调一次、丢返回值 | 非阻塞 socket 部分发送，响应被截断 | 写缓冲 + 循环 + EPOLLOUT |
| 字符串拼 JSON | 一个引号让所有人 `/get_msgs` 失败 | `json::escape` 全字段转义 |
| `redisCommand(redis, cmd.c_str())` | printf 变参 + 用户输入 = 格式化字符串漏洞 | `redisCommandArgv`，参数与命令分离 |
| 同步 Redis + 每请求新建连接 | 阻塞事件循环、连接风暴、失败路径泄漏 context | 长连接 + 专职线程 + 回调投递 |
| `X-User` 头决定身份 | 改个头就能读别人私聊 | 身份绑定在服务端连接上 |
| 800ms 轮询 + 每次返回全量历史 | O(人数 × 历史) 带宽，短连接风暴 | WebSocket 推送 + 登录时一次性历史 |
| `Connection(Connection&&) = default` + 析构 close | 移动后双 close | 移动构造直接 delete |
| 工作线程无 try/catch | 一个异常 terminate 整个进程 | 线程池统一捕获 |
| `../web/index.html` 相对路径 | 换个目录启动就 404 | 编译期写入绝对路径 |

---

## 8. 性能压测

压测客户端在 `tools/bench.cpp`（epoll 多路复用：少量线程管大量连接，
一条连接一个线程的写法会把客户端自己的调度延迟算进 RTT）。

```bash
./build/server 8888 4              # 端口、从 Reactor 数量
./build/bench 8888 200 10 20 4     # 200 连接 / 10 秒 / 每条 20 msg/s / 4 个客户端线程
./build/bench 8888 200 10 0 4      # 不限速，压到饱和
```

关键数据（WSL2 / 8 vCPU / 客户端与服务端同机，详见 [docs/BENCHMARK.md](docs/BENCHMARK.md)）：

| 场景 | 结果 |
| --- | --- |
| 1000 条长连接建立（WebSocket 握手 + Redis 校验） | 0.42 s，0 失败 |
| 200 连接 × 20 msg/s（扇出 80 万帧/s） | P50 4.7 ms / P99 24.7 ms，服务端 2.8 核 |
| 200 连接不限速（8 从 Reactor） | 9,407 msg/s，扇出 176 万帧/s |
| HTTP 静态页（wrk） | 84,128 req/s，P50 1.31 ms |

广播路径做了两处优化——"按 Reactor 批量投递"替代"每条连接投递一次"、
eventfd 唤醒去重——4 / 8 从 Reactor 的吞吐分别提升 **57% / 66%**，
而服务端 CPU 几乎不变。

---

## 9. 文档索引

| 文档 | 内容 |
| --- | --- |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | 分层、线程模型、逐文件职责、关键调用链、设计取舍、面试提纲 |
| [`docs/BENCHMARK.md`](docs/BENCHMARK.md) | 压测环境、方法论、五组数据、优化前后对比、局限性 |
| [`docs/bench-raw/`](docs/bench-raw/) | 压测的原始终端输出与复现命令 |

