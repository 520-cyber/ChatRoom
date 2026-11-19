# 压测原始输出

这里保存的是 [`docs/BENCHMARK.md`](../BENCHMARK.md) 里所有数字的**原始终端输出**，
用来回答「这些数据是真的吗、怎么跑出来的」。

## 文件索引

| 文件 | 对应测试 | 说明 |
| --- | --- | --- |
| `final.txt` | 优化前基线 | 1000 连接建立；200 连接 × 20 msg/s 延迟；1/2/4/8 从 Reactor 吞吐；wrk 静态页 |
| `scaling.txt` | 优化后 | 同一组 1/2/4/8 从 Reactor 吞吐，用于和 `final.txt` 对比 |
| `latency.txt` | 优化后 | 200 连接 4000 msg/s 的 P50/P90/P99；1000 连接低速；wrk |
| `out.txt` | 单次运行 | `./build/bench` 的一次完整输出（含登录耗时、客户端 CPU 占用） |

`final.txt` 与 `scaling.txt` 是配套的一对：

- `final.txt` 是**广播批量化优化之前**跑的
- `scaling.txt` 是**优化之后**跑的

两者对比即得到 BENCHMARK.md 中「4 / 8 从 Reactor 吞吐分别提升 57% / 66%」。

## 测试环境

| 项 | 值 |
| --- | --- |
| 运行环境 | WSL2 / Ubuntu 24.04 |
| CPU | 8 vCPU（宿主 Intel Core Ultra 7 155H） |
| 内存 | 8 GB |
| 编译 | g++ 13.3，Release（-O2，零警告） |
| 网络 | 本机 loopback（客户端与服务端同机） |
| 依赖 | Redis 7 |

**注意**：压测客户端和服务端跑在同一台机器上，抢同样的 8 个核。

所以绝对吞吐偏低，只适合做横向对比（不同从 Reactor 数量、优化前后）。
面试时这一点要主动说明，否则会被认为在虚报数据。

## 复现方法

```bash
# 1. 编译
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# 2. 起 Redis
redis-server --daemonize yes

# 3. 起服务端：端口 8888，4 个从 Reactor
CHAT_RATE_LIMIT=0 ./build/server 8888 4

# 4. 压测
./build/bench 8888 200 10 0 4     # 200 连接 / 10 秒 / 不限速 / 4 个客户端线程
./build/bench 8888 200 10 20 4    # 200 连接 / 每条每秒 20 条
```

`CHAT_RATE_LIMIT=0` 表示关闭服务端每连接 30 条/秒的限流（默认开启）。
不关的话压测会被限流截断，测不到真实上限。

## 术语

- **msg/s**：服务端成功处理并广播的聊天消息条数
- **frames/s**：扇出到所有客户端的 WebSocket 帧总数，约等于 `msg/s × 在线人数`
- **RTT**：客户端发出消息 → 该消息经服务端广播回到自己 的端到端时间，含排队时间
- **服务端 CPU**：采样 `/proc/<pid>/stat` 的 utime + stime 差值，除以实际压测时长
