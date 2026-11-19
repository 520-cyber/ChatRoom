#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

inline int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 每条连接每秒最多发送多少条业务消息。
// 默认 30，可通过环境变量 CHAT_RATE_LIMIT 覆盖（压测时临时放开）。
inline std::atomic<int> g_rateLimitPerSec{30};
