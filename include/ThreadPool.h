#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

// 通用线程池。在本项目中只用于「一定不能阻塞 Reactor」的任务，
// 目前是聊天记录的异步落盘。
//
// 相对旧版的改动：
//   1. 任务执行包 try/catch —— 任何一个任务抛异常不会 terminate 整个进程；
//   2. 队列有上限，满了直接丢弃并计数，避免生产者跑太快把内存打爆；
//   3. 析构时把队列里剩余任务跑完再退出（优雅关闭）。
class ThreadPool {
public:
    explicit ThreadPool(std::size_t numThreads, std::size_t maxQueue = 100000);
    ~ThreadPool();

    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;

    template <typename F, typename... Args>
    bool enqueue(F &&f, Args &&...args) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (stop_) return false;
            if (tasks_.size() >= maxQueue_) {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            tasks_.emplace([f = std::forward<F>(f), ... args = std::forward<Args>(args)]() mutable {
                f(std::move(args)...);
            });
        }
        cv_.notify_one();
        return true;
    }

    std::size_t pending() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return tasks_.size();
    }

    std::uint64_t dropped() const { return dropped_.load(std::memory_order_relaxed); }

private:
    void workerLoop();

    std::vector<std::thread> workers_;
    mutable std::mutex queue_mutex_;
    std::queue<std::function<void()>> tasks_;
    std::condition_variable cv_;
    std::size_t maxQueue_;
    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> dropped_{0};
};
