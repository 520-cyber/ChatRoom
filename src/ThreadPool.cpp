#include "ThreadPool.h"

#include <iostream>

ThreadPool::ThreadPool(std::size_t numThreads, std::size_t maxQueue)
    : maxQueue_(maxQueue) {
    if (numThreads == 0) numThreads = 1;
    workers_.reserve(numThreads);
    for (std::size_t i = 0; i < numThreads; ++i) {
        workers_.emplace_back([this] { workerLoop(); });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto &w : workers_) {
        if (w.joinable()) w.join();
    }
}

void ThreadPool::workerLoop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            // 谓词版 wait：免疫虚假唤醒
            cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });

            // 收到停止信号：队列已空才退出，剩下的任务先跑完（优雅关闭）
            if (stop_ && tasks_.empty()) return;

            task = std::move(tasks_.front());
            tasks_.pop();
        }

        // 任何任务抛异常都不能 terminate 整个进程
        try {
            task();
        } catch (const std::exception &e) {
            std::cerr << "[ThreadPool] 任务异常: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[ThreadPool] 任务未知异常" << std::endl;
        }
    }
}
