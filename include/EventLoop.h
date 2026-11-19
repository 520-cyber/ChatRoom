#pragma once

#include <sys/epoll.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

// 一个 EventLoop 就是一个 Reactor：持有独立的 epoll fd 和一批 fd -> 回调。
//
// 线程模型（这是“主从 Reactor”的地基）：
//   * 每个 EventLoop 只被一个线程 loop()，所以 loop 内部所有回调天然串行；
//   * 一条连接只注册到某一个 EventLoop 上，因此这条连接的所有读写、
//     所有状态变更都只发生在那一个线程里 —— 不需要给 Connection 加锁；
//   * 跨线程投递统一走 runInLoop()：内部用 eventfd 唤醒 epoll_wait。
//
// 易错点：EventLoop 必须在将要执行 loop() 的那个线程里构造，
// 因为构造时记录的 tid_ 被用来判断 isInLoopThread()。
class EventLoop {
public:
    using Fn = std::function<void()>;
    using EventHandler = std::function<void(uint32_t events)>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop &) = delete;
    EventLoop &operator=(const EventLoop &) = delete;

    void loop();
    void stop();

    // 线程安全：在 loop 线程内直接执行，否则入队 + 唤醒
    void runInLoop(Fn fn);
    void queueInLoop(Fn fn);

    // 下面三个只允许在 loop 线程内调用
    void addFd(int fd, uint32_t events, EventHandler handler);
    void modFd(int fd, uint32_t events);
    void delFd(int fd);

    // 每个 tick（约 1s）执行一次，用于空闲连接清理、心跳等
    void setTickCallback(Fn fn) { tick_ = std::move(fn); }

    bool isInLoopThread() const { return tid_ == std::this_thread::get_id(); }
    int epfd() const { return epFd_; }

private:
    void wakeup();
    void handleWakeup();
    void doPendingFns();

    int epFd_ = -1;
    int wakeFd_ = -1;
    std::atomic<bool> quit_{false};
    std::thread::id tid_;
    std::mutex mtx_;
    std::vector<Fn> pending_;
    std::unordered_map<int, EventHandler> handlers_;
    std::vector<epoll_event> events_;
    Fn tick_;
};

// 一个自带线程的 EventLoop
class EventLoopThread {
public:
    EventLoopThread() = default;
    ~EventLoopThread();

    EventLoopThread(const EventLoopThread &) = delete;
    EventLoopThread &operator=(const EventLoopThread &) = delete;

    EventLoop *start();
    void stop();
    void join();

private:
    EventLoop *loop_ = nullptr;
    std::thread thread_;
    std::mutex mtx_;
    std::condition_variable cv_;
};

// 从 Reactor 池：主 Reactor 把新连接按 round-robin 交给它们
class EventLoopThreadPool {
public:
    explicit EventLoopThreadPool(size_t n);
    ~EventLoopThreadPool();

    void start();
    EventLoop *next();
    EventLoop *get(size_t i) { return loops_[i]; }
    size_t size() const { return loops_.size(); }

    void stop();

private:
    size_t n_ = 0;
    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop *> loops_;
    size_t next_ = 0;
};
