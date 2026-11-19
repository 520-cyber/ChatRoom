#include "EventLoop.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>

EventLoop::EventLoop() : tid_(std::this_thread::get_id()) {
    epFd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epFd_ < 0) {
        std::cerr << "[EventLoop] epoll_create1 失败: " << std::strerror(errno) << std::endl;
        std::abort();
    }

    wakeFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeFd_ < 0) {
        std::cerr << "[EventLoop] eventfd 失败: " << std::strerror(errno) << std::endl;
        std::abort();
    }

    events_.resize(1024);
    addFd(wakeFd_, EPOLLIN, [this](uint32_t) { handleWakeup(); });
}

EventLoop::~EventLoop() {
    if (wakeFd_ >= 0) ::close(wakeFd_);
    if (epFd_ >= 0) ::close(epFd_);
}

void EventLoop::loop() {
    while (!quit_.load(std::memory_order_relaxed)) {
        // 1s 超时：既让 tick 定时器能跑，也保证 stop() 之后最多 1s 内退出
        const int n =
            ::epoll_wait(epFd_, events_.data(), static_cast<int>(events_.size()), 1000);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[EventLoop] epoll_wait 失败: " << std::strerror(errno) << std::endl;
            break;
        }

        for (int i = 0; i < n; ++i) {
            const int fd = events_[i].data.fd;
            auto it = handlers_.find(fd);
            if (it == handlers_.end()) continue;

            // 回调里可能 delFd（例如连接被关闭），
            // 所以先拷贝一份再调用，避免 std::function 在自身执行期间被销毁。
            EventHandler handler = it->second;
            handler(events_[i].events);
        }

        doPendingFns();

        if (tick_) tick_();
    }
}

void EventLoop::stop() {
    quit_.store(true, std::memory_order_relaxed);
    wakeup();
}

void EventLoop::runInLoop(Fn fn) {
    if (isInLoopThread()) {
        fn();
    } else {
        queueInLoop(std::move(fn));
    }
}

void EventLoop::queueInLoop(Fn fn) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // 队列非空说明 loop 已经被唤醒过一次、迟早会处理这批任务，
        // 此时再写 eventfd 只是白白多一次系统调用。
        const bool needWake = pending_.empty();
        pending_.push_back(std::move(fn));
        if (!needWake) return;
    }
    wakeup();
}

void EventLoop::addFd(int fd, uint32_t events, EventHandler handler) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;

    handlers_[fd] = std::move(handler);
    if (::epoll_ctl(epFd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        if (errno == EEXIST) {
            ::epoll_ctl(epFd_, EPOLL_CTL_MOD, fd, &ev);
        } else {
            std::cerr << "[EventLoop] epoll_ctl ADD fd=" << fd
                      << " 失败: " << std::strerror(errno) << std::endl;
            handlers_.erase(fd);
        }
    }
}

void EventLoop::modFd(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epFd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        std::cerr << "[EventLoop] epoll_ctl MOD fd=" << fd
                  << " 失败: " << std::strerror(errno) << std::endl;
    }
}

void EventLoop::delFd(int fd) {
    ::epoll_ctl(epFd_, EPOLL_CTL_DEL, fd, nullptr);
    handlers_.erase(fd);
}

void EventLoop::wakeup() {
    const uint64_t one = 1;
    const ssize_t n = ::write(wakeFd_, &one, sizeof(one));
    (void)n;
}

void EventLoop::handleWakeup() {
    uint64_t v;
    while (::read(wakeFd_, &v, sizeof(v)) > 0) {
    }
}

void EventLoop::doPendingFns() {
    std::vector<Fn> fns;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        fns.swap(pending_);
    }
    for (auto &fn : fns) fn();
}

// ---------------- EventLoopThread ----------------

EventLoopThread::~EventLoopThread() {
    stop();
    join();
}

EventLoop *EventLoopThread::start() {
    std::unique_lock<std::mutex> lock(mtx_);
    thread_ = std::thread([this] {
        EventLoop loop;  // 栈上创建，线程退出时自动析构
        {
            std::lock_guard<std::mutex> guard(mtx_);
            loop_ = &loop;
        }
        cv_.notify_one();
        loop.loop();
        {
            std::lock_guard<std::mutex> guard(mtx_);
            loop_ = nullptr;
        }
    });
    cv_.wait(lock, [this] { return loop_ != nullptr; });
    return loop_;
}

void EventLoopThread::stop() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (loop_) loop_->stop();
}

void EventLoopThread::join() {
    if (thread_.joinable()) thread_.join();
}

// ---------------- EventLoopThreadPool ----------------

EventLoopThreadPool::EventLoopThreadPool(size_t n) : n_(n) {}

EventLoopThreadPool::~EventLoopThreadPool() { stop(); }

void EventLoopThreadPool::start() {
    if (!threads_.empty()) return;
    threads_.reserve(n_);
    loops_.reserve(n_);
    for (size_t i = 0; i < n_; ++i) {
        auto t = std::make_unique<EventLoopThread>();
        loops_.push_back(t->start());
        threads_.push_back(std::move(t));
    }
}

EventLoop *EventLoopThreadPool::next() {
    if (loops_.empty()) return nullptr;
    EventLoop *loop = loops_[next_ % loops_.size()];
    ++next_;
    return loop;
}

void EventLoopThreadPool::stop() {
    for (auto &t : threads_) t->stop();
    for (auto &t : threads_) t->join();
}
