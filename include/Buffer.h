#pragma once

#include <cstddef>
#include <string>
#include <vector>

// 应用层缓冲区。读缓冲、写缓冲共用这一份实现。
//
// 设计要点：
//   1. 内部是 vector<char> + 读偏移 readIdx_，retrieve 只移动偏移，不做 memmove；
//   2. 只有偏移过大、且浪费超过一半时才 compact 一次，摊还 O(1)；
//   3. 不提供 operator[] 越界访问，只暴露 peek/readable/retrieve 三个动作，
//      调用方必须先判 readable() 再用 peek()。
class Buffer {
public:
    Buffer() = default;

    size_t readable() const { return buf_.size() - readIdx_; }
    bool empty() const { return readable() == 0; }

    const char *peek() const { return buf_.empty() ? "" : buf_.data() + readIdx_; }

    // 丢弃前 n 字节（n 大于可读长度时等价于 clear）
    void retrieve(size_t n) {
        if (n >= readable()) {
            buf_.clear();
            readIdx_ = 0;
            return;
        }
        readIdx_ += n;
        // 只有“已读部分既多、又占了一半以上”才搬一次，避免频繁 memmove
        if (readIdx_ > 64 * 1024 && readIdx_ * 2 > buf_.size()) {
            compact();
        }
    }

    void append(const void *data, size_t n) {
        if (n == 0) return;
        if (readIdx_ > 0 && buf_.size() + n > buf_.capacity()) {
            compact();
        }
        const char *p = static_cast<const char *>(data);
        buf_.insert(buf_.end(), p, p + n);
    }

    void append(const std::string &s) { append(s.data(), s.size()); }

    std::string toString() const { return std::string(peek(), readable()); }

    void clear() {
        buf_.clear();
        readIdx_ = 0;
    }

private:
    void compact() {
        if (readIdx_ == 0) return;
        buf_.erase(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(readIdx_));
        readIdx_ = 0;
    }

    std::vector<char> buf_;
    size_t readIdx_ = 0;
};
