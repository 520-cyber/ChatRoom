#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>

#include "ChatServer.h"

namespace {

void onSignal(int) {
    // 只做一个原子置位，真正的关闭动作在主循环的定时器里完成
    requestStop();
}

int parsePositive(const char *s, int fallback) {
    try {
        const int v = std::stoi(s);
        return v > 0 ? v : fallback;
    } catch (...) {
        return fallback;
    }
}

}  // namespace

int main(int argc, char **argv) {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    int port = 8888;
    int ioThreads = 2;
    if (argc > 1) port = parsePositive(argv[1], 8888);
    if (argc > 2) ioThreads = parsePositive(argv[2], 2);

    try {
        ChatServer server(port, static_cast<size_t>(ioThreads));
        server.run();
    } catch (const std::exception &e) {
        std::cerr << "[致命] " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
