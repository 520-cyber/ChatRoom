#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include <thread>
#include <atomic>
#define PORT 8888

std::atomic<bool> g_running{true};
// 子程序接收消息
void receive_thread(int sock_fd)
{
    char buffer[1024];
    while (g_running)
    {
        memset(buffer, 0, sizeof(buffer));
        int len = recv(sock_fd, buffer, sizeof(buffer) - 1, 0);

        if (len > 0)
        {
            // 收到消息直接打印
            std::cout << "\n[新消息]: " << buffer << "\n请输入: " << std::flush;
        }
        else if (len == 0)
        {
            if (g_running)
            {
                std::cout << "\n服务器已关闭连接。" << std::endl;
            }
            g_running = false;
            break;
        }
        else
        {
            break;
        }
    }
}

int main()
{
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd == -1)
    {
        std::cerr << "创建 socket 失败！" << std::endl;
        return -1;
    }

    struct sockaddr_in server_addr{
        .sin_family = AF_INET,
        .sin_port = htons(PORT)};
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1)
    {
        std::cerr << "连接失败！请确认服务器已启动。" << std::endl;
        close(sock_fd);
        return -1;
    }
    std::cout << "连接成功！可以开始聊天了..." << std::endl;

    // 启动一个子线程专门负责接收消息
    std::thread recv_t(receive_thread, sock_fd);
    recv_t.detach();

    // 主线程发送消息
    while (g_running)
    {
        std::string data;
        std::cout << "请输入: ";
        std::getline(std::cin, data);

        if (data.empty())
            continue;
        if (data == "exit")
            break;
        // 检查异常
        if (send(sock_fd, data.c_str(), data.size(), 0) == -1)
        {
            std::cerr << "发送消息失败！" << std::endl;
            break;
        }
    }
    // 退出时关闭socket并清理
    g_running = false;
    shutdown(sock_fd, SHUT_RDWR);
    std::cout << "客户端已退出。" << std::endl;
    close(sock_fd);
    return 0;
}