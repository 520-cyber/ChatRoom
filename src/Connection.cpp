#include "Connection.h"
#include <arpa/inet.h>
#include <cstdint>
Connection::Connection(int fd, const struct sockaddr_in &addr) : fd_(fd)
{
    uint32_t ip_host = ntohl(addr.sin_addr.s_addr);
    ip_ = std::to_string((ip_host >> 24) & 0xFF) + "." +
          std::to_string((ip_host >> 16) & 0xFF) + "." +
          std::to_string((ip_host >> 8) & 0xFF) + "." +
          std::to_string(ip_host & 0xFF);
    port_ = ntohs(addr.sin_port);
}

Connection::~Connection()
{
    if (fd_ != -1)
    {
        close(fd_);
        std::cout << "关闭连接: " << fd_ << std::endl;
    }
}