#pragma once
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>

class Connection
{
public:
    Connection(int fd, const struct sockaddr_in &addr);
    ~Connection();
    // 禁止拷贝构造和赋值操作
    Connection(const Connection &) = delete;
    Connection &operator=(const Connection &) = delete;
    // 允许移动构造和移动赋值操作
    Connection(Connection &&) = default;
    Connection &operator=(Connection &&) = default;

    int get_fd() const { return fd_; };
    std::string get_ip() const { return ip_; };
    int get_port() const { return port_; };

private:
    int fd_;
    std::string ip_;
    int port_;
};