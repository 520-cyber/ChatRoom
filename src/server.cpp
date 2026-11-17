#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <vector>
#include <mutex>
#include <algorithm>
#include <string>
#include <memory>
#include <unordered_map>
#include <fstream>
#include <hiredis/hiredis.h>

#include "Connection.h"
#include "ThreadPool.h"

#define MAX_EVENTS 1024
#define PORT 8888

std::unordered_map<int, std::shared_ptr<Connection>> connections;
std::mutex connections_mutex;

std::vector<std::string> chat_history;
std::mutex chat_mutex;

int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

std::string build_http_response(const std::string &content, const std::string &type)
{
    return "HTTP/1.1 200 OK\r\nContent-Type: " + type + "\r\nContent-Length: " + std::to_string(content.length()) + "\r\nConnection: close\r\n\r\n" + content;
}

int main()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr{
        .sin_family = AF_INET,
        .sin_port = htons(PORT),
        .sin_addr = {.s_addr = INADDR_ANY}};

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1)
        return -1;
    if (listen(server_fd, 128) == -1)
        return -1;

    set_nonblocking(server_fd);

    int epfd = epoll_create(1);
    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    redisContext *redis = redisConnect("127.0.0.1", 6379);
    if (redis == nullptr || redis->err)
    {
        std::cerr << "Redis 连接失败！" << std::endl;
        return -1;
    }
    std::cout << "Redis 连接成功！" << std::endl;
    redisFree(redis);

    std::cout << "服务器已启动，请在浏览器中访问: http://localhost:" << PORT << std::endl;
    ThreadPool pool(8);

    while (true)
    {
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        for (int i = 0; i < nfds; ++i)
        {
            int active_fd = events[i].data.fd;

            if (active_fd == server_fd)
            {
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);

                if (client_fd != -1)
                {
                    set_nonblocking(client_fd);
                    auto conn = std::make_shared<Connection>(client_fd, client_addr);
                    {
                        std::lock_guard<std::mutex> lock(connections_mutex);
                        connections[client_fd] = conn;
                    }
                    ev.events = EPOLLIN;
                    ev.data.fd = client_fd;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev);
                }
            }
            else
            {
                pool.enqueue([active_fd, epfd]()
                             {
                    // 将缓冲区扩大，防止读取长消息截断
                    char buffer[8192];
                    memset(buffer, 0, sizeof(buffer));
                    int bytes_read = recv(active_fd, buffer, sizeof(buffer) - 1, 0);
                    
                    if (bytes_read > 0) {
                        std::string request(buffer);
                        std::string response;

                        // 防截断保护：如果数据过大，直接踢掉连接
                        if (request.length() >= 8191) {
                            std::cerr << "警告：请求数据过大被截断！" << std::endl;
                            epoll_ctl(epfd, EPOLL_CTL_DEL, active_fd, nullptr);
                            std::lock_guard<std::mutex> lock(connections_mutex);
                            connections.erase(active_fd);
                            return;
                        }

                        // 1. 网页请求
                        if (request.find("GET / ") == 0 || request.find("GET /index.html") == 0) {
                            std::ifstream file("../web/index.html");
                            if (file.is_open()) {
                                std::string html((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
                                response = build_http_response(html, "text/html; charset=utf-8");
                            } else {
                                response = "HTTP/1.1 404 Not Found\r\n\r\n找不到网页文件";
                            }
                        }
                        // 2. 登录请求
                        else if (request.find("POST /login") == 0) {
                            size_t pos = request.find("\r\n\r\n");
                            if (pos != std::string::npos) {
                                std::string username = request.substr(pos + 4);
                                
                                redisContext* redis = redisConnect("127.0.0.1", 6379);
                                if (redis != nullptr && redis->err == 0) {
                                    std::string cmd = "SET user:" + username + " online";
                                    redisReply* reply = (redisReply*)redisCommand(redis, cmd.c_str());
                                    freeReplyObject(reply);
                                    redisFree(redis);
                                }
                                std::cout << "[系统] 用户 " << username << " 登录成功！" << std::endl;
                            }
                            response = build_http_response("OK", "text/plain");
                        }
                        // 3. 发送消息请求
                        else if (request.find("POST /send") == 0) {
                            size_t pos = request.find("\r\n\r\n");
                            if (pos != std::string::npos) {
                                std::string payload = request.substr(pos + 4);
                                std::lock_guard<std::mutex> lock(chat_mutex);
                                chat_history.push_back(payload);
                                if (chat_history.size() > 100) {
                                    chat_history.erase(chat_history.begin());
                                }
                            }
                            response = build_http_response("OK", "text/plain");
                        }
                        // 4. 拉取消息请求
                        else if (request.find("POST /get_msgs") == 0) {
                            std::string requester = "";
                            size_t user_pos = request.find("X-User: ");
                            if (user_pos != std::string::npos) {
                                user_pos += 8;
                                size_t end_pos = request.find("\r\n", user_pos);
                                requester = request.substr(user_pos, end_pos - user_pos);
                            }

                            std::string json = "[";
                            std::lock_guard<std::mutex> lock(chat_mutex);
                            
                            bool first_msg = true;

                            for (size_t i = 0; i < chat_history.size(); ++i) {
                                std::string full_msg = chat_history[i];
                                size_t split_pos = full_msg.find("|||");
                                if (split_pos == std::string::npos) continue;

                                std::string sender = full_msg.substr(0, split_pos);
                                std::string msg = full_msg.substr(split_pos + 3);

                                if (msg.empty()) continue;

                                bool should_send = false;
                                
                                if (msg[0] != '@') {
                                    if (sender != requester) {
                                        should_send = true;
                                    }
                                } 
                                else {
                                    size_t space_pos = msg.find(' ');
                                    if (space_pos != std::string::npos) {
                                        std::string receiver = msg.substr(1, space_pos - 1);
                                        if (receiver == requester && sender != requester) {
                                            should_send = true;
                                            msg = "【私聊】 " + msg; 
                                        }
                                    }
                                }

                                if (should_send) {
                                    if (!first_msg) json += ",";
                                    json += "{\"sender\":\"" + sender + "\",\"text\":\"" + msg + "\"}";
                                    first_msg = false;
                                }
                            }
                            json += "]";
                            response = build_http_response(json, "application/json");
                        }
                        else {
                            response = "HTTP/1.1 404 Not Found\r\n\r\n";
                        }

                        send(active_fd, response.c_str(), response.length(), MSG_NOSIGNAL);
                        epoll_ctl(epfd, EPOLL_CTL_DEL, active_fd, nullptr);
                        std::lock_guard<std::mutex> lock(connections_mutex);
                        connections.erase(active_fd);
                    } 
                    else if (bytes_read == 0) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, active_fd, nullptr);
                        std::lock_guard<std::mutex> lock(connections_mutex);
                        connections.erase(active_fd); 
                    }
                    else if (bytes_read == -1) {
                        if (errno != EAGAIN && errno != EWOULDBLOCK) {
                             epoll_ctl(epfd, EPOLL_CTL_DEL, active_fd, nullptr);
                             std::lock_guard<std::mutex> lock(connections_mutex);
                             connections.erase(active_fd);
                        }
                    } });
            }
        }
    }
    return 0;
}