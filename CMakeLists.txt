cmake_minimum_required(VERSION 3.16)
project(HighConcurrencyServer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# accept4 / SOCK_NONBLOCK / SOCK_CLOEXEC
add_compile_definitions(_GNU_SOURCE)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

add_executable(server
    src/main.cpp
    src/EventLoop.cpp
    src/Connection.cpp
    src/ChatServer.cpp
    src/RedisClient.cpp
    src/WebSocket.cpp
    src/Json.cpp
    src/ThreadPool.cpp
)

target_include_directories(server PRIVATE include ${HIREDIS_INCLUDE_DIRS})
target_link_libraries(server PRIVATE Threads::Threads ${HIREDIS_LIBRARIES})

# 静态页面的绝对路径，避免“必须在某个目录下启动”的隐式依赖
target_compile_definitions(server PRIVATE
    SERVER_WEB_ROOT="${CMAKE_CURRENT_SOURCE_DIR}/web")

target_compile_options(server PRIVATE -Wall -Wextra -Wpedantic)

option(ENABLE_ASAN "Build with AddressSanitizer" OFF)
option(ENABLE_TSAN "Build with ThreadSanitizer" OFF)

if(ENABLE_ASAN)
    target_compile_options(server PRIVATE -fsanitize=address -fno-omit-frame-pointer -g)
    target_link_options(server PRIVATE -fsanitize=address)
endif()

if(ENABLE_TSAN)
    target_compile_options(server PRIVATE -fsanitize=thread -fno-omit-frame-pointer -g)
    target_link_options(server PRIVATE -fsanitize=thread)
endif()


# 压测客户端（独立目标，不依赖 hiredis）
add_executable(bench tools/bench.cpp)
target_include_directories(bench PRIVATE include)
target_compile_options(bench PRIVATE -Wall -Wextra -Wpedantic)
