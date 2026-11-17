cmake_minimum_required(VERSION 3.10)
project(HighConcurrencyServer)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include_directories(include)

find_package(Threads REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

add_executable(server src/server.cpp src/ThreadPool.cpp src/Connection.cpp)
target_link_libraries(server Threads::Threads ${HIREDIS_LIBRARIES})
target_include_directories(server PRIVATE ${HIREDIS_INCLUDE_DIRS})

add_executable(client src/client.cpp)
target_link_libraries(client Threads::Threads)