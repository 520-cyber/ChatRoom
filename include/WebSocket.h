#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "Buffer.h"

// 最小可用的 WebSocket（RFC 6455）实现：
//   * 服务端握手：Sec-WebSocket-Accept = base64(sha1(key + GUID))
//   * 帧解析：处理 7 / 16 / 64 位长度、客户端掩码、分片
//   * 帧编码：服务端发出的帧不加掩码
// 只做聊天室需要的那部分，不实现扩展（permessage-deflate 等）。
namespace ws {

constexpr uint8_t kOpContinuation = 0x0;
constexpr uint8_t kOpText = 0x1;
constexpr uint8_t kOpBinary = 0x2;
constexpr uint8_t kOpClose = 0x8;
constexpr uint8_t kOpPing = 0x9;
constexpr uint8_t kOpPong = 0xA;

constexpr size_t kMaxFramePayload = 1 << 20;  // 单帧上限 1MB，防内存打爆

std::string acceptKey(const std::string &clientKey);

std::string encodeFrame(uint8_t opcode, const std::string &payload);

enum class ParseResult { Incomplete, Ok, Error };

struct Frame {
    bool fin = true;
    uint8_t opcode = 0;
    std::string payload;
};

// 从缓冲区里尝试取出一个完整帧：
//   Ok         取出成功，已从 buf 消费
//   Incomplete 数据还不够，什么都没消费
//   Error      协议非法（未加掩码、RSV 非 0、超长），调用方应关闭连接
ParseResult parseFrame(Buffer &buf, Frame &out);

// 暴露给单测
std::string sha1Raw(const std::string &data);
std::string base64Encode(const uint8_t *data, size_t len);

}  // namespace ws
