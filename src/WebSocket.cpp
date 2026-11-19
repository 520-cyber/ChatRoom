#include "WebSocket.h"

#include <cstring>

namespace ws {
namespace {

constexpr char kGuid[] = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

inline uint32_t rotl32(uint32_t x, int n) {
    return (x << n) | (x >> (32 - n));
}

}  // namespace

std::string base64Encode(const uint8_t *data, size_t len) {
    static const char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((len + 2) / 3 * 4);
    for (size_t i = 0; i < len; i += 3) {
        uint32_t v = static_cast<uint32_t>(data[i]) << 16;
        if (i + 1 < len) v |= static_cast<uint32_t>(data[i + 1]) << 8;
        if (i + 2 < len) v |= static_cast<uint32_t>(data[i + 2]);
        out.push_back(tbl[(v >> 18) & 0x3F]);
        out.push_back(tbl[(v >> 12) & 0x3F]);
        out.push_back(i + 1 < len ? tbl[(v >> 6) & 0x3F] : '=');
        out.push_back(i + 2 < len ? tbl[v & 0x3F] : '=');
    }
    return out;
}

std::string sha1Raw(const std::string &data) {
    const uint64_t bitLen = static_cast<uint64_t>(data.size()) * 8;

    std::string msg = data;
    msg.push_back(static_cast<char>(0x80));
    while (msg.size() % 64 != 56) msg.push_back('\0');
    for (int i = 7; i >= 0; --i) {
        msg.push_back(static_cast<char>((bitLen >> (i * 8)) & 0xFF));
    }

    uint32_t h0 = 0x67452301, h1 = 0xEFCDAB89, h2 = 0x98BADCFE, h3 = 0x10325476,
             h4 = 0xC3D2E1F0;

    for (size_t chunk = 0; chunk < msg.size(); chunk += 64) {
        uint32_t w[80];
        for (int i = 0; i < 16; ++i) {
            const size_t o = chunk + static_cast<size_t>(i) * 4;
            w[i] = (static_cast<uint32_t>(static_cast<uint8_t>(msg[o])) << 24) |
                   (static_cast<uint32_t>(static_cast<uint8_t>(msg[o + 1])) << 16) |
                   (static_cast<uint32_t>(static_cast<uint8_t>(msg[o + 2])) << 8) |
                   static_cast<uint32_t>(static_cast<uint8_t>(msg[o + 3]));
        }
        for (int i = 16; i < 80; ++i) {
            w[i] = rotl32(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }

        uint32_t a = h0, b = h1, c = h2, d = h3, e = h4;
        for (int i = 0; i < 80; ++i) {
            uint32_t f, k;
            if (i < 20) {
                f = (b & c) | (~b & d);
                k = 0x5A827999;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ED9EBA1;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8F1BBCDC;
            } else {
                f = b ^ c ^ d;
                k = 0xCA62C1D6;
            }
            const uint32_t tmp = rotl32(a, 5) + f + e + k + w[i];
            e = d;
            d = c;
            c = rotl32(b, 30);
            b = a;
            a = tmp;
        }
        h0 += a;
        h1 += b;
        h2 += c;
        h3 += d;
        h4 += e;
    }

    std::string out(20, '\0');
    const uint32_t hs[5] = {h0, h1, h2, h3, h4};
    for (int i = 0; i < 5; ++i) {
        out[i * 4] = static_cast<char>((hs[i] >> 24) & 0xFF);
        out[i * 4 + 1] = static_cast<char>((hs[i] >> 16) & 0xFF);
        out[i * 4 + 2] = static_cast<char>((hs[i] >> 8) & 0xFF);
        out[i * 4 + 3] = static_cast<char>(hs[i] & 0xFF);
    }
    return out;
}

std::string acceptKey(const std::string &clientKey) {
    const std::string raw = sha1Raw(clientKey + kGuid);
    return base64Encode(reinterpret_cast<const uint8_t *>(raw.data()), raw.size());
}

std::string encodeFrame(uint8_t opcode, const std::string &payload) {
    std::string frame;
    const size_t len = payload.size();
    frame.reserve(len + 10);

    frame.push_back(static_cast<char>(0x80 | opcode));  // FIN=1

    if (len < 126) {
        frame.push_back(static_cast<char>(len));
    } else if (len < 65536) {
        frame.push_back(static_cast<char>(126));
        frame.push_back(static_cast<char>((len >> 8) & 0xFF));
        frame.push_back(static_cast<char>(len & 0xFF));
    } else {
        frame.push_back(static_cast<char>(127));
        for (int i = 7; i >= 0; --i) {
            frame.push_back(static_cast<char>((static_cast<uint64_t>(len) >> (i * 8)) & 0xFF));
        }
    }
    frame.append(payload);
    return frame;
}

ParseResult parseFrame(Buffer &buf, Frame &out) {
    const size_t avail = buf.readable();
    if (avail < 2) return ParseResult::Incomplete;

    const uint8_t *p = reinterpret_cast<const uint8_t *>(buf.peek());

    const bool fin = (p[0] & 0x80) != 0;
    const uint8_t opcode = p[0] & 0x0F;
    // 没有协商任何扩展，RSV 必须为 0
    if (p[0] & 0x70) return ParseResult::Error;

    const bool masked = (p[1] & 0x80) != 0;
    uint64_t len = p[1] & 0x7F;
    size_t off = 2;

    if (len == 126) {
        if (avail < 4) return ParseResult::Incomplete;
        len = (static_cast<uint64_t>(p[2]) << 8) | p[3];
        off = 4;
    } else if (len == 127) {
        if (avail < 10) return ParseResult::Incomplete;
        len = 0;
        for (int i = 0; i < 8; ++i) {
            len = (len << 8) | p[2 + i];
        }
        off = 10;
    }

    if (len > kMaxFramePayload) return ParseResult::Error;

    // RFC 6455：客户端发来的帧必须加掩码
    if (!masked) return ParseResult::Error;

    const size_t maskOff = off;
    off += 4;
    if (avail < off) return ParseResult::Incomplete;
    if (avail - off < len) return ParseResult::Incomplete;

    out.fin = fin;
    out.opcode = opcode;
    out.payload.assign(reinterpret_cast<const char *>(p + off), static_cast<size_t>(len));

    const uint8_t *mask = p + maskOff;
    for (size_t i = 0; i < out.payload.size(); ++i) {
        out.payload[i] = static_cast<char>(
            static_cast<uint8_t>(out.payload[i]) ^ mask[i % 4]);
    }

    buf.retrieve(off + static_cast<size_t>(len));
    return ParseResult::Ok;
}

}  // namespace ws
