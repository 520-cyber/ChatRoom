#include "Json.h"

#include <cctype>
#include <cstdio>

namespace json {

std::string escape(const std::string &s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    return out;
}

namespace {

void skipSpace(const std::string &s, size_t &p) {
    while (p < s.size() && std::isspace(static_cast<unsigned char>(s[p]))) ++p;
}

}  // namespace

bool getString(const std::string &obj, const std::string &key, std::string &out) {
    const std::string pat = "\"" + key + "\"";
    size_t p = obj.find(pat);
    if (p == std::string::npos) return false;

    p += pat.size();
    skipSpace(obj, p);
    if (p >= obj.size() || obj[p] != ':') return false;
    ++p;
    skipSpace(obj, p);
    if (p >= obj.size() || obj[p] != '"') return false;
    ++p;

    out.clear();
    while (p < obj.size()) {
        char c = obj[p++];
        if (c == '"') return true;
        if (c != '\\') {
            out += c;
            continue;
        }
        if (p >= obj.size()) return false;
        char e = obj[p++];
        switch (e) {
            case '"': out += '"'; break;
            case '\\': out += '\\'; break;
            case '/': out += '/'; break;
            case 'b': out += '\b'; break;
            case 'f': out += '\f'; break;
            case 'n': out += '\n'; break;
            case 'r': out += '\r'; break;
            case 't': out += '\t'; break;
            case 'u': {
                // 只处理 BMP 的基本场景：要求 4 位十六进制
                if (p + 4 > obj.size()) return false;
                unsigned code = 0;
                for (int i = 0; i < 4; ++i) {
                    char h = obj[p + i];
                    code <<= 4;
                    if (h >= '0' && h <= '9') code |= static_cast<unsigned>(h - '0');
                    else if (h >= 'a' && h <= 'f') code |= static_cast<unsigned>(h - 'a' + 10);
                    else if (h >= 'A' && h <= 'F') code |= static_cast<unsigned>(h - 'A' + 10);
                    else return false;
                }
                p += 4;
                // UTF-8 编码
                if (code < 0x80) {
                    out += static_cast<char>(code);
                } else if (code < 0x800) {
                    out += static_cast<char>(0xC0 | (code >> 6));
                    out += static_cast<char>(0x80 | (code & 0x3F));
                } else {
                    out += static_cast<char>(0xE0 | (code >> 12));
                    out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
                    out += static_cast<char>(0x80 | (code & 0x3F));
                }
                break;
            }
            default: return false;
        }
    }
    return false;
}

bool getBool(const std::string &obj, const std::string &key, bool &out) {
    const std::string pat = "\"" + key + "\"";
    size_t p = obj.find(pat);
    if (p == std::string::npos) return false;
    p += pat.size();
    skipSpace(obj, p);
    if (p >= obj.size() || obj[p] != ':') return false;
    ++p;
    skipSpace(obj, p);
    if (obj.compare(p, 4, "true") == 0) {
        out = true;
        return true;
    }
    if (obj.compare(p, 5, "false") == 0) {
        out = false;
        return true;
    }
    return false;
}

}  // namespace json
