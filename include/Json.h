#pragma once

#include <string>

// 极简 JSON 工具。只服务于本项目的报文格式（一层对象 + 字符串字段），
// 不是通用 JSON 库；但对输入做了完整的转义/反转义处理，
// 避免“用户输入拼坏 JSON”那类问题。
namespace json {

// 把一个字符串转义成可以安全放进 JSON 字符串字面量的形式
std::string escape(const std::string &s);

// 从 {"key":"value"} 里取出 key 对应的字符串值（支持 \" \\ \/ \b \f \n \r \t）。
// 找不到或格式非法返回 false。
bool getString(const std::string &obj, const std::string &key, std::string &out);

// 判断某个 key 是否存在于对象里（值必须是 true/false）
bool getBool(const std::string &obj, const std::string &key, bool &out);

}  // namespace json
