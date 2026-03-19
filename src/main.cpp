#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <regex>
#include <array>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#ifndef _WIN32
using SOCKET = int;
constexpr int INVALID_SOCKET = -1;
constexpr int SOCKET_ERROR = -1;
constexpr int SD_BOTH = SHUT_RDWR;
#define closesocket close
#endif

namespace {

using Clock = std::chrono::system_clock;

std::string trim(const std::string& input) {
    std::size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start])) != 0) {
        ++start;
    }
    std::size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1])) != 0) {
        --end;
    }
    return input.substr(start, end - start);
}

std::string bool_string(const bool value) {
    return value ? "true" : "false";
}

std::string to_lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string now_iso8601() {
    const auto now = Clock::now();
    const auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - seconds).count();
    const std::time_t time = Clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &time);
#else
    gmtime_r(&time, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << ms << 'Z';
    return oss.str();
}

struct Json;
using JsonObject = std::map<std::string, Json>;
using JsonArray = std::vector<Json>;

struct Json {
    using Value = std::variant<std::nullptr_t, bool, double, std::string, JsonArray, JsonObject>;
    Value value;

    Json() : value(nullptr) {}
    Json(std::nullptr_t) : value(nullptr) {}
    Json(bool v) : value(v) {}
    Json(double v) : value(v) {}
    Json(int v) : value(static_cast<double>(v)) {}
    Json(std::string v) : value(std::move(v)) {}
    Json(const char* v) : value(std::string(v)) {}
    Json(JsonArray v) : value(std::move(v)) {}
    Json(JsonObject v) : value(std::move(v)) {}

    bool is_null() const { return std::holds_alternative<std::nullptr_t>(value); }
    bool is_bool() const { return std::holds_alternative<bool>(value); }
    bool is_number() const { return std::holds_alternative<double>(value); }
    bool is_string() const { return std::holds_alternative<std::string>(value); }
    bool is_array() const { return std::holds_alternative<JsonArray>(value); }
    bool is_object() const { return std::holds_alternative<JsonObject>(value); }

    bool as_bool() const { return std::get<bool>(value); }
    double as_number() const { return std::get<double>(value); }
    const std::string& as_string() const { return std::get<std::string>(value); }
    const JsonArray& as_array() const { return std::get<JsonArray>(value); }
    const JsonObject& as_object() const { return std::get<JsonObject>(value); }
    JsonArray& as_array() { return std::get<JsonArray>(value); }
    JsonObject& as_object() { return std::get<JsonObject>(value); }
};

class JsonParser {
public:
    explicit JsonParser(const std::string& text) : text_(text) {}

    Json parse() {
        skip_ws();
        Json value = parse_value();
        skip_ws();
        if (pos_ != text_.size()) {
            throw std::runtime_error("Trailing data after JSON");
        }
        return value;
    }

private:
    const std::string& text_;
    std::size_t pos_ = 0;

    void skip_ws() {
        while (pos_ < text_.size() && std::isspace(static_cast<unsigned char>(text_[pos_])) != 0) {
            ++pos_;
        }
    }

    char peek() const {
        if (pos_ >= text_.size()) {
            throw std::runtime_error("Unexpected end of JSON");
        }
        return text_[pos_];
    }

    char consume() {
        char ch = peek();
        ++pos_;
        return ch;
    }

    void expect(const char ch) {
        if (consume() != ch) {
            throw std::runtime_error("Unexpected JSON token");
        }
    }

    Json parse_value() {
        const char ch = peek();
        if (ch == '{') {
            return parse_object();
        }
        if (ch == '[') {
            return parse_array();
        }
        if (ch == '"') {
            return Json(parse_string());
        }
        if (ch == 't') {
            consume_literal("true");
            return Json(true);
        }
        if (ch == 'f') {
            consume_literal("false");
            return Json(false);
        }
        if (ch == 'n') {
            consume_literal("null");
            return Json(nullptr);
        }
        if (ch == '-' || std::isdigit(static_cast<unsigned char>(ch)) != 0) {
            return Json(parse_number());
        }
        throw std::runtime_error("Unsupported JSON value");
    }

    void consume_literal(const std::string& literal) {
        if (text_.compare(pos_, literal.size(), literal) != 0) {
            throw std::runtime_error("Unexpected JSON literal");
        }
        pos_ += literal.size();
    }

    Json parse_object() {
        expect('{');
        skip_ws();
        JsonObject object;
        if (peek() == '}') {
            consume();
            return Json(std::move(object));
        }
        while (true) {
            skip_ws();
            const std::string key = parse_string();
            skip_ws();
            expect(':');
            skip_ws();
            object.emplace(key, parse_value());
            skip_ws();
            const char delimiter = consume();
            if (delimiter == '}') {
                break;
            }
            if (delimiter != ',') {
                throw std::runtime_error("Expected object delimiter");
            }
            skip_ws();
        }
        return Json(std::move(object));
    }

    Json parse_array() {
        expect('[');
        skip_ws();
        JsonArray array;
        if (peek() == ']') {
            consume();
            return Json(std::move(array));
        }
        while (true) {
            skip_ws();
            array.push_back(parse_value());
            skip_ws();
            const char delimiter = consume();
            if (delimiter == ']') {
                break;
            }
            if (delimiter != ',') {
                throw std::runtime_error("Expected array delimiter");
            }
            skip_ws();
        }
        return Json(std::move(array));
    }

    std::string parse_string() {
        expect('"');
        std::string result;
        while (true) {
            char ch = consume();
            if (ch == '"') {
                break;
            }
            if (ch == '\\') {
                char escaped = consume();
                switch (escaped) {
                    case '"': result.push_back('"'); break;
                    case '\\': result.push_back('\\'); break;
                    case '/': result.push_back('/'); break;
                    case 'b': result.push_back('\b'); break;
                    case 'f': result.push_back('\f'); break;
                    case 'n': result.push_back('\n'); break;
                    case 'r': result.push_back('\r'); break;
                    case 't': result.push_back('\t'); break;
                    default:
                        throw std::runtime_error("Unsupported escape sequence");
                }
                continue;
            }
            result.push_back(ch);
        }
        return result;
    }

    double parse_number() {
        std::size_t consumed = 0;
        const double value = std::stod(text_.substr(pos_), &consumed);
        pos_ += consumed;
        return value;
    }
};

std::string escape_json(const std::string& input) {
    std::ostringstream oss;
    for (char ch : input) {
        switch (ch) {
            case '"': oss << "\\\""; break;
            case '\\': oss << "\\\\"; break;
            case '\b': oss << "\\b"; break;
            case '\f': oss << "\\f"; break;
            case '\n': oss << "\\n"; break;
            case '\r': oss << "\\r"; break;
            case '\t': oss << "\\t"; break;
            default:
                if (static_cast<unsigned char>(ch) < 0x20U) {
                    oss << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                        << static_cast<int>(static_cast<unsigned char>(ch)) << std::dec;
                } else {
                    oss << ch;
                }
        }
    }
    return oss.str();
}

std::string dump_json(const Json& json) {
    if (json.is_null()) {
        return "null";
    }
    if (json.is_bool()) {
        return json.as_bool() ? "true" : "false";
    }
    if (json.is_number()) {
        std::ostringstream oss;
        const double value = json.as_number();
        if (value == static_cast<long long>(value)) {
            oss << static_cast<long long>(value);
        } else {
            oss << value;
        }
        return oss.str();
    }
    if (json.is_string()) {
        return "\"" + escape_json(json.as_string()) + "\"";
    }
    if (json.is_array()) {
        std::ostringstream oss;
        oss << "[";
        bool first = true;
        for (const auto& item : json.as_array()) {
            if (!first) {
                oss << ",";
            }
            first = false;
            oss << dump_json(item);
        }
        oss << "]";
        return oss.str();
    }
    std::ostringstream oss;
    oss << "{";
    bool first = true;
    for (const auto& [key, value] : json.as_object()) {
        if (!first) {
            oss << ",";
        }
        first = false;
        oss << "\"" << escape_json(key) << "\":" << dump_json(value);
    }
    oss << "}";
    return oss.str();
}

const JsonObject& require_object(const Json& json) {
    if (!json.is_object()) {
        throw std::runtime_error("JSON object expected");
    }
    return json.as_object();
}

std::optional<std::string> optional_string(const JsonObject& object, const std::string& key) {
    const auto it = object.find(key);
    if (it == object.end() || it->second.is_null()) {
        return std::nullopt;
    }
    if (!it->second.is_string()) {
        throw std::runtime_error("Expected string field: " + key);
    }
    return it->second.as_string();
}

std::string required_string(const JsonObject& object, const std::string& key) {
    const auto value = optional_string(object, key);
    if (!value.has_value()) {
        throw std::runtime_error("Missing string field: " + key);
    }
    return *value;
}

int parse_int_query(const std::unordered_map<std::string, std::string>& query, const std::string& key, int fallback) {
    const auto it = query.find(key);
    if (it == query.end()) {
        return fallback;
    }
    return std::max(0, std::stoi(it->second));
}

struct Request {
    std::string method;
    std::string path;
    std::unordered_map<std::string, std::string> query;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

struct Response {
    int status = 200;
    Json body = JsonObject{};
    std::unordered_map<std::string, std::string> headers;
};

struct HttpError : public std::runtime_error {
    int status;
    std::string code;

    HttpError(int status_code, std::string error_code, const std::string& message)
        : std::runtime_error(message), status(status_code), code(std::move(error_code)) {}
};

struct RelationshipRecord {
    std::string user_id;
    std::string target_user_id;
    std::string status;
    std::string created_at;
    std::string updated_at;
};

struct BlockRecord {
    std::string user_id;
    std::string target_user_id;
    std::optional<std::string> reason;
    std::string created_at;
};

struct ProjectionRecord {
    std::string user_id;
    std::string entity_type;
    std::string entity_id;
    std::string relation_role;
    std::string visibility_status = "visible";
    std::optional<std::string> counterpart_user_id;
    std::string created_at;
    std::string updated_at;
};

struct UserProfile {
    std::string user_id;
    std::string display_name;
    std::optional<std::string> username;
    std::optional<std::string> avatar_object_id;
    std::optional<std::string> bio;
    std::optional<std::string> locale;
    std::optional<std::string> time_zone;
    std::string profile_status = "active";
    std::string created_at;
    std::string updated_at;
    std::optional<std::string> deleted_at;
};

struct PrivacySettings {
    std::string user_id;
    std::string profile_visibility = "public";
    std::string dm_policy = "everyone";
    std::string friend_request_policy = "everyone";
    std::string last_seen_visibility = "public";
    std::string avatar_visibility = "public";
    std::string created_at;
    std::string updated_at;
};

struct DeviceSession {
    std::string device_id;
    std::string user_id;
    std::string session_id;
    std::string platform;
    std::string created_at;
    std::string updated_at;
};

struct RelationshipSummary {
    bool is_friend = false;
    bool is_blocked = false;
    bool is_blocked_by_target = false;
};

std::string pair_key(const std::string& lhs, const std::string& rhs) {
    return lhs + "|" + rhs;
}

std::optional<std::string> get_env(const char* key) {
#ifdef _WIN32
    char* value = nullptr;
    std::size_t size = 0;
    if (_dupenv_s(&value, &size, key) == 0 && value != nullptr) {
        std::string result(value);
        std::free(value);
        return result;
    }
    return std::nullopt;
#else
    const char* value = std::getenv(key);
    if (value == nullptr) {
        return std::nullopt;
    }
    return std::string(value);
#endif
}

bool is_uuid_like(const std::string& value) {
    static const std::regex pattern("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    return std::regex_match(value, pattern);
}

std::string hash_to_uuid(const std::string& value) {
    std::uint64_t h1 = 1469598103934665603ULL;
    std::uint64_t h2 = 1099511628211ULL;
    for (unsigned char ch : value) {
        h1 ^= ch;
        h1 *= 1099511628211ULL;
        h2 += ch + 0x9e3779b97f4a7c15ULL + (h2 << 6U) + (h2 >> 2U);
    }
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(8) << static_cast<std::uint32_t>(h1 >> 32U) << "-"
        << std::setw(4) << static_cast<std::uint16_t>((h1 >> 16U) & 0xffffU) << "-"
        << std::setw(4) << static_cast<std::uint16_t>((h1 & 0x0fffU) | 0x4000U) << "-"
        << std::setw(4) << static_cast<std::uint16_t>((h2 & 0x3fffU) | 0x8000U) << "-"
        << std::setw(12) << (h2 & 0xffffffffffffULL);
    return oss.str();
}

std::string canonical_user_id(const std::string& raw_user_id) {
    return is_uuid_like(raw_user_id) ? to_lower(raw_user_id) : hash_to_uuid(raw_user_id);
}

std::string shell_escape_single_quotes(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() * 2U);
    for (char ch : value) {
        if (ch == '\'') {
            escaped += "''";
        } else {
            escaped.push_back(ch);
        }
    }
    return escaped;
}

struct ExecResult {
    int exit_code = 0;
    std::string output;
};

ExecResult run_command_capture(const std::string& command) {
    ExecResult result;
#ifdef _WIN32
    FILE* pipe = _popen(command.c_str(), "r");
#else
    FILE* pipe = popen(command.c_str(), "r");
#endif
    if (pipe == nullptr) {
        result.exit_code = 1;
        result.output = "failed to spawn command";
        return result;
    }
    char buffer[512];
    while (std::fgets(buffer, static_cast<int>(sizeof(buffer)), pipe) != nullptr) {
        result.output += buffer;
    }
#ifdef _WIN32
    result.exit_code = _pclose(pipe);
#else
    result.exit_code = pclose(pipe);
#endif
    return result;
}

std::filesystem::path write_temp_sql_file(const std::string& sql) {
    const auto temp_dir = std::filesystem::temp_directory_path();
    std::string stamp = now_iso8601();
    for (char& ch : stamp) {
        if (!(std::isalnum(static_cast<unsigned char>(ch)) != 0)) {
            ch = '_';
        }
    }
    const auto filename = "user-service-" + stamp + "-" + std::to_string(std::rand()) + ".sql";
    const auto path = temp_dir / filename;
    std::ofstream stream(path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Failed to create temp SQL file");
    }
    stream << sql;
    stream.close();
    return path;
}

std::string base64url_decode(std::string input) {
    std::replace(input.begin(), input.end(), '-', '+');
    std::replace(input.begin(), input.end(), '_', '/');
    while ((input.size() % 4U) != 0U) {
        input.push_back('=');
    }
    static const std::string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string output;
    int val = 0;
    int valb = -8;
    for (unsigned char ch : input) {
        if (std::isspace(ch) != 0 || ch == '=') {
            continue;
        }
        const auto pos = alphabet.find(static_cast<char>(ch));
        if (pos == std::string::npos) {
            throw std::runtime_error("Invalid base64url input");
        }
        val = (val << 6) + static_cast<int>(pos);
        valb += 6;
        if (valb >= 0) {
            output.push_back(static_cast<char>((val >> valb) & 0xff));
            valb -= 8;
        }
    }
    return output;
}

std::string base64url_encode(const std::string& input) {
    static const char* alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string output;
    int val = 0;
    int valb = -6;
    for (unsigned char ch : input) {
        val = (val << 8) + ch;
        valb += 8;
        while (valb >= 0) {
            output.push_back(alphabet[(val >> valb) & 0x3f]);
            valb -= 6;
        }
    }
    if (valb > -6) {
        output.push_back(alphabet[((val << 8) >> (valb + 8)) & 0x3f]);
    }
    return output;
}

std::uint32_t rotr32(const std::uint32_t value, const std::uint32_t bits) {
    return (value >> bits) | (value << (32U - bits));
}

std::array<std::uint8_t, 32> sha256_bytes(const std::string& input) {
    static const std::uint32_t k[64] = {
        0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
        0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U, 0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
        0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU, 0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
        0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
        0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U, 0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
        0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U, 0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
        0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
        0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U, 0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U
    };
    std::array<std::uint32_t, 8> h = {
        0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU,
        0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U
    };
    std::vector<std::uint8_t> msg(input.begin(), input.end());
    const std::uint64_t bit_length = static_cast<std::uint64_t>(msg.size()) * 8ULL;
    msg.push_back(0x80U);
    while ((msg.size() % 64U) != 56U) {
        msg.push_back(0U);
    }
    for (int i = 7; i >= 0; --i) {
        msg.push_back(static_cast<std::uint8_t>((bit_length >> (i * 8)) & 0xffU));
    }
    for (std::size_t offset = 0; offset < msg.size(); offset += 64U) {
        std::uint32_t w[64]{};
        for (std::size_t i = 0; i < 16U; ++i) {
            const std::size_t j = offset + i * 4U;
            w[i] = (static_cast<std::uint32_t>(msg[j]) << 24U) |
                   (static_cast<std::uint32_t>(msg[j + 1]) << 16U) |
                   (static_cast<std::uint32_t>(msg[j + 2]) << 8U) |
                   static_cast<std::uint32_t>(msg[j + 3]);
        }
        for (std::size_t i = 16U; i < 64U; ++i) {
            const std::uint32_t s0 = rotr32(w[i - 15U], 7U) ^ rotr32(w[i - 15U], 18U) ^ (w[i - 15U] >> 3U);
            const std::uint32_t s1 = rotr32(w[i - 2U], 17U) ^ rotr32(w[i - 2U], 19U) ^ (w[i - 2U] >> 10U);
            w[i] = w[i - 16U] + s0 + w[i - 7U] + s1;
        }
        std::uint32_t a = h[0];
        std::uint32_t b = h[1];
        std::uint32_t c = h[2];
        std::uint32_t d = h[3];
        std::uint32_t e = h[4];
        std::uint32_t f = h[5];
        std::uint32_t g = h[6];
        std::uint32_t hh = h[7];
        for (std::size_t i = 0; i < 64U; ++i) {
            const std::uint32_t s1 = rotr32(e, 6U) ^ rotr32(e, 11U) ^ rotr32(e, 25U);
            const std::uint32_t ch = (e & f) ^ ((~e) & g);
            const std::uint32_t temp1 = hh + s1 + ch + k[i] + w[i];
            const std::uint32_t s0 = rotr32(a, 2U) ^ rotr32(a, 13U) ^ rotr32(a, 22U);
            const std::uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            const std::uint32_t temp2 = s0 + maj;
            hh = g; g = f; f = e; e = d + temp1; d = c; c = b; b = a; a = temp1 + temp2;
        }
        h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += hh;
    }
    std::array<std::uint8_t, 32> out{};
    for (std::size_t i = 0; i < h.size(); ++i) {
        out[i * 4U] = static_cast<std::uint8_t>((h[i] >> 24U) & 0xffU);
        out[i * 4U + 1U] = static_cast<std::uint8_t>((h[i] >> 16U) & 0xffU);
        out[i * 4U + 2U] = static_cast<std::uint8_t>((h[i] >> 8U) & 0xffU);
        out[i * 4U + 3U] = static_cast<std::uint8_t>(h[i] & 0xffU);
    }
    return out;
}

std::string hmac_sha256(const std::string& key, const std::string& message) {
    constexpr std::size_t block_size = 64U;
    std::string normalized_key = key;
    if (normalized_key.size() > block_size) {
        const auto hashed = sha256_bytes(normalized_key);
        normalized_key.assign(reinterpret_cast<const char*>(hashed.data()), hashed.size());
    }
    normalized_key.resize(block_size, '\0');
    std::string o_key_pad(block_size, '\0');
    std::string i_key_pad(block_size, '\0');
    for (std::size_t i = 0; i < block_size; ++i) {
        o_key_pad[i] = static_cast<char>(normalized_key[i] ^ 0x5c);
        i_key_pad[i] = static_cast<char>(normalized_key[i] ^ 0x36);
    }
    const auto inner = sha256_bytes(i_key_pad + message);
    const std::string inner_bytes(reinterpret_cast<const char*>(inner.data()), inner.size());
    const auto outer = sha256_bytes(o_key_pad + inner_bytes);
    return std::string(reinterpret_cast<const char*>(outer.data()), outer.size());
}

bool constant_time_equals(const std::string& lhs, const std::string& rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    unsigned char diff = 0;
    for (std::size_t i = 0; i < lhs.size(); ++i) {
        diff |= static_cast<unsigned char>(lhs[i] ^ rhs[i]);
    }
    return diff == 0;
}

struct JwtPrincipal {
    std::string raw_user_id;
    std::string canonical_id;
    std::string subject;
    std::optional<std::string> issuer;
    std::optional<std::string> audience;
    std::optional<long long> exp;
    std::optional<std::string> display_name;
};

JwtPrincipal parse_jwt_without_signature_validation(
    const std::string& token,
    const std::optional<std::string>& required_issuer,
    const std::optional<std::string>& required_audience,
    const std::optional<std::string>& required_secret) {
    std::stringstream ss(token);
    std::string header_part;
    std::string payload_part;
    std::string signature_part;
    std::getline(ss, header_part, '.');
    std::getline(ss, payload_part, '.');
    std::getline(ss, signature_part, '.');
    if (header_part.empty() || payload_part.empty()) {
        throw std::runtime_error("Malformed JWT");
    }
    const auto header = require_object(JsonParser(base64url_decode(header_part)).parse());
    const auto payload = require_object(JsonParser(base64url_decode(payload_part)).parse());
    if (header.count("typ") != 0 && header.at("typ").is_string() && to_lower(header.at("typ").as_string()) != "jwt") {
        throw std::runtime_error("Unsupported JWT typ");
    }
    if (!required_secret.has_value()) {
        throw std::runtime_error("JWT secret is not configured");
    }
    if (required_string(header, "alg") != "HS256") {
        throw std::runtime_error("Unsupported JWT alg");
    }
    if (signature_part.empty()) {
        throw std::runtime_error("Malformed JWT");
    }
    const std::string signing_input = header_part + "." + payload_part;
    const std::string expected_signature = base64url_encode(hmac_sha256(*required_secret, signing_input));
    if (!constant_time_equals(expected_signature, signature_part)) {
        throw std::runtime_error("JWT signature mismatch");
    }
    if (required_issuer.has_value()) {
        const auto issuer = required_string(payload, "iss");
        if (issuer != *required_issuer) {
            throw std::runtime_error("JWT issuer mismatch");
        }
    }
    if (required_audience.has_value()) {
        bool audience_ok = false;
        const auto it = payload.find("aud");
        if (it != payload.end()) {
            if (it->second.is_string()) {
                audience_ok = it->second.as_string() == *required_audience;
            } else if (it->second.is_array()) {
                for (const auto& entry : it->second.as_array()) {
                    if (entry.is_string() && entry.as_string() == *required_audience) {
                        audience_ok = true;
                        break;
                    }
                }
            }
        }
        if (!audience_ok) {
            throw std::runtime_error("JWT audience mismatch");
        }
    }
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
    if (payload.count("exp") != 0 && payload.at("exp").is_number() && static_cast<long long>(payload.at("exp").as_number()) < now) {
        throw std::runtime_error("JWT expired");
    }
    const std::string raw_user_id = optional_string(payload, "uid").value_or(required_string(payload, "sub"));
    return JwtPrincipal{
        .raw_user_id = raw_user_id,
        .canonical_id = canonical_user_id(raw_user_id),
        .subject = required_string(payload, "sub"),
        .issuer = optional_string(payload, "iss"),
        .audience = required_audience,
        .exp = payload.count("exp") != 0 && payload.at("exp").is_number() ? std::optional<long long>(static_cast<long long>(payload.at("exp").as_number())) : std::nullopt,
        .display_name = optional_string(payload, "name"),
    };
}

struct DbConfig {
    std::string host;
    std::string port;
    std::string name;
    std::string user;
    std::string password;
    std::string sslmode;
};

class PostgresPsqlAdapter {
public:
    PostgresPsqlAdapter() {
        auto host = get_env("POSTGRES_HOST");
        auto port = get_env("POSTGRES_PORT");
        auto name = get_env("POSTGRES_DB");
        auto user = get_env("POSTGRES_USER");
        auto password = get_env("POSTGRES_PASSWORD");
        auto sslmode = get_env("POSTGRES_SSLMODE");
        enabled_ = host.has_value() && port.has_value() && name.has_value() && user.has_value() && password.has_value() && sslmode.has_value();
        if (enabled_) {
            config_ = DbConfig{*host, *port, *name, *user, *password, *sslmode};
        }
    }

    bool enabled() const {
        return enabled_;
    }

    bool ready() const {
        if (!enabled_) {
            return false;
        }
        const auto result = query_scalar(
            "SELECT COUNT(*)::text FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name IN ("
            "'user_profiles','user_privacy_settings','user_relationships','user_blocks','user_entity_projection','user_event_outbox');");
        return result.has_value() && *result == "6";
    }

    std::optional<JsonObject> get_profile(const std::string& user_id) const {
        if (!enabled_) {
            return std::nullopt;
        }
        const std::string sql =
            "SELECT user_id::text, display_name, COALESCE(username,''), COALESCE(avatar_object_id,''), "
            "COALESCE(bio,''), COALESCE(locale,''), COALESCE(time_zone,''), profile_status, "
            "to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), "
            "to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), "
            "COALESCE(to_char(deleted_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),'') "
            "FROM user_profiles WHERE user_id='" + shell_escape_single_quotes(user_id) + "';";
        const auto result = query_rows(sql);
        if (!result.has_value() || result->empty()) {
            return std::nullopt;
        }
        const auto& columns = result->front();
        if (columns.size() != 11U) {
            throw std::runtime_error("Unexpected profile row shape");
        }
        return JsonObject{
            {"userId", columns[0]},
            {"displayName", columns[1]},
            {"username", columns[2].empty() ? Json(nullptr) : Json(columns[2])},
            {"avatarObjectId", columns[3].empty() ? Json(nullptr) : Json(columns[3])},
            {"bio", columns[4].empty() ? Json(nullptr) : Json(columns[4])},
            {"locale", columns[5].empty() ? Json(nullptr) : Json(columns[5])},
            {"timeZone", columns[6].empty() ? Json(nullptr) : Json(columns[6])},
            {"profileStatus", columns[7]},
            {"createdAt", columns[8]},
            {"updatedAt", columns[9]},
            {"deletedAt", columns[10].empty() ? Json(nullptr) : Json(columns[10])},
        };
    }

    std::optional<JsonObject> get_privacy_settings(const std::string& user_id) const {
        if (!enabled_) {
            return std::nullopt;
        }
        const std::string sql =
            "SELECT user_id::text, profile_visibility, dm_policy, friend_request_policy, last_seen_visibility, avatar_visibility, "
            "to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), "
            "to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') "
            "FROM user_privacy_settings WHERE user_id='" + shell_escape_single_quotes(user_id) + "';";
        const auto result = query_rows(sql);
        if (!result.has_value() || result->empty()) {
            return std::nullopt;
        }
        const auto& columns = result->front();
        if (columns.size() != 8U) {
            throw std::runtime_error("Unexpected privacy row shape");
        }
        return JsonObject{
            {"userId", columns[0]},
            {"profileVisibility", columns[1]},
            {"dmPolicy", columns[2]},
            {"friendRequestPolicy", columns[3]},
            {"lastSeenVisibility", columns[4]},
            {"avatarVisibility", columns[5]},
            {"createdAt", columns[6]},
            {"updatedAt", columns[7]},
        };
    }

    bool has_block(const std::string& user_id, const std::string& target_user_id) const {
        if (!enabled_) {
            return false;
        }
        const std::string sql =
            "SELECT 1 FROM user_blocks "
            "WHERE user_id='" + shell_escape_single_quotes(user_id) + "' "
            "AND target_user_id='" + shell_escape_single_quotes(target_user_id) + "' "
            "LIMIT 1;";
        return query_scalar(sql).has_value();
    }

    bool are_friends(const std::string& user_id, const std::string& target_user_id) const {
        if (!enabled_) {
            return false;
        }
        const std::string sql =
            "SELECT 1 "
            "FROM user_relationships lhs "
            "JOIN user_relationships rhs "
            "  ON rhs.user_id='" + shell_escape_single_quotes(target_user_id) + "' "
            " AND rhs.target_user_id='" + shell_escape_single_quotes(user_id) + "' "
            " AND rhs.status='accepted' "
            "WHERE lhs.user_id='" + shell_escape_single_quotes(user_id) + "' "
            "AND lhs.target_user_id='" + shell_escape_single_quotes(target_user_id) + "' "
            "AND lhs.status='accepted' "
            "LIMIT 1;";
        return query_scalar(sql).has_value();
    }

    std::optional<std::string> get_relationship_status(const std::string& user_id, const std::string& target_user_id) const {
        if (!enabled_) {
            return std::nullopt;
        }
        const std::string sql =
            "SELECT status FROM user_relationships "
            "WHERE user_id='" + shell_escape_single_quotes(user_id) + "' "
            "AND target_user_id='" + shell_escape_single_quotes(target_user_id) + "' "
            "AND relation_type='friend' LIMIT 1;";
        return query_scalar(sql);
    }

    bool has_mutual_friend(const std::string& user_id, const std::string& target_user_id) const {
        if (!enabled_) {
            return false;
        }
        const std::string sql =
            "SELECT 1 FROM user_relationships ua "
            "JOIN user_relationships ub ON ub.user_id='" + shell_escape_single_quotes(target_user_id) + "' "
            "AND ub.target_user_id=ua.target_user_id AND ub.relation_type='friend' AND ub.status='accepted' "
            "WHERE ua.user_id='" + shell_escape_single_quotes(user_id) + "' "
            "AND ua.target_user_id NOT IN ('" + shell_escape_single_quotes(user_id) + "','" + shell_escape_single_quotes(target_user_id) + "') "
            "AND ua.relation_type='friend' AND ua.status='accepted' LIMIT 1;";
        return query_scalar(sql).has_value();
    }

    void create_friend_request(const std::string& actor_user_id, const std::string& target_user_id) const {
        if (!enabled_) {
            return;
        }
        const auto now = now_iso8601();
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "INSERT INTO user_relationships (relation_id, user_id, target_user_id, relation_type, status, created_at, updated_at) VALUES ("
            << "'" << shell_escape_single_quotes(hash_to_uuid("rel-out-" + actor_user_id + "-" + target_user_id + "-" + now)) << "','"
            << shell_escape_single_quotes(actor_user_id) << "','" << shell_escape_single_quotes(target_user_id) << "','friend','pending_outgoing',NOW(),NOW()) "
            << "ON CONFLICT (user_id, target_user_id, relation_type) DO UPDATE SET status='pending_outgoing', updated_at=NOW();";
        sql << "INSERT INTO user_relationships (relation_id, user_id, target_user_id, relation_type, status, created_at, updated_at) VALUES ("
            << "'" << shell_escape_single_quotes(hash_to_uuid("rel-in-" + target_user_id + "-" + actor_user_id + "-" + now)) << "','"
            << shell_escape_single_quotes(target_user_id) << "','" << shell_escape_single_quotes(actor_user_id) << "','friend','pending_incoming',NOW(),NOW()) "
            << "ON CONFLICT (user_id, target_user_id, relation_type) DO UPDATE SET status='pending_incoming', updated_at=NOW();";
        sql << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) VALUES ("
            << "'" << shell_escape_single_quotes(hash_to_uuid("friend-request-created-" + actor_user_id + "-" + target_user_id + "-" + now)) << "','user_relationship','"
            << shell_escape_single_quotes(actor_user_id) << "','user.friend_request_created','{\"userId\":\""
            << shell_escape_single_quotes(actor_user_id) << "\",\"targetUserId\":\"" << shell_escape_single_quotes(target_user_id)
            << "\"}'::jsonb,NOW(),NULL);";
        sql << "COMMIT;";
        exec_sql(sql.str());
    }

    bool accept_friend_request(const std::string& actor_user_id, const std::string& target_user_id) const {
        if (get_relationship_status(actor_user_id, target_user_id) != std::optional<std::string>("pending_incoming") ||
            get_relationship_status(target_user_id, actor_user_id) != std::optional<std::string>("pending_outgoing")) {
            return false;
        }
        const auto now = now_iso8601();
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "UPDATE user_relationships SET status='accepted', updated_at=NOW() WHERE relation_type='friend' AND "
            << "((user_id='" << shell_escape_single_quotes(actor_user_id) << "' AND target_user_id='" << shell_escape_single_quotes(target_user_id) << "') OR "
            << "(user_id='" << shell_escape_single_quotes(target_user_id) << "' AND target_user_id='" << shell_escape_single_quotes(actor_user_id) << "'));";
        sql << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) VALUES ("
            << "'" << shell_escape_single_quotes(hash_to_uuid("friend-request-accepted-" + actor_user_id + "-" + target_user_id + "-" + now)) << "','user_relationship','"
            << shell_escape_single_quotes(actor_user_id) << "','user.friend_request_accepted','{\"userId\":\""
            << shell_escape_single_quotes(actor_user_id) << "\",\"targetUserId\":\"" << shell_escape_single_quotes(target_user_id)
            << "\"}'::jsonb,NOW(),NULL);";
        sql << "COMMIT;";
        exec_sql(sql.str());
        return true;
    }

    bool decline_friend_request(const std::string& actor_user_id, const std::string& target_user_id) const {
        if (get_relationship_status(actor_user_id, target_user_id) != std::optional<std::string>("pending_incoming") ||
            get_relationship_status(target_user_id, actor_user_id) != std::optional<std::string>("pending_outgoing")) {
            return false;
        }
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "UPDATE user_relationships SET status='declined', updated_at=NOW() WHERE relation_type='friend' AND "
            << "((user_id='" << shell_escape_single_quotes(actor_user_id) << "' AND target_user_id='" << shell_escape_single_quotes(target_user_id) << "') OR "
            << "(user_id='" << shell_escape_single_quotes(target_user_id) << "' AND target_user_id='" << shell_escape_single_quotes(actor_user_id) << "'));";
        sql << "COMMIT;";
        exec_sql(sql.str());
        return true;
    }

    bool remove_friend(const std::string& actor_user_id, const std::string& target_user_id) const {
        if (!are_friends(actor_user_id, target_user_id)) {
            return false;
        }
        const auto now = now_iso8601();
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "UPDATE user_relationships SET status='removed', updated_at=NOW() WHERE relation_type='friend' AND "
            << "((user_id='" << shell_escape_single_quotes(actor_user_id) << "' AND target_user_id='" << shell_escape_single_quotes(target_user_id) << "') OR "
            << "(user_id='" << shell_escape_single_quotes(target_user_id) << "' AND target_user_id='" << shell_escape_single_quotes(actor_user_id) << "'));";
        sql << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) VALUES ("
            << "'" << shell_escape_single_quotes(hash_to_uuid("friend-removed-" + actor_user_id + "-" + target_user_id + "-" + now)) << "','user_relationship','"
            << shell_escape_single_quotes(actor_user_id) << "','user.friend_removed','{\"userId\":\""
            << shell_escape_single_quotes(actor_user_id) << "\",\"targetUserId\":\"" << shell_escape_single_quotes(target_user_id)
            << "\"}'::jsonb,NOW(),NULL);";
        sql << "COMMIT;";
        exec_sql(sql.str());
        return true;
    }

    void patch_privacy_settings(const std::string& user_id, const JsonObject& patch) const {
        if (!enabled_) {
            return;
        }
        std::vector<std::string> assignments;
        if (const auto value = optional_string(patch, "profileVisibility")) {
            assignments.push_back("profile_visibility='" + shell_escape_single_quotes(*value) + "'");
        }
        if (const auto value = optional_string(patch, "dmPolicy")) {
            assignments.push_back("dm_policy='" + shell_escape_single_quotes(*value) + "'");
        }
        if (const auto value = optional_string(patch, "friendRequestPolicy")) {
            assignments.push_back("friend_request_policy='" + shell_escape_single_quotes(*value) + "'");
        }
        if (const auto value = optional_string(patch, "lastSeenVisibility")) {
            assignments.push_back("last_seen_visibility='" + shell_escape_single_quotes(*value) + "'");
        }
        if (const auto value = optional_string(patch, "avatarVisibility")) {
            assignments.push_back("avatar_visibility='" + shell_escape_single_quotes(*value) + "'");
        }
        if (assignments.empty()) {
            return;
        }
        assignments.push_back("updated_at=NOW()");
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "UPDATE user_privacy_settings SET ";
        for (std::size_t i = 0; i < assignments.size(); ++i) {
            if (i != 0U) {
                sql << ",";
            }
            sql << assignments[i];
        }
        sql << " WHERE user_id='" << shell_escape_single_quotes(user_id) << "';";
        sql << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) VALUES (";
        sql << "'" << shell_escape_single_quotes(hash_to_uuid("privacy-updated-" + user_id + now_iso8601())) << "','user_privacy','"
            << shell_escape_single_quotes(user_id) << "','user.privacy_updated','{\"userId\":\""
            << shell_escape_single_quotes(user_id) << "\"}'::jsonb,NOW(),NULL);";
        sql << "COMMIT;";
        exec_sql(sql.str());
    }

    void ensure_profile_exists(const std::string& user_id, const std::string& display_name) const {
        if (!enabled_) {
            return;
        }
        const std::string escaped_user_id = shell_escape_single_quotes(user_id);
        const std::string escaped_display_name = shell_escape_single_quotes(display_name);
        const std::string sql =
            "BEGIN;"
            "INSERT INTO user_profiles (user_id, display_name, username, avatar_object_id, bio, locale, time_zone, profile_status, created_at, updated_at, deleted_at) "
            "VALUES ('" + escaped_user_id + "','" + escaped_display_name + "',NULL,NULL,NULL,NULL,NULL,'active',NOW(),NOW(),NULL) "
            "ON CONFLICT (user_id) DO NOTHING;"
            "INSERT INTO user_privacy_settings (user_id, profile_visibility, dm_policy, friend_request_policy, last_seen_visibility, avatar_visibility, created_at, updated_at) "
            "VALUES ('" + escaped_user_id + "','public','everyone','everyone','public','public',NOW(),NOW()) "
            "ON CONFLICT (user_id) DO NOTHING;"
            "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) "
            "VALUES ('" + shell_escape_single_quotes(hash_to_uuid("profile-created-" + user_id + now_iso8601())) + "','user_profile','" + escaped_user_id + "','user.profile_created',"
            "'{\"userId\":\"" + escaped_user_id + "\"}'::jsonb,NOW(),NULL);"
            "COMMIT;";
        exec_sql(sql);
    }

    void patch_profile(const std::string& user_id, const JsonObject& patch) const {
        if (!enabled_) {
            return;
        }
        std::vector<std::string> assignments;
        if (const auto value = optional_string(patch, "displayName")) {
            assignments.push_back("display_name='" + shell_escape_single_quotes(*value) + "'");
        }
        if (patch.count("username") != 0) {
            if (patch.at("username").is_null()) {
                assignments.push_back("username=NULL");
            } else {
                assignments.push_back("username='" + shell_escape_single_quotes(required_string(patch, "username")) + "'");
            }
        }
        if (patch.count("avatarObjectId") != 0) {
            if (patch.at("avatarObjectId").is_null()) {
                assignments.push_back("avatar_object_id=NULL");
            } else {
                assignments.push_back("avatar_object_id='" + shell_escape_single_quotes(required_string(patch, "avatarObjectId")) + "'");
            }
        }
        for (const auto field : {"bio", "locale", "timeZone"}) {
            if (patch.count(field) != 0) {
                const std::string column = std::string(field == std::string("timeZone") ? "time_zone" : to_lower(field));
                if (patch.at(field).is_null()) {
                    assignments.push_back(column + "=NULL");
                } else {
                    assignments.push_back(column + "='" + shell_escape_single_quotes(required_string(patch, field)) + "'");
                }
            }
        }
        if (assignments.empty()) {
            return;
        }
        assignments.push_back("updated_at=NOW()");
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "UPDATE user_profiles SET ";
        for (std::size_t i = 0; i < assignments.size(); ++i) {
            if (i != 0U) {
                sql << ",";
            }
            sql << assignments[i];
        }
        sql << " WHERE user_id='" << shell_escape_single_quotes(user_id) << "';";
        sql << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) VALUES (";
        sql << "'" << shell_escape_single_quotes(hash_to_uuid("profile-updated-" + user_id + now_iso8601())) << "','user_profile','"
            << shell_escape_single_quotes(user_id) << "','user.profile_updated','{\"userId\":\""
            << shell_escape_single_quotes(user_id) << "\"}'::jsonb,NOW(),NULL);";
        sql << "COMMIT;";
        exec_sql(sql.str());
    }

    int migrate_up(const std::string& migrations_dir) const;
    int migrate_down(const std::string& migrations_dir) const;
    int migrate_status(const std::string& migrations_dir) const;

private:
    bool enabled_ = false;
    std::optional<DbConfig> config_;

    std::string psql_command_prefix() const {
        const auto& cfg = *config_;
        return "PGPASSWORD='" + shell_escape_single_quotes(cfg.password) + "' psql -X -v ON_ERROR_STOP=1 -h '" + shell_escape_single_quotes(cfg.host) +
               "' -p '" + shell_escape_single_quotes(cfg.port) + "' -U '" + shell_escape_single_quotes(cfg.user) +
               "' -d '" + shell_escape_single_quotes(cfg.name) + "' ";
    }

    void exec_sql(const std::string& sql) const {
        const auto temp_file = write_temp_sql_file(sql);
        const auto result = run_command_capture(psql_command_prefix() + "-q -f '" + shell_escape_single_quotes(temp_file.string()) + "' 2>&1");
        std::error_code ec;
        std::filesystem::remove(temp_file, ec);
        if (result.exit_code != 0) {
            throw std::runtime_error("psql exec failed: " + result.output);
        }
    }

    std::optional<std::string> query_scalar(const std::string& sql) const {
        const auto rows = query_rows(sql);
        if (!rows.has_value() || rows->empty() || rows->front().empty()) {
            return std::nullopt;
        }
        return rows->front().front();
    }

    std::optional<std::vector<std::vector<std::string>>> query_rows(const std::string& sql) const {
        const auto temp_file = write_temp_sql_file(sql);
        const auto result = run_command_capture(psql_command_prefix() + "-At -F '|' -f '" + shell_escape_single_quotes(temp_file.string()) + "' 2>&1");
        std::error_code ec;
        std::filesystem::remove(temp_file, ec);
        if (result.exit_code != 0) {
            throw std::runtime_error("psql query failed: " + result.output);
        }
        std::vector<std::vector<std::string>> rows;
        std::stringstream ss(result.output);
        std::string line;
        while (std::getline(ss, line)) {
            line = trim(line);
            if (line.empty()) {
                continue;
            }
            std::vector<std::string> columns;
            std::size_t start = 0;
            while (true) {
                const auto pos = line.find('|', start);
                if (pos == std::string::npos) {
                    columns.push_back(line.substr(start));
                    break;
                }
                columns.push_back(line.substr(start, pos - start));
                start = pos + 1;
            }
            rows.push_back(columns);
        }
        return rows;
    }
};

std::vector<std::filesystem::path> list_migration_files(const std::string& migrations_dir, const std::string& suffix) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(migrations_dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().filename().string().ends_with(suffix)) {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

std::string read_file_text(const std::filesystem::path& path) {
    std::ifstream stream(path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Cannot open file: " + path.string());
    }
    std::ostringstream buffer;
    buffer << stream.rdbuf();
    return buffer.str();
}

int PostgresPsqlAdapter::migrate_up(const std::string& migrations_dir) const {
    if (!enabled_) {
        throw std::runtime_error("Database env is not configured");
    }
    exec_sql("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT NOW());");
    for (const auto& file : list_migration_files(migrations_dir, ".up.sql")) {
        const auto version = file.filename().string().substr(0, 4);
        if (query_scalar("SELECT version FROM schema_migrations WHERE version='" + version + "';").has_value()) {
            continue;
        }
        exec_sql("BEGIN;" + read_file_text(file) + "INSERT INTO schema_migrations(version, applied_at) VALUES ('" + version + "', NOW());COMMIT;");
    }
    return 0;
}

int PostgresPsqlAdapter::migrate_down(const std::string& migrations_dir) const {
    if (!enabled_) {
        throw std::runtime_error("Database env is not configured");
    }
    exec_sql("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT NOW());");
    const auto current = query_scalar("SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1;");
    if (!current.has_value()) {
        std::cout << "No migrations applied\n";
        return 0;
    }
    std::filesystem::path path;
    for (const auto& candidate : list_migration_files(migrations_dir, ".down.sql")) {
        if (candidate.filename().string().rfind(*current + "_", 0) == 0) {
            path = candidate;
            break;
        }
    }
    if (path.empty()) {
        throw std::runtime_error("Down migration file not found for version " + *current);
    }
    exec_sql("BEGIN;" + read_file_text(path) + "DELETE FROM schema_migrations WHERE version='" + *current + "';COMMIT;");
    return 0;
}

int PostgresPsqlAdapter::migrate_status(const std::string& migrations_dir) const {
    if (!enabled_) {
        throw std::runtime_error("Database env is not configured");
    }
    exec_sql("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT NOW());");
    for (const auto& file : list_migration_files(migrations_dir, ".up.sql")) {
        const auto version = file.filename().string().substr(0, 4);
        const bool applied = query_scalar("SELECT version FROM schema_migrations WHERE version='" + version + "';").has_value();
        std::cout << version << " " << (applied ? "up" : "pending") << "\n";
    }
    return 0;
}

class ServiceState {
public:
    Response handle(const Request& request) {
        std::lock_guard<std::mutex> guard(mutex_);
        current_request_method_ = request.method;
        current_request_path_ = request.path;
        current_request_id_ = request_id_for(request);
        current_jwt_principal_.reset();
        try {
            auto response = route(request);
            reset_request_context();
            return response;
        } catch (const HttpError& ex) {
            auto response = error_response(ex.status, ex.code, ex.what());
            reset_request_context();
            return response;
        } catch (const std::exception& ex) {
            auto response = error_response(400, "bad_request", ex.what());
            reset_request_context();
            return response;
        }
    }

private:
    std::mutex mutex_;
    PostgresPsqlAdapter db_;
    std::optional<std::string> jwt_issuer_ = get_env("JWT_ISSUER");
    std::optional<std::string> jwt_audience_ = get_env("JWT_AUDIENCE");
    std::optional<std::string> jwt_secret_ = get_env("JWT_SECRET");
    std::optional<std::string> internal_token_ = get_env("INTERNAL_TOKEN");
    std::unordered_map<std::string, UserProfile> profiles_;
    std::unordered_map<std::string, PrivacySettings> privacy_;
    std::unordered_map<std::string, RelationshipRecord> relationships_;
    std::unordered_map<std::string, BlockRecord> blocks_;
    std::unordered_map<std::string, ProjectionRecord> projections_;
    std::unordered_map<std::string, DeviceSession> sessions_;
    std::vector<JsonObject> outbox_;
    std::vector<JsonObject> audit_log_;
    std::set<std::string> processed_event_ids_;
    std::unordered_map<std::string, int> metrics_;
    std::string current_request_id_ = "-";
    std::string current_request_method_ = "-";
    std::string current_request_path_ = "-";
    std::optional<JwtPrincipal> current_jwt_principal_;

    void reset_request_context() {
        current_request_id_ = "-";
        current_request_method_ = "-";
        current_request_path_ = "-";
        current_jwt_principal_.reset();
    }

    std::string request_id_for(const Request& request) const {
        const auto it = request.headers.find("x-request-id");
        if (it != request.headers.end() && !trim(it->second).empty()) {
            return trim(it->second);
        }
        std::ostringstream oss;
        oss << "req-" << std::hex << reinterpret_cast<std::uintptr_t>(&request);
        return oss.str();
    }

    static std::string sanitize_log_value(std::string value) {
        if (value.empty()) {
            return "-";
        }
        for (char& ch : value) {
            if (std::isspace(static_cast<unsigned char>(ch)) != 0 || ch == '=') {
                ch = '_';
            }
        }
        return value;
    }

    static std::string optional_log_value(const std::optional<std::string>& value) {
        return value.has_value() ? sanitize_log_value(*value) : "-";
    }

    static std::string optional_log_value(const std::optional<long long>& value) {
        return value.has_value() ? std::to_string(*value) : "-";
    }

    std::string current_actor_for_log() const {
        return current_jwt_principal_.has_value() ? sanitize_log_value(current_jwt_principal_->canonical_id) : "-";
    }

    void log_event(
        const std::string& phase,
        const std::string& status,
        const std::string& actor_user_id,
        const std::string& target_user_id,
        const std::string& decision,
        const std::string& reason,
        const std::vector<std::pair<std::string, std::string>>& extra = {}) const {
        std::ostringstream oss;
        oss << "phase=" << sanitize_log_value(phase)
            << " method=" << sanitize_log_value(current_request_method_)
            << " path=" << sanitize_log_value(current_request_path_)
            << " status=" << sanitize_log_value(status)
            << " requestId=" << sanitize_log_value(current_request_id_)
            << " actorUserId=" << sanitize_log_value(actor_user_id)
            << " targetUserId=" << sanitize_log_value(target_user_id)
            << " decision=" << sanitize_log_value(decision)
            << " reason=" << sanitize_log_value(reason);
        for (const auto& [key, value] : extra) {
            oss << ' ' << sanitize_log_value(key) << '=' << sanitize_log_value(value);
        }
        std::cout << oss.str() << "\n";
    }

    void log_auth_start(const Request& request, const bool auth_header_present, const bool bearer_present) const {
        const auto forwarded = request.headers.find("x-forwarded-for");
        const auto real_ip = request.headers.find("x-real-ip");
        log_event(
            "user.auth.start",
            "-",
            "-",
            "-",
            "info",
            "-",
            {
                {"authHeaderPresent", bool_string(auth_header_present)},
                {"bearerPresent", bool_string(bearer_present)},
                {"xForwardedFor", forwarded != request.headers.end() ? trim(forwarded->second) : "-"},
                {"xRealIp", real_ip != request.headers.end() ? trim(real_ip->second) : "-"},
            });
    }

    void log_auth_result(
        const std::string& status,
        const std::string& actor_user_id,
        const std::string& decision,
        const std::string& reason,
        const std::optional<JwtPrincipal>& principal,
        const std::string& token_signature_valid,
        const bool auth_header_present,
        const bool bearer_present,
        const bool token_parse_ok) const {
        log_event(
            "user.auth.result",
            status,
            actor_user_id,
            "-",
            decision,
            reason,
            {
                {"authHeaderPresent", bool_string(auth_header_present)},
                {"bearerPresent", bool_string(bearer_present)},
                {"tokenParseOk", bool_string(token_parse_ok)},
                {"tokenSignatureValid", token_signature_valid},
                {"tokenSub", principal.has_value() ? principal->subject : "-"},
                {"tokenUserId", principal.has_value() ? principal->canonical_id : "-"},
                {"tokenIssuer", principal.has_value() ? optional_log_value(principal->issuer) : "-"},
                {"tokenAudience", principal.has_value() ? optional_log_value(principal->audience) : "-"},
                {"tokenExp", principal.has_value() ? optional_log_value(principal->exp) : "-"},
                {"now", std::to_string(std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count())},
            });
    }

    void log_privacy_resolution(
        const std::string& actor_user_id,
        const std::string& target_user_id,
        const std::string& policy_type,
        const std::string& decision,
        const std::string& reason,
        const std::string& actor_privacy_exists,
        const std::string& target_privacy_exists,
        const std::string& resolved_policy,
        const std::string& mutuals_satisfied = "-") const {
        log_event(
            "user.privacy.resolve",
            "-",
            actor_user_id,
            target_user_id,
            decision,
            reason,
            {
                {"policyType", policy_type},
                {"actorPrivacyExists", actor_privacy_exists},
                {"targetPrivacyExists", target_privacy_exists},
                {"resolvedPolicy", resolved_policy},
                {"mutualsSatisfied", mutuals_satisfied},
            });
    }

    void log_friend_request_decision(
        const std::string& status,
        const std::string& actor_user_id,
        const std::string& target_user_id,
        const std::string& decision,
        const std::string& reason,
        const std::string& relationship_state_before,
        const bool is_already_friend,
        const bool has_pending_outgoing,
        const bool has_pending_incoming,
        const std::string& target_policy,
        const std::string& rows_inserted) const {
        log_event(
            "user.friend_request.create",
            status,
            actor_user_id,
            target_user_id,
            decision,
            reason,
            {
                {"relationshipStateBefore", relationship_state_before},
                {"isAlreadyFriend", bool_string(is_already_friend)},
                {"hasPendingOutgoing", bool_string(has_pending_outgoing)},
                {"hasPendingIncoming", bool_string(has_pending_incoming)},
                {"targetFriendRequestPolicy", target_policy},
                {"rowsInserted", rows_inserted},
            });
    }

    void log_friend_remove_decision(
        const std::string& status,
        const std::string& actor_user_id,
        const std::string& target_user_id,
        const std::string& decision,
        const std::string& reason,
        const std::string& relationship_state_before,
        const bool friend_edge_exists,
        const std::string& rows_deleted) const {
        log_event(
            "user.friend.remove",
            status,
            actor_user_id,
            target_user_id,
            decision,
            reason,
            {
                {"relationshipStateBefore", relationship_state_before},
                {"friendEdgeExists", bool_string(friend_edge_exists)},
                {"rowsDeleted", rows_deleted},
            });
    }

    std::string relationship_state_before(const std::string& actor_user_id, const std::string& target_user_id) const {
        const auto it = relationships_.find(pair_key(actor_user_id, target_user_id));
        return it == relationships_.end() ? "none" : it->second.status;
    }

    bool has_pending_outgoing(const std::string& actor_user_id, const std::string& target_user_id) const {
        const auto it = relationships_.find(pair_key(actor_user_id, target_user_id));
        return it != relationships_.end() && it->second.status == "pending_outgoing";
    }

    bool has_pending_incoming(const std::string& actor_user_id, const std::string& target_user_id) const {
        const auto it = relationships_.find(pair_key(actor_user_id, target_user_id));
        return it != relationships_.end() && it->second.status == "pending_incoming";
    }

    Response route(const Request& request) {
        if (request.method == "GET" && request.path == "/healthz") {
            return json_response(200, JsonObject{{"status", "ok"}});
        }
        if (request.method == "GET" && request.path == "/health") {
            if (db_.enabled()) {
                bool ready = false;
                try {
                    ready = db_.ready();
                } catch (const std::exception& ex) {
                    return json_response(503, JsonObject{{"status", "degraded"}, {"ready", false}, {"message", ex.what()}});
                }
                if (!ready) {
                    return json_response(503, JsonObject{{"status", "degraded"}, {"ready", false}});
                }
                return json_response(200, JsonObject{{"status", "ok"}, {"ready", true}, {"storage", "postgres"}});
            }
            return json_response(200, JsonObject{{"status", "ok"}, {"ready", true}, {"storage", "memory"}});
        }
        if (request.method == "GET" && request.path == "/internal/metrics") {
            return internal_metrics(request);
        }
        if (request.method == "GET" && request.path == "/internal/outbox") {
            return internal_outbox(request);
        }
        if (request.method == "GET" && request.path == "/internal/audit-log") {
            return internal_audit_log(request);
        }
        if (request.method == "POST" && request.path == "/internal/events") {
            return ingest_event(request);
        }
        if (starts_with(request.path, "/internal/users/") && ends_with(request.path, "/profile") && request.method == "GET") {
            return internal_get_profile(request);
        }
        if (request.method == "POST" && request.path == "/internal/users/relationships/check") {
            return internal_relationship_check(request);
        }
        if (starts_with(request.path, "/internal/users/") && ends_with(request.path, "/authorize-profile-read") && request.method == "POST") {
            return internal_authorize_profile_read(request);
        }
        if (request.method == "GET" && request.path == "/v1/users/me") {
            return get_me(request);
        }
        if (request.method == "PATCH" && request.path == "/v1/users/me") {
            return patch_me(request);
        }
        if (request.method == "GET" && request.path == "/v1/users/me/privacy") {
            return get_my_privacy(request);
        }
        if (request.method == "PATCH" && request.path == "/v1/users/me/privacy") {
            return patch_my_privacy(request);
        }
        if (request.method == "GET" && request.path == "/v1/users/me/rooms") {
            return list_projection_entities(request, "room");
        }
        if (request.method == "GET" && request.path == "/v1/users/me/conversations") {
            return list_projection_entities(request, "conversation");
        }
        if (request.method == "GET" && request.path == "/v1/users/me/contacts") {
            return list_contacts(request);
        }
        if (starts_with(request.path, "/v1/users/")) {
            return handle_user_scoped_route(request);
        }
        return error_response(404, "not_found", "Route not found");
    }

    static bool starts_with(const std::string& value, const std::string& prefix) {
        return value.rfind(prefix, 0) == 0;
    }

    static bool ends_with(const std::string& value, const std::string& suffix) {
        return value.size() >= suffix.size() && value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
    }

    Response handle_user_scoped_route(const Request& request) {
        const auto segments = split_path(request.path);
        const auto target_user_id = segments.size() >= 3 ? canonical_user_id(segments[2]) : std::string{};
        if (segments.size() == 3 && request.method == "GET") {
            return get_user_by_id(request, target_user_id);
        }
        if (segments.size() == 4 && segments[3] == "friend-request" && request.method == "POST") {
            return send_friend_request(request, target_user_id);
        }
        if (segments.size() == 5 && segments[3] == "friend-request" && segments[4] == "accept" && request.method == "POST") {
            return accept_friend_request(request, target_user_id);
        }
        if (segments.size() == 5 && segments[3] == "friend-request" && segments[4] == "decline" && request.method == "POST") {
            return decline_friend_request(request, target_user_id);
        }
        if (segments.size() == 4 && segments[3] == "friend" && request.method == "DELETE") {
            return remove_friend(request, target_user_id);
        }
        if (segments.size() == 4 && segments[3] == "block" && request.method == "POST") {
            return block_user(request, target_user_id);
        }
        if (segments.size() == 4 && segments[3] == "block" && request.method == "DELETE") {
            return unblock_user(request, target_user_id);
        }
        return error_response(404, "not_found", "Route not found");
    }

    std::vector<std::string> split_path(const std::string& path) const {
        std::vector<std::string> segments;
        std::stringstream ss(path);
        std::string item;
        while (std::getline(ss, item, '/')) {
            if (!item.empty()) {
                segments.push_back(item);
            }
        }
        return segments;
    }

    Response json_response(int status, Json body) {
        Response response;
        response.status = status;
        response.body = std::move(body);
        return response;
    }

    Response error_response(int status, const std::string& code, const std::string& message) {
        increment_metric("http.error." + std::to_string(status));
        return json_response(status, JsonObject{{"error", code}, {"message", message}});
    }

    void increment_metric(const std::string& key) {
        ++metrics_[key];
    }

    std::string require_actor_user_id(const Request& request) {
        const auto it = request.headers.find("authorization");
        const bool auth_header_present = it != request.headers.end();
        std::string raw_header = auth_header_present ? trim(it->second) : "";
        const std::string header = to_lower(raw_header);
        const bool bearer_present = header.rfind("bearer ", 0) == 0;
        log_auth_start(request, auth_header_present, bearer_present);
        if (it == request.headers.end()) {
            log_auth_result("401", "-", "deny", "missing_bearer", std::nullopt, "false", false, false, false);
            throw HttpError(401, "unauthorized", "Missing Authorization header");
        }
        const std::string prefix = "bearer user:";
        if (header.rfind(prefix, 0) == 0) {
            const auto actor_user_id = canonical_user_id(trim(raw_header.substr(prefix.size())));
            current_jwt_principal_ = JwtPrincipal{
                .raw_user_id = actor_user_id,
                .canonical_id = actor_user_id,
                .subject = actor_user_id,
                .issuer = std::nullopt,
                .audience = std::nullopt,
                .exp = std::nullopt,
                .display_name = std::nullopt,
            };
            log_auth_result("200", actor_user_id, "allow", "ok", std::nullopt, "legacy", true, true, true);
            return actor_user_id;
        }
        const std::string bearer_prefix = "bearer ";
        if (header.rfind(bearer_prefix, 0) != 0) {
            log_auth_result("401", "-", "deny", "missing_bearer", std::nullopt, "false", true, false, false);
            throw HttpError(401, "unauthorized", "Expected Authorization: Bearer <jwt> or Bearer user:<user-id>");
        }
        std::optional<JwtPrincipal> principal;
        try {
            principal = parse_jwt_without_signature_validation(trim(raw_header.substr(bearer_prefix.size())), jwt_issuer_, jwt_audience_, jwt_secret_);
        } catch (const std::exception& ex) {
            std::string reason = "claims_mismatch";
            const std::string message = ex.what();
            if (message.find("issuer mismatch") != std::string::npos) {
                reason = "bad_issuer";
            } else if (message.find("audience mismatch") != std::string::npos) {
                reason = "bad_audience";
            } else if (message.find("expired") != std::string::npos) {
                reason = "expired";
            } else if (message.find("signature mismatch") != std::string::npos || message.find("Malformed JWT") != std::string::npos || message.find("base64url") != std::string::npos || message.find("secret") != std::string::npos || message.find("alg") != std::string::npos) {
                reason = "bad_signature";
            }
            log_auth_result("401", "-", "deny", reason, std::nullopt, "false", true, true, false);
            throw HttpError(401, "unauthorized", ex.what());
        }
        current_jwt_principal_ = principal;
        log_auth_result("200", principal->canonical_id, "allow", "ok", principal, "true", true, true, true);
        return principal->canonical_id;
    }

    std::optional<std::string> actor_display_name_from_jwt(const Request& request) {
        if (current_jwt_principal_.has_value()) {
            return current_jwt_principal_->display_name;
        }
        const auto it = request.headers.find("authorization");
        if (it == request.headers.end()) {
            return std::nullopt;
        }
        const std::string raw_header = trim(it->second);
        const std::string bearer_prefix = "bearer ";
        const std::string header = to_lower(raw_header);
        if (header.rfind(bearer_prefix, 0) != 0 || header.rfind("bearer user:", 0) == 0) {
            return std::nullopt;
        }
        return parse_jwt_without_signature_validation(trim(raw_header.substr(bearer_prefix.size())), jwt_issuer_, jwt_audience_, jwt_secret_).display_name;
    }

    void require_internal_token(const Request& request) {
        if (request.headers.find("authorization") != request.headers.end()) {
            require_actor_user_id(request);
            return;
        }
        const auto it = request.headers.find("x-internal-token");
        const std::string expected = internal_token_.value_or("internal-secret");
        if (it == request.headers.end() || trim(it->second) != expected) {
            throw HttpError(401, "unauthorized", "Missing or invalid internal authentication");
        }
    }

    UserProfile& require_profile(const std::string& user_id) {
        auto it = profiles_.find(user_id);
        if (it == profiles_.end()) {
            throw std::runtime_error("User profile not found: " + user_id);
        }
        return it->second;
    }

    PrivacySettings& require_privacy(const std::string& user_id) {
        auto it = privacy_.find(user_id);
        if (it == privacy_.end()) {
            throw std::runtime_error("User privacy settings not found: " + user_id);
        }
        return it->second;
    }

    const UserProfile& require_profile_const(const std::string& user_id) const {
        auto it = profiles_.find(user_id);
        if (it == profiles_.end()) {
            throw std::runtime_error("User profile not found: " + user_id);
        }
        return it->second;
    }

    const PrivacySettings& require_privacy_const(const std::string& user_id) const {
        auto it = privacy_.find(user_id);
        if (it == privacy_.end()) {
            throw std::runtime_error("User privacy settings not found: " + user_id);
        }
        return it->second;
    }

    JsonObject ensure_db_profile_exists_and_load(const std::string& user_id, const std::optional<std::string>& preferred_display_name) {
        db_.ensure_profile_exists(user_id, preferred_display_name.value_or("User " + user_id.substr(0, std::min<std::size_t>(8, user_id.size()))));
        const auto profile = db_.get_profile(user_id);
        if (!profile.has_value()) {
            throw std::runtime_error("Failed to load DB profile after ensure");
        }
        return *profile;
    }

    JsonObject ensure_db_privacy_exists_and_load(const std::string& user_id) {
        db_.ensure_profile_exists(user_id, "User " + user_id.substr(0, std::min<std::size_t>(8, user_id.size())));
        const auto settings = db_.get_privacy_settings(user_id);
        if (!settings.has_value()) {
            throw std::runtime_error("Failed to load DB privacy after ensure");
        }
        return *settings;
    }

    UserProfile& ensure_memory_profile_exists(const std::string& user_id, const std::optional<std::string>& preferred_display_name) {
        auto it = profiles_.find(user_id);
        if (it != profiles_.end()) {
            return it->second;
        }
        const auto timestamp = now_iso8601();
        UserProfile profile;
        profile.user_id = user_id;
        profile.display_name = preferred_display_name.value_or("User " + user_id.substr(0, std::min<std::size_t>(8, user_id.size())));
        profile.created_at = timestamp;
        profile.updated_at = timestamp;
        profiles_[user_id] = profile;

        PrivacySettings settings;
        settings.user_id = user_id;
        settings.created_at = timestamp;
        settings.updated_at = timestamp;
        privacy_[user_id] = settings;
        return profiles_.at(user_id);
    }

    bool has_block(const std::string& user_id, const std::string& target_user_id) const {
        return blocks_.find(pair_key(user_id, target_user_id)) != blocks_.end();
    }

    bool is_friend(const std::string& user_id, const std::string& target_user_id) const {
        const auto lhs = relationships_.find(pair_key(user_id, target_user_id));
        const auto rhs = relationships_.find(pair_key(target_user_id, user_id));
        return lhs != relationships_.end() && rhs != relationships_.end() &&
               lhs->second.status == "accepted" && rhs->second.status == "accepted";
    }

    RelationshipSummary relationship_summary(const std::string& actor_user_id, const std::string& target_user_id) const {
        return RelationshipSummary{
            .is_friend = is_friend(actor_user_id, target_user_id),
            .is_blocked = has_block(actor_user_id, target_user_id),
            .is_blocked_by_target = has_block(target_user_id, actor_user_id),
        };
    }

    RelationshipSummary db_relationship_summary(const std::string& actor_user_id, const std::string& target_user_id) const {
        return RelationshipSummary{
            .is_friend = db_.are_friends(actor_user_id, target_user_id),
            .is_blocked = db_.has_block(actor_user_id, target_user_id),
            .is_blocked_by_target = db_.has_block(target_user_id, actor_user_id),
        };
    }

    Json profile_to_json(const UserProfile& profile, bool include_private_fields) const {
        JsonObject object{
            {"userId", profile.user_id},
            {"displayName", profile.display_name},
            {"profileStatus", profile.profile_status},
            {"createdAt", profile.created_at},
            {"updatedAt", profile.updated_at},
        };
        object["username"] = profile.username.has_value() ? Json(*profile.username) : Json(nullptr);
        object["bio"] = profile.bio.has_value() ? Json(*profile.bio) : Json(nullptr);
        object["locale"] = profile.locale.has_value() ? Json(*profile.locale) : Json(nullptr);
        object["timeZone"] = profile.time_zone.has_value() ? Json(*profile.time_zone) : Json(nullptr);
        object["avatarObjectId"] = profile.avatar_object_id.has_value() ? Json(*profile.avatar_object_id) : Json(nullptr);
        object["deletedAt"] = profile.deleted_at.has_value() ? Json(*profile.deleted_at) : Json(nullptr);
        if (!include_private_fields) {
            const auto& settings = require_privacy_const(profile.user_id);
            if (settings.avatar_visibility == "private") {
                object["avatarObjectId"] = Json(nullptr);
            }
        }
        return object;
    }

    Json privacy_to_json(const PrivacySettings& privacy) const {
        return JsonObject{
            {"userId", privacy.user_id},
            {"profileVisibility", privacy.profile_visibility},
            {"dmPolicy", privacy.dm_policy},
            {"friendRequestPolicy", privacy.friend_request_policy},
            {"lastSeenVisibility", privacy.last_seen_visibility},
            {"avatarVisibility", privacy.avatar_visibility},
            {"createdAt", privacy.created_at},
            {"updatedAt", privacy.updated_at},
        };
    }

    void validate_display_name(const std::string& display_name) const {
        if (display_name.empty() || display_name.size() > 64) {
            throw std::runtime_error("displayName must be 1..64 characters");
        }
    }

    void validate_username(const std::string& username) const {
        if (username.empty() || username.size() > 32) {
            throw std::runtime_error("username must be 1..32 characters");
        }
        for (char ch : username) {
            if (!(std::isalnum(static_cast<unsigned char>(ch)) != 0 || ch == '_' || ch == '.')) {
                throw std::runtime_error("username supports only [A-Za-z0-9_.]");
            }
        }
    }

    void validate_profile_visibility(const std::string& value) const {
        static const std::set<std::string> allowed = {"public", "friends_only", "private"};
        if (!allowed.count(value)) {
            throw std::runtime_error("Invalid profileVisibility");
        }
    }

    void validate_dm_policy(const std::string& value) const {
        static const std::set<std::string> allowed = {"everyone", "friends_only", "nobody"};
        if (!allowed.count(value)) {
            throw std::runtime_error("Invalid dmPolicy");
        }
    }

    void validate_friend_request_policy(const std::string& value) const {
        static const std::set<std::string> allowed = {"everyone", "mutuals_only", "nobody"};
        if (!allowed.count(value)) {
            throw std::runtime_error("Invalid friendRequestPolicy");
        }
    }

    void validate_last_seen_visibility(const std::string& value) const {
        static const std::set<std::string> allowed = {"public", "friends_only", "private"};
        if (!allowed.count(value)) {
            throw std::runtime_error("Invalid lastSeenVisibility");
        }
    }

    void validate_avatar_visibility(const std::string& value) const {
        static const std::set<std::string> allowed = {"public", "friends_only", "private"};
        if (!allowed.count(value)) {
            throw std::runtime_error("Invalid avatarVisibility");
        }
    }

    void publish_event(const std::string& type, const JsonObject& payload) {
        JsonObject event{
            {"eventType", type},
            {"occurredAt", now_iso8601()},
            {"payload", payload},
        };
        outbox_.push_back(event);
    }

    void audit(const std::string& action, const std::string& actor_user_id, const std::string& target_user_id, const JsonObject& details = {}) {
        JsonObject record{
            {"action", action},
            {"actorUserId", actor_user_id},
            {"targetUserId", target_user_id},
            {"createdAt", now_iso8601()},
            {"details", details},
        };
        audit_log_.push_back(record);
    }

    bool authorize_profile_read(const std::string& actor_user_id, const std::string& target_user_id) const {
        if (actor_user_id == target_user_id) {
            return true;
        }
        const auto summary = relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            return false;
        }
        const auto& profile = require_profile_const(target_user_id);
        if (profile.profile_status != "active") {
            return false;
        }
        const auto& settings = require_privacy_const(target_user_id);
        if (settings.profile_visibility == "public") {
            return true;
        }
        if (settings.profile_visibility == "friends_only") {
            return summary.is_friend;
        }
        return false;
    }

    bool authorize_dm_start(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) const {
        if (actor_user_id == target_user_id) {
            reason = "self_dm_not_supported";
            return false;
        }
        const auto summary = relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked) {
            reason = "blocked_by_actor";
            return false;
        }
        if (summary.is_blocked_by_target) {
            reason = "blocked_by_target";
            return false;
        }
        const auto& profile = require_profile_const(target_user_id);
        if (profile.profile_status != "active") {
            reason = "target_inactive";
            return false;
        }
        const auto& settings = require_privacy_const(target_user_id);
        if (settings.dm_policy == "everyone") {
            reason.clear();
            return true;
        }
        if (settings.dm_policy == "friends_only" && summary.is_friend) {
            reason.clear();
            return true;
        }
        reason = "dm_policy_denied";
        return false;
    }

    bool authorize_dm_action_db(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) {
        if (actor_user_id == target_user_id) {
            reason = "self_dm_not_supported";
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "-", "-");
            return false;
        }
        const auto summary = db_relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked) {
            reason = "blocked_by_actor";
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "-", "-");
            return false;
        }
        if (summary.is_blocked_by_target) {
            reason = "blocked_by_target";
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "-", "-");
            return false;
        }
        const auto profile = ensure_db_profile_exists_and_load(target_user_id, std::nullopt);
        if (required_string(profile, "profileStatus") != "active") {
            reason = "target_inactive";
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "-", "-");
            return false;
        }
        const auto settings = ensure_db_privacy_exists_and_load(target_user_id);
        const auto dm_policy = required_string(settings, "dmPolicy");
        if (dm_policy == "everyone") {
            reason.clear();
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "allow", "policy_resolved", "-", "true", dm_policy);
            return true;
        }
        if (dm_policy == "friends_only" && summary.is_friend) {
            reason.clear();
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "allow", "policy_resolved", "-", "true", dm_policy);
            return true;
        }
        reason = "dm_policy_denied";
        log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "true", dm_policy);
        return false;
    }

    bool authorize_profile_read_db(const std::string& actor_user_id, const std::string& target_user_id) {
        if (actor_user_id == target_user_id) {
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "allow", "policy_resolved", "-", "-", "self");
            return true;
        }
        const auto summary = db_relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "deny", "blocked", "-", "-", "-");
            return false;
        }
        const auto profile = ensure_db_profile_exists_and_load(target_user_id, std::nullopt);
        if (required_string(profile, "profileStatus") != "active") {
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "deny", "target_inactive", "-", "-", "-");
            return false;
        }
        const auto settings = ensure_db_privacy_exists_and_load(target_user_id);
        const auto profile_visibility = required_string(settings, "profileVisibility");
        if (profile_visibility == "public") {
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "allow", "policy_resolved", "-", "true", profile_visibility);
            return true;
        }
        if (profile_visibility == "friends_only") {
            const auto allowed = summary.is_friend;
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", allowed ? "allow" : "deny", allowed ? "policy_resolved" : "profile_visibility_denied", "-", "true", profile_visibility);
            return allowed;
        }
        log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "deny", "profile_visibility_denied", "-", "true", profile_visibility);
        return false;
    }

    bool allows_friend_request(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) const {
        const auto summary = relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            reason = "blocked";
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", reason, bool_string(privacy_.count(actor_user_id) != 0), bool_string(privacy_.count(target_user_id) != 0), "-");
            return false;
        }
        const auto target_privacy_exists = privacy_.count(target_user_id) != 0;
        if (!target_privacy_exists) {
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "error", "target_privacy_missing", bool_string(privacy_.count(actor_user_id) != 0), "false", "-");
        }
        const auto& settings = require_privacy_const(target_user_id);
        if (settings.friend_request_policy == "everyone") {
            reason.clear();
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "allow", "policy_resolved", bool_string(privacy_.count(actor_user_id) != 0), "true", settings.friend_request_policy, "-");
            return true;
        }
        if (settings.friend_request_policy == "mutuals_only") {
            for (const auto& [key, relation] : relationships_) {
                if (relation.user_id == actor_user_id && relation.status == "accepted" && is_friend(target_user_id, relation.target_user_id)) {
                    reason.clear();
                    log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "allow", "policy_resolved", bool_string(privacy_.count(actor_user_id) != 0), "true", settings.friend_request_policy, "true");
                    return true;
                }
            }
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", "friend_request_policy_denied", bool_string(privacy_.count(actor_user_id) != 0), "true", settings.friend_request_policy, "false");
        }
        reason = "friend_request_policy_denied";
        if (settings.friend_request_policy != "mutuals_only") {
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", reason, bool_string(privacy_.count(actor_user_id) != 0), "true", settings.friend_request_policy, "-");
        }
        return false;
    }

    bool allows_friend_request_db(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) {
        const auto summary = db_relationship_summary(actor_user_id, target_user_id);
        ensure_db_privacy_exists_and_load(actor_user_id);
        const auto settings = ensure_db_privacy_exists_and_load(target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            reason = "blocked";
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", reason, "true", "true", "-");
            return false;
        }
        const auto policy = required_string(settings, "friendRequestPolicy");
        if (policy == "everyone") {
            reason.clear();
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "allow", "policy_resolved", "true", "true", policy, "-");
            return true;
        }
        if (policy == "mutuals_only") {
            const bool allowed = db_.has_mutual_friend(actor_user_id, target_user_id);
            reason = allowed ? "" : "friend_request_policy_denied";
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", allowed ? "allow" : "deny", allowed ? "policy_resolved" : reason, "true", "true", policy, allowed ? "true" : "false");
            return allowed;
        }
        reason = "friend_request_policy_denied";
        log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", reason, "true", "true", policy, "-");
        return false;
    }

    void ensure_profile_exists(const std::string& user_id) const {
        if (db_.enabled()) {
            if (db_.get_profile(user_id).has_value()) {
                return;
            }
            throw std::runtime_error("Unknown userId: " + user_id);
        }
        if (!profiles_.count(user_id)) {
            throw std::runtime_error("Unknown userId: " + user_id);
        }
    }

    void set_relationship(const std::string& user_id, const std::string& target_user_id, const std::string& status) {
        const auto key = pair_key(user_id, target_user_id);
        const auto now = now_iso8601();
        auto it = relationships_.find(key);
        if (it == relationships_.end()) {
            relationships_[key] = RelationshipRecord{user_id, target_user_id, status, now, now};
        } else {
            it->second.status = status;
            it->second.updated_at = now;
        }
    }

    void hide_dm_projections_between(const std::string& user_a, const std::string& user_b) {
        for (auto& [key, projection] : projections_) {
            if (projection.counterpart_user_id.has_value() &&
                ((projection.user_id == user_a && *projection.counterpart_user_id == user_b) ||
                 (projection.user_id == user_b && *projection.counterpart_user_id == user_a))) {
                projection.visibility_status = "hidden";
                projection.updated_at = now_iso8601();
            }
        }
    }

    Response internal_metrics(const Request& request) {
        require_internal_token(request);
        JsonObject counters;
        for (const auto& [name, value] : metrics_) {
            counters[name] = value;
        }
        return json_response(200, JsonObject{{"counters", counters}});
    }

    Response internal_outbox(const Request& request) {
        require_internal_token(request);
        JsonArray events;
        for (const auto& event : outbox_) {
            events.emplace_back(event);
        }
        return json_response(200, JsonObject{{"events", events}});
    }

    Response internal_audit_log(const Request& request) {
        require_internal_token(request);
        JsonArray entries;
        for (const auto& entry : audit_log_) {
            entries.emplace_back(entry);
        }
        return json_response(200, JsonObject{{"entries", entries}});
    }

    Response ingest_event(const Request& request) {
        require_internal_token(request);
        increment_metric("event.ingest");
        const Json body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        const std::string type = required_string(object, "type");
        const std::string event_id = required_string(object, "eventId");
        const auto& payload = require_object(object.at("payload"));

        if (processed_event_ids_.count(event_id)) {
            return json_response(200, JsonObject{{"status", "ignored_duplicate"}, {"eventId", event_id}});
        }
        processed_event_ids_.insert(event_id);

        if (type == "auth.user_registered") {
            const std::string user_id = canonical_user_id(required_string(payload, "userId"));
            const std::string timestamp = now_iso8601();
            UserProfile profile;
            profile.user_id = user_id;
            profile.display_name = optional_string(payload, "displayName").value_or("User " + user_id.substr(0, std::min<std::size_t>(8, user_id.size())));
            validate_display_name(profile.display_name);
            profile.username = optional_string(payload, "username");
            if (profile.username.has_value()) {
                validate_username(*profile.username);
            }
            profile.created_at = timestamp;
            profile.updated_at = timestamp;
            profiles_[user_id] = profile;

            PrivacySettings settings;
            settings.user_id = user_id;
            settings.created_at = timestamp;
            settings.updated_at = timestamp;
            privacy_[user_id] = settings;

            if (payload.count("deviceId") && payload.count("sessionId")) {
                const auto device_id = required_string(payload, "deviceId");
                sessions_[device_id] = DeviceSession{
                    .device_id = device_id,
                    .user_id = user_id,
                    .session_id = required_string(payload, "sessionId"),
                    .platform = optional_string(payload, "platform").value_or("unknown"),
                    .created_at = timestamp,
                    .updated_at = timestamp,
                };
            }
            publish_event("user.profile_created", JsonObject{{"userId", user_id}});
            increment_metric("profile.created");
            return json_response(201, JsonObject{{"status", "created"}, {"userId", user_id}});
        }

        if (type == "auth.user_disabled" || type == "auth.user_enabled" || type == "auth.user_deleted") {
            const std::string user_id = canonical_user_id(required_string(payload, "userId"));
            auto& profile = require_profile(user_id);
            profile.updated_at = now_iso8601();
            if (type == "auth.user_disabled") {
                profile.profile_status = "disabled";
                publish_event("user.disabled", JsonObject{{"userId", user_id}});
                increment_metric("profile.disabled");
            } else if (type == "auth.user_enabled") {
                profile.profile_status = "active";
                publish_event("user.enabled", JsonObject{{"userId", user_id}});
                increment_metric("profile.enabled");
            } else {
                profile.profile_status = "deleted";
                profile.deleted_at = now_iso8601();
                publish_event("user.deleted", JsonObject{{"userId", user_id}});
                increment_metric("profile.deleted");
            }
            return json_response(200, JsonObject{{"status", "updated"}, {"userId", user_id}});
        }

        if (type == "room.member_added" || type == "conversation.member_added") {
            const std::string user_id = required_string(payload, "userId");
            ensure_profile_exists(user_id);
            const std::string entity_type = required_string(payload, "entityType");
            const std::string entity_id = required_string(payload, "entityId");
            const std::string role = optional_string(payload, "relationRole").value_or("member");
            const auto counterpart = optional_string(payload, "counterpartUserId");
            const auto key = pair_key(user_id, entity_type + "|" + entity_id);
            const auto timestamp = now_iso8601();
            projections_[key] = ProjectionRecord{
                .user_id = user_id,
                .entity_type = entity_type,
                .entity_id = entity_id,
                .relation_role = role,
                .visibility_status = "visible",
                .counterpart_user_id = counterpart,
                .created_at = timestamp,
                .updated_at = timestamp,
            };
            increment_metric("projection.added");
            return json_response(200, JsonObject{{"status", "added"}});
        }

        if (type == "room.member_removed" || type == "conversation.member_removed") {
            const std::string user_id = required_string(payload, "userId");
            const std::string entity_type = required_string(payload, "entityType");
            const std::string entity_id = required_string(payload, "entityId");
            projections_.erase(pair_key(user_id, entity_type + "|" + entity_id));
            increment_metric("projection.removed");
            return json_response(200, JsonObject{{"status", "removed"}});
        }

        throw std::runtime_error("Unsupported event type: " + type);
    }

    Response internal_get_profile(const Request& request) {
        require_internal_token(request);
        const auto user_id = extract_user_id_from_internal_path(request.path, "/profile");
        if (db_.enabled()) {
            const auto profile = db_.get_profile(user_id);
            if (!profile.has_value()) {
                return error_response(404, "not_found", "User profile not found");
            }
            return json_response(200, JsonObject{
                {"userId", profile->at("userId")},
                {"displayName", profile->at("displayName")},
                {"avatarObjectId", profile->at("avatarObjectId")},
                {"profileStatus", profile->at("profileStatus")},
            });
        }
        const auto& profile = require_profile_const(user_id);
        return json_response(200, JsonObject{
            {"userId", profile.user_id},
            {"displayName", profile.display_name},
            {"avatarObjectId", profile.avatar_object_id.has_value() ? Json(*profile.avatar_object_id) : Json(nullptr)},
            {"profileStatus", profile.profile_status},
        });
    }

    std::string extract_user_id_from_internal_path(const std::string& path, const std::string& suffix) const {
        const std::string prefix = "/internal/users/";
        if (!starts_with(path, prefix) || !ends_with(path, suffix)) {
            throw std::runtime_error("Unexpected path");
        }
        return canonical_user_id(path.substr(prefix.size(), path.size() - prefix.size() - suffix.size()));
    }

    Response internal_relationship_check(const Request& request) {
        require_internal_token(request);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        const std::string actor_user_id = canonical_user_id(required_string(object, "actorUserId"));
        const std::string target_user_id = canonical_user_id(required_string(object, "targetUserId"));
        const std::string action = required_string(object, "action");

        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);

        bool allowed = false;
        std::string reason;
        if (action == "dm.start" || action == "dm.read" || action == "dm.write") {
            allowed = db_.enabled()
                ? authorize_dm_action_db(actor_user_id, target_user_id, reason)
                : authorize_dm_start(actor_user_id, target_user_id, reason);
        } else if (action == "profile.read") {
            allowed = db_.enabled()
                ? authorize_profile_read_db(actor_user_id, target_user_id)
                : authorize_profile_read(actor_user_id, target_user_id);
            if (!allowed) {
                reason = "profile_visibility_denied";
            }
        } else if (action == "friend.request.send") {
            allowed = db_.enabled()
                ? allows_friend_request_db(actor_user_id, target_user_id, reason)
                : allows_friend_request(actor_user_id, target_user_id, reason);
        } else {
            throw std::runtime_error("Unsupported action: " + action);
        }

        const auto summary = db_.enabled()
            ? db_relationship_summary(actor_user_id, target_user_id)
            : relationship_summary(actor_user_id, target_user_id);
        return json_response(200, JsonObject{
            {"allowed", allowed},
            {"reason", reason.empty() ? Json(nullptr) : Json(reason)},
            {"relationship", JsonObject{
                {"isFriend", summary.is_friend},
                {"isBlocked", summary.is_blocked},
                {"isBlockedByTarget", summary.is_blocked_by_target},
            }},
        });
    }

    Response internal_authorize_profile_read(const Request& request) {
        require_internal_token(request);
        const auto user_id = extract_user_id_from_internal_path(request.path, "/authorize-profile-read");
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        const std::string actor_user_id = canonical_user_id(required_string(object, "actorUserId"));
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(user_id);
        return json_response(200, JsonObject{{"allowed", db_.enabled() ? Json(authorize_profile_read_db(actor_user_id, user_id)) : Json(authorize_profile_read(actor_user_id, user_id))}});
    }

    Response get_me(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        const auto preferred_display_name = actor_display_name_from_jwt(request);
        if (db_.enabled()) {
            increment_metric("profile.read.self");
            return json_response(200, ensure_db_profile_exists_and_load(actor_user_id, preferred_display_name));
        }
        const auto& profile = ensure_memory_profile_exists(actor_user_id, preferred_display_name);
        increment_metric("profile.read.self");
        return json_response(200, profile_to_json(profile, true));
    }

    Response patch_me(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        const auto preferred_display_name = actor_display_name_from_jwt(request);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        if (db_.enabled()) {
            if (const auto display_name = optional_string(object, "displayName")) {
                validate_display_name(*display_name);
            }
            if (const auto username = optional_string(object, "username")) {
                validate_username(*username);
            }
            db_.ensure_profile_exists(actor_user_id, preferred_display_name.value_or("User " + actor_user_id.substr(0, std::min<std::size_t>(8, actor_user_id.size()))));
            db_.patch_profile(actor_user_id, object);
            increment_metric("profile.updated");
            audit("profile.update", actor_user_id, actor_user_id);
            return json_response(200, ensure_db_profile_exists_and_load(actor_user_id, preferred_display_name));
        }
        auto& profile = ensure_memory_profile_exists(actor_user_id, preferred_display_name);

        if (const auto display_name = optional_string(object, "displayName")) {
            validate_display_name(*display_name);
            profile.display_name = *display_name;
        }
        if (const auto username = optional_string(object, "username")) {
            validate_username(*username);
            profile.username = username;
        } else if (object.count("username") != 0 && object.at("username").is_null()) {
            profile.username = std::nullopt;
        }
        profile.avatar_object_id = optional_string(object, "avatarObjectId");
        profile.bio = optional_string(object, "bio");
        profile.locale = optional_string(object, "locale");
        profile.time_zone = optional_string(object, "timeZone");
        profile.updated_at = now_iso8601();

        publish_event("user.profile_updated", JsonObject{{"userId", actor_user_id}});
        if (object.count("avatarObjectId") != 0) {
            publish_event("user.avatar_updated", JsonObject{{"userId", actor_user_id}});
        }
        audit("profile.update", actor_user_id, actor_user_id);
        increment_metric("profile.updated");
        return json_response(200, profile_to_json(profile, true));
    }

    Response get_user_by_id(const Request& request, const std::string& user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(user_id);
        if (db_.enabled()) {
            if (!authorize_profile_read_db(actor_user_id, user_id)) {
                return error_response(403, "forbidden", "Profile visibility denied");
            }
            increment_metric("profile.read.other");
            return json_response(200, ensure_db_profile_exists_and_load(user_id, std::nullopt));
        }
        const auto& profile = require_profile_const(user_id);
        if (!authorize_profile_read(actor_user_id, user_id)) {
            return error_response(403, "forbidden", "Profile visibility denied");
        }
        increment_metric("profile.read.other");
        return json_response(200, profile_to_json(profile, actor_user_id == user_id));
    }

    Response get_my_privacy(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        if (db_.enabled()) {
            increment_metric("privacy.read");
            return json_response(200, ensure_db_privacy_exists_and_load(actor_user_id));
        }
        const auto& settings = require_privacy_const(actor_user_id);
        increment_metric("privacy.read");
        return json_response(200, privacy_to_json(settings));
    }

    Response patch_my_privacy(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        if (db_.enabled()) {
            if (const auto value = optional_string(object, "profileVisibility")) {
                validate_profile_visibility(*value);
            }
            if (const auto value = optional_string(object, "dmPolicy")) {
                validate_dm_policy(*value);
            }
            if (const auto value = optional_string(object, "friendRequestPolicy")) {
                validate_friend_request_policy(*value);
            }
            if (const auto value = optional_string(object, "lastSeenVisibility")) {
                validate_last_seen_visibility(*value);
            }
            if (const auto value = optional_string(object, "avatarVisibility")) {
                validate_avatar_visibility(*value);
            }
            db_.ensure_profile_exists(actor_user_id, "User " + actor_user_id.substr(0, std::min<std::size_t>(8, actor_user_id.size())));
            db_.patch_privacy_settings(actor_user_id, object);
            publish_event("user.privacy_updated", JsonObject{{"userId", actor_user_id}});
            audit("privacy.update", actor_user_id, actor_user_id);
            increment_metric("privacy.updated");
            return json_response(200, ensure_db_privacy_exists_and_load(actor_user_id));
        }
        auto& settings = require_privacy(actor_user_id);

        if (const auto value = optional_string(object, "profileVisibility")) {
            validate_profile_visibility(*value);
            settings.profile_visibility = *value;
        }
        if (const auto value = optional_string(object, "dmPolicy")) {
            validate_dm_policy(*value);
            settings.dm_policy = *value;
        }
        if (const auto value = optional_string(object, "friendRequestPolicy")) {
            validate_friend_request_policy(*value);
            settings.friend_request_policy = *value;
        }
        if (const auto value = optional_string(object, "lastSeenVisibility")) {
            validate_last_seen_visibility(*value);
            settings.last_seen_visibility = *value;
        }
        if (const auto value = optional_string(object, "avatarVisibility")) {
            validate_avatar_visibility(*value);
            settings.avatar_visibility = *value;
        }
        settings.updated_at = now_iso8601();

        publish_event("user.privacy_updated", JsonObject{{"userId", actor_user_id}});
        audit("privacy.update", actor_user_id, actor_user_id);
        increment_metric("privacy.updated");
        return json_response(200, privacy_to_json(settings));
    }

    Response send_friend_request(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        const auto relationship_before = db_.enabled()
            ? db_.get_relationship_status(actor_user_id, target_user_id).value_or("none")
            : relationship_state_before(actor_user_id, target_user_id);
        const bool already_friend = db_.enabled() ? db_.are_friends(actor_user_id, target_user_id) : is_friend(actor_user_id, target_user_id);
        const bool pending_outgoing = db_.enabled()
            ? db_.get_relationship_status(actor_user_id, target_user_id) == std::optional<std::string>("pending_outgoing")
            : has_pending_outgoing(actor_user_id, target_user_id);
        const bool pending_incoming = db_.enabled()
            ? db_.get_relationship_status(actor_user_id, target_user_id) == std::optional<std::string>("pending_incoming")
            : has_pending_incoming(actor_user_id, target_user_id);
        const std::string target_policy = db_.enabled()
            ? required_string(ensure_db_privacy_exists_and_load(target_user_id), "friendRequestPolicy")
            : (privacy_.count(target_user_id) != 0 ? privacy_.at(target_user_id).friend_request_policy : "-");
        if (actor_user_id == target_user_id) {
            log_friend_request_decision("400", actor_user_id, target_user_id, "deny", "self_friend_not_supported", relationship_before, already_friend, pending_outgoing, pending_incoming, target_policy, "0");
            return error_response(400, "bad_request", "Cannot friend yourself");
        }
        std::string reason;
        const bool allowed = db_.enabled()
            ? allows_friend_request_db(actor_user_id, target_user_id, reason)
            : allows_friend_request(actor_user_id, target_user_id, reason);
        if (!allowed) {
            log_friend_request_decision("403", actor_user_id, target_user_id, "deny", reason, relationship_before, already_friend, pending_outgoing, pending_incoming, target_policy, "0");
            return error_response(403, "forbidden", reason);
        }
        if (already_friend) {
            log_friend_request_decision("409", actor_user_id, target_user_id, "deny", "already_friends", relationship_before, already_friend, pending_outgoing, pending_incoming, target_policy, "0");
            return error_response(409, "conflict", "Users are already friends");
        }
        if (db_.enabled()) {
            db_.create_friend_request(actor_user_id, target_user_id);
        } else {
            set_relationship(actor_user_id, target_user_id, "pending_outgoing");
            set_relationship(target_user_id, actor_user_id, "pending_incoming");
        }
        log_friend_request_decision("201", actor_user_id, target_user_id, "allow", "created", relationship_before, already_friend, pending_outgoing, pending_incoming, target_policy, "1");
        if (!db_.enabled()) {
            publish_event("user.friend_request_created", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
        }
        audit("friend_request.create", actor_user_id, target_user_id);
        increment_metric("friend_request.created");
        return json_response(201, JsonObject{{"status", "pending"}, {"targetUserId", target_user_id}});
    }

    Response accept_friend_request(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        if (db_.enabled()) {
            if (!db_.accept_friend_request(actor_user_id, target_user_id)) {
                return error_response(409, "conflict", "No pending friend request to accept");
            }
        } else {
            const auto incoming_key = pair_key(actor_user_id, target_user_id);
            const auto outgoing_key = pair_key(target_user_id, actor_user_id);
            if (!relationships_.count(incoming_key) || !relationships_.count(outgoing_key) ||
                relationships_[incoming_key].status != "pending_incoming" ||
                relationships_[outgoing_key].status != "pending_outgoing") {
                return error_response(409, "conflict", "No pending friend request to accept");
            }
            set_relationship(actor_user_id, target_user_id, "accepted");
            set_relationship(target_user_id, actor_user_id, "accepted");
            publish_event("user.friend_request_accepted", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
        }
        audit("friend_request.accept", actor_user_id, target_user_id);
        increment_metric("friend_request.accepted");
        return json_response(200, JsonObject{{"status", "accepted"}, {"targetUserId", target_user_id}});
    }

    Response decline_friend_request(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        if (db_.enabled()) {
            if (!db_.decline_friend_request(actor_user_id, target_user_id)) {
                return error_response(409, "conflict", "No pending friend request to decline");
            }
        } else {
            const auto incoming_key = pair_key(actor_user_id, target_user_id);
            const auto outgoing_key = pair_key(target_user_id, actor_user_id);
            if (!relationships_.count(incoming_key) || !relationships_.count(outgoing_key) ||
                relationships_[incoming_key].status != "pending_incoming" ||
                relationships_[outgoing_key].status != "pending_outgoing") {
                return error_response(409, "conflict", "No pending friend request to decline");
            }
            set_relationship(actor_user_id, target_user_id, "declined");
            set_relationship(target_user_id, actor_user_id, "declined");
        }
        audit("friend_request.decline", actor_user_id, target_user_id);
        increment_metric("friend_request.declined");
        return json_response(200, JsonObject{{"status", "declined"}, {"targetUserId", target_user_id}});
    }

    Response remove_friend(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        const auto relationship_before = db_.enabled()
            ? db_.get_relationship_status(actor_user_id, target_user_id).value_or("none")
            : relationship_state_before(actor_user_id, target_user_id);
        const bool friend_edge_exists = db_.enabled() ? db_.are_friends(actor_user_id, target_user_id) : is_friend(actor_user_id, target_user_id);
        if (!friend_edge_exists) {
            log_friend_remove_decision("409", actor_user_id, target_user_id, "deny", "not_friends", relationship_before, false, "0");
            return error_response(409, "conflict", "Users are not friends");
        }
        if (db_.enabled()) {
            if (!db_.remove_friend(actor_user_id, target_user_id)) {
                log_friend_remove_decision("409", actor_user_id, target_user_id, "deny", "not_friends", relationship_before, false, "0");
                return error_response(409, "conflict", "Users are not friends");
            }
        } else {
            set_relationship(actor_user_id, target_user_id, "removed");
            set_relationship(target_user_id, actor_user_id, "removed");
            publish_event("user.friend_removed", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
        }
        log_friend_remove_decision("200", actor_user_id, target_user_id, "allow", "friend_removed", relationship_before, true, "1");
        audit("friend.remove", actor_user_id, target_user_id);
        increment_metric("friend.removed");
        return json_response(200, JsonObject{{"status", "removed"}, {"targetUserId", target_user_id}});
    }

    Response block_user(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        if (actor_user_id == target_user_id) {
            return error_response(400, "bad_request", "Cannot block yourself");
        }
        std::optional<std::string> reason;
        if (!request.body.empty()) {
            const auto body = JsonParser(request.body).parse();
            const auto& object = require_object(body);
            reason = optional_string(object, "reason");
        }
        const auto key = pair_key(actor_user_id, target_user_id);
        blocks_[key] = BlockRecord{actor_user_id, target_user_id, reason, now_iso8601()};
        set_relationship(actor_user_id, target_user_id, "removed");
        set_relationship(target_user_id, actor_user_id, "removed");
        hide_dm_projections_between(actor_user_id, target_user_id);
        publish_event("user.block_created", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
        audit("block.create", actor_user_id, target_user_id);
        increment_metric("block.created");
        return json_response(201, JsonObject{{"status", "blocked"}, {"targetUserId", target_user_id}});
    }

    Response unblock_user(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        blocks_.erase(pair_key(actor_user_id, target_user_id));
        publish_event("user.block_removed", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
        audit("block.remove", actor_user_id, target_user_id);
        increment_metric("block.removed");
        return json_response(200, JsonObject{{"status", "unblocked"}, {"targetUserId", target_user_id}});
    }

    Response list_projection_entities(const Request& request, const std::string& entity_type) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const int limit = parse_int_query(request.query, "limit", 50);
        const int offset = parse_int_query(request.query, "offset", 0);

        JsonArray items;
        int seen = 0;
        for (const auto& [key, projection] : projections_) {
            if (projection.user_id != actor_user_id || projection.entity_type != entity_type || projection.visibility_status != "visible") {
                continue;
            }
            if (seen++ < offset) {
                continue;
            }
            if (static_cast<int>(items.size()) >= limit) {
                break;
            }
            items.emplace_back(JsonObject{
                {"entityType", projection.entity_type},
                {"entityId", projection.entity_id},
                {"relationRole", projection.relation_role},
                {"visibilityStatus", projection.visibility_status},
                {"counterpartUserId", projection.counterpart_user_id.has_value() ? Json(*projection.counterpart_user_id) : Json(nullptr)},
                {"createdAt", projection.created_at},
                {"updatedAt", projection.updated_at},
            });
        }
        increment_metric("projection.list." + entity_type);
        return json_response(200, JsonObject{{"items", items}, {"limit", limit}, {"offset", offset}});
    }

    Response list_contacts(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const int limit = parse_int_query(request.query, "limit", 50);
        const int offset = parse_int_query(request.query, "offset", 0);

        JsonArray items;
        int seen = 0;
        for (const auto& [key, relation] : relationships_) {
            if (relation.user_id != actor_user_id || relation.status != "accepted") {
                continue;
            }
            if (has_block(actor_user_id, relation.target_user_id) || has_block(relation.target_user_id, actor_user_id)) {
                continue;
            }
            if (seen++ < offset) {
                continue;
            }
            if (static_cast<int>(items.size()) >= limit) {
                break;
            }
            const auto& profile = require_profile_const(relation.target_user_id);
            items.emplace_back(JsonObject{
                {"userId", profile.user_id},
                {"displayName", profile.display_name},
                {"username", profile.username.has_value() ? Json(*profile.username) : Json(nullptr)},
                {"profileStatus", profile.profile_status},
                {"relationStatus", relation.status},
            });
        }
        increment_metric("contact.list");
        return json_response(200, JsonObject{{"items", items}, {"limit", limit}, {"offset", offset}});
    }
};

std::unordered_map<std::string, std::string> parse_query(const std::string& raw) {
    std::unordered_map<std::string, std::string> query;
    if (raw.empty()) {
        return query;
    }
    std::stringstream ss(raw);
    std::string pair;
    while (std::getline(ss, pair, '&')) {
        const auto pos = pair.find('=');
        if (pos == std::string::npos) {
            query[pair] = "";
        } else {
            query[pair.substr(0, pos)] = pair.substr(pos + 1);
        }
    }
    return query;
}

Request parse_request(const std::string& raw) {
    Request request;
    const auto header_end = raw.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        throw std::runtime_error("Malformed HTTP request");
    }
    const std::string header_blob = raw.substr(0, header_end);
    request.body = raw.substr(header_end + 4);

    std::stringstream stream(header_blob);
    std::string request_line;
    std::getline(stream, request_line);
    if (!request_line.empty() && request_line.back() == '\r') {
        request_line.pop_back();
    }
    std::stringstream request_line_stream(request_line);
    std::string target;
    request_line_stream >> request.method >> target;
    const auto query_pos = target.find('?');
    request.path = query_pos == std::string::npos ? target : target.substr(0, query_pos);
    request.query = query_pos == std::string::npos ? std::unordered_map<std::string, std::string>{} : parse_query(target.substr(query_pos + 1));

    std::string line;
    while (std::getline(stream, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        const auto colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        request.headers[to_lower(trim(line.substr(0, colon)))] = trim(line.substr(colon + 1));
    }
    return request;
}

std::string status_text(int status) {
    switch (status) {
        case 200: return "OK";
        case 201: return "Created";
        case 400: return "Bad Request";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 409: return "Conflict";
        case 503: return "Service Unavailable";
        default: return "OK";
    }
}

std::string build_http_response(const Response& response) {
    const std::string body = dump_json(response.body);
    std::ostringstream oss;
    oss << "HTTP/1.1 " << response.status << ' ' << status_text(response.status) << "\r\n";
    oss << "Content-Type: application/json\r\n";
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "Connection: close\r\n";
    for (const auto& [name, value] : response.headers) {
        oss << name << ": " << value << "\r\n";
    }
    oss << "\r\n" << body;
    return oss.str();
}

bool socket_send_all(SOCKET socket, const std::string& data) {
    std::size_t sent_total = 0;
    while (sent_total < data.size()) {
        const int sent = send(socket, data.data() + sent_total, static_cast<int>(data.size() - sent_total), 0);
        if (sent == SOCKET_ERROR) {
            return false;
        }
        sent_total += static_cast<std::size_t>(sent);
    }
    return true;
}

std::string receive_http_request(SOCKET client_socket) {
    std::string data;
    char buffer[4096];
    int expected_body_length = -1;
    std::size_t header_end = std::string::npos;

    while (true) {
        const int bytes = recv(client_socket, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            break;
        }
        data.append(buffer, bytes);
        if (header_end == std::string::npos) {
            header_end = data.find("\r\n\r\n");
            if (header_end != std::string::npos) {
                const auto content_length_pos = to_lower(data.substr(0, header_end)).find("content-length:");
                if (content_length_pos != std::string::npos) {
                    const auto line_end = data.find("\r\n", content_length_pos);
                    const auto value = trim(data.substr(content_length_pos + 15, line_end - (content_length_pos + 15)));
                    expected_body_length = std::stoi(value);
                } else {
                    expected_body_length = 0;
                }
            }
        }
        if (header_end != std::string::npos && expected_body_length >= 0) {
            const std::size_t received_body_length = data.size() - (header_end + 4);
            if (received_body_length >= static_cast<std::size_t>(expected_body_length)) {
                break;
            }
        }
    }
    return data;
}

int run_server(unsigned short port) {
#ifdef _WIN32
    WSADATA wsa_data{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        std::cerr << "WSAStartup failed\n";
        return 1;
    }
#endif

    SOCKET server_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_socket == INVALID_SOCKET) {
        std::cerr << "socket() failed\n";
#ifdef _WIN32
        WSACleanup();
#endif
        return 1;
    }

#ifdef _WIN32
    BOOL opt = TRUE;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));
#else
    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);

    if (bind(server_socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR) {
        std::cerr << "bind() failed\n";
        closesocket(server_socket);
#ifdef _WIN32
        WSACleanup();
#endif
        return 1;
    }
    if (listen(server_socket, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "listen() failed\n";
        closesocket(server_socket);
#ifdef _WIN32
        WSACleanup();
#endif
        return 1;
    }

    std::cout << "user-service listening on port " << port << std::endl;
    ServiceState state;

    while (true) {
        SOCKET client_socket = accept(server_socket, nullptr, nullptr);
        if (client_socket == INVALID_SOCKET) {
            continue;
        }
        const std::string raw_request = receive_http_request(client_socket);
        Response response;
        try {
            response = state.handle(parse_request(raw_request));
        } catch (const std::exception& ex) {
            response.status = 400;
            response.body = JsonObject{{"error", "bad_request"}, {"message", ex.what()}};
        }
        const std::string payload = build_http_response(response);
        socket_send_all(client_socket, payload);
        shutdown(client_socket, SD_BOTH);
        closesocket(client_socket);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc >= 3 && std::string(argv[1]) == "migrate") {
        const auto base_dir = std::filesystem::current_path();
        const auto migrations_dir = (base_dir / "migrations").string();
        PostgresPsqlAdapter db;
        if (std::string(argv[2]) == "up") {
            return db.migrate_up(migrations_dir);
        }
        if (std::string(argv[2]) == "down") {
            return db.migrate_down(migrations_dir);
        }
        if (std::string(argv[2]) == "status") {
            return db.migrate_status(migrations_dir);
        }
        std::cerr << "Unknown migrate command\n";
        return 1;
    }
    unsigned short port = 8080;
    if (const auto env_port = get_env("USER_SERVICE_PORT")) {
        port = static_cast<unsigned short>(std::stoi(*env_port));
    } else if (const auto http_addr = get_env("HTTP_ADDR")) {
        const auto colon = http_addr->rfind(':');
        if (colon != std::string::npos) {
            port = static_cast<unsigned short>(std::stoi(http_addr->substr(colon + 1)));
        }
    } else if (argc > 1) {
        port = static_cast<unsigned short>(std::stoi(argv[1]));
    }
    return run_server(port);
}
