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

class JsonParseError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class JsonParser {
public:
    explicit JsonParser(const std::string& text) : text_(text) {}

    Json parse() {
        skip_ws();
        Json value = parse_value();
        skip_ws();
        if (pos_ != text_.size()) {
            throw JsonParseError("Trailing data after JSON");
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
            throw JsonParseError("Unexpected end of JSON");
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
            throw JsonParseError("Unexpected JSON token");
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
        throw JsonParseError("Unsupported JSON value");
    }

    void consume_literal(const std::string& literal) {
        if (text_.compare(pos_, literal.size(), literal) != 0) {
            throw JsonParseError("Unexpected JSON literal");
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
                throw JsonParseError("Expected object delimiter");
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
                throw JsonParseError("Expected array delimiter");
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
                        throw JsonParseError("Unsupported escape sequence");
                }
                continue;
            }
            result.push_back(ch);
        }
        return result;
    }

    double parse_number() {
        std::size_t consumed = 0;
        double value = 0.0;
        try {
            value = std::stod(text_.substr(pos_), &consumed);
        } catch (const std::exception&) {
            throw JsonParseError("Invalid JSON number");
        }
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

std::vector<std::string> required_string_array(const JsonObject& object, const std::string& key) {
    const auto it = object.find(key);
    if (it == object.end() || !it->second.is_array()) {
        throw std::runtime_error("Expected string array field: " + key);
    }
    std::vector<std::string> values;
    for (const auto& entry : it->second.as_array()) {
        if (!entry.is_string()) {
            throw std::runtime_error("Expected string array field: " + key);
        }
        values.push_back(entry.as_string());
    }
    return values;
}

std::optional<long long> optional_int64(const JsonObject& object, const std::string& key) {
    const auto it = object.find(key);
    if (it == object.end() || it->second.is_null()) {
        return std::nullopt;
    }
    if (!it->second.is_number()) {
        throw std::runtime_error("Expected integer field: " + key);
    }
    return static_cast<long long>(it->second.as_number());
}

long long required_int64(const JsonObject& object, const std::string& key) {
    const auto value = optional_int64(object, key);
    if (!value.has_value()) {
        throw std::runtime_error("Missing integer field: " + key);
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

long long now_epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();
}

Json json_int64(const long long value) {
    return Json(static_cast<double>(value));
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

struct RelationshipListItem {
    std::string user_id;
    std::string display_name;
    std::optional<std::string> username;
    std::string profile_status;
    std::string relation_status;
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

struct PresenceSessionRecord {
    std::string user_id;
    std::string session_id;
    std::optional<std::string> device_id;
    std::string platform = "unknown";
    std::string state = "connected";
    std::string last_pulse_at;
    std::optional<std::string> last_disconnect_at;
    long long last_pulse_epoch_seconds = 0;
    std::optional<long long> last_disconnect_epoch_seconds;
    std::string created_at;
    std::string updated_at;
};

struct PresenceSnapshot {
    std::string user_id;
    std::string presence = "red";
    bool is_online = false;
    std::optional<std::string> last_seen_at;
    int connected_session_count = 0;
    int recent_session_count = 0;
};

struct CallHistoryRecord {
    std::string history_id;
    std::string call_id;
    std::string owner_user_id;
    std::string initiator_user_id;
    std::string call_type;
    std::string direction;
    std::string status;
    std::vector<std::string> participant_user_ids;
    int participant_count = 0;
    std::string started_at;
    std::optional<std::string> ended_at;
    int duration_seconds = 0;
    std::optional<std::string> room_id;
    std::optional<std::string> conversation_id;
    std::string created_at;
    std::string updated_at;
};

struct ReminderRecord {
    std::string reminder_id;
    std::string user_id;
    std::string source_type = "chat_message";
    std::string message_id;
    std::string conversation_id;
    std::string conversation_type;
    std::string room_id;
    std::string call_id;
    std::string message_preview_text;
    std::string message_author_user_id;
    std::string message_author_display_name;
    std::optional<long long> message_ts_ms;
    std::string note;
    long long remind_at_ms = 0;
    std::string state = "scheduled";
    std::optional<long long> fired_at_ms;
    std::optional<long long> dismissed_at_ms;
    long long created_at_ms = 0;
    long long updated_at_ms = 0;
};

struct RelationshipSummary {
    bool is_friend = false;
    bool is_blocked = false;
    bool is_blocked_by_target = false;
};

struct AuthorizationDecision {
    bool allowed = false;
    std::string reason;
};

std::string pair_key(const std::string& lhs, const std::string& rhs) {
    return lhs + "|" + rhs;
}

std::string join_strings(const std::vector<std::string>& values, const std::string& delimiter) {
    std::ostringstream oss;
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i != 0U) {
            oss << delimiter;
        }
        oss << values[i];
    }
    return oss.str();
}

std::vector<std::string> split_string(const std::string& value, const char delimiter) {
    std::vector<std::string> parts;
    std::stringstream ss(value);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
        if (!item.empty()) {
            parts.push_back(item);
        }
    }
    return parts;
}

std::optional<std::string> counterpart_relationship_status(const std::string& status) {
    if (status == "accepted") {
        return std::string("accepted");
    }
    if (status == "pending_outgoing") {
        return std::string("pending_incoming");
    }
    if (status == "pending_incoming") {
        return std::string("pending_outgoing");
    }
    return std::nullopt;
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

int parse_int_env(const char* key, const int fallback) {
    const auto value = get_env(key);
    if (!value.has_value()) {
        return fallback;
    }
    try {
        return std::stoi(*value);
    } catch (...) {
        return fallback;
    }
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
            "'user_profiles','user_privacy_settings','user_relationships','user_blocks','user_entity_projection','user_call_history','user_presence_sessions','user_reminders','user_event_outbox');");
        return result.has_value() && *result == "9";
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

    void upsert_presence_session(
        const std::string& user_id,
        const std::string& session_id,
        const std::optional<std::string>& device_id,
        const std::string& platform) const {
        if (!enabled_) {
            return;
        }
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "INSERT INTO user_presence_sessions (user_id, session_id, device_id, platform, state, last_pulse_at, last_disconnect_at, created_at, updated_at) VALUES ('"
            << shell_escape_single_quotes(user_id) << "','" << shell_escape_single_quotes(session_id) << "',"
            << (device_id.has_value() ? "'" + shell_escape_single_quotes(*device_id) + "'" : "NULL") << ",'"
            << shell_escape_single_quotes(platform) << "','connected',NOW(),NULL,NOW(),NOW()) "
            << "ON CONFLICT (user_id, session_id) DO UPDATE SET "
            << "device_id=EXCLUDED.device_id,"
            << "platform=EXCLUDED.platform,"
            << "state='connected',"
            << "last_pulse_at=NOW(),"
            << "last_disconnect_at=NULL,"
            << "updated_at=NOW();";
        sql << "COMMIT;";
        exec_sql(sql.str());
    }

    bool disconnect_presence_session(const std::string& user_id, const std::string& session_id) const {
        if (!enabled_) {
            return false;
        }
        std::ostringstream sql;
        sql << "UPDATE user_presence_sessions SET state='disconnected', last_disconnect_at=NOW(), updated_at=NOW() "
            << "WHERE user_id='" << shell_escape_single_quotes(user_id) << "' "
            << "AND session_id='" << shell_escape_single_quotes(session_id) << "' "
            << "RETURNING 1;";
        const auto rows = query_rows(sql.str());
        return rows.has_value() && !rows->empty();
    }

    std::vector<PresenceSnapshot> list_presence(const std::vector<std::string>& user_ids, const int green_ttl_seconds) const {
        if (!enabled_ || user_ids.empty()) {
            return {};
        }
        std::ostringstream in_clause;
        for (std::size_t i = 0; i < user_ids.size(); ++i) {
            if (i != 0U) {
                in_clause << ",";
            }
            in_clause << "'" << shell_escape_single_quotes(user_ids[i]) << "'";
        }
        std::ostringstream sql;
        sql << "SELECT user_id::text, "
            << "CASE "
            << "WHEN COUNT(*) FILTER (WHERE state='connected' AND last_pulse_at >= NOW() - INTERVAL '" << std::max(green_ttl_seconds, 1) << " seconds') > 0 THEN 'green' "
            << "WHEN COUNT(*) FILTER (WHERE state='connected') > 0 THEN 'yellow' "
            << "ELSE 'red' END, "
            << "CASE WHEN COUNT(*) FILTER (WHERE state='connected' AND last_pulse_at >= NOW() - INTERVAL '" << std::max(green_ttl_seconds, 1) << " seconds') > 0 THEN 'true' ELSE 'false' END, "
            << "COALESCE(to_char(MAX(COALESCE(last_disconnect_at, last_pulse_at)) AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),''), "
            << "COUNT(*) FILTER (WHERE state='connected')::text, "
            << "COUNT(*) FILTER (WHERE state='connected' AND last_pulse_at >= NOW() - INTERVAL '" << std::max(green_ttl_seconds, 1) << " seconds')::text "
            << "FROM user_presence_sessions WHERE user_id IN (" << in_clause.str() << ") "
            << "GROUP BY user_id;";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value()) {
            return {};
        }
        std::vector<PresenceSnapshot> items;
        items.reserve(rows->size());
        for (const auto& row : *rows) {
            if (row.size() != 6U) {
                throw std::runtime_error("Unexpected presence row shape");
            }
            items.push_back(PresenceSnapshot{
                .user_id = row[0],
                .presence = row[1],
                .is_online = row[2] == "true",
                .last_seen_at = row[3].empty() ? std::nullopt : std::optional<std::string>(row[3]),
                .connected_session_count = std::stoi(row[4]),
                .recent_session_count = std::stoi(row[5]),
            });
        }
        return items;
    }

    void upsert_call_history(
        const std::string& call_id,
        const std::string& initiator_user_id,
        const std::vector<std::string>& participant_user_ids,
        const std::string& call_type,
        const std::string& status,
        const std::string& started_at,
        const std::optional<std::string>& ended_at,
        const int duration_seconds,
        const std::optional<std::string>& room_id,
        const std::optional<std::string>& conversation_id) const {
        if (!enabled_) {
            return;
        }

        const auto now = now_iso8601();
        const std::string escaped_call_id = shell_escape_single_quotes(call_id);
        const std::string escaped_initiator_user_id = shell_escape_single_quotes(initiator_user_id);
        const std::string escaped_call_type = shell_escape_single_quotes(call_type);
        const std::string escaped_status = shell_escape_single_quotes(status);
        const std::string escaped_started_at = shell_escape_single_quotes(started_at);
        const std::string ended_at_sql = ended_at.has_value() ? "'" + shell_escape_single_quotes(*ended_at) + "'::timestamptz" : "NULL";
        const std::string room_id_sql = room_id.has_value() ? "'" + shell_escape_single_quotes(*room_id) + "'" : "NULL";
        const std::string conversation_id_sql = conversation_id.has_value() ? "'" + shell_escape_single_quotes(*conversation_id) + "'" : "NULL";
        const std::string participants_csv = join_strings(participant_user_ids, ",");
        const std::string escaped_participants_csv = shell_escape_single_quotes(participants_csv);
        std::ostringstream sql;
        sql << "BEGIN;";
        for (const auto& participant_user_id : participant_user_ids) {
            const std::string canonical_participant_user_id = canonical_user_id(participant_user_id);
            const std::string escaped_owner_user_id = shell_escape_single_quotes(canonical_participant_user_id);
            const std::string direction = canonical_participant_user_id == initiator_user_id ? "outgoing" : "incoming";
            sql << "INSERT INTO user_call_history (history_id, call_id, owner_user_id, initiator_user_id, call_type, direction, call_status, participant_user_ids, participant_count, started_at, ended_at, duration_seconds, room_id, conversation_id, created_at, updated_at) VALUES ("
                << "'" << shell_escape_single_quotes(hash_to_uuid("call-history-" + canonical_participant_user_id + "-" + call_id)) << "','"
                << escaped_call_id << "','" << escaped_owner_user_id << "','" << escaped_initiator_user_id << "','"
                << escaped_call_type << "','" << direction << "','" << escaped_status << "','"
                << escaped_participants_csv << "'," << static_cast<int>(participant_user_ids.size()) << ",'"
                << escaped_started_at << "'::timestamptz," << ended_at_sql << "," << duration_seconds << ","
                << room_id_sql << "," << conversation_id_sql << ",NOW(),NOW()) "
                << "ON CONFLICT (owner_user_id, call_id) DO UPDATE SET "
                << "initiator_user_id=EXCLUDED.initiator_user_id,"
                << "call_type=EXCLUDED.call_type,"
                << "direction=EXCLUDED.direction,"
                << "call_status=EXCLUDED.call_status,"
                << "participant_user_ids=EXCLUDED.participant_user_ids,"
                << "participant_count=EXCLUDED.participant_count,"
                << "started_at=EXCLUDED.started_at,"
                << "ended_at=EXCLUDED.ended_at,"
                << "duration_seconds=EXCLUDED.duration_seconds,"
                << "room_id=EXCLUDED.room_id,"
                << "conversation_id=EXCLUDED.conversation_id,"
                << "updated_at=NOW();";
            sql << "DELETE FROM user_call_history WHERE history_id IN ("
                << "SELECT history_id FROM user_call_history "
                << "WHERE owner_user_id='" << escaped_owner_user_id << "' "
                << "ORDER BY started_at DESC, updated_at DESC OFFSET 50"
                << ");";
        }
        sql << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) VALUES ('"
            << shell_escape_single_quotes(hash_to_uuid("call-history-recorded-" + call_id + "-" + now)) << "','user_call_history','"
            << escaped_initiator_user_id << "','user.call_history_recorded','{\"callId\":\""
            << escaped_call_id << "\"}'::jsonb,NOW(),NULL);";
        sql << "COMMIT;";
        exec_sql(sql.str());
    }

    std::vector<CallHistoryRecord> list_call_history(const std::string& owner_user_id, const int limit, const int offset) const {
        if (!enabled_) {
            return {};
        }
        std::ostringstream sql;
        sql << "SELECT history_id::text, call_id, owner_user_id::text, initiator_user_id::text, call_type, direction, call_status, "
            << "participant_user_ids, participant_count::text, "
            << "to_char(started_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), "
            << "COALESCE(to_char(ended_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),''), "
            << "duration_seconds::text, COALESCE(room_id,''), COALESCE(conversation_id,''), "
            << "to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), "
            << "to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') "
            << "FROM user_call_history WHERE owner_user_id='" << shell_escape_single_quotes(owner_user_id) << "' "
            << "ORDER BY started_at DESC, updated_at DESC LIMIT " << std::max(limit, 0) << " OFFSET " << std::max(offset, 0) << ";";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value()) {
            return {};
        }
        std::vector<CallHistoryRecord> items;
        items.reserve(rows->size());
        for (const auto& row : *rows) {
            if (row.size() != 16U) {
                throw std::runtime_error("Unexpected call history row shape");
            }
            items.push_back(CallHistoryRecord{
                .history_id = row[0],
                .call_id = row[1],
                .owner_user_id = row[2],
                .initiator_user_id = row[3],
                .call_type = row[4],
                .direction = row[5],
                .status = row[6],
                .participant_user_ids = split_string(row[7], ','),
                .participant_count = std::stoi(row[8]),
                .started_at = row[9],
                .ended_at = row[10].empty() ? std::nullopt : std::optional<std::string>(row[10]),
                .duration_seconds = std::stoi(row[11]),
                .room_id = row[12].empty() ? std::nullopt : std::optional<std::string>(row[12]),
                .conversation_id = row[13].empty() ? std::nullopt : std::optional<std::string>(row[13]),
                .created_at = row[14],
                .updated_at = row[15],
            });
        }
        return items;
    }

    bool delete_call_history(const std::string& owner_user_id, const std::string& history_id) const {
        if (!enabled_) {
            return false;
        }
        const auto now = now_iso8601();
        std::ostringstream sql;
        sql << "WITH deleted AS (DELETE FROM user_call_history WHERE owner_user_id='" << shell_escape_single_quotes(owner_user_id)
            << "' AND history_id='" << shell_escape_single_quotes(history_id) << "' RETURNING call_id) "
            << ", event_insert AS ("
            << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) "
            << "SELECT '" << shell_escape_single_quotes(hash_to_uuid("call-history-deleted-" + owner_user_id + "-" + history_id + "-" + now)) << "',"
            << "'user_call_history','" << shell_escape_single_quotes(owner_user_id) << "','user.call_history_deleted',"
            << "'{\"historyId\":\"" << shell_escape_single_quotes(history_id) << "\"}'::jsonb,NOW(),NULL "
            << "FROM deleted RETURNING 1) "
            << "SELECT COUNT(*)::text FROM deleted;";
        return query_scalar(sql.str()) == std::optional<std::string>("1");
    }

    int clear_call_history(const std::string& owner_user_id) const {
        if (!enabled_) {
            return 0;
        }
        const auto now = now_iso8601();
        std::ostringstream sql;
        sql << "WITH deleted AS (DELETE FROM user_call_history WHERE owner_user_id='" << shell_escape_single_quotes(owner_user_id) << "' RETURNING history_id) "
            << ", event_insert AS ("
            << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) "
            << "SELECT '" << shell_escape_single_quotes(hash_to_uuid("call-history-cleared-" + owner_user_id + "-" + now)) << "',"
            << "'user_call_history','" << shell_escape_single_quotes(owner_user_id) << "','user.call_history_cleared',"
            << "'{\"userId\":\"" << shell_escape_single_quotes(owner_user_id) << "\"}'::jsonb,NOW(),NULL "
            << "FROM (SELECT COUNT(*) AS deleted_rows FROM deleted) counts WHERE counts.deleted_rows > 0 RETURNING 1) "
            << "SELECT COUNT(*)::text FROM deleted;";
        const auto deleted = query_scalar(sql.str());
        return deleted.has_value() ? std::stoi(*deleted) : 0;
    }

    std::optional<ReminderRecord> create_reminder(const ReminderRecord& record) const {
        if (!enabled_) {
            return std::nullopt;
        }
        if (const auto existing = find_scheduled_reminder(record.user_id, record.message_id, record.remind_at_ms)) {
            return existing;
        }
        std::ostringstream sql;
        sql << "INSERT INTO user_reminders ("
            << "reminder_id, user_id, source_type, message_id, conversation_id, conversation_type, room_id, call_id, "
            << "message_preview_text, message_author_user_id, message_author_display_name, message_ts_ms, note, "
            << "remind_at_ms, state, fired_at_ms, dismissed_at_ms, created_at_ms, updated_at_ms"
            << ") VALUES ("
            << "'" << shell_escape_single_quotes(record.reminder_id) << "',"
            << "'" << shell_escape_single_quotes(record.user_id) << "',"
            << "'" << shell_escape_single_quotes(record.source_type) << "',"
            << "'" << shell_escape_single_quotes(record.message_id) << "',"
            << "'" << shell_escape_single_quotes(record.conversation_id) << "',"
            << "'" << shell_escape_single_quotes(record.conversation_type) << "',"
            << "'" << shell_escape_single_quotes(record.room_id) << "',"
            << "'" << shell_escape_single_quotes(record.call_id) << "',"
            << "'" << shell_escape_single_quotes(record.message_preview_text) << "',"
            << "'" << shell_escape_single_quotes(record.message_author_user_id) << "',"
            << "'" << shell_escape_single_quotes(record.message_author_display_name) << "',"
            << (record.message_ts_ms.has_value() ? std::to_string(*record.message_ts_ms) : "NULL") << ","
            << "'" << shell_escape_single_quotes(record.note) << "',"
            << record.remind_at_ms << ","
            << "'" << shell_escape_single_quotes(record.state) << "',"
            << (record.fired_at_ms.has_value() ? std::to_string(*record.fired_at_ms) : "NULL") << ","
            << (record.dismissed_at_ms.has_value() ? std::to_string(*record.dismissed_at_ms) : "NULL") << ","
            << record.created_at_ms << ","
            << record.updated_at_ms << ") "
            << "RETURNING reminder_id::text, user_id, source_type, message_id::text, conversation_id, "
            << "COALESCE(conversation_type,''), COALESCE(room_id,''), COALESCE(call_id,''), COALESCE(message_preview_text,''), "
            << "COALESCE(message_author_user_id,''), COALESCE(message_author_display_name,''), COALESCE(message_ts_ms::text,''), "
            << "COALESCE(note,''), remind_at_ms::text, state, COALESCE(fired_at_ms::text,''), COALESCE(dismissed_at_ms::text,''), "
            << "created_at_ms::text, updated_at_ms::text;";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value() || rows->empty()) {
            return std::nullopt;
        }
        return reminder_from_row(rows->front());
    }

    std::vector<ReminderRecord> list_reminders(
        const std::string& user_id,
        const std::optional<std::string>& state,
        const int limit,
        const int offset,
        const std::optional<long long>& from_ms,
        const std::optional<long long>& to_ms) const {
        if (!enabled_) {
            return {};
        }
        std::ostringstream sql;
        sql << "SELECT reminder_id::text, user_id, source_type, message_id::text, conversation_id, "
            << "COALESCE(conversation_type,''), COALESCE(room_id,''), COALESCE(call_id,''), COALESCE(message_preview_text,''), "
            << "COALESCE(message_author_user_id,''), COALESCE(message_author_display_name,''), COALESCE(message_ts_ms::text,''), "
            << "COALESCE(note,''), remind_at_ms::text, state, COALESCE(fired_at_ms::text,''), COALESCE(dismissed_at_ms::text,''), "
            << "created_at_ms::text, updated_at_ms::text "
            << "FROM user_reminders WHERE user_id='" << shell_escape_single_quotes(user_id) << "' ";
        if (state.has_value()) {
            sql << "AND state='" << shell_escape_single_quotes(*state) << "' ";
        }
        if (from_ms.has_value()) {
            sql << "AND remind_at_ms >= " << *from_ms << " ";
        }
        if (to_ms.has_value()) {
            sql << "AND remind_at_ms <= " << *to_ms << " ";
        }
        if (state == std::optional<std::string>("scheduled")) {
            sql << "ORDER BY remind_at_ms ASC, updated_at_ms DESC ";
        } else if (state.has_value()) {
            sql << "ORDER BY updated_at_ms DESC, remind_at_ms ASC ";
        } else {
            sql << "ORDER BY CASE WHEN state='scheduled' THEN 0 ELSE 1 END ASC, "
                << "CASE WHEN state='scheduled' THEN remind_at_ms ELSE NULL END ASC, "
                << "CASE WHEN state='scheduled' THEN NULL ELSE updated_at_ms END DESC, created_at_ms DESC ";
        }
        sql << "LIMIT " << std::max(limit, 0) << " OFFSET " << std::max(offset, 0) << ";";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value()) {
            return {};
        }
        std::vector<ReminderRecord> items;
        items.reserve(rows->size());
        for (const auto& row : *rows) {
            items.push_back(reminder_from_row(row));
        }
        return items;
    }

    std::optional<ReminderRecord> get_reminder(const std::string& user_id, const std::string& reminder_id) const {
        if (!enabled_) {
            return std::nullopt;
        }
        std::ostringstream sql;
        sql << "SELECT reminder_id::text, user_id, source_type, message_id::text, conversation_id, "
            << "COALESCE(conversation_type,''), COALESCE(room_id,''), COALESCE(call_id,''), COALESCE(message_preview_text,''), "
            << "COALESCE(message_author_user_id,''), COALESCE(message_author_display_name,''), COALESCE(message_ts_ms::text,''), "
            << "COALESCE(note,''), remind_at_ms::text, state, COALESCE(fired_at_ms::text,''), COALESCE(dismissed_at_ms::text,''), "
            << "created_at_ms::text, updated_at_ms::text "
            << "FROM user_reminders WHERE user_id='" << shell_escape_single_quotes(user_id) << "' "
            << "AND reminder_id='" << shell_escape_single_quotes(reminder_id) << "' LIMIT 1;";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value() || rows->empty()) {
            return std::nullopt;
        }
        return reminder_from_row(rows->front());
    }

    std::optional<ReminderRecord> update_reminder(const ReminderRecord& record) const {
        if (!enabled_) {
            return std::nullopt;
        }
        std::ostringstream sql;
        sql << "UPDATE user_reminders SET "
            << "conversation_type='" << shell_escape_single_quotes(record.conversation_type) << "',"
            << "room_id='" << shell_escape_single_quotes(record.room_id) << "',"
            << "call_id='" << shell_escape_single_quotes(record.call_id) << "',"
            << "message_preview_text='" << shell_escape_single_quotes(record.message_preview_text) << "',"
            << "message_author_user_id='" << shell_escape_single_quotes(record.message_author_user_id) << "',"
            << "message_author_display_name='" << shell_escape_single_quotes(record.message_author_display_name) << "',"
            << "message_ts_ms=" << (record.message_ts_ms.has_value() ? std::to_string(*record.message_ts_ms) : "NULL") << ","
            << "note='" << shell_escape_single_quotes(record.note) << "',"
            << "remind_at_ms=" << record.remind_at_ms << ","
            << "state='" << shell_escape_single_quotes(record.state) << "',"
            << "fired_at_ms=" << (record.fired_at_ms.has_value() ? std::to_string(*record.fired_at_ms) : "NULL") << ","
            << "dismissed_at_ms=" << (record.dismissed_at_ms.has_value() ? std::to_string(*record.dismissed_at_ms) : "NULL") << ","
            << "updated_at_ms=" << record.updated_at_ms << " "
            << "WHERE user_id='" << shell_escape_single_quotes(record.user_id) << "' "
            << "AND reminder_id='" << shell_escape_single_quotes(record.reminder_id) << "' "
            << "RETURNING reminder_id::text, user_id, source_type, message_id::text, conversation_id, "
            << "COALESCE(conversation_type,''), COALESCE(room_id,''), COALESCE(call_id,''), COALESCE(message_preview_text,''), "
            << "COALESCE(message_author_user_id,''), COALESCE(message_author_display_name,''), COALESCE(message_ts_ms::text,''), "
            << "COALESCE(note,''), remind_at_ms::text, state, COALESCE(fired_at_ms::text,''), COALESCE(dismissed_at_ms::text,''), "
            << "created_at_ms::text, updated_at_ms::text;";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value() || rows->empty()) {
            return std::nullopt;
        }
        return reminder_from_row(rows->front());
    }

    bool delete_reminder(const std::string& user_id, const std::string& reminder_id) const {
        if (!enabled_) {
            return false;
        }
        std::ostringstream sql;
        sql << "DELETE FROM user_reminders WHERE user_id='" << shell_escape_single_quotes(user_id) << "' "
            << "AND reminder_id='" << shell_escape_single_quotes(reminder_id) << "' RETURNING 1;";
        const auto rows = query_rows(sql.str());
        return rows.has_value() && !rows->empty();
    }

    std::vector<ReminderRecord> fire_due_reminders(const long long now_ms, const int limit) const {
        if (!enabled_) {
            return {};
        }
        std::ostringstream sql;
        sql << "WITH due AS ("
            << "SELECT reminder_id FROM user_reminders "
            << "WHERE state='scheduled' AND remind_at_ms <= " << now_ms << " "
            << "ORDER BY remind_at_ms ASC LIMIT " << std::max(limit, 1)
            << ") "
            << "UPDATE user_reminders SET "
            << "state='fired', "
            << "fired_at_ms=COALESCE(fired_at_ms," << now_ms << "), "
            << "updated_at_ms=" << now_ms << " "
            << "WHERE reminder_id IN (SELECT reminder_id FROM due) "
            << "RETURNING reminder_id::text, user_id, source_type, message_id::text, conversation_id, "
            << "COALESCE(conversation_type,''), COALESCE(room_id,''), COALESCE(call_id,''), COALESCE(message_preview_text,''), "
            << "COALESCE(message_author_user_id,''), COALESCE(message_author_display_name,''), COALESCE(message_ts_ms::text,''), "
            << "COALESCE(note,''), remind_at_ms::text, state, COALESCE(fired_at_ms::text,''), COALESCE(dismissed_at_ms::text,''), "
            << "created_at_ms::text, updated_at_ms::text;";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value()) {
            return {};
        }
        std::vector<ReminderRecord> items;
        items.reserve(rows->size());
        for (const auto& row : *rows) {
            items.push_back(reminder_from_row(row));
        }
        return items;
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

    std::vector<RelationshipListItem> list_relationships(const std::string& user_id, const std::string& status, const int limit, const int offset) const {
        if (!enabled_) {
            return {};
        }
        std::ostringstream sql;
        const auto counterpart_status = counterpart_relationship_status(status);
        sql << "SELECT p.user_id::text, p.display_name, COALESCE(p.username, ''), p.profile_status, "
            << "r.status, "
            << "to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), "
            << "to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') "
            << "FROM user_relationships r ";
        if (counterpart_status.has_value()) {
            sql << "JOIN user_relationships inverse ON inverse.user_id=r.target_user_id "
                << "AND inverse.target_user_id=r.user_id "
                << "AND inverse.relation_type='friend' "
                << "AND inverse.status='" << shell_escape_single_quotes(*counterpart_status) << "' ";
        }
        sql << "JOIN user_profiles p ON p.user_id=r.target_user_id "
            << "WHERE r.user_id='" << shell_escape_single_quotes(user_id) << "' "
            << "AND r.relation_type='friend' "
            << "AND r.status='" << shell_escape_single_quotes(status) << "' "
            << "AND NOT EXISTS (SELECT 1 FROM user_blocks b "
            << "WHERE (b.user_id='" << shell_escape_single_quotes(user_id) << "' AND b.target_user_id=r.target_user_id) "
            << "OR (b.user_id=r.target_user_id AND b.target_user_id='" << shell_escape_single_quotes(user_id) << "')) "
            << "ORDER BY r.updated_at DESC "
            << "LIMIT " << std::max(limit, 0) << " OFFSET " << std::max(offset, 0) << ";";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value()) {
            return {};
        }
        std::vector<RelationshipListItem> items;
        items.reserve(rows->size());
        for (const auto& row : *rows) {
            if (row.size() != 7U) {
                throw std::runtime_error("Unexpected relationship list row shape");
            }
            items.push_back(RelationshipListItem{
                .user_id = row[0],
                .display_name = row[1],
                .username = row[2].empty() ? std::nullopt : std::optional<std::string>(row[2]),
                .profile_status = row[3],
                .relation_status = row[4],
                .created_at = row[5],
                .updated_at = row[6],
            });
        }
        return items;
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
        const auto now = now_iso8601();
        std::ostringstream sql;
        sql << "WITH updated AS ("
            << "UPDATE user_relationships SET status='accepted', updated_at=NOW() "
            << "WHERE relation_type='friend' "
            << "AND EXISTS (SELECT 1 FROM user_relationships incoming "
            << "WHERE incoming.user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND incoming.target_user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND incoming.relation_type='friend' AND incoming.status='pending_incoming') "
            << "AND EXISTS (SELECT 1 FROM user_relationships outgoing "
            << "WHERE outgoing.user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND outgoing.target_user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND outgoing.relation_type='friend' AND outgoing.status='pending_outgoing') "
            << "AND ((user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND target_user_id='" << shell_escape_single_quotes(target_user_id) << "' AND status='pending_incoming') "
            << "OR (user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND target_user_id='" << shell_escape_single_quotes(actor_user_id) << "' AND status='pending_outgoing')) "
            << "RETURNING 1"
            << "), event_insert AS ("
            << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) "
            << "SELECT '" << shell_escape_single_quotes(hash_to_uuid("friend-request-accepted-" + actor_user_id + "-" + target_user_id + "-" + now)) << "',"
            << "'user_relationship','" << shell_escape_single_quotes(actor_user_id) << "','user.friend_request_accepted',"
            << "'{\"userId\":\"" << shell_escape_single_quotes(actor_user_id) << "\",\"targetUserId\":\"" << shell_escape_single_quotes(target_user_id)
            << "\"}'::jsonb,NOW(),NULL "
            << "FROM (SELECT COUNT(*) AS updated_rows FROM updated) counts WHERE counts.updated_rows=2 "
            << "RETURNING 1"
            << ") SELECT COUNT(*)::text FROM updated;";
        return query_scalar(sql.str()) == std::optional<std::string>("2");
    }

    bool decline_friend_request(const std::string& actor_user_id, const std::string& target_user_id) const {
        std::ostringstream sql;
        sql << "WITH updated AS ("
            << "UPDATE user_relationships SET status='declined', updated_at=NOW() "
            << "WHERE relation_type='friend' "
            << "AND EXISTS (SELECT 1 FROM user_relationships incoming "
            << "WHERE incoming.user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND incoming.target_user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND incoming.relation_type='friend' AND incoming.status='pending_incoming') "
            << "AND EXISTS (SELECT 1 FROM user_relationships outgoing "
            << "WHERE outgoing.user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND outgoing.target_user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND outgoing.relation_type='friend' AND outgoing.status='pending_outgoing') "
            << "AND ((user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND target_user_id='" << shell_escape_single_quotes(target_user_id) << "' AND status='pending_incoming') "
            << "OR (user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND target_user_id='" << shell_escape_single_quotes(actor_user_id) << "' AND status='pending_outgoing')) "
            << "RETURNING 1"
            << ") SELECT COUNT(*)::text FROM updated;";
        return query_scalar(sql.str()) == std::optional<std::string>("2");
    }

    bool remove_friend(const std::string& actor_user_id, const std::string& target_user_id) const {
        const auto now = now_iso8601();
        std::ostringstream sql;
        sql << "WITH updated AS ("
            << "UPDATE user_relationships SET status='removed', updated_at=NOW() "
            << "WHERE relation_type='friend' "
            << "AND EXISTS (SELECT 1 FROM user_relationships lhs "
            << "WHERE lhs.user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND lhs.target_user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND lhs.relation_type='friend' AND lhs.status='accepted') "
            << "AND EXISTS (SELECT 1 FROM user_relationships rhs "
            << "WHERE rhs.user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND rhs.target_user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND rhs.relation_type='friend' AND rhs.status='accepted') "
            << "AND ((user_id='" << shell_escape_single_quotes(actor_user_id) << "' "
            << "AND target_user_id='" << shell_escape_single_quotes(target_user_id) << "' AND status='accepted') "
            << "OR (user_id='" << shell_escape_single_quotes(target_user_id) << "' "
            << "AND target_user_id='" << shell_escape_single_quotes(actor_user_id) << "' AND status='accepted')) "
            << "RETURNING 1"
            << "), event_insert AS ("
            << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) "
            << "SELECT '" << shell_escape_single_quotes(hash_to_uuid("friend-removed-" + actor_user_id + "-" + target_user_id + "-" + now)) << "',"
            << "'user_relationship','" << shell_escape_single_quotes(actor_user_id) << "','user.friend_removed',"
            << "'{\"userId\":\"" << shell_escape_single_quotes(actor_user_id) << "\",\"targetUserId\":\"" << shell_escape_single_quotes(target_user_id)
            << "\"}'::jsonb,NOW(),NULL "
            << "FROM (SELECT COUNT(*) AS updated_rows FROM updated) counts WHERE counts.updated_rows=2 "
            << "RETURNING 1"
            << ") SELECT COUNT(*)::text FROM updated;";
        return query_scalar(sql.str()) == std::optional<std::string>("2");
    }

    void create_block(const std::string& actor_user_id, const std::string& target_user_id, const std::optional<std::string>& reason) const {
        if (!enabled_) {
            return;
        }
        const auto now = now_iso8601();
        const std::string escaped_reason = reason.has_value() ? "'" + shell_escape_single_quotes(*reason) + "'" : "NULL";
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "INSERT INTO user_blocks (block_id, user_id, target_user_id, reason, created_at) VALUES ('"
            << shell_escape_single_quotes(hash_to_uuid("block-" + actor_user_id + "-" + target_user_id + "-" + now)) << "','"
            << shell_escape_single_quotes(actor_user_id) << "','" << shell_escape_single_quotes(target_user_id) << "',"
            << escaped_reason << ",NOW()) ON CONFLICT (user_id, target_user_id) DO NOTHING;";
        sql << "UPDATE user_relationships SET status='removed', updated_at=NOW() WHERE relation_type='friend' AND "
            << "((user_id='" << shell_escape_single_quotes(actor_user_id) << "' AND target_user_id='" << shell_escape_single_quotes(target_user_id) << "') OR "
            << "(user_id='" << shell_escape_single_quotes(target_user_id) << "' AND target_user_id='" << shell_escape_single_quotes(actor_user_id) << "'));";
        sql << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) VALUES ('"
            << shell_escape_single_quotes(hash_to_uuid("block-created-" + actor_user_id + "-" + target_user_id + "-" + now)) << "','user_block','"
            << shell_escape_single_quotes(actor_user_id) << "','user.block_created','{\"userId\":\""
            << shell_escape_single_quotes(actor_user_id) << "\",\"targetUserId\":\"" << shell_escape_single_quotes(target_user_id)
            << "\"}'::jsonb,NOW(),NULL);";
        sql << "COMMIT;";
        exec_sql(sql.str());
    }

    void remove_block(const std::string& actor_user_id, const std::string& target_user_id) const {
        if (!enabled_) {
            return;
        }
        const auto now = now_iso8601();
        std::ostringstream sql;
        sql << "BEGIN;";
        sql << "DELETE FROM user_blocks WHERE user_id='" << shell_escape_single_quotes(actor_user_id)
            << "' AND target_user_id='" << shell_escape_single_quotes(target_user_id) << "';";
        sql << "INSERT INTO user_event_outbox (event_id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) VALUES ('"
            << shell_escape_single_quotes(hash_to_uuid("block-removed-" + actor_user_id + "-" + target_user_id + "-" + now)) << "','user_block','"
            << shell_escape_single_quotes(actor_user_id) << "','user.block_removed','{\"userId\":\""
            << shell_escape_single_quotes(actor_user_id) << "\",\"targetUserId\":\"" << shell_escape_single_quotes(target_user_id)
            << "\"}'::jsonb,NOW(),NULL);";
        sql << "COMMIT;";
        exec_sql(sql.str());
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

    std::optional<ReminderRecord> find_scheduled_reminder(
        const std::string& user_id,
        const std::string& message_id,
        const long long remind_at_ms) const {
        std::ostringstream sql;
        sql << "SELECT reminder_id::text, user_id, source_type, message_id::text, conversation_id, "
            << "COALESCE(conversation_type,''), COALESCE(room_id,''), COALESCE(call_id,''), COALESCE(message_preview_text,''), "
            << "COALESCE(message_author_user_id,''), COALESCE(message_author_display_name,''), COALESCE(message_ts_ms::text,''), "
            << "COALESCE(note,''), remind_at_ms::text, state, COALESCE(fired_at_ms::text,''), COALESCE(dismissed_at_ms::text,''), "
            << "created_at_ms::text, updated_at_ms::text "
            << "FROM user_reminders WHERE user_id='" << shell_escape_single_quotes(user_id) << "' "
            << "AND message_id='" << shell_escape_single_quotes(message_id) << "' "
            << "AND remind_at_ms=" << remind_at_ms << " AND state='scheduled' LIMIT 1;";
        const auto rows = query_rows(sql.str());
        if (!rows.has_value() || rows->empty()) {
            return std::nullopt;
        }
        return reminder_from_row(rows->front());
    }

    ReminderRecord reminder_from_row(const std::vector<std::string>& row) const {
        if (row.size() != 19U) {
            throw std::runtime_error("Unexpected reminder row shape");
        }
        return ReminderRecord{
            .reminder_id = row[0],
            .user_id = row[1],
            .source_type = row[2],
            .message_id = row[3],
            .conversation_id = row[4],
            .conversation_type = row[5],
            .room_id = row[6],
            .call_id = row[7],
            .message_preview_text = row[8],
            .message_author_user_id = row[9],
            .message_author_display_name = row[10],
            .message_ts_ms = row[11].empty() ? std::nullopt : std::optional<long long>(std::stoll(row[11])),
            .note = row[12],
            .remind_at_ms = std::stoll(row[13]),
            .state = row[14],
            .fired_at_ms = row[15].empty() ? std::nullopt : std::optional<long long>(std::stoll(row[15])),
            .dismissed_at_ms = row[16].empty() ? std::nullopt : std::optional<long long>(std::stoll(row[16])),
            .created_at_ms = std::stoll(row[17]),
            .updated_at_ms = std::stoll(row[18]),
        };
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
        } catch (const JsonParseError&) {
            auto response = invalid_json_response();
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

    void run_background_jobs() {
        std::lock_guard<std::mutex> guard(mutex_);
        run_background_jobs_locked();
    }

private:
    std::mutex mutex_;
    PostgresPsqlAdapter db_;
    std::optional<std::string> jwt_issuer_ = get_env("JWT_ISSUER");
    std::optional<std::string> jwt_audience_ = get_env("JWT_AUDIENCE");
    std::optional<std::string> jwt_secret_ = get_env("JWT_SECRET");
    std::optional<std::string> internal_jwt_issuer_ = get_env("INTERNAL_JWT_ISSUER");
    std::optional<std::string> internal_jwt_audience_ = get_env("INTERNAL_JWT_AUDIENCE");
    std::optional<std::string> internal_jwt_secret_ = get_env("INTERNAL_JWT_SECRET");
    std::optional<std::string> internal_token_ = get_env("INTERNAL_TOKEN");
    int presence_green_ttl_seconds_ = parse_int_env("PRESENCE_GREEN_TTL_SECONDS", 30);
    int reminder_scan_interval_seconds_ = parse_int_env("REMINDER_SCAN_INTERVAL_SECONDS", 15);
    std::unordered_map<std::string, UserProfile> profiles_;
    std::unordered_map<std::string, PrivacySettings> privacy_;
    std::unordered_map<std::string, RelationshipRecord> relationships_;
    std::unordered_map<std::string, BlockRecord> blocks_;
    std::unordered_map<std::string, ProjectionRecord> projections_;
    std::unordered_map<std::string, CallHistoryRecord> call_history_;
    std::unordered_map<std::string, ReminderRecord> reminders_;
    std::unordered_map<std::string, PresenceSessionRecord> presence_sessions_;
    std::unordered_map<std::string, DeviceSession> sessions_;
    std::vector<JsonObject> outbox_;
    std::vector<JsonObject> audit_log_;
    std::set<std::string> processed_event_ids_;
    std::unordered_map<std::string, int> metrics_;
    std::string current_request_id_ = "-";
    std::string current_request_method_ = "-";
    std::string current_request_path_ = "-";
    std::optional<JwtPrincipal> current_jwt_principal_;
    std::unordered_map<std::string, RelationshipSummary> request_relationship_summary_cache_;
    std::unordered_map<std::string, JsonObject> request_db_profile_cache_;
    std::unordered_map<std::string, JsonObject> request_db_privacy_cache_;
    std::unordered_map<std::string, AuthorizationDecision> request_authorization_decision_cache_;
    std::set<std::string> request_privacy_log_cache_;
    std::set<std::string> request_profile_log_cache_;
    long long next_reminder_scan_at_ms_ = 0;

    void reset_request_context() {
        current_request_id_ = "-";
        current_request_method_ = "-";
        current_request_path_ = "-";
        current_jwt_principal_.reset();
        request_relationship_summary_cache_.clear();
        request_db_profile_cache_.clear();
        request_db_privacy_cache_.clear();
        request_authorization_decision_cache_.clear();
        request_privacy_log_cache_.clear();
        request_profile_log_cache_.clear();
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

    static std::string request_cache_key(const std::string& scope, const std::string& actor_user_id, const std::string& target_user_id) {
        return scope + "|" + actor_user_id + "|" + target_user_id;
    }

    static std::string request_cache_key(const std::string& scope, const std::string& user_id) {
        return scope + "|" + user_id;
    }

    std::optional<AuthorizationDecision> cached_authorization_decision(
        const std::string& scope,
        const std::string& actor_user_id,
        const std::string& target_user_id) const {
        const auto it = request_authorization_decision_cache_.find(request_cache_key(scope, actor_user_id, target_user_id));
        if (it == request_authorization_decision_cache_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    AuthorizationDecision cache_authorization_decision(
        const std::string& scope,
        const std::string& actor_user_id,
        const std::string& target_user_id,
        const bool allowed,
        const std::string& reason) {
        const AuthorizationDecision decision{allowed, reason};
        request_authorization_decision_cache_[request_cache_key(scope, actor_user_id, target_user_id)] = decision;
        return decision;
    }

    std::optional<std::string> effective_internal_jwt_secret() const {
        if (internal_jwt_secret_.has_value()) {
            return internal_jwt_secret_;
        }
        return jwt_secret_;
    }

    std::optional<std::string> effective_internal_jwt_issuer() const {
        return internal_jwt_issuer_.has_value() ? internal_jwt_issuer_ : std::optional<std::string>("auth-service");
    }

    std::optional<std::string> effective_internal_jwt_audience() const {
        return internal_jwt_audience_.has_value() ? internal_jwt_audience_ : std::optional<std::string>("internal");
    }

    static std::string auth_failure_reason(const std::string& message) {
        if (message.find("issuer mismatch") != std::string::npos) {
            return "bad_issuer";
        }
        if (message.find("audience mismatch") != std::string::npos) {
            return "bad_audience";
        }
        if (message.find("expired") != std::string::npos) {
            return "expired";
        }
        if (message.find("signature mismatch") != std::string::npos ||
            message.find("Malformed JWT") != std::string::npos ||
            message.find("base64url") != std::string::npos ||
            message.find("secret") != std::string::npos ||
            message.find("alg") != std::string::npos) {
            return "bad_signature";
        }
        return "claims_mismatch";
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
        std::cout << oss.str() << std::endl;
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
        const std::string& mutuals_satisfied = "-") {
        const std::string cache_key = actor_user_id + "|" + target_user_id + "|" + policy_type + "|" + decision + "|" + reason + "|" +
                                      actor_privacy_exists + "|" + target_privacy_exists + "|" + resolved_policy + "|" + mutuals_satisfied;
        if (!request_privacy_log_cache_.insert(cache_key).second) {
            return;
        }
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

    void log_profile_resolution(
        const std::string& status,
        const std::string& actor_user_id,
        const std::string& target_user_id,
        const std::string& surface,
        const std::string& decision,
        const std::string& reason,
        const std::string& profile_status,
        const std::string& profile_visibility,
        const std::string& avatar_visibility,
        const std::string& is_friend,
        const std::string& avatar_object_present,
        const std::string& avatar_object_exposed) {
        const std::string cache_key = "resolve|" + status + "|" + actor_user_id + "|" + target_user_id + "|" + surface + "|" + decision + "|" + reason + "|" +
                                      profile_status + "|" + profile_visibility + "|" + avatar_visibility + "|" + is_friend + "|" +
                                      avatar_object_present + "|" + avatar_object_exposed;
        if (!request_profile_log_cache_.insert(cache_key).second) {
            return;
        }
        log_event(
            "user.profile.resolve",
            status,
            actor_user_id,
            target_user_id,
            decision,
            reason,
            {
                {"surface", surface},
                {"profileStatus", profile_status},
                {"profileVisibility", profile_visibility},
                {"avatarVisibility", avatar_visibility},
                {"isFriend", is_friend},
                {"avatarObjectPresent", avatar_object_present},
                {"avatarObjectExposed", avatar_object_exposed},
            });
    }

    void log_profile_masking_decision(
        const std::string& actor_user_id,
        const std::string& target_user_id,
        const std::string& surface,
        const std::string& decision,
        const std::string& reason,
        const std::string& include_private_fields,
        const std::string& avatar_visibility,
        const std::string& is_friend,
        const std::string& mask_applied) {
        const std::string cache_key = "mask|" + actor_user_id + "|" + target_user_id + "|" + surface + "|" + decision + "|" + reason + "|" +
                                      include_private_fields + "|" + avatar_visibility + "|" + is_friend + "|" + mask_applied;
        if (!request_profile_log_cache_.insert(cache_key).second) {
            return;
        }
        log_event(
            "user.profile.mask",
            "-",
            actor_user_id,
            target_user_id,
            decision,
            reason,
            {
                {"surface", surface},
                {"includePrivateFields", include_private_fields},
                {"avatarVisibility", avatar_visibility},
                {"isFriend", is_friend},
                {"maskApplied", mask_applied},
            });
    }

    void log_avatar_visibility_outcome(
        const std::string& actor_user_id,
        const std::string& target_user_id,
        const std::string& surface,
        const std::string& decision,
        const std::string& reason,
        const std::string& avatar_visibility,
        const std::string& is_friend,
        const std::string& avatar_object_present,
        const std::string& avatar_object_exposed) {
        const std::string cache_key = "avatar|" + actor_user_id + "|" + target_user_id + "|" + surface + "|" + decision + "|" + reason + "|" +
                                      avatar_visibility + "|" + is_friend + "|" + avatar_object_present + "|" + avatar_object_exposed;
        if (!request_profile_log_cache_.insert(cache_key).second) {
            return;
        }
        log_event(
            "user.avatar.visibility",
            "-",
            actor_user_id,
            target_user_id,
            decision,
            reason,
            {
                {"surface", surface},
                {"avatarVisibility", avatar_visibility},
                {"isFriend", is_friend},
                {"avatarObjectPresent", avatar_object_present},
                {"avatarObjectExposed", avatar_object_exposed},
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

    bool has_expected_counterpart_relationship(const std::string& user_id, const std::string& target_user_id, const std::string& status) const {
        const auto counterpart_status = counterpart_relationship_status(status);
        if (!counterpart_status.has_value()) {
            return true;
        }
        const auto counterpart_it = relationships_.find(pair_key(target_user_id, user_id));
        return counterpart_it != relationships_.end() && counterpart_it->second.status == *counterpart_status;
    }

    Response route(const Request& request) {
        run_background_jobs_locked();
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
        if (request.method == "POST" && request.path == "/internal/users/batch") {
            return internal_batch_profiles(request);
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
        if (request.method == "POST" && is_reminder_collection_path(request.path)) {
            return create_my_reminder(request);
        }
        if (request.method == "GET" && is_reminder_collection_path(request.path)) {
            return list_my_reminders(request);
        }
        if (request.method == "PATCH" && is_reminder_item_path(request.path)) {
            return patch_my_reminder(request);
        }
        if (request.method == "DELETE" && is_reminder_item_path(request.path)) {
            return delete_my_reminder(request);
        }
        if (request.method == "POST" && request.path == "/v1/users/me/presence/pulse") {
            return pulse_my_presence(request);
        }
        if (request.method == "POST" && request.path == "/v1/users/me/presence/disconnect") {
            return disconnect_my_presence(request);
        }
        if (request.method == "POST" && request.path == "/v1/users/presence/query") {
            return query_presence(request);
        }
        if (request.method == "GET" && request.path == "/v1/users/me/rooms") {
            return list_projection_entities(request, "room");
        }
        if (request.method == "GET" && request.path == "/v1/users/me/conversations") {
            return list_projection_entities(request, "conversation");
        }
        if (request.method == "GET" && request.path == "/v1/users/me/call-history") {
            return list_my_call_history(request);
        }
        if (request.method == "DELETE" && request.path == "/v1/users/me/call-history") {
            return clear_my_call_history(request);
        }
        if (starts_with(request.path, "/v1/users/me/call-history/") && request.method == "DELETE") {
            return delete_my_call_history(request);
        }
        if (request.method == "GET" && request.path == "/v1/users/me/friends") {
            return list_relationship_collection(request, "accepted", "friend.list");
        }
        if (request.method == "GET" && request.path == "/v1/users/me/contacts") {
            return list_relationship_collection(request, "accepted", "contact.list");
        }
        if (request.method == "GET" && request.path == "/v1/users/me/friend-requests/incoming") {
            return list_relationship_collection(request, "pending_incoming", "friend_request.list.incoming");
        }
        if (request.method == "GET" && request.path == "/v1/users/me/friend-requests/outgoing") {
            return list_relationship_collection(request, "pending_outgoing", "friend_request.list.outgoing");
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

    static bool is_reminder_collection_path(const std::string& path) {
        return path == "/v1/reminders" || path == "/user/v1/reminders";
    }

    static bool is_reminder_item_path(const std::string& path) {
        return starts_with(path, "/v1/reminders/") || starts_with(path, "/user/v1/reminders/");
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

    Response invalid_json_response() {
        increment_metric("http.error.400");
        return json_response(400, JsonObject{{"error", "invalid json"}});
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
            const std::string reason = auth_failure_reason(ex.what());
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
        const auto auth_it = request.headers.find("authorization");
        if (auth_it != request.headers.end()) {
            const std::string raw_header = trim(auth_it->second);
            const std::string header = to_lower(raw_header);
            const bool bearer_present = header.rfind("bearer ", 0) == 0;
            log_auth_start(request, true, bearer_present);
            const std::string bearer_prefix = "bearer ";
            if (header.rfind(bearer_prefix, 0) != 0 || header.rfind("bearer user:", 0) == 0) {
                log_auth_result("401", "-", "deny", "missing_bearer", std::nullopt, "false", true, bearer_present, false);
                throw HttpError(401, "unauthorized", "Expected Authorization: Bearer <jwt>");
            }
            std::optional<JwtPrincipal> principal;
            try {
                principal = parse_jwt_without_signature_validation(
                    trim(raw_header.substr(bearer_prefix.size())),
                    effective_internal_jwt_issuer(),
                    effective_internal_jwt_audience(),
                    effective_internal_jwt_secret());
            } catch (const std::exception& ex) {
                const std::string reason = auth_failure_reason(ex.what());
                log_auth_result("401", "-", "deny", reason, std::nullopt, "false", true, true, false);
                throw HttpError(401, "unauthorized", ex.what());
            }
            current_jwt_principal_ = principal;
            log_auth_result("200", principal->canonical_id, "allow", "ok", principal, "true", true, true, true);
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

    const JsonObject& ensure_db_profile_exists_and_load(const std::string& user_id, const std::optional<std::string>& preferred_display_name) {
        const auto cache_it = request_db_profile_cache_.find(request_cache_key("db_profile", user_id));
        if (cache_it != request_db_profile_cache_.end()) {
            return cache_it->second;
        }
        db_.ensure_profile_exists(user_id, preferred_display_name.value_or("User " + user_id.substr(0, std::min<std::size_t>(8, user_id.size()))));
        const auto profile = db_.get_profile(user_id);
        if (!profile.has_value()) {
            throw std::runtime_error("Failed to load DB profile after ensure");
        }
        return request_db_profile_cache_.emplace(request_cache_key("db_profile", user_id), *profile).first->second;
    }

    const JsonObject& ensure_db_privacy_exists_and_load(const std::string& user_id) {
        const auto cache_it = request_db_privacy_cache_.find(request_cache_key("db_privacy", user_id));
        if (cache_it != request_db_privacy_cache_.end()) {
            return cache_it->second;
        }
        db_.ensure_profile_exists(user_id, "User " + user_id.substr(0, std::min<std::size_t>(8, user_id.size())));
        const auto settings = db_.get_privacy_settings(user_id);
        if (!settings.has_value()) {
            throw std::runtime_error("Failed to load DB privacy after ensure");
        }
        return request_db_privacy_cache_.emplace(request_cache_key("db_privacy", user_id), *settings).first->second;
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

    RelationshipSummary relationship_summary(const std::string& actor_user_id, const std::string& target_user_id) {
        const auto cache_it = request_relationship_summary_cache_.find(request_cache_key("memory_relationship", actor_user_id, target_user_id));
        if (cache_it != request_relationship_summary_cache_.end()) {
            return cache_it->second;
        }
        const RelationshipSummary summary{
            .is_friend = is_friend(actor_user_id, target_user_id),
            .is_blocked = has_block(actor_user_id, target_user_id),
            .is_blocked_by_target = has_block(target_user_id, actor_user_id),
        };
        request_relationship_summary_cache_[request_cache_key("memory_relationship", actor_user_id, target_user_id)] = summary;
        return summary;
    }

    RelationshipSummary db_relationship_summary(const std::string& actor_user_id, const std::string& target_user_id) {
        const auto cache_it = request_relationship_summary_cache_.find(request_cache_key("db_relationship", actor_user_id, target_user_id));
        if (cache_it != request_relationship_summary_cache_.end()) {
            return cache_it->second;
        }
        const RelationshipSummary summary{
            .is_friend = db_.are_friends(actor_user_id, target_user_id),
            .is_blocked = db_.has_block(actor_user_id, target_user_id),
            .is_blocked_by_target = db_.has_block(target_user_id, actor_user_id),
        };
        request_relationship_summary_cache_[request_cache_key("db_relationship", actor_user_id, target_user_id)] = summary;
        return summary;
    }

    JsonObject raw_profile_to_json(const UserProfile& profile) const {
        return JsonObject{
            {"userId", profile.user_id},
            {"displayName", profile.display_name},
            {"profileStatus", profile.profile_status},
            {"createdAt", profile.created_at},
            {"updatedAt", profile.updated_at},
        };
    }

    JsonObject profile_to_json(
        JsonObject object,
        const std::optional<std::string>& profile_visibility,
        const std::optional<std::string>& avatar_visibility,
        const bool include_private_fields,
        const std::string& actor_user_id,
        const std::string& surface,
        const std::string& reason,
        const bool is_friend) {
        const std::string target_user_id = required_string(object, "userId");
        const std::string profile_status = required_string(object, "profileStatus");
        const std::string resolved_profile_visibility = profile_visibility.value_or("-");
        const std::string resolved_avatar_visibility = avatar_visibility.value_or("-");
        const bool avatar_object_present = object.count("avatarObjectId") != 0 && !object.at("avatarObjectId").is_null();

        bool avatar_allowed = include_private_fields;
        std::string avatar_reason = include_private_fields ? "private_fields_included" : "avatar_available";
        if (!include_private_fields) {
            if (resolved_avatar_visibility == "public") {
                avatar_allowed = true;
                avatar_reason = "avatar_public";
            } else if (resolved_avatar_visibility == "friends_only") {
                avatar_allowed = is_friend;
                avatar_reason = is_friend ? "avatar_friends_only_friend" : "avatar_friends_only_denied";
            } else if (resolved_avatar_visibility == "private") {
                avatar_allowed = false;
                avatar_reason = "avatar_private";
            } else {
                avatar_allowed = true;
                avatar_reason = "avatar_visibility_unset";
            }
        }

        const bool mask_applied = avatar_object_present && !avatar_allowed;
        if (mask_applied) {
            object["avatarObjectId"] = Json(nullptr);
        }
        const bool avatar_object_exposed = object.count("avatarObjectId") != 0 && !object.at("avatarObjectId").is_null();

        log_profile_resolution(
            "200",
            actor_user_id,
            target_user_id,
            surface,
            "allow",
            reason,
            profile_status,
            resolved_profile_visibility,
            resolved_avatar_visibility,
            bool_string(is_friend),
            bool_string(avatar_object_present),
            bool_string(avatar_object_exposed));
        log_profile_masking_decision(
            actor_user_id,
            target_user_id,
            surface,
            mask_applied ? "mask" : "pass",
            mask_applied ? avatar_reason : (include_private_fields ? "private_fields_included" : "avatar_not_masked"),
            bool_string(include_private_fields),
            resolved_avatar_visibility,
            bool_string(is_friend),
            bool_string(mask_applied));
        log_avatar_visibility_outcome(
            actor_user_id,
            target_user_id,
            surface,
            avatar_object_exposed ? "visible" : "hidden",
            mask_applied ? avatar_reason : (avatar_object_present ? avatar_reason : "avatar_missing"),
            resolved_avatar_visibility,
            bool_string(is_friend),
            bool_string(avatar_object_present),
            bool_string(avatar_object_exposed));
        return object;
    }

    JsonObject profile_to_json(
        const UserProfile& profile,
        const PrivacySettings& privacy,
        const bool include_private_fields,
        const std::string& actor_user_id,
        const std::string& surface,
        const std::string& reason,
        const bool is_friend) {
        auto object = raw_profile_to_json(profile);
        object["username"] = profile.username.has_value() ? Json(*profile.username) : Json(nullptr);
        object["bio"] = profile.bio.has_value() ? Json(*profile.bio) : Json(nullptr);
        object["locale"] = profile.locale.has_value() ? Json(*profile.locale) : Json(nullptr);
        object["timeZone"] = profile.time_zone.has_value() ? Json(*profile.time_zone) : Json(nullptr);
        object["avatarObjectId"] = profile.avatar_object_id.has_value() ? Json(*profile.avatar_object_id) : Json(nullptr);
        object["deletedAt"] = profile.deleted_at.has_value() ? Json(*profile.deleted_at) : Json(nullptr);
        return profile_to_json(
            std::move(object),
            privacy.profile_visibility,
            privacy.avatar_visibility,
            include_private_fields,
            actor_user_id,
            surface,
            reason,
            is_friend);
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

    Json call_history_to_json(const CallHistoryRecord& record) {
        JsonArray participants;
        for (const auto& participant_user_id : record.participant_user_ids) {
            if (db_.enabled()) {
                const auto& profile = ensure_db_profile_exists_and_load(participant_user_id, std::nullopt);
                participants.emplace_back(JsonObject{
                    {"userId", required_string(profile, "userId")},
                    {"displayName", required_string(profile, "displayName")},
                    {"avatarObjectId", profile.at("avatarObjectId")},
                    {"profileStatus", required_string(profile, "profileStatus")},
                });
                continue;
            }
            const auto& profile = require_profile_const(participant_user_id);
            participants.emplace_back(JsonObject{
                {"userId", profile.user_id},
                {"displayName", profile.display_name},
                {"avatarObjectId", profile.avatar_object_id.has_value() ? Json(*profile.avatar_object_id) : Json(nullptr)},
                {"profileStatus", profile.profile_status},
            });
        }
        return JsonObject{
            {"historyId", record.history_id},
            {"callId", record.call_id},
            {"ownerUserId", record.owner_user_id},
            {"initiatorUserId", record.initiator_user_id},
            {"callType", record.call_type},
            {"direction", record.direction},
            {"status", record.status},
            {"participantCount", record.participant_count},
            {"participants", participants},
            {"startedAt", record.started_at},
            {"endedAt", record.ended_at.has_value() ? Json(*record.ended_at) : Json(nullptr)},
            {"durationSec", record.duration_seconds},
            {"roomId", record.room_id.has_value() ? Json(*record.room_id) : Json(nullptr)},
            {"conversationId", record.conversation_id.has_value() ? Json(*record.conversation_id) : Json(nullptr)},
            {"createdAt", record.created_at},
            {"updatedAt", record.updated_at},
        };
    }

    Json presence_to_json(const PresenceSnapshot& snapshot) const {
        return JsonObject{
            {"userId", snapshot.user_id},
            {"presence", snapshot.presence},
            {"isOnline", snapshot.is_online},
            {"lastSeenAt", snapshot.last_seen_at.has_value() ? Json(*snapshot.last_seen_at) : Json(nullptr)},
            {"connectedSessionCount", snapshot.connected_session_count},
            {"recentSessionCount", snapshot.recent_session_count},
        };
    }

    Json reminder_to_json(const ReminderRecord& record) const {
        return JsonObject{
            {"reminderId", record.reminder_id},
            {"sourceType", record.source_type},
            {"messageId", record.message_id},
            {"conversationId", record.conversation_id},
            {"conversationType", record.conversation_type},
            {"roomId", record.room_id},
            {"callId", record.call_id},
            {"messagePreviewText", record.message_preview_text},
            {"messageAuthorUserId", record.message_author_user_id},
            {"messageAuthorDisplayName", record.message_author_display_name},
            {"messageTsMs", record.message_ts_ms.has_value() ? json_int64(*record.message_ts_ms) : Json(nullptr)},
            {"note", record.note},
            {"remindAtMs", json_int64(record.remind_at_ms)},
            {"state", record.state},
            {"firedAtMs", record.fired_at_ms.has_value() ? json_int64(*record.fired_at_ms) : Json(nullptr)},
            {"dismissedAtMs", record.dismissed_at_ms.has_value() ? json_int64(*record.dismissed_at_ms) : Json(nullptr)},
            {"createdAtMs", json_int64(record.created_at_ms)},
            {"updatedAtMs", json_int64(record.updated_at_ms)},
        };
    }

    Response reminder_error_response(const int status, const std::string& error) {
        increment_metric("http.error." + std::to_string(status));
        return json_response(status, JsonObject{{"ok", false}, {"error", error}});
    }

    std::optional<long long> parse_optional_long_long_query(const Request& request, const std::string& key) const {
        const auto it = request.query.find(key);
        if (it == request.query.end() || trim(it->second).empty()) {
            return std::nullopt;
        }
        return std::stoll(it->second);
    }

    std::string extract_reminder_id_from_path(const std::string& path) const {
        const std::vector<std::string> prefixes = {"/v1/reminders/", "/user/v1/reminders/"};
        for (const auto& prefix : prefixes) {
            if (!starts_with(path, prefix)) {
                continue;
            }
            const std::string reminder_id = path.substr(prefix.size());
            if (!reminder_id.empty() && reminder_id.find('/') == std::string::npos) {
                return reminder_id;
            }
        }
        throw HttpError(404, "not_found", "Route not found");
    }

    void validate_reminder_state(const std::string& value) const {
        static const std::set<std::string> allowed = {"scheduled", "fired", "dismissed", "cancelled"};
        if (!allowed.count(value)) {
            throw std::runtime_error("Invalid reminder state");
        }
    }

    ReminderRecord build_reminder_from_create_request(
        const JsonObject& object,
        const std::string& actor_user_id,
        const long long now_ms) const {
        const auto source_type = optional_string(object, "sourceType");
        if (!source_type.has_value()) {
            throw HttpError(400, "sourceType is required", "sourceType is required");
        }
        if (*source_type != "chat_message") {
            throw HttpError(400, "unsupported sourceType", "unsupported sourceType");
        }
        const auto message_id = optional_string(object, "messageId");
        if (!message_id.has_value() || message_id->empty()) {
            throw HttpError(400, "messageId is required", "messageId is required");
        }
        if (!is_uuid_like(*message_id)) {
            throw HttpError(400, "messageId must be uuid", "messageId must be uuid");
        }
        const auto conversation_id = optional_string(object, "conversationId");
        if (!conversation_id.has_value() || conversation_id->empty()) {
            throw HttpError(400, "conversationId is required", "conversationId is required");
        }
        const auto remind_at_ms = optional_int64(object, "remindAtMs");
        if (!remind_at_ms.has_value()) {
            throw HttpError(400, "remindAtMs is required", "remindAtMs is required");
        }
        if (*remind_at_ms <= now_ms - 5000) {
            throw HttpError(400, "remindAtMs must be in the future", "remindAtMs must be in the future");
        }
        const std::string message_preview_text = optional_string(object, "messagePreviewText").value_or("");
        if (message_preview_text.size() > 512U) {
            throw HttpError(400, "messagePreviewText too long", "messagePreviewText too long");
        }
        const std::string note = optional_string(object, "note").value_or("");
        if (note.size() > 1000U) {
            throw HttpError(400, "note too long", "note too long");
        }
        return ReminderRecord{
            .reminder_id = hash_to_uuid("reminder-" + actor_user_id + "-" + *message_id + "-" + std::to_string(*remind_at_ms) + "-" + std::to_string(now_ms)),
            .user_id = actor_user_id,
            .source_type = *source_type,
            .message_id = *message_id,
            .conversation_id = *conversation_id,
            .conversation_type = optional_string(object, "conversationType").value_or(""),
            .room_id = optional_string(object, "roomId").value_or(""),
            .call_id = optional_string(object, "callId").value_or(""),
            .message_preview_text = message_preview_text,
            .message_author_user_id = optional_string(object, "messageAuthorUserId").value_or(""),
            .message_author_display_name = optional_string(object, "messageAuthorDisplayName").value_or(""),
            .message_ts_ms = optional_int64(object, "messageTsMs"),
            .note = note,
            .remind_at_ms = *remind_at_ms,
            .state = "scheduled",
            .fired_at_ms = std::nullopt,
            .dismissed_at_ms = std::nullopt,
            .created_at_ms = now_ms,
            .updated_at_ms = now_ms,
        };
    }

    bool can_transition_reminder_state(
        const std::string& current_state,
        const std::string& next_state,
        const bool remind_at_ms_present) const {
        if (next_state == "fired") {
            return false;
        }
        if (current_state == next_state) {
            return current_state != "cancelled";
        }
        if (current_state == "scheduled" && (next_state == "dismissed" || next_state == "cancelled")) {
            return true;
        }
        if (current_state == "dismissed" && next_state == "scheduled" && remind_at_ms_present) {
            return true;
        }
        if (current_state == "fired" && next_state == "dismissed") {
            return true;
        }
        return false;
    }

    std::vector<std::string> normalize_participant_user_ids(
        const std::vector<std::string>& participant_user_ids,
        const std::string& initiator_user_id) const {
        std::vector<std::string> normalized;
        std::set<std::string> seen;
        for (const auto& value : participant_user_ids) {
            const auto canonical = canonical_user_id(value);
            if (seen.insert(canonical).second) {
                normalized.push_back(canonical);
            }
        }
        if (seen.insert(initiator_user_id).second) {
            normalized.insert(normalized.begin(), initiator_user_id);
        }
        if (normalized.empty()) {
            throw std::runtime_error("participantUserIds must not be empty");
        }
        return normalized;
    }

    void upsert_memory_call_history(
        const std::string& call_id,
        const std::string& initiator_user_id,
        const std::vector<std::string>& participant_user_ids,
        const std::string& call_type,
        const std::string& status,
        const std::string& started_at,
        const std::optional<std::string>& ended_at,
        const int duration_seconds,
        const std::optional<std::string>& room_id,
        const std::optional<std::string>& conversation_id) {
        const auto timestamp = now_iso8601();
        for (const auto& owner_user_id : participant_user_ids) {
            const std::string key = pair_key(owner_user_id, call_id);
            const auto existing = call_history_.find(key);
            const std::string created_at = existing != call_history_.end() ? existing->second.created_at : timestamp;
            call_history_[key] = CallHistoryRecord{
                .history_id = hash_to_uuid("call-history-" + owner_user_id + "-" + call_id),
                .call_id = call_id,
                .owner_user_id = owner_user_id,
                .initiator_user_id = initiator_user_id,
                .call_type = call_type,
                .direction = owner_user_id == initiator_user_id ? "outgoing" : "incoming",
                .status = status,
                .participant_user_ids = participant_user_ids,
                .participant_count = static_cast<int>(participant_user_ids.size()),
                .started_at = started_at,
                .ended_at = ended_at,
                .duration_seconds = duration_seconds,
                .room_id = room_id,
                .conversation_id = conversation_id,
                .created_at = created_at,
                .updated_at = timestamp,
            };
            trim_memory_call_history(owner_user_id, 50);
        }
    }

    void trim_memory_call_history(const std::string& owner_user_id, const std::size_t max_items) {
        std::vector<CallHistoryRecord> items;
        for (const auto& [key, record] : call_history_) {
            if (record.owner_user_id == owner_user_id) {
                items.push_back(record);
            }
        }
        std::sort(items.begin(), items.end(), [](const CallHistoryRecord& lhs, const CallHistoryRecord& rhs) {
            if (lhs.started_at == rhs.started_at) {
                return lhs.updated_at > rhs.updated_at;
            }
            return lhs.started_at > rhs.started_at;
        });
        if (items.size() <= max_items) {
            return;
        }
        for (std::size_t index = max_items; index < items.size(); ++index) {
            call_history_.erase(pair_key(owner_user_id, items[index].call_id));
        }
    }

    std::vector<CallHistoryRecord> list_memory_call_history(const std::string& owner_user_id, const int limit, const int offset) const {
        std::vector<CallHistoryRecord> items;
        for (const auto& [key, record] : call_history_) {
            if (record.owner_user_id == owner_user_id) {
                items.push_back(record);
            }
        }
        std::sort(items.begin(), items.end(), [](const CallHistoryRecord& lhs, const CallHistoryRecord& rhs) {
            if (lhs.started_at == rhs.started_at) {
                return lhs.updated_at > rhs.updated_at;
            }
            return lhs.started_at > rhs.started_at;
        });
        const int safe_offset = std::max(offset, 0);
        const int safe_limit = std::max(limit, 0);
        if (safe_offset >= static_cast<int>(items.size())) {
            return {};
        }
        const auto begin = items.begin() + safe_offset;
        const auto end = begin + std::min(safe_limit, static_cast<int>(items.end() - begin));
        return std::vector<CallHistoryRecord>(begin, end);
    }

    bool delete_memory_call_history(const std::string& owner_user_id, const std::string& history_id) {
        for (auto it = call_history_.begin(); it != call_history_.end(); ++it) {
            if (it->second.owner_user_id == owner_user_id && it->second.history_id == history_id) {
                call_history_.erase(it);
                return true;
            }
        }
        return false;
    }

    int clear_memory_call_history(const std::string& owner_user_id) {
        int deleted = 0;
        for (auto it = call_history_.begin(); it != call_history_.end();) {
            if (it->second.owner_user_id == owner_user_id) {
                it = call_history_.erase(it);
                ++deleted;
            } else {
                ++it;
            }
        }
        return deleted;
    }

    ReminderRecord create_memory_reminder(const ReminderRecord& record) {
        for (const auto& [key, existing] : reminders_) {
            if (existing.user_id == record.user_id &&
                existing.message_id == record.message_id &&
                existing.remind_at_ms == record.remind_at_ms &&
                existing.state == "scheduled") {
                return existing;
            }
        }
        reminders_[record.reminder_id] = record;
        return reminders_.at(record.reminder_id);
    }

    std::optional<ReminderRecord> get_memory_reminder(const std::string& user_id, const std::string& reminder_id) const {
        const auto it = reminders_.find(reminder_id);
        if (it == reminders_.end() || it->second.user_id != user_id) {
            return std::nullopt;
        }
        return it->second;
    }

    std::vector<ReminderRecord> list_memory_reminders(
        const std::string& user_id,
        const std::optional<std::string>& state,
        const int limit,
        const int offset,
        const std::optional<long long>& from_ms,
        const std::optional<long long>& to_ms) const {
        std::vector<ReminderRecord> items;
        for (const auto& [key, record] : reminders_) {
            if (record.user_id != user_id) {
                continue;
            }
            if (state.has_value() && record.state != *state) {
                continue;
            }
            if (from_ms.has_value() && record.remind_at_ms < *from_ms) {
                continue;
            }
            if (to_ms.has_value() && record.remind_at_ms > *to_ms) {
                continue;
            }
            items.push_back(record);
        }
        std::sort(items.begin(), items.end(), [state](const ReminderRecord& lhs, const ReminderRecord& rhs) {
            if (state == std::optional<std::string>("scheduled")) {
                if (lhs.remind_at_ms == rhs.remind_at_ms) {
                    return lhs.updated_at_ms > rhs.updated_at_ms;
                }
                return lhs.remind_at_ms < rhs.remind_at_ms;
            }
            if (state.has_value()) {
                if (lhs.updated_at_ms == rhs.updated_at_ms) {
                    return lhs.remind_at_ms < rhs.remind_at_ms;
                }
                return lhs.updated_at_ms > rhs.updated_at_ms;
            }
            if (lhs.state == "scheduled" && rhs.state != "scheduled") {
                return true;
            }
            if (lhs.state != "scheduled" && rhs.state == "scheduled") {
                return false;
            }
            if (lhs.state == "scheduled" && rhs.state == "scheduled") {
                if (lhs.remind_at_ms == rhs.remind_at_ms) {
                    return lhs.updated_at_ms > rhs.updated_at_ms;
                }
                return lhs.remind_at_ms < rhs.remind_at_ms;
            }
            if (lhs.updated_at_ms == rhs.updated_at_ms) {
                return lhs.remind_at_ms < rhs.remind_at_ms;
            }
            return lhs.updated_at_ms > rhs.updated_at_ms;
        });
        const int safe_offset = std::max(offset, 0);
        const int safe_limit = std::max(limit, 0);
        if (safe_offset >= static_cast<int>(items.size())) {
            return {};
        }
        const auto begin = items.begin() + safe_offset;
        const auto end = begin + std::min(safe_limit, static_cast<int>(items.end() - begin));
        return std::vector<ReminderRecord>(begin, end);
    }

    std::optional<ReminderRecord> update_memory_reminder(const ReminderRecord& record) {
        const auto it = reminders_.find(record.reminder_id);
        if (it == reminders_.end() || it->second.user_id != record.user_id) {
            return std::nullopt;
        }
        it->second = record;
        return it->second;
    }

    bool delete_memory_reminder(const std::string& user_id, const std::string& reminder_id) {
        const auto it = reminders_.find(reminder_id);
        if (it == reminders_.end() || it->second.user_id != user_id) {
            return false;
        }
        reminders_.erase(it);
        return true;
    }

    std::vector<ReminderRecord> fire_due_memory_reminders(const long long now_ms, const int limit) {
        std::vector<ReminderRecord> due_items;
        for (const auto& [key, record] : reminders_) {
            if (record.state == "scheduled" && record.remind_at_ms <= now_ms) {
                due_items.push_back(record);
            }
        }
        std::sort(due_items.begin(), due_items.end(), [](const ReminderRecord& lhs, const ReminderRecord& rhs) {
            if (lhs.remind_at_ms == rhs.remind_at_ms) {
                return lhs.created_at_ms < rhs.created_at_ms;
            }
            return lhs.remind_at_ms < rhs.remind_at_ms;
        });
        if (static_cast<int>(due_items.size()) > limit) {
            due_items.resize(limit);
        }
        std::vector<ReminderRecord> fired_items;
        fired_items.reserve(due_items.size());
        for (auto& item : due_items) {
            auto stored = reminders_.at(item.reminder_id);
            stored.state = "fired";
            stored.fired_at_ms = now_ms;
            stored.updated_at_ms = now_ms;
            reminders_[stored.reminder_id] = stored;
            fired_items.push_back(stored);
        }
        return fired_items;
    }

    void upsert_memory_presence_session(
        const std::string& user_id,
        const std::string& session_id,
        const std::optional<std::string>& device_id,
        const std::string& platform) {
        const auto timestamp = now_iso8601();
        const auto epoch_seconds = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
        const std::string key = pair_key(user_id, session_id);
        const auto existing = presence_sessions_.find(key);
        const std::string created_at = existing != presence_sessions_.end() ? existing->second.created_at : timestamp;
        presence_sessions_[key] = PresenceSessionRecord{
            .user_id = user_id,
            .session_id = session_id,
            .device_id = device_id,
            .platform = platform,
            .state = "connected",
            .last_pulse_at = timestamp,
            .last_disconnect_at = std::nullopt,
            .last_pulse_epoch_seconds = epoch_seconds,
            .last_disconnect_epoch_seconds = std::nullopt,
            .created_at = created_at,
            .updated_at = timestamp,
        };
    }

    bool disconnect_memory_presence_session(const std::string& user_id, const std::string& session_id) {
        const std::string key = pair_key(user_id, session_id);
        const auto it = presence_sessions_.find(key);
        if (it == presence_sessions_.end()) {
            return false;
        }
        const auto timestamp = now_iso8601();
        const auto epoch_seconds = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
        it->second.state = "disconnected";
        it->second.last_disconnect_at = timestamp;
        it->second.last_disconnect_epoch_seconds = epoch_seconds;
        it->second.updated_at = timestamp;
        return true;
    }

    std::vector<PresenceSnapshot> list_memory_presence(const std::vector<std::string>& user_ids) const {
        std::vector<PresenceSnapshot> items;
        const auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
        for (const auto& user_id : user_ids) {
            int connected_session_count = 0;
            int recent_session_count = 0;
            std::optional<long long> last_seen_epoch_seconds;
            std::optional<std::string> last_seen_at;
            for (const auto& [key, session] : presence_sessions_) {
                if (session.user_id != user_id) {
                    continue;
                }
                const auto session_last_seen_epoch = session.last_disconnect_epoch_seconds.has_value()
                    ? std::max(session.last_pulse_epoch_seconds, *session.last_disconnect_epoch_seconds)
                    : session.last_pulse_epoch_seconds;
                const auto session_last_seen_at = session.last_disconnect_epoch_seconds.has_value() && *session.last_disconnect_epoch_seconds >= session.last_pulse_epoch_seconds
                    ? session.last_disconnect_at
                    : std::optional<std::string>(session.last_pulse_at);
                if (!last_seen_epoch_seconds.has_value() || session_last_seen_epoch > *last_seen_epoch_seconds) {
                    last_seen_epoch_seconds = session_last_seen_epoch;
                    last_seen_at = session_last_seen_at;
                }
                if (session.state == "connected") {
                    ++connected_session_count;
                    if ((now_seconds - session.last_pulse_epoch_seconds) <= presence_green_ttl_seconds_) {
                        ++recent_session_count;
                    }
                }
            }
            const std::string presence = recent_session_count > 0 ? "green" : (connected_session_count > 0 ? "yellow" : "red");
            items.push_back(PresenceSnapshot{
                .user_id = user_id,
                .presence = presence,
                .is_online = recent_session_count > 0,
                .last_seen_at = last_seen_at,
                .connected_session_count = connected_session_count,
                .recent_session_count = recent_session_count,
            });
        }
        return items;
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

    void validate_call_type(const std::string& value) const {
        static const std::set<std::string> allowed = {"audio", "video"};
        if (!allowed.count(value)) {
            throw std::runtime_error("Invalid callType");
        }
    }

    void validate_call_status(const std::string& value) const {
        static const std::set<std::string> allowed = {"completed", "missed", "cancelled", "declined", "ongoing"};
        if (!allowed.count(value)) {
            throw std::runtime_error("Invalid callStatus");
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

    bool authorize_profile_read(const std::string& actor_user_id, const std::string& target_user_id) {
        if (const auto cached = cached_authorization_decision("profile_read_memory", actor_user_id, target_user_id)) {
            return cached->allowed;
        }
        if (actor_user_id == target_user_id) {
            return cache_authorization_decision("profile_read_memory", actor_user_id, target_user_id, true, "policy_resolved").allowed;
        }
        const auto summary = relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            return cache_authorization_decision("profile_read_memory", actor_user_id, target_user_id, false, "blocked").allowed;
        }
        const auto& profile = require_profile_const(target_user_id);
        if (profile.profile_status != "active") {
            return cache_authorization_decision("profile_read_memory", actor_user_id, target_user_id, false, "target_inactive").allowed;
        }
        const auto& settings = require_privacy_const(target_user_id);
        if (settings.profile_visibility == "public") {
            return cache_authorization_decision("profile_read_memory", actor_user_id, target_user_id, true, "policy_resolved").allowed;
        }
        if (settings.profile_visibility == "friends_only") {
            return cache_authorization_decision(
                "profile_read_memory",
                actor_user_id,
                target_user_id,
                summary.is_friend,
                summary.is_friend ? "policy_resolved" : "profile_visibility_denied").allowed;
        }
        return cache_authorization_decision("profile_read_memory", actor_user_id, target_user_id, false, "profile_visibility_denied").allowed;
    }

    bool authorize_dm_start(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) {
        if (const auto cached = cached_authorization_decision("dm_memory", actor_user_id, target_user_id)) {
            reason = cached->reason;
            return cached->allowed;
        }
        if (actor_user_id == target_user_id) {
            reason = "self_dm_not_supported";
            return cache_authorization_decision("dm_memory", actor_user_id, target_user_id, false, reason).allowed;
        }
        const auto summary = relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked) {
            reason = "blocked_by_actor";
            return cache_authorization_decision("dm_memory", actor_user_id, target_user_id, false, reason).allowed;
        }
        if (summary.is_blocked_by_target) {
            reason = "blocked_by_target";
            return cache_authorization_decision("dm_memory", actor_user_id, target_user_id, false, reason).allowed;
        }
        const auto& profile = require_profile_const(target_user_id);
        if (profile.profile_status != "active") {
            reason = "target_inactive";
            return cache_authorization_decision("dm_memory", actor_user_id, target_user_id, false, reason).allowed;
        }
        const auto& settings = require_privacy_const(target_user_id);
        if (settings.dm_policy == "everyone") {
            reason.clear();
            return cache_authorization_decision("dm_memory", actor_user_id, target_user_id, true, reason).allowed;
        }
        if (settings.dm_policy == "friends_only" && summary.is_friend) {
            reason.clear();
            return cache_authorization_decision("dm_memory", actor_user_id, target_user_id, true, reason).allowed;
        }
        reason = "dm_policy_denied";
        return cache_authorization_decision("dm_memory", actor_user_id, target_user_id, false, reason).allowed;
    }

    bool authorize_dm_action_db(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) {
        if (const auto cached = cached_authorization_decision("dm_db", actor_user_id, target_user_id)) {
            reason = cached->reason;
            return cached->allowed;
        }
        if (actor_user_id == target_user_id) {
            reason = "self_dm_not_supported";
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "-", "-");
            return cache_authorization_decision("dm_db", actor_user_id, target_user_id, false, reason).allowed;
        }
        const auto summary = db_relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked) {
            reason = "blocked_by_actor";
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "-", "-");
            return cache_authorization_decision("dm_db", actor_user_id, target_user_id, false, reason).allowed;
        }
        if (summary.is_blocked_by_target) {
            reason = "blocked_by_target";
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "-", "-");
            return cache_authorization_decision("dm_db", actor_user_id, target_user_id, false, reason).allowed;
        }
        const auto& profile = ensure_db_profile_exists_and_load(target_user_id, std::nullopt);
        if (required_string(profile, "profileStatus") != "active") {
            reason = "target_inactive";
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "-", "-");
            return cache_authorization_decision("dm_db", actor_user_id, target_user_id, false, reason).allowed;
        }
        const auto& settings = ensure_db_privacy_exists_and_load(target_user_id);
        const auto dm_policy = required_string(settings, "dmPolicy");
        if (dm_policy == "everyone") {
            reason.clear();
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "allow", "policy_resolved", "-", "true", dm_policy);
            return cache_authorization_decision("dm_db", actor_user_id, target_user_id, true, reason).allowed;
        }
        if (dm_policy == "friends_only" && summary.is_friend) {
            reason.clear();
            log_privacy_resolution(actor_user_id, target_user_id, "dm", "allow", "policy_resolved", "-", "true", dm_policy);
            return cache_authorization_decision("dm_db", actor_user_id, target_user_id, true, reason).allowed;
        }
        reason = "dm_policy_denied";
        log_privacy_resolution(actor_user_id, target_user_id, "dm", "deny", reason, "-", "true", dm_policy);
        return cache_authorization_decision("dm_db", actor_user_id, target_user_id, false, reason).allowed;
    }

    bool authorize_profile_read_db(const std::string& actor_user_id, const std::string& target_user_id) {
        if (const auto cached = cached_authorization_decision("profile_read_db", actor_user_id, target_user_id)) {
            return cached->allowed;
        }
        if (actor_user_id == target_user_id) {
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "allow", "policy_resolved", "-", "-", "self");
            return cache_authorization_decision("profile_read_db", actor_user_id, target_user_id, true, "policy_resolved").allowed;
        }
        const auto summary = db_relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "deny", "blocked", "-", "-", "-");
            return cache_authorization_decision("profile_read_db", actor_user_id, target_user_id, false, "blocked").allowed;
        }
        const auto& profile = ensure_db_profile_exists_and_load(target_user_id, std::nullopt);
        if (required_string(profile, "profileStatus") != "active") {
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "deny", "target_inactive", "-", "-", "-");
            return cache_authorization_decision("profile_read_db", actor_user_id, target_user_id, false, "target_inactive").allowed;
        }
        const auto& settings = ensure_db_privacy_exists_and_load(target_user_id);
        const auto profile_visibility = required_string(settings, "profileVisibility");
        if (profile_visibility == "public") {
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "allow", "policy_resolved", "-", "true", profile_visibility);
            return cache_authorization_decision("profile_read_db", actor_user_id, target_user_id, true, "policy_resolved").allowed;
        }
        if (profile_visibility == "friends_only") {
            const auto allowed = summary.is_friend;
            log_privacy_resolution(actor_user_id, target_user_id, "profile_read", allowed ? "allow" : "deny", allowed ? "policy_resolved" : "profile_visibility_denied", "-", "true", profile_visibility);
            return cache_authorization_decision(
                "profile_read_db",
                actor_user_id,
                target_user_id,
                allowed,
                allowed ? "policy_resolved" : "profile_visibility_denied").allowed;
        }
        log_privacy_resolution(actor_user_id, target_user_id, "profile_read", "deny", "profile_visibility_denied", "-", "true", profile_visibility);
        return cache_authorization_decision("profile_read_db", actor_user_id, target_user_id, false, "profile_visibility_denied").allowed;
    }

    bool allows_friend_request(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) {
        if (const auto cached = cached_authorization_decision("friend_request_memory", actor_user_id, target_user_id)) {
            reason = cached->reason;
            return cached->allowed;
        }
        const auto summary = relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            reason = "blocked";
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", reason, bool_string(privacy_.count(actor_user_id) != 0), bool_string(privacy_.count(target_user_id) != 0), "-");
            return cache_authorization_decision("friend_request_memory", actor_user_id, target_user_id, false, reason).allowed;
        }
        const auto target_privacy_exists = privacy_.count(target_user_id) != 0;
        if (!target_privacy_exists) {
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "error", "target_privacy_missing", bool_string(privacy_.count(actor_user_id) != 0), "false", "-");
        }
        const auto& settings = require_privacy_const(target_user_id);
        if (settings.friend_request_policy == "everyone") {
            reason.clear();
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "allow", "policy_resolved", bool_string(privacy_.count(actor_user_id) != 0), "true", settings.friend_request_policy, "-");
            return cache_authorization_decision("friend_request_memory", actor_user_id, target_user_id, true, reason).allowed;
        }
        if (settings.friend_request_policy == "mutuals_only") {
            for (const auto& [key, relation] : relationships_) {
                if (relation.user_id == actor_user_id && relation.status == "accepted" && is_friend(target_user_id, relation.target_user_id)) {
                    reason.clear();
                    log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "allow", "policy_resolved", bool_string(privacy_.count(actor_user_id) != 0), "true", settings.friend_request_policy, "true");
                    return cache_authorization_decision("friend_request_memory", actor_user_id, target_user_id, true, reason).allowed;
                }
            }
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", "friend_request_policy_denied", bool_string(privacy_.count(actor_user_id) != 0), "true", settings.friend_request_policy, "false");
        }
        reason = "friend_request_policy_denied";
        if (settings.friend_request_policy != "mutuals_only") {
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", reason, bool_string(privacy_.count(actor_user_id) != 0), "true", settings.friend_request_policy, "-");
        }
        return cache_authorization_decision("friend_request_memory", actor_user_id, target_user_id, false, reason).allowed;
    }

    bool allows_friend_request_db(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) {
        if (const auto cached = cached_authorization_decision("friend_request_db", actor_user_id, target_user_id)) {
            reason = cached->reason;
            return cached->allowed;
        }
        const auto summary = db_relationship_summary(actor_user_id, target_user_id);
        ensure_db_privacy_exists_and_load(actor_user_id);
        const auto& settings = ensure_db_privacy_exists_and_load(target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            reason = "blocked";
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", reason, "true", "true", "-");
            return cache_authorization_decision("friend_request_db", actor_user_id, target_user_id, false, reason).allowed;
        }
        const auto policy = required_string(settings, "friendRequestPolicy");
        if (policy == "everyone") {
            reason.clear();
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "allow", "policy_resolved", "true", "true", policy, "-");
            return cache_authorization_decision("friend_request_db", actor_user_id, target_user_id, true, reason).allowed;
        }
        if (policy == "mutuals_only") {
            const bool allowed = db_.has_mutual_friend(actor_user_id, target_user_id);
            reason = allowed ? "" : "friend_request_policy_denied";
            log_privacy_resolution(actor_user_id, target_user_id, "friend_request", allowed ? "allow" : "deny", allowed ? "policy_resolved" : reason, "true", "true", policy, allowed ? "true" : "false");
            return cache_authorization_decision("friend_request_db", actor_user_id, target_user_id, allowed, reason).allowed;
        }
        reason = "friend_request_policy_denied";
        log_privacy_resolution(actor_user_id, target_user_id, "friend_request", "deny", reason, "true", "true", policy, "-");
        return cache_authorization_decision("friend_request_db", actor_user_id, target_user_id, false, reason).allowed;
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

        if (type == "call.history_recorded") {
            const std::string call_id = required_string(payload, "callId");
            const std::string initiator_user_id = canonical_user_id(required_string(payload, "initiatorUserId"));
            const std::vector<std::string> participant_user_ids = normalize_participant_user_ids(
                required_string_array(payload, "participantUserIds"),
                initiator_user_id);
            const std::string call_type = required_string(payload, "callType");
            const std::string call_status = required_string(payload, "callStatus");
            const std::string started_at = required_string(payload, "startedAt");
            const auto ended_at = optional_string(payload, "endedAt");
            const auto room_id = optional_string(payload, "roomId");
            const auto conversation_id = optional_string(payload, "conversationId");
            int duration_seconds = 0;
            const auto duration_it = payload.find("durationSec");
            if (duration_it != payload.end()) {
                if (!duration_it->second.is_number()) {
                    throw std::runtime_error("Expected number field: durationSec");
                }
                duration_seconds = std::max(0, static_cast<int>(duration_it->second.as_number()));
            }
            validate_call_type(call_type);
            validate_call_status(call_status);
            for (const auto& participant_user_id : participant_user_ids) {
                if (db_.enabled()) {
                    db_.ensure_profile_exists(participant_user_id, "User " + participant_user_id.substr(0, std::min<std::size_t>(8, participant_user_id.size())));
                } else {
                    ensure_memory_profile_exists(participant_user_id, std::nullopt);
                }
            }
            if (db_.enabled()) {
                db_.upsert_call_history(
                    call_id,
                    initiator_user_id,
                    participant_user_ids,
                    call_type,
                    call_status,
                    started_at,
                    ended_at,
                    duration_seconds,
                    room_id,
                    conversation_id);
            } else {
                upsert_memory_call_history(
                    call_id,
                    initiator_user_id,
                    participant_user_ids,
                    call_type,
                    call_status,
                    started_at,
                    ended_at,
                    duration_seconds,
                    room_id,
                    conversation_id);
                publish_event("user.call_history_recorded", JsonObject{{"callId", call_id}});
            }
            increment_metric("call_history.recorded");
            return json_response(200, JsonObject{
                {"status", "recorded"},
                {"callId", call_id},
                {"participantCount", static_cast<int>(participant_user_ids.size())},
            });
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

    Response internal_contract_error_response(const int status, const std::string& error) {
        increment_metric("http.error." + std::to_string(status));
        return json_response(status, JsonObject{{"error", error}});
    }

    JsonObject internal_profile_contract_json(const JsonObject& profile) const {
        return JsonObject{
            {"userId", required_string(profile, "userId")},
            {"displayName", required_string(profile, "displayName")},
            {"profileStatus", required_string(profile, "profileStatus")},
        };
    }

    JsonObject internal_profile_contract_json(const UserProfile& profile) const {
        return JsonObject{
            {"userId", profile.user_id},
            {"displayName", profile.display_name},
            {"profileStatus", profile.profile_status},
        };
    }

    Response internal_batch_profiles(const Request& request) {
        try {
            require_internal_token(request);
        } catch (const std::exception&) {
            return internal_contract_error_response(401, "UNAUTHORIZED");
        }

        std::vector<std::string> user_ids;
        try {
            const auto body = JsonParser(request.body).parse();
            const auto& object = require_object(body);
            const auto requested_user_ids = required_string_array(object, "userIds");
            if (requested_user_ids.size() > 100U) {
                return internal_contract_error_response(400, "VALIDATION_ERROR");
            }

            std::set<std::string> seen;
            for (const auto& raw_user_id : requested_user_ids) {
                if (!is_uuid_like(raw_user_id)) {
                    return internal_contract_error_response(400, "VALIDATION_ERROR");
                }
                const auto user_id = canonical_user_id(raw_user_id);
                if (seen.insert(user_id).second) {
                    user_ids.push_back(user_id);
                }
            }
        } catch (const std::exception&) {
            return internal_contract_error_response(400, "VALIDATION_ERROR");
        }

        try {
            JsonArray users;
            if (db_.enabled()) {
                for (const auto& user_id : user_ids) {
                    const auto profile = db_.get_profile(user_id);
                    if (profile.has_value()) {
                        users.emplace_back(internal_profile_contract_json(*profile));
                    }
                }
            } else {
                for (const auto& user_id : user_ids) {
                    const auto profile_it = profiles_.find(user_id);
                    if (profile_it != profiles_.end()) {
                        users.emplace_back(internal_profile_contract_json(profile_it->second));
                    }
                }
            }
            return json_response(200, JsonObject{{"users", users}});
        } catch (const std::exception&) {
            return internal_contract_error_response(503, "USER_SERVICE_UNAVAILABLE");
        }
    }

    Response internal_get_profile(const Request& request) {
        require_internal_token(request);
        const auto user_id = extract_user_id_from_internal_path(request.path, "/profile");
        if (db_.enabled()) {
            const auto profile = db_.get_profile(user_id);
            if (!profile.has_value()) {
                log_profile_resolution("404", current_actor_for_log(), user_id, "internal_profile", "deny", "not_found", "-", "-", "-", "-", "false", "false");
                return error_response(404, "not_found", "User profile not found");
            }
            const auto privacy = db_.get_privacy_settings(user_id);
            const auto resolved = profile_to_json(
                *profile,
                privacy.has_value() ? optional_string(*privacy, "profileVisibility") : std::nullopt,
                privacy.has_value() ? optional_string(*privacy, "avatarVisibility") : std::nullopt,
                true,
                current_actor_for_log(),
                "internal_profile",
                "internal_contract",
                false);
            return json_response(200, JsonObject{
                {"userId", resolved.at("userId")},
                {"displayName", resolved.at("displayName")},
                {"avatarObjectId", resolved.at("avatarObjectId")},
                {"profileStatus", resolved.at("profileStatus")},
            });
        }
        const auto& profile = require_profile_const(user_id);
        const auto& privacy = require_privacy_const(user_id);
        const auto resolved = profile_to_json(profile, privacy, true, current_actor_for_log(), "internal_profile", "internal_contract", false);
        return json_response(200, JsonObject{
            {"userId", resolved.at("userId")},
            {"displayName", resolved.at("displayName")},
            {"avatarObjectId", resolved.at("avatarObjectId")},
            {"profileStatus", resolved.at("profileStatus")},
        });
    }

    std::string extract_user_id_from_internal_path(const std::string& path, const std::string& suffix) const {
        const std::string prefix = "/internal/users/";
        if (!starts_with(path, prefix) || !ends_with(path, suffix)) {
            throw std::runtime_error("Unexpected path");
        }
        return canonical_user_id(path.substr(prefix.size(), path.size() - prefix.size() - suffix.size()));
    }

    std::string normalize_relationship_action(const JsonObject& object) const {
        if (const auto action = optional_string(object, "action")) {
            return *action;
        }
        if (const auto policy_type = optional_string(object, "policyType")) {
            if (*policy_type == "dm") {
                return "dm.start";
            }
            if (*policy_type == "friend_request") {
                return "friend.request.send";
            }
            if (*policy_type == "profile_read") {
                return "profile.read";
            }
            if (*policy_type == "friendship") {
                return "friendship.status";
            }
            if (*policy_type == "block") {
                return "block.status";
            }
            throw std::runtime_error("Unsupported policyType: " + *policy_type);
        }
        throw std::runtime_error("Missing string field: action");
    }

    Response internal_relationship_check(const Request& request) {
        require_internal_token(request);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        const std::string actor_user_id = canonical_user_id(required_string(object, "actorUserId"));
        const std::string target_user_id = canonical_user_id(required_string(object, "targetUserId"));
        const std::string action = normalize_relationship_action(object);

        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);

        const auto summary = db_.enabled()
            ? db_relationship_summary(actor_user_id, target_user_id)
            : relationship_summary(actor_user_id, target_user_id);

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
        } else if (action == "friendship.status") {
            allowed = summary.is_friend;
            if (!allowed) {
                reason = "not_friends";
            }
        } else if (action == "block.status") {
            allowed = !(summary.is_blocked || summary.is_blocked_by_target);
            if (!allowed) {
                reason = summary.is_blocked ? "blocked_by_actor" : "blocked_by_target";
            }
        } else {
            throw std::runtime_error("Unsupported action: " + action);
        }
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
            const auto& profile = ensure_db_profile_exists_and_load(actor_user_id, preferred_display_name);
            const auto& privacy = ensure_db_privacy_exists_and_load(actor_user_id);
            return json_response(200, profile_to_json(
                profile,
                optional_string(privacy, "profileVisibility"),
                optional_string(privacy, "avatarVisibility"),
                true,
                actor_user_id,
                "self_profile",
                "self_read",
                false));
        }
        const auto& profile = ensure_memory_profile_exists(actor_user_id, preferred_display_name);
        const auto& privacy = require_privacy_const(actor_user_id);
        increment_metric("profile.read.self");
        return json_response(200, profile_to_json(profile, privacy, true, actor_user_id, "self_profile", "self_read", false));
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
            const auto& profile = ensure_db_profile_exists_and_load(actor_user_id, preferred_display_name);
            const auto& privacy = ensure_db_privacy_exists_and_load(actor_user_id);
            return json_response(200, profile_to_json(
                profile,
                optional_string(privacy, "profileVisibility"),
                optional_string(privacy, "avatarVisibility"),
                true,
                actor_user_id,
                "self_profile",
                "profile_updated",
                false));
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
        return json_response(200, profile_to_json(profile, require_privacy_const(actor_user_id), true, actor_user_id, "self_profile", "profile_updated", false));
    }

    Response get_user_by_id(const Request& request, const std::string& user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(user_id);
        if (db_.enabled()) {
            if (!authorize_profile_read_db(actor_user_id, user_id)) {
                return error_response(403, "forbidden", "Profile visibility denied");
            }
            const auto summary = db_relationship_summary(actor_user_id, user_id);
            const auto& profile = ensure_db_profile_exists_and_load(user_id, std::nullopt);
            const auto& privacy = ensure_db_privacy_exists_and_load(user_id);
            increment_metric("profile.read.other");
            return json_response(200, profile_to_json(
                profile,
                optional_string(privacy, "profileVisibility"),
                optional_string(privacy, "avatarVisibility"),
                actor_user_id == user_id,
                actor_user_id,
                actor_user_id == user_id ? "self_profile" : "public_profile",
                "authorized_profile_read",
                summary.is_friend));
        }
        const auto& profile = require_profile_const(user_id);
        const auto& privacy = require_privacy_const(user_id);
        if (!authorize_profile_read(actor_user_id, user_id)) {
            return error_response(403, "forbidden", "Profile visibility denied");
        }
        const auto summary = relationship_summary(actor_user_id, user_id);
        increment_metric("profile.read.other");
        return json_response(200, profile_to_json(
            profile,
            privacy,
            actor_user_id == user_id,
            actor_user_id,
            actor_user_id == user_id ? "self_profile" : "public_profile",
            "authorized_profile_read",
            summary.is_friend));
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
        if (db_.enabled()) {
            db_.create_block(actor_user_id, target_user_id, reason);
        } else {
            const auto key = pair_key(actor_user_id, target_user_id);
            blocks_[key] = BlockRecord{actor_user_id, target_user_id, reason, now_iso8601()};
            set_relationship(actor_user_id, target_user_id, "removed");
            set_relationship(target_user_id, actor_user_id, "removed");
            hide_dm_projections_between(actor_user_id, target_user_id);
            publish_event("user.block_created", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
        }
        audit("block.create", actor_user_id, target_user_id);
        increment_metric("block.created");
        return json_response(201, JsonObject{{"status", "blocked"}, {"targetUserId", target_user_id}});
    }

    Response unblock_user(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        if (db_.enabled()) {
            db_.remove_block(actor_user_id, target_user_id);
        } else {
            blocks_.erase(pair_key(actor_user_id, target_user_id));
            publish_event("user.block_removed", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
        }
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

    Response pulse_my_presence(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        const std::string session_id = required_string(object, "sessionId");
        const auto device_id = optional_string(object, "deviceId");
        const std::string platform = optional_string(object, "platform").value_or("unknown");
        if (db_.enabled()) {
            db_.upsert_presence_session(actor_user_id, session_id, device_id, platform);
        } else {
            upsert_memory_presence_session(actor_user_id, session_id, device_id, platform);
        }
        increment_metric("presence.pulse");
        return json_response(200, JsonObject{
            {"status", "ok"},
            {"userId", actor_user_id},
            {"sessionId", session_id},
            {"presence", "green"},
            {"isOnline", true},
        });
    }

    Response disconnect_my_presence(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        const std::string session_id = required_string(object, "sessionId");
        const bool updated = db_.enabled()
            ? db_.disconnect_presence_session(actor_user_id, session_id)
            : disconnect_memory_presence_session(actor_user_id, session_id);
        if (!updated) {
            return error_response(404, "not_found", "Presence session not found");
        }
        increment_metric("presence.disconnect");
        return json_response(200, JsonObject{
            {"status", "ok"},
            {"userId", actor_user_id},
            {"sessionId", session_id},
            {"presence", "red"},
            {"isOnline", false},
        });
    }

    Response query_presence(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        auto requested_user_ids = required_string_array(object, "userIds");
        if (requested_user_ids.size() > 200U) {
            return error_response(400, "bad_request", "userIds supports at most 200 items");
        }
        std::vector<std::string> normalized_user_ids;
        std::set<std::string> seen;
        for (const auto& raw_user_id : requested_user_ids) {
            const auto user_id = canonical_user_id(raw_user_id);
            if (seen.insert(user_id).second) {
                normalized_user_ids.push_back(user_id);
            }
        }
        std::vector<PresenceSnapshot> snapshots = db_.enabled()
            ? db_.list_presence(normalized_user_ids, presence_green_ttl_seconds_)
            : list_memory_presence(normalized_user_ids);
        std::unordered_map<std::string, PresenceSnapshot> by_user_id;
        for (const auto& snapshot : snapshots) {
            by_user_id[snapshot.user_id] = snapshot;
        }
        JsonArray items;
        for (const auto& user_id : normalized_user_ids) {
            ensure_profile_exists(user_id);
            const auto it = by_user_id.find(user_id);
            if (it != by_user_id.end()) {
                items.emplace_back(presence_to_json(it->second));
            } else {
                items.emplace_back(presence_to_json(PresenceSnapshot{.user_id = user_id}));
            }
        }
        increment_metric("presence.query");
        return json_response(200, JsonObject{{"items", items}});
    }

    Response list_my_call_history(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const int limit = parse_int_query(request.query, "limit", 50);
        const int offset = parse_int_query(request.query, "offset", 0);
        JsonArray items;
        if (db_.enabled()) {
            for (const auto& item : db_.list_call_history(actor_user_id, limit, offset)) {
                items.emplace_back(call_history_to_json(item));
            }
        } else {
            for (const auto& item : list_memory_call_history(actor_user_id, limit, offset)) {
                items.emplace_back(call_history_to_json(item));
            }
        }
        increment_metric("call_history.list");
        return json_response(200, JsonObject{{"items", items}, {"limit", limit}, {"offset", offset}});
    }

    Response delete_my_call_history(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const auto segments = split_path(request.path);
        if (segments.size() != 5) {
            return error_response(404, "not_found", "Route not found");
        }
        const std::string history_id = segments[4];
        const bool deleted = db_.enabled()
            ? db_.delete_call_history(actor_user_id, history_id)
            : delete_memory_call_history(actor_user_id, history_id);
        if (!deleted) {
            return error_response(404, "not_found", "Call history item not found");
        }
        if (!db_.enabled()) {
            publish_event("user.call_history_deleted", JsonObject{{"historyId", history_id}, {"userId", actor_user_id}});
        }
        audit("call_history.delete", actor_user_id, actor_user_id);
        increment_metric("call_history.deleted");
        return json_response(200, JsonObject{{"status", "deleted"}, {"historyId", history_id}});
    }

    Response clear_my_call_history(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const int deleted_count = db_.enabled()
            ? db_.clear_call_history(actor_user_id)
            : clear_memory_call_history(actor_user_id);
        if (!db_.enabled() && deleted_count > 0) {
            publish_event("user.call_history_cleared", JsonObject{{"userId", actor_user_id}, {"deletedCount", deleted_count}});
        }
        audit("call_history.clear", actor_user_id, actor_user_id);
        increment_metric("call_history.cleared");
        return json_response(200, JsonObject{{"status", "cleared"}, {"deletedCount", deleted_count}});
    }

    Response create_my_reminder(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ReminderRecord reminder;
        try {
            const auto body = JsonParser(request.body).parse();
            const auto& object = require_object(body);
            reminder = build_reminder_from_create_request(object, actor_user_id, now_epoch_ms());
        } catch (const HttpError& ex) {
            return reminder_error_response(ex.status, ex.code);
        } catch (const JsonParseError&) {
            return invalid_json_response();
        } catch (const std::runtime_error& ex) {
            return reminder_error_response(400, ex.what());
        }
        try {
            const auto stored = db_.enabled()
                ? db_.create_reminder(reminder)
                : std::optional<ReminderRecord>(create_memory_reminder(reminder));
            if (!stored.has_value()) {
                return reminder_error_response(500, "db");
            }
            audit("reminder.create", actor_user_id, actor_user_id, JsonObject{{"reminderId", stored->reminder_id}});
            increment_metric("reminder.created");
            return json_response(200, JsonObject{
                {"ok", true},
                {"reminder", JsonObject{
                    {"reminderId", stored->reminder_id},
                    {"state", stored->state},
                    {"remindAtMs", json_int64(stored->remind_at_ms)},
                }},
            });
        } catch (const std::exception&) {
            return reminder_error_response(500, "db");
        }
    }

    Response list_my_reminders(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        std::optional<std::string> state;
        int limit = 50;
        int offset = 0;
        std::optional<long long> from_ms;
        std::optional<long long> to_ms;
        try {
            state = request.query.count("state") != 0 && !trim(request.query.at("state")).empty()
                ? std::optional<std::string>(trim(request.query.at("state")))
                : std::nullopt;
            if (state.has_value()) {
                validate_reminder_state(*state);
            }
            limit = parse_int_query(request.query, "limit", 50);
            offset = parse_int_query(request.query, "offset", 0);
            from_ms = parse_optional_long_long_query(request, "fromMs");
            to_ms = parse_optional_long_long_query(request, "toMs");
        } catch (const std::runtime_error& ex) {
            return reminder_error_response(400, ex.what());
        }
        JsonArray items;
        try {
            const auto reminders = db_.enabled()
                ? db_.list_reminders(actor_user_id, state, limit, offset, from_ms, to_ms)
                : list_memory_reminders(actor_user_id, state, limit, offset, from_ms, to_ms);
            for (const auto& reminder : reminders) {
                items.emplace_back(reminder_to_json(reminder));
            }
        } catch (const std::exception&) {
            return reminder_error_response(500, "db");
        }
        increment_metric("reminder.list");
        return json_response(200, JsonObject{{"items", items}});
    }

    Response patch_my_reminder(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const std::string reminder_id = extract_reminder_id_from_path(request.path);
        std::optional<ReminderRecord> existing;
        try {
            existing = db_.enabled()
                ? db_.get_reminder(actor_user_id, reminder_id)
                : get_memory_reminder(actor_user_id, reminder_id);
        } catch (const std::exception&) {
            return reminder_error_response(500, "db");
        }
        if (!existing.has_value()) {
            return reminder_error_response(404, "reminder_not_found");
        }
        ReminderRecord updated = *existing;
        try {
            const auto body = JsonParser(request.body).parse();
            const auto& object = require_object(body);
            const auto state = optional_string(object, "state");
            const auto remind_at_ms = optional_int64(object, "remindAtMs");
            const auto note = optional_string(object, "note");
            const auto now_ms = now_epoch_ms();

            if (note.has_value()) {
                if (note->size() > 1000U) {
                    return reminder_error_response(400, "note too long");
                }
                updated.note = *note;
            }

            const std::string next_state = state.value_or(existing->state);
            validate_reminder_state(next_state);
            if (!can_transition_reminder_state(existing->state, next_state, remind_at_ms.has_value())) {
                return reminder_error_response(400, "invalid_state_transition");
            }
            if (remind_at_ms.has_value() && next_state != "scheduled") {
                return reminder_error_response(400, "invalid_state_transition");
            }
            if (remind_at_ms.has_value()) {
                if (*remind_at_ms <= now_ms - 5000) {
                    return reminder_error_response(400, "remindAtMs must be in the future");
                }
                updated.remind_at_ms = *remind_at_ms;
            }

            updated.state = next_state;
            updated.updated_at_ms = now_ms;
            if (next_state == "dismissed") {
                updated.dismissed_at_ms = now_ms;
            }
            if (next_state == "scheduled") {
                if (!remind_at_ms.has_value() && existing->state != "scheduled") {
                    return reminder_error_response(400, "invalid_state_transition");
                }
                updated.fired_at_ms = existing->fired_at_ms;
                updated.dismissed_at_ms = std::nullopt;
            }
            if (next_state == "cancelled") {
                updated.dismissed_at_ms = existing->dismissed_at_ms;
            }
        } catch (const JsonParseError&) {
            return invalid_json_response();
        } catch (const std::runtime_error& ex) {
            return reminder_error_response(400, ex.what());
        }
        try {
            auto persisted = db_.enabled()
                ? db_.update_reminder(updated)
                : update_memory_reminder(updated);
            if (!persisted.has_value()) {
                return reminder_error_response(404, "reminder_not_found");
            }
            audit("reminder.update", actor_user_id, actor_user_id, JsonObject{{"reminderId", persisted->reminder_id}, {"state", persisted->state}});
            increment_metric("reminder.updated");
            return json_response(200, JsonObject{
                {"ok", true},
                {"reminderId", persisted->reminder_id},
                {"state", persisted->state},
                {"updatedAtMs", json_int64(persisted->updated_at_ms)},
            });
        } catch (const std::exception&) {
            return reminder_error_response(500, "db");
        }
    }

    Response delete_my_reminder(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const std::string reminder_id = extract_reminder_id_from_path(request.path);
        try {
            const bool deleted = db_.enabled()
                ? db_.delete_reminder(actor_user_id, reminder_id)
                : delete_memory_reminder(actor_user_id, reminder_id);
            if (!deleted) {
                return reminder_error_response(404, "reminder_not_found");
            }
            audit("reminder.delete", actor_user_id, actor_user_id, JsonObject{{"reminderId", reminder_id}});
            increment_metric("reminder.deleted");
            return json_response(200, JsonObject{{"ok", true}, {"reminderId", reminder_id}});
        } catch (const std::exception&) {
            return reminder_error_response(500, "db");
        }
    }

    Response list_relationship_collection(const Request& request, const std::string& status, const std::string& metric_name) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        const int limit = parse_int_query(request.query, "limit", 50);
        const int offset = parse_int_query(request.query, "offset", 0);

        JsonArray items;
        if (db_.enabled()) {
            for (const auto& item : db_.list_relationships(actor_user_id, status, limit, offset)) {
                items.emplace_back(JsonObject{
                    {"userId", item.user_id},
                    {"displayName", item.display_name},
                    {"username", item.username.has_value() ? Json(*item.username) : Json(nullptr)},
                    {"profileStatus", item.profile_status},
                    {"relationStatus", item.relation_status},
                    {"createdAt", item.created_at},
                    {"updatedAt", item.updated_at},
                });
            }
        } else {
            int seen = 0;
            for (const auto& [key, relation] : relationships_) {
                if (relation.user_id != actor_user_id || relation.status != status) {
                    continue;
                }
                if (!has_expected_counterpart_relationship(actor_user_id, relation.target_user_id, relation.status)) {
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
                    {"createdAt", relation.created_at},
                    {"updatedAt", relation.updated_at},
                });
            }
        }
        increment_metric(metric_name);
        return json_response(200, JsonObject{{"items", items}, {"limit", limit}, {"offset", offset}});
    }

    void run_background_jobs_locked() {
        const auto now_ms = now_epoch_ms();
        if (now_ms < next_reminder_scan_at_ms_) {
            return;
        }
        next_reminder_scan_at_ms_ = now_ms + (std::max(reminder_scan_interval_seconds_, 1) * 1000LL);
        std::vector<ReminderRecord> fired;
        try {
            fired = db_.enabled()
                ? db_.fire_due_reminders(now_ms, 200)
                : fire_due_memory_reminders(now_ms, 200);
        } catch (const std::exception&) {
            return;
        }
        for (const auto& reminder : fired) {
            increment_metric("reminder.fired");
            if (!db_.enabled()) {
                publish_event("reminder_fired", JsonObject{
                    {"type", "reminder_fired"},
                    {"reminder", JsonObject{
                        {"reminderId", reminder.reminder_id},
                        {"sourceType", reminder.source_type},
                        {"messageId", reminder.message_id},
                        {"conversationId", reminder.conversation_id},
                        {"messagePreviewText", reminder.message_preview_text},
                        {"messageAuthorDisplayName", reminder.message_author_display_name},
                        {"remindAtMs", json_int64(reminder.remind_at_ms)},
                        {"firedAtMs", reminder.fired_at_ms.has_value() ? json_int64(*reminder.fired_at_ms) : Json(nullptr)},
                    }},
                });
            }
        }
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
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 409: return "Conflict";
        case 500: return "Internal Server Error";
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

struct ListenAddress {
    std::string host = "0.0.0.0";
    unsigned short port = 8080;
};

ListenAddress parse_http_addr(const std::string& raw_addr) {
    const std::string value = trim(raw_addr);
    const auto colon = value.rfind(':');
    if (colon == std::string::npos || colon + 1 >= value.size()) {
        throw std::runtime_error("HTTP_ADDR must be host:port or :port");
    }
    ListenAddress address;
    address.host = value.substr(0, colon);
    address.port = static_cast<unsigned short>(std::stoi(value.substr(colon + 1)));
    if (address.host.empty()) {
        address.host = "0.0.0.0";
    }
    return address;
}

ListenAddress listen_address_from_port(const unsigned short port) {
    ListenAddress address;
    address.port = port;
    return address;
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

bool assign_ipv4_address(const std::string& host, sockaddr_in& address) {
    if (host == "0.0.0.0" || host == "*" || host.empty()) {
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        return true;
    }
    const std::string normalized_host = host == "localhost" ? "127.0.0.1" : host;
    return inet_pton(AF_INET, normalized_host.c_str(), &address.sin_addr) == 1;
}

int run_server(const ListenAddress& listen_address) {
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
    if (!assign_ipv4_address(listen_address.host, address)) {
        std::cerr << "Invalid HTTP_ADDR host: " << listen_address.host << "\n";
        closesocket(server_socket);
#ifdef _WIN32
        WSACleanup();
#endif
        return 1;
    }
    address.sin_port = htons(listen_address.port);

    if (bind(server_socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR) {
        std::cerr << "bind() failed for " << listen_address.host << ":" << listen_address.port << "\n";
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

    std::cout << "user-service listening on " << listen_address.host << ":" << listen_address.port << std::endl;
    ServiceState state;

    while (true) {
        fd_set read_set;
        FD_ZERO(&read_set);
        FD_SET(server_socket, &read_set);
        timeval timeout{};
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        const int select_result = select(
#ifdef _WIN32
            0,
#else
            server_socket + 1,
#endif
            &read_set,
            nullptr,
            nullptr,
            &timeout);
        if (select_result == 0) {
            state.run_background_jobs();
            continue;
        }
        if (select_result == SOCKET_ERROR) {
            continue;
        }
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
    ListenAddress listen_address;
    if (const auto http_addr = get_env("HTTP_ADDR")) {
        listen_address = parse_http_addr(*http_addr);
    } else if (const auto env_port = get_env("USER_SERVICE_PORT")) {
        listen_address = listen_address_from_port(static_cast<unsigned short>(std::stoi(*env_port)));
    } else if (argc > 1) {
        listen_address = listen_address_from_port(static_cast<unsigned short>(std::stoi(argv[1])));
    }
    return run_server(listen_address);
}
