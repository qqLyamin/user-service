#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

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

class ServiceState {
public:
    Response handle(const Request& request) {
        std::lock_guard<std::mutex> guard(mutex_);
        try {
            return route(request);
        } catch (const std::exception& ex) {
            return error_response(400, "bad_request", ex.what());
        }
    }

private:
    std::mutex mutex_;
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

    Response route(const Request& request) {
        if (request.method == "GET" && request.path == "/healthz") {
            return json_response(200, JsonObject{{"status", "ok"}});
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
        if (segments.size() == 3 && request.method == "GET") {
            return get_user_by_id(request, segments[2]);
        }
        if (segments.size() == 4 && segments[3] == "friend-request" && request.method == "POST") {
            return send_friend_request(request, segments[2]);
        }
        if (segments.size() == 5 && segments[3] == "friend-request" && segments[4] == "accept" && request.method == "POST") {
            return accept_friend_request(request, segments[2]);
        }
        if (segments.size() == 5 && segments[3] == "friend-request" && segments[4] == "decline" && request.method == "POST") {
            return decline_friend_request(request, segments[2]);
        }
        if (segments.size() == 4 && segments[3] == "friend" && request.method == "DELETE") {
            return remove_friend(request, segments[2]);
        }
        if (segments.size() == 4 && segments[3] == "block" && request.method == "POST") {
            return block_user(request, segments[2]);
        }
        if (segments.size() == 4 && segments[3] == "block" && request.method == "DELETE") {
            return unblock_user(request, segments[2]);
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

    std::string require_actor_user_id(const Request& request) const {
        const auto it = request.headers.find("authorization");
        if (it == request.headers.end()) {
            throw std::runtime_error("Missing Authorization header");
        }
        const std::string raw_header = trim(it->second);
        const std::string prefix = "bearer user:";
        const std::string header = to_lower(raw_header);
        if (header.rfind(prefix, 0) != 0) {
            throw std::runtime_error("Expected Authorization: Bearer user:<user-id>");
        }
        return trim(raw_header.substr(prefix.size()));
    }

    void require_internal_token(const Request& request) const {
        const auto it = request.headers.find("x-internal-token");
        if (it == request.headers.end() || trim(it->second) != "internal-secret") {
            throw std::runtime_error("Missing or invalid x-internal-token");
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

    bool allows_friend_request(const std::string& actor_user_id, const std::string& target_user_id, std::string& reason) const {
        const auto summary = relationship_summary(actor_user_id, target_user_id);
        if (summary.is_blocked || summary.is_blocked_by_target) {
            reason = "blocked";
            return false;
        }
        const auto& settings = require_privacy_const(target_user_id);
        if (settings.friend_request_policy == "everyone") {
            reason.clear();
            return true;
        }
        if (settings.friend_request_policy == "mutuals_only") {
            for (const auto& [key, relation] : relationships_) {
                if (relation.user_id == actor_user_id && relation.status == "accepted" && is_friend(target_user_id, relation.target_user_id)) {
                    reason.clear();
                    return true;
                }
            }
        }
        reason = "friend_request_policy_denied";
        return false;
    }

    void ensure_profile_exists(const std::string& user_id) const {
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
            const std::string user_id = required_string(payload, "userId");
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
            const std::string user_id = required_string(payload, "userId");
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
        return path.substr(prefix.size(), path.size() - prefix.size() - suffix.size());
    }

    Response internal_relationship_check(const Request& request) {
        require_internal_token(request);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);
        const std::string actor_user_id = required_string(object, "actorUserId");
        const std::string target_user_id = required_string(object, "targetUserId");
        const std::string action = required_string(object, "action");

        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);

        bool allowed = false;
        std::string reason;
        if (action == "dm.start") {
            allowed = authorize_dm_start(actor_user_id, target_user_id, reason);
        } else if (action == "profile.read") {
            allowed = authorize_profile_read(actor_user_id, target_user_id);
            if (!allowed) {
                reason = "profile_visibility_denied";
            }
        } else if (action == "friend.request.send") {
            allowed = allows_friend_request(actor_user_id, target_user_id, reason);
        } else {
            throw std::runtime_error("Unsupported action: " + action);
        }

        const auto summary = relationship_summary(actor_user_id, target_user_id);
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
        const std::string actor_user_id = required_string(object, "actorUserId");
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(user_id);
        return json_response(200, JsonObject{{"allowed", authorize_profile_read(actor_user_id, user_id)}});
    }

    Response get_me(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        const auto& profile = require_profile_const(actor_user_id);
        increment_metric("profile.read.self");
        return json_response(200, profile_to_json(profile, true));
    }

    Response patch_me(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        auto& profile = require_profile(actor_user_id);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);

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
        const auto& profile = require_profile_const(user_id);
        if (!authorize_profile_read(actor_user_id, user_id)) {
            return error_response(403, "forbidden", "Profile visibility denied");
        }
        increment_metric("profile.read.other");
        return json_response(200, profile_to_json(profile, actor_user_id == user_id));
    }

    Response get_my_privacy(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        const auto& settings = require_privacy_const(actor_user_id);
        increment_metric("privacy.read");
        return json_response(200, privacy_to_json(settings));
    }

    Response patch_my_privacy(const Request& request) {
        const auto actor_user_id = require_actor_user_id(request);
        auto& settings = require_privacy(actor_user_id);
        const auto body = JsonParser(request.body).parse();
        const auto& object = require_object(body);

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
        if (actor_user_id == target_user_id) {
            return error_response(400, "bad_request", "Cannot friend yourself");
        }
        std::string reason;
        if (!allows_friend_request(actor_user_id, target_user_id, reason)) {
            return error_response(403, "forbidden", reason);
        }
        if (is_friend(actor_user_id, target_user_id)) {
            return error_response(409, "conflict", "Users are already friends");
        }
        set_relationship(actor_user_id, target_user_id, "pending_outgoing");
        set_relationship(target_user_id, actor_user_id, "pending_incoming");
        publish_event("user.friend_request_created", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
        audit("friend_request.create", actor_user_id, target_user_id);
        increment_metric("friend_request.created");
        return json_response(201, JsonObject{{"status", "pending"}, {"targetUserId", target_user_id}});
    }

    Response accept_friend_request(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
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
        audit("friend_request.accept", actor_user_id, target_user_id);
        increment_metric("friend_request.accepted");
        return json_response(200, JsonObject{{"status", "accepted"}, {"targetUserId", target_user_id}});
    }

    Response decline_friend_request(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        const auto incoming_key = pair_key(actor_user_id, target_user_id);
        const auto outgoing_key = pair_key(target_user_id, actor_user_id);
        if (!relationships_.count(incoming_key) || !relationships_.count(outgoing_key) ||
            relationships_[incoming_key].status != "pending_incoming" ||
            relationships_[outgoing_key].status != "pending_outgoing") {
            return error_response(409, "conflict", "No pending friend request to decline");
        }
        set_relationship(actor_user_id, target_user_id, "declined");
        set_relationship(target_user_id, actor_user_id, "declined");
        audit("friend_request.decline", actor_user_id, target_user_id);
        increment_metric("friend_request.declined");
        return json_response(200, JsonObject{{"status", "declined"}, {"targetUserId", target_user_id}});
    }

    Response remove_friend(const Request& request, const std::string& target_user_id) {
        const auto actor_user_id = require_actor_user_id(request);
        ensure_profile_exists(actor_user_id);
        ensure_profile_exists(target_user_id);
        if (!is_friend(actor_user_id, target_user_id)) {
            return error_response(409, "conflict", "Users are not friends");
        }
        set_relationship(actor_user_id, target_user_id, "removed");
        set_relationship(target_user_id, actor_user_id, "removed");
        publish_event("user.friend_removed", JsonObject{{"userId", actor_user_id}, {"targetUserId", target_user_id}});
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
    WSADATA wsa_data{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        std::cerr << "WSAStartup failed\n";
        return 1;
    }

    SOCKET server_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_socket == INVALID_SOCKET) {
        std::cerr << "socket() failed\n";
        WSACleanup();
        return 1;
    }

    BOOL opt = TRUE;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);

    if (bind(server_socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR) {
        std::cerr << "bind() failed\n";
        closesocket(server_socket);
        WSACleanup();
        return 1;
    }
    if (listen(server_socket, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "listen() failed\n";
        closesocket(server_socket);
        WSACleanup();
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
    unsigned short port = 8080;
    char* env_port = nullptr;
    std::size_t env_port_size = 0;
    if (_dupenv_s(&env_port, &env_port_size, "USER_SERVICE_PORT") == 0 && env_port != nullptr) {
        port = static_cast<unsigned short>(std::stoi(env_port));
        std::free(env_port);
    } else if (argc > 1) {
        port = static_cast<unsigned short>(std::stoi(argv[1]));
    }
    return run_server(port);
}
