import base64
import base64
import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = ROOT / "build"
EXE = BUILD_DIR / "user-service.exe"
if not EXE.exists():
    EXE = BUILD_DIR / "user-service"
BASE_URL = "http://127.0.0.1:18080"
INTERNAL_JWT_SECRET = "test-secret"
INTERNAL_JWT_ISSUER = "auth-service"
INTERNAL_JWT_AUDIENCE = "internal"


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def make_internal_token(secret: str, subject: str = "signaling") -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": INTERNAL_JWT_ISSUER,
        "sub": subject,
        "aud": [INTERNAL_JWT_AUDIENCE],
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
    }
    signing_input = f"{b64url(json.dumps(header, separators=(',', ':')).encode())}.{b64url(json.dumps(payload, separators=(',', ':')).encode())}"
    signature = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{b64url(signature)}"


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"service did not start on {host}:{port}")


def request(method: str, path: str, body=None, user_id=None, internal=False, internal_token=None, expected_status=200):
    headers = {"Content-Type": "application/json"}
    if user_id is not None:
        headers["Authorization"] = f"Bearer user:{user_id}"
    if internal:
        headers["Authorization"] = f"Bearer {internal_token or make_internal_token(INTERNAL_JWT_SECRET)}"
    payload = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(f"{BASE_URL}{path}", data=payload, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw = resp.read().decode("utf-8")
            assert resp.status == expected_status, (resp.status, raw)
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        if exc.code != expected_status:
            raise AssertionError(f"{method} {path} expected {expected_status}, got {exc.code}: {raw}") from exc
        return json.loads(raw)


def post_event(event_type: str, event_id: str, payload: dict, expected_status=200):
    return request(
        "POST",
        "/internal/events",
        {"type": event_type, "eventId": event_id, "payload": payload},
        internal=True,
        expected_status=expected_status,
    )


def main() -> int:
    if not EXE.exists():
        raise RuntimeError(f"binary not found: {EXE}")

    env = os.environ.copy()
    env["USER_SERVICE_PORT"] = "18080"
    env["JWT_SECRET"] = INTERNAL_JWT_SECRET
    env["JWT_ISSUER"] = INTERNAL_JWT_ISSUER
    env["JWT_AUDIENCE"] = INTERNAL_JWT_AUDIENCE
    env["PRESENCE_GREEN_TTL_SECONDS"] = "1"
    proc = subprocess.Popen([str(EXE)], cwd=str(ROOT), env=env)
    try:
        wait_for_port("127.0.0.1", 18080)

        invalid_internal_auth = request(
            "GET",
            "/internal/metrics",
            internal=True,
            internal_token=make_internal_token("wrong-secret"),
            expected_status=401,
        )
        assert invalid_internal_auth["error"] == "unauthorized"

        alice = "11111111-1111-1111-1111-111111111111"
        bob = "22222222-2222-2222-2222-222222222222"
        charlie = "33333333-3333-3333-3333-333333333333"

        post_event("auth.user_registered", "evt-1", {
            "userId": alice,
            "displayName": "Alice",
            "username": "alice",
            "deviceId": "dev-1",
            "sessionId": "sess-1",
            "platform": "windows",
        }, expected_status=201)
        duplicate = post_event("auth.user_registered", "evt-1", {
            "userId": alice,
            "displayName": "Alice",
        })
        assert duplicate["status"] == "ignored_duplicate"

        post_event("auth.user_registered", "evt-2", {"userId": bob, "displayName": "Bob", "username": "bob"}, expected_status=201)
        post_event("auth.user_registered", "evt-3", {"userId": charlie, "displayName": "Charlie", "username": "charlie"}, expected_status=201)

        me = request("GET", "/v1/users/me", user_id=alice)
        assert me["displayName"] == "Alice"
        assert me["username"] == "alice"

        red_presence = request("POST", "/v1/users/presence/query", {"userIds": [alice, bob]}, user_id=alice)
        assert red_presence["items"][0]["presence"] == "red"
        assert red_presence["items"][1]["presence"] == "red"

        pulse = request("POST", "/v1/users/me/presence/pulse", {
            "sessionId": "alice-desktop",
            "deviceId": "alice-device",
            "platform": "windows",
        }, user_id=alice)
        assert pulse["presence"] == "green"

        green_presence = request("POST", "/v1/users/presence/query", {"userIds": [alice]}, user_id=bob)
        assert green_presence["items"][0]["presence"] == "green"
        assert green_presence["items"][0]["isOnline"] is True

        time.sleep(2.2)
        yellow_presence = request("POST", "/v1/users/presence/query", {"userIds": [alice]}, user_id=bob)
        assert yellow_presence["items"][0]["presence"] == "yellow"
        assert yellow_presence["items"][0]["isOnline"] is False

        disconnect_presence = request("POST", "/v1/users/me/presence/disconnect", {
            "sessionId": "alice-desktop",
        }, user_id=alice)
        assert disconnect_presence["presence"] == "red"

        red_after_disconnect = request("POST", "/v1/users/presence/query", {"userIds": [alice]}, user_id=bob)
        assert red_after_disconnect["items"][0]["presence"] == "red"

        patched = request("PATCH", "/v1/users/me", {
            "displayName": "Alice Cooper",
            "avatarObjectId": "obj_avatar_alice",
            "bio": "MVP owner",
            "locale": "en-US",
            "timeZone": "Europe/Warsaw",
        }, user_id=alice)
        assert patched["displayName"] == "Alice Cooper"
        assert patched["avatarObjectId"] == "obj_avatar_alice"

        alice_privacy = request("PATCH", "/v1/users/me/privacy", {
            "avatarVisibility": "public",
        }, user_id=alice)
        assert alice_privacy["avatarVisibility"] == "public"

        alice_internal_profile = request("GET", f"/internal/users/{alice}/profile", internal=True)
        assert alice_internal_profile["avatarObjectId"] == "obj_avatar_alice"

        privacy = request("PATCH", "/v1/users/me/privacy", {
            "profileVisibility": "friends_only",
            "dmPolicy": "friends_only",
            "friendRequestPolicy": "everyone",
            "lastSeenVisibility": "private",
            "avatarVisibility": "friends_only",
        }, user_id=bob)
        assert privacy["dmPolicy"] == "friends_only"

        denied_profile = request("POST", f"/internal/users/{bob}/authorize-profile-read", {"actorUserId": alice}, internal=True)
        assert denied_profile["allowed"] is False

        denied_dm = request("POST", "/internal/users/relationships/check", {
            "actorUserId": alice,
            "targetUserId": bob,
            "action": "dm.start",
        }, internal=True)
        assert denied_dm["allowed"] is False
        assert denied_dm["reason"] == "dm_policy_denied"

        pending = request("POST", f"/v1/users/{bob}/friend-request", user_id=alice, expected_status=201)
        assert pending["status"] == "pending"

        accepted = request("POST", f"/v1/users/{alice}/friend-request/accept", user_id=bob)
        assert accepted["status"] == "accepted"

        allowed_profile = request("POST", f"/internal/users/{bob}/authorize-profile-read", {"actorUserId": alice}, internal=True)
        assert allowed_profile["allowed"] is True

        bob_view = request("GET", f"/v1/users/{bob}", user_id=alice)
        assert bob_view["displayName"] == "Bob"
        assert bob_view["avatarObjectId"] is None

        allowed_dm = request("POST", "/internal/users/relationships/check", {
            "actorUserId": alice,
            "targetUserId": bob,
            "action": "dm.start",
        }, internal=True)
        assert allowed_dm["allowed"] is True
        assert allowed_dm["relationship"]["isFriend"] is True

        contacts = request("GET", "/v1/users/me/contacts?limit=10&offset=0", user_id=alice)
        assert len(contacts["items"]) == 1
        assert contacts["items"][0]["userId"] == bob
        friends = request("GET", "/v1/users/me/friends?limit=10&offset=0", user_id=alice)
        assert len(friends["items"]) == 1
        assert friends["items"][0]["userId"] == bob
        reciprocal_friends = request("GET", "/v1/users/me/friends?limit=10&offset=0", user_id=bob)
        assert len(reciprocal_friends["items"]) == 1
        assert reciprocal_friends["items"][0]["userId"] == alice
        cleared_outgoing = request("GET", "/v1/users/me/friend-requests/outgoing?limit=10&offset=0", user_id=alice)
        assert cleared_outgoing["items"] == []
        cleared_incoming = request("GET", "/v1/users/me/friend-requests/incoming?limit=10&offset=0", user_id=bob)
        assert cleared_incoming["items"] == []

        declined = request("POST", f"/v1/users/{charlie}/friend-request", user_id=bob, expected_status=201)
        assert declined["status"] == "pending"
        outgoing = request("GET", "/v1/users/me/friend-requests/outgoing?limit=10&offset=0", user_id=bob)
        assert len(outgoing["items"]) == 1
        assert outgoing["items"][0]["userId"] == charlie
        incoming = request("GET", "/v1/users/me/friend-requests/incoming?limit=10&offset=0", user_id=charlie)
        assert len(incoming["items"]) == 1
        assert incoming["items"][0]["userId"] == bob
        decline_result = request("POST", f"/v1/users/{bob}/friend-request/decline", user_id=charlie)
        assert decline_result["status"] == "declined"

        post_event("room.member_added", "evt-4", {
            "userId": alice,
            "entityType": "room",
            "entityId": "room-1",
            "relationRole": "owner",
        })
        post_event("conversation.member_added", "evt-5", {
            "userId": alice,
            "entityType": "conversation",
            "entityId": "conv-1",
            "relationRole": "member",
            "counterpartUserId": bob,
        })
        post_event("conversation.member_added", "evt-6", {
            "userId": bob,
            "entityType": "conversation",
            "entityId": "conv-1",
            "relationRole": "member",
            "counterpartUserId": alice,
        })

        rooms = request("GET", "/v1/users/me/rooms?limit=10&offset=0", user_id=alice)
        assert len(rooms["items"]) == 1
        assert rooms["items"][0]["entityId"] == "room-1"

        conversations = request("GET", "/v1/users/me/conversations?limit=10&offset=0", user_id=alice)
        assert len(conversations["items"]) == 1
        assert conversations["items"][0]["entityId"] == "conv-1"

        post_event("call.history_recorded", "evt-6a", {
            "callId": "call-1",
            "initiatorUserId": alice,
            "participantUserIds": [alice, bob, charlie],
            "callType": "video",
            "callStatus": "completed",
            "startedAt": "2026-03-30T10:00:00.000Z",
            "endedAt": "2026-03-30T10:25:00.000Z",
            "durationSec": 1500,
            "conversationId": "conv-1",
        })

        alice_history = request("GET", "/v1/users/me/call-history?limit=10&offset=0", user_id=alice)
        assert len(alice_history["items"]) == 1
        assert alice_history["items"][0]["callId"] == "call-1"
        assert alice_history["items"][0]["callType"] == "video"
        assert alice_history["items"][0]["participantCount"] == 3
        assert {participant["userId"] for participant in alice_history["items"][0]["participants"]} == {alice, bob, charlie}

        bob_history = request("GET", "/v1/users/me/call-history?limit=10&offset=0", user_id=bob)
        assert len(bob_history["items"]) == 1
        assert bob_history["items"][0]["direction"] == "incoming"

        delete_result = request("DELETE", f"/v1/users/me/call-history/{alice_history['items'][0]['historyId']}", user_id=alice)
        assert delete_result["status"] == "deleted"
        alice_history_after_delete = request("GET", "/v1/users/me/call-history?limit=10&offset=0", user_id=alice)
        assert alice_history_after_delete["items"] == []
        bob_history_after_delete = request("GET", "/v1/users/me/call-history?limit=10&offset=0", user_id=bob)
        assert len(bob_history_after_delete["items"]) == 1
        clear_result = request("DELETE", "/v1/users/me/call-history", user_id=bob)
        assert clear_result["status"] == "cleared"
        assert clear_result["deletedCount"] == 1
        bob_history_after_clear = request("GET", "/v1/users/me/call-history?limit=10&offset=0", user_id=bob)
        assert bob_history_after_clear["items"] == []

        blocked = request("POST", f"/v1/users/{bob}/block", {"reason": "abuse"}, user_id=alice, expected_status=201)
        assert blocked["status"] == "blocked"

        blocked_profile = request("POST", f"/internal/users/{bob}/authorize-profile-read", {"actorUserId": alice}, internal=True)
        assert blocked_profile["allowed"] is False
        blocked_dm = request("POST", "/internal/users/relationships/check", {
            "actorUserId": alice,
            "targetUserId": bob,
            "action": "dm.start",
        }, internal=True)
        assert blocked_dm["allowed"] is False
        assert blocked_dm["relationship"]["isBlocked"] is True

        hidden_conversations = request("GET", "/v1/users/me/conversations?limit=10&offset=0", user_id=alice)
        assert hidden_conversations["items"] == []

        request("DELETE", f"/v1/users/{bob}/block", user_id=alice)
        removed = request("DELETE", f"/v1/users/{bob}/friend", user_id=alice, expected_status=409)
        assert removed["error"] == "conflict"

        post_event("auth.user_disabled", "evt-7", {"userId": charlie})
        snapshot = request("GET", f"/internal/users/{charlie}/profile", internal=True)
        assert snapshot["profileStatus"] == "disabled"

        post_event("auth.user_enabled", "evt-8", {"userId": charlie})
        snapshot = request("GET", f"/internal/users/{charlie}/profile", internal=True)
        assert snapshot["profileStatus"] == "active"

        post_event("auth.user_deleted", "evt-9", {"userId": charlie})
        snapshot = request("GET", f"/internal/users/{charlie}/profile", internal=True)
        assert snapshot["profileStatus"] == "deleted"

        outbox = request("GET", "/internal/outbox", internal=True)
        event_types = {event["eventType"] for event in outbox["events"]}
        for required in {
            "user.profile_created",
            "user.profile_updated",
            "user.avatar_updated",
            "user.friend_request_created",
            "user.friend_request_accepted",
            "user.block_created",
            "user.block_removed",
            "user.call_history_recorded",
            "user.call_history_deleted",
            "user.call_history_cleared",
            "user.privacy_updated",
            "user.disabled",
            "user.deleted",
        }:
            assert required in event_types, required

        audit = request("GET", "/internal/audit-log", internal=True)
        actions = {entry["action"] for entry in audit["entries"]}
        for required in {
            "profile.update",
            "privacy.update",
            "friend_request.create",
            "friend_request.accept",
            "friend_request.decline",
            "block.create",
            "block.remove",
            "call_history.delete",
            "call_history.clear",
        }:
            assert required in actions, required

        metrics = request("GET", "/internal/metrics", internal=True)
        counters = metrics["counters"]
        for required in [
            "profile.created",
            "profile.updated",
            "privacy.updated",
            "friend_request.created",
            "friend_request.accepted",
            "friend_request.declined",
            "block.created",
            "block.removed",
            "call_history.recorded",
            "call_history.deleted",
            "call_history.cleared",
            "projection.added",
        ]:
            assert required in counters, required

        print("ALL TESTS PASSED")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
