# user-service

User domain service in C++20 with a self-contained HTTP server, deploy assets, migration files, and smoke-compatible JWT auth.

## What is implemented

- Profile source of truth: `displayName`, `username`, `avatarObjectId`, `bio`, `locale`, `timeZone`, lifecycle projection status
- Privacy source of truth: profile visibility, DM policy, friend request policy, last seen visibility, avatar visibility
- Directional friendship model: pending outgoing/incoming, accepted, declined, removed
- Block list with immediate DM visibility suppression in projections
- Internal relationship authorization API
- Internal event ingestion with idempotency via `eventId`
- User-owned entity projection read model for rooms and conversations
- Per-user call history for audio/video calls with multi-participant snapshots
- Presence read model with `green/yellow/red` aggregation over client sessions
- Outbox, audit log, and simple counters exposed via internal endpoints
- `/health` endpoint with DB-aware readiness semantics
- Lazy profile bootstrap for JWT-driven smoke flow on `GET /v1/users/me`
- PostgreSQL migration files in `migrations/` and `user-service migrate up|down|status`
- Deploy assets: `.env.example`, `deploy/user-service.service`, `.github/workflows/deploy.yml`

## Auth model used in the MVP

- Public endpoints accept `Authorization: Bearer <jwt>` for smoke/deploy flow and still accept `Authorization: Bearer user:<user-id>` for local/internal development tests
- Internal endpoints accept `Authorization: Bearer <jwt>` signed with `JWT_SECRET`, with `iss=auth-service` and `aud=internal`
- `X-Internal-Token: internal-secret` remains a legacy fallback for local compatibility when `Authorization` is absent

## Build

```powershell
.\tools\build_release.cmd
```

The repository also contains `CMakeLists.txt`, but the checked-in build script is the deterministic path used for local verification on this machine.

On Linux CI/deploy hosts:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

## Run

```powershell
$env:HTTP_ADDR="127.0.0.1:8812"
.\build\user-service.exe
```

`HTTP_ADDR` is the preferred bind setting and is used as an exact IPv4 listen address. `HTTP_ADDR=127.0.0.1:8812` binds only loopback; `HTTP_ADDR=:8812` explicitly binds all interfaces.

Legacy port-only fallback is still supported when `HTTP_ADDR` is not set:

```powershell
$env:USER_SERVICE_PORT=8080
.\build\user-service.exe
```

## API summary

### Public

- `GET /v1/users/me`
- `PATCH /v1/users/me`
- `GET /health`
- `GET /v1/users/{userId}`
- `POST /v1/users/{userId}/friend-request`
- `POST /v1/users/{userId}/friend-request/accept`
- `POST /v1/users/{userId}/friend-request/decline`
- `DELETE /v1/users/{userId}/friend`
- `POST /v1/users/{userId}/block`
- `DELETE /v1/users/{userId}/block`
- `GET /v1/users/me/privacy`
- `PATCH /v1/users/me/privacy`
- `POST /v1/users/me/presence/pulse`
- `POST /v1/users/me/presence/disconnect`
- `POST /v1/users/presence/query`
- `GET /v1/users/me/rooms?limit=50&offset=0`
- `GET /v1/users/me/conversations?limit=50&offset=0`
- `GET /v1/users/me/contacts?limit=50&offset=0`
- `GET /v1/users/me/call-history?limit=50&offset=0`
- `DELETE /v1/users/me/call-history`
- `DELETE /v1/users/me/call-history/{historyId}`

### Internal

- `POST /internal/events`
- `GET /internal/users/{userId}/profile`
- `POST /internal/users/batch`
- `POST /internal/users/relationships/check`
- `POST /internal/users/{userId}/authorize-profile-read`
- `GET /internal/outbox`
- `GET /internal/audit-log`
- `GET /internal/metrics`

## Internal user profile batch

`POST /internal/users/batch`

```json
{
  "userIds": [
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222"
  ]
}
```

Response:

```json
{
  "users": [
    {
      "userId": "11111111-1111-1111-1111-111111111111",
      "displayName": "IGOR22",
      "profileStatus": "active"
    }
  ]
}
```

The endpoint requires internal auth, accepts up to 100 UUID values, preserves first-seen request order, and omits unknown users. Contract errors are `400 {"error":"VALIDATION_ERROR"}`, `401 {"error":"UNAUTHORIZED"}`, and `503 {"error":"USER_SERVICE_UNAVAILABLE"}`.

## Internal event ingestion

`POST /internal/events`

```json
{
  "type": "auth.user_registered",
  "eventId": "evt-1",
  "payload": {
    "userId": "11111111-1111-1111-1111-111111111111",
    "displayName": "Igor",
    "username": "igor",
    "deviceId": "device-1",
    "sessionId": "session-1",
    "platform": "windows"
  }
}
```

Supported `type` values:

- `auth.user_registered`
- `auth.user_disabled`
- `auth.user_enabled`
- `auth.user_deleted`
- `room.member_added`
- `room.member_removed`
- `conversation.member_added`
- `conversation.member_removed`
- `call.history_recorded`

`call.history_recorded` payload example:

```json
{
  "type": "call.history_recorded",
  "eventId": "evt-call-1",
  "payload": {
    "callId": "call-1",
    "initiatorUserId": "11111111-1111-1111-1111-111111111111",
    "participantUserIds": [
      "11111111-1111-1111-1111-111111111111",
      "22222222-2222-2222-2222-222222222222",
      "33333333-3333-3333-3333-333333333333"
    ],
    "callType": "video",
    "callStatus": "completed",
    "startedAt": "2026-03-30T10:00:00.000Z",
    "endedAt": "2026-03-30T10:25:00.000Z",
    "durationSec": 1500,
    "conversationId": "conv-1",
    "roomId": null
  }
}
```

Notes:

- The event is an upsert by `(ownerUserId, callId)`, so producers can resend the same `callId` when participants expand from 1-1 to `+1/+N`.
- A row is stored for each participant, and each user can delete only their own copy through the public API.

## Presence API

Presence is aggregated per user across client sessions:

- `green`: there is at least one connected session with a recent pulse
- `yellow`: there is at least one connected session, but no recent pulse
- `red`: all known sessions are explicitly disconnected, or no presence session exists

The recent pulse threshold defaults to `30` seconds and can be overridden with `PRESENCE_GREEN_TTL_SECONDS`.

Client pulse:

```json
POST /v1/users/me/presence/pulse
{
  "sessionId": "desktop-1",
  "deviceId": "device-1",
  "platform": "windows"
}
```

Client explicit disconnect:

```json
POST /v1/users/me/presence/disconnect
{
  "sessionId": "desktop-1"
}
```

Batch query for friends / room participants / any known user ids:

```json
POST /v1/users/presence/query
{
  "userIds": [
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222"
  ]
}
```

Response item shape:

```json
{
  "userId": "22222222-2222-2222-2222-222222222222",
  "presence": "green",
  "isOnline": true,
  "lastSeenAt": "2026-03-30T12:00:00.000Z",
  "connectedSessionCount": 1,
  "recentSessionCount": 1
}
```

## Design notes

- When PostgreSQL env vars are present, deploy/health/migration flow is DB-aware and `migrate up|down|status` is available through the service binary.
- Local Windows verification still runs against the in-memory path because `psql` is not present in this environment.
- The repository includes two Python tests: the legacy domain e2e and a JWT smoke-compatible `GET/PATCH/GET /v1/users/me` flow.
