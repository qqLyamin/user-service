# user-service

MVP user domain service in C++20 with a self-contained HTTP server and no third-party runtime dependencies.

## What is implemented

- Profile source of truth: `displayName`, `username`, `avatarObjectId`, `bio`, `locale`, `timeZone`, lifecycle projection status
- Privacy source of truth: profile visibility, DM policy, friend request policy, last seen visibility, avatar visibility
- Directional friendship model: pending outgoing/incoming, accepted, declined, removed
- Block list with immediate DM visibility suppression in projections
- Internal relationship authorization API
- Internal event ingestion with idempotency via `eventId`
- User-owned entity projection read model for rooms and conversations
- Outbox, audit log, and simple counters exposed via internal endpoints

## Auth model used in the MVP

- Public endpoints require `Authorization: Bearer user:<user-id>`
- Internal endpoints require `X-Internal-Token: internal-secret`

This is only a transport/testing shim. The service does not mint or validate JWTs.

## Build

```powershell
.\tools\build_release.cmd
```

The repository also contains `CMakeLists.txt`, but the checked-in build script is the deterministic path used for local verification on this machine.

## Run

```powershell
.\build\user-service.exe 8080
```

or

```powershell
$env:USER_SERVICE_PORT=8080
.\build\user-service.exe
```

## API summary

### Public

- `GET /v1/users/me`
- `PATCH /v1/users/me`
- `GET /v1/users/{userId}`
- `POST /v1/users/{userId}/friend-request`
- `POST /v1/users/{userId}/friend-request/accept`
- `POST /v1/users/{userId}/friend-request/decline`
- `DELETE /v1/users/{userId}/friend`
- `POST /v1/users/{userId}/block`
- `DELETE /v1/users/{userId}/block`
- `GET /v1/users/me/privacy`
- `PATCH /v1/users/me/privacy`
- `GET /v1/users/me/rooms?limit=50&offset=0`
- `GET /v1/users/me/conversations?limit=50&offset=0`
- `GET /v1/users/me/contacts?limit=50&offset=0`

### Internal

- `POST /internal/events`
- `GET /internal/users/{userId}/profile`
- `POST /internal/users/relationships/check`
- `POST /internal/users/{userId}/authorize-profile-read`
- `GET /internal/outbox`
- `GET /internal/audit-log`
- `GET /internal/metrics`

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

## Design notes

- Storage is in-memory for the MVP executable.
- Outbox and audit log are process-local read models.
- The project includes a Python spec test that boots the binary and checks the scenarios end to end.
