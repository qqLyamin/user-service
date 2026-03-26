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

- When PostgreSQL env vars are present, deploy/health/migration flow is DB-aware and `migrate up|down|status` is available through the service binary.
- Local Windows verification still runs against the in-memory path because `psql` is not present in this environment.
- The repository includes two Python tests: the legacy domain e2e and a JWT smoke-compatible `GET/PATCH/GET /v1/users/me` flow.
