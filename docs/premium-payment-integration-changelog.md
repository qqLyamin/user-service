# Changelog for Premium/Payment

Date: 2026-04-27
Service: user-service

## Done for the requested contract

`GET /internal/users/{userId}/profile` already existed and remains available for single-user reads.

Added the P1 batch endpoint:

```http
POST /internal/users/batch
X-Internal-Token: <token>
Content-Type: application/json
```

Request:

```json
{
  "userIds": [
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222"
  ]
}
```

Success response:

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

The response intentionally contains only the fields premium-service needs:

- `userId`
- `displayName`
- `profileStatus`

## Auth

Internal endpoints accept the existing internal auth mechanisms:

- preferred: `Authorization: Bearer <internal jwt>`
- legacy/local fallback: `X-Internal-Token: <token>` when `INTERNAL_TOKEN` is configured

There is no default `internal-secret` token when `INTERNAL_TOKEN` is not set.

## Batch behavior

- Max request size is 100 ids.
- Every `userIds` item must be a UUID string.
- UUIDs are normalized to lowercase in responses.
- Duplicate ids are deduplicated.
- First-seen request order is preserved.
- Unknown users are omitted from `users`; the endpoint still returns `200`.
- Existing lifecycle statuses are returned as-is, including `active`, `disabled`, and `deleted`.
- The endpoint does not create missing users.

## Errors

`400 VALIDATION_ERROR`

Returned when:

- request body is not valid JSON
- `userIds` is missing or is not an array of strings
- any id is not a UUID
- more than 100 ids are sent

Response:

```json
{
  "error": "VALIDATION_ERROR"
}
```

`401 UNAUTHORIZED`

Returned when internal auth is missing or invalid.

Response:

```json
{
  "error": "UNAUTHORIZED"
}
```

`503 USER_SERVICE_UNAVAILABLE`

Returned when user-service cannot read profile data from its backing storage.

Response:

```json
{
  "error": "USER_SERVICE_UNAVAILABLE"
}
```

## What premium-service can rely on

premium-service can use `POST /internal/users/batch` for contributor/user display data instead of making N calls to `GET /internal/users/{userId}/profile`.

Expected premium-service use:

- send up to 100 contributor ids per request
- handle missing ids by applying its own display-name fallback
- treat `profileStatus != "active"` according to premium business rules
- retry only `503` and transport/timeouts
- do not retry `400` or `401`

## What user-service expects from premium-service

- Use internal auth on every request.
- Send UUID strings only.
- Split requests larger than 100 ids into multiple batches.
- Do not assume that all requested ids are returned.
- Use `displayName` as the user-facing name when present in the response.

## Payment-service note

No direct payment-service endpoint was added in user-service. If payment-service needs user display data, the preferred boundary is still through premium-service unless a separate direct contract is explicitly requested.

## Verification

Checked locally:

- `tools\build_release.cmd` passed.
- Manual HTTP smoke against the built binary passed for:
  - successful batch response
  - duplicate id normalization/deduplication
  - invalid UUID -> `400 VALIDATION_ERROR`
  - more than 100 ids -> `400 VALIDATION_ERROR`
  - missing internal auth -> `401 UNAUTHORIZED`

Full `tests\spec_test.py` was updated with batch scenarios, but was not run in this Windows environment because Python is not installed.
