# Auth Flow — Technical Reference

Complete guide for new engineers joining the team.

Version 1.0 · July 2026

---

## Table of Contents

1. [Overview & Architecture](#1-overview--architecture)
2. [Tech Stack](#2-tech-stack)
3. [Environment Variables](#3-environment-variables)
4. [Registration Flow (send-otp → verify-otp)](#4-registration-flow)
5. [Login Flow](#5-login-flow)
6. [Token Rotation (Silent Refresh)](#6-token-rotation-silent-refresh)
7. [Security Design Decisions](#7-security-design-decisions)
8. [Error Handling](#8-error-handling)
9. [Redis Key Reference](#9-redis-key-reference)
10. [File Structure](#10-file-structure)
11. [Known Bugs Fixed](#11-known-bugs-fixed)
12. [Onboarding Checklist](#12-onboarding-checklist)

---

## 1. Overview & Architecture

This service handles all authentication for the platform. It follows a two-step email-verified registration, JWT-based session management with access + refresh tokens, and a device-scoped token rotation strategy to detect and block refresh token reuse attacks.

### High-Level Flow

```
 ┌──────────┐       ┌────────────────┐       ┌───────────┐       ┌─────────┐
 │  Client  │──────▶│  Auth Router   │──────▶│ Controller│──────▶│ Service │
 └──────────┘       └────────────────┘       └───────────┘       └────┬────┘
                                                                        │
                                            ┌───────────────────────────┼───────────────┐
                                            ▼                           ▼               ▼
                                        Postgres                     Redis           Resend
                                       (Users DB)              (OTP / Tokens)       (Email)
```

---

## 2. Tech Stack

| Component          | Technology                   | Purpose                                      |
| ------------------ | ---------------------------- | -------------------------------------------- |
| HTTP Framework     | Express.js + TypeScript      | Routing, middleware, controllers             |
| Database           | PostgreSQL via Prisma ORM    | Persistent user storage                      |
| Cache / Session    | Redis (ioredis)              | OTP sessions, refresh token JTIs, user cache |
| Password Hashing   | bcrypt (cost 12)             | Secure password storage                      |
| Tokens             | jsonwebtoken (JWT)           | Stateless access tokens + refresh tokens     |
| OTP Generation     | otp-generator                | 6-digit numeric OTPs                         |
| OTP Security       | Node.js crypto (HMAC-SHA256) | Never store plaintext OTPs                   |
| Email Delivery     | Resend SDK                   | Transactional email with retry logic         |
| Validation         | Zod v4                       | Request body schema validation               |
| Device Fingerprint | SHA-256 of headers           | Scope sessions per device                    |

---

## 3. Environment Variables

| Variable                  | Example                 | Description                                            |
| ------------------------- | ----------------------- | ------------------------------------------------------ |
| `OTP_HMAC_SECRET`         | `09dc0abb…`             | Secret key for HMAC-SHA256 OTP hashing                 |
| `OTP_TTL`                 | `300`                   | OTP session lifetime in seconds (5 min)                |
| `OTP_RATE_MAX_PER_HOUR`   | `5`                     | Max OTP sends per email per hour                       |
| `OTP_MAX_VERIFY_ATTEMPTS` | `5`                     | Max wrong guesses before lockout                       |
| `JWT_ACCESS_SECRET`       | `0f8bf908…`             | Signing secret for access tokens                       |
| `JWT_REFRESH_SECRET`      | `826d2c0e…`             | Signing secret for refresh tokens                      |
| `ACCESS_TOKEN_EXP`        | `15m`                   | Access token lifetime (string, e.g. `15m`)             |
| `REFRESH_TOKEN_EXP`       | `7d`                    | Refresh token lifetime (string, e.g. `7d`)             |
| `ACCESS_TOKEN_EXP_SEC`    | `900`                   | Access token lifetime in seconds (for cookie `maxAge`) |
| `REFRESH_TOKEN_EXP_SEC`   | `604800`                | Refresh token lifetime in seconds                      |
| `REDIS_USER_TTL`          | `86400`                 | How long to cache user object in Redis (seconds)       |
| `MAIL_SEND`               | `onboarding@resend.dev` | From address for all emails                            |
| `RESEND_API_KEY`          | `re_xxx…`               | Resend API key for email delivery                      |

> **Note:** `ACCESS_TOKEN_EXP` and `ACCESS_TOKEN_EXP_SEC` both exist because JWT's `expiresIn` takes a string (e.g. `"15m"`) while cookie `maxAge` requires milliseconds — both are needed.

---

## 4. Registration Flow

Registration is intentionally split into two HTTP requests. No user row is created in Postgres until the email address has been verified. All intermediate state lives in Redis with a TTL.

### 4.1 Step 1 — `POST /auth/send-otp`

**What the client sends**

```http
POST /auth/send-otp
Content-Type: application/json

{
  "firstName": "Alice",
  "lastName":  "Smith",
  "email":     "alice@example.com",
  "password":  "SecurePass1"
}
```

**What happens inside**

1. Zod validates the body (`zSendOtp` schema).
   - `firstName`: min 4, max 40 chars
   - `email`: valid format, lowercased, trimmed
   - `password`: min 8 chars, must contain uppercase, lowercase, and digit
2. Service checks Postgres — throws `409 ConflictError` if email already registered.
3. Password is bcrypt-hashed immediately (cost 12) — plaintext is never stored anywhere.
4. OTP utility runs:
   - Checks Redis rate key `otp:rate:{email}` — throws `429` if ≥ 5 requests in the last hour
   - Generates a 6-digit numeric OTP via `otp-generator`
   - Creates a UUID as the `otpSessionId`
   - Computes `HMAC-SHA256(OTP_HMAC_SECRET, email:otp)` — stores only the hash, never the OTP
   - Writes `{ hashedOtp, meta }` to Redis at `otp:session:{otpSessionId}` with TTL = `OTP_TTL`
   - Increments `otp:rate:{email}` and sets a 1-hour expiry
5. Resend sends the OTP email with retry logic (up to 3 attempts, exponential backoff).
6. `otpSessionId` is set as an httpOnly, secure, `sameSite=strict` cookie named `otp_session`.

**What the client receives**

```http
HTTP 200
Set-Cookie: otp_session=<uuid>; HttpOnly; Secure; SameSite=Strict; Max-Age=300

{ "success": true, "message": "OTP sent successfully" }
```

> **Security:** The OTP is never stored in Redis. Only its HMAC is. Even if Redis is compromised, an attacker cannot recover the OTP from the hash without knowing `OTP_HMAC_SECRET`.

### 4.2 Step 2 — `POST /auth/verify-otp`

**What the client sends**

```http
POST /auth/verify-otp
Content-Type: application/json
Cookie: otp_session=<uuid>

{ "otp": "482910" }
```

**What happens inside**

1. Zod validates the body (`zVerifyOtp` schema) — must be exactly 6 numeric digits.
2. `otpSessionId` is read from `req.cookies.otp_session` — throws `400` if missing.
3. OTP verification runs:
   - Fetches `{ hashedOtp, meta }` from Redis at `otp:session:{otpSessionId}`
   - Returns `null` (→ `400`) if the key is missing or expired
   - Checks `otp:attempt:{email}` — throws `429` if ≥ `OTP_MAX_VERIFY_ATTEMPTS` failures
   - Re-computes HMAC of the submitted OTP
   - Compares using `crypto.timingSafeEqual` to prevent timing attacks
   - On success: deletes `otp:session`, `otp:attempt`, `otp:rate` keys from Redis
   - On failure: increments `otp:attempt` counter
4. If OTP is valid, Prisma creates the user row with `emailVerified: true`.

**What the client receives**

```http
HTTP 201

{
  "message": "Account is created",
  "data": { "id": "...", "firstName": "Alice", "email": "alice@example.com", ... }
}
```

> **Note:** The password field is never returned in the user object. The service strips it before returning.

---

## 5. Login Flow

`POST /auth/login`

**What the client sends**

```http
POST /auth/login
Content-Type: application/json

{
  "email":    "alice@example.com",
  "password": "SecurePass1"
}
```

**What happens inside**

1. Zod validates the body (`zLogin` schema).
2. Device fingerprint is computed from `req.headers[user-agent]` + `req.ip` + `req.headers[accept]`, then SHA-256 hashed and sliced to 16 chars.
3. Prisma looks up the user by email — throws `400` if not found.
4. `bcrypt.compare` checks the submitted password against the stored hash — throws `400` if wrong.
5. Two tokens are generated:
   - Access token: `{ id: userId }`, signed with `JWT_ACCESS_SECRET`, expires in 15m
   - Refresh token: `{ id: userId, jti: randomUUID() }`, signed with `JWT_REFRESH_SECRET`, expires in 7d
6. The refresh token's JTI is stored in Redis at `refresh:{userId}:{deviceId}` with TTL = `REFRESH_TOKEN_EXP_SEC`.
7. The safe user object (password stripped) is cached in Redis at `user:{userId}` with TTL = `REDIS_USER_TTL`.
8. Both tokens are set as httpOnly cookies. The user object is returned in the response body.

**What the client receives**

```http
HTTP 200
Set-Cookie: accessToken=<jwt>; HttpOnly; Secure; SameSite=Strict; Max-Age=900
Set-Cookie: refreshToken=<jwt>; HttpOnly; Secure; SameSite=Strict; Max-Age=604800

{
  "success": true,
  "message": "Logged in successfully",
  "data": { "id": "...", "firstName": "Alice", ... }
}
```

> **Security:** Tokens are set as httpOnly cookies so they are never accessible via `document.cookie` in the browser. This eliminates XSS-based token theft.

---

## 6. Token Rotation (Silent Refresh)

`POST /auth/refresh`

When the access token expires (15m), the client silently calls `/auth/refresh`. No user interaction is needed — the browser automatically sends the httpOnly cookies.

**What happens inside**

1. Reads `refreshToken` from `req.cookies` — throws `401` if missing.
2. Computes the device fingerprint (same algorithm as login).
3. `jwt.verify` checks the signature and expiry — throws if tampered or expired.
4. Extracts `userId` and `jti` from the payload.
5. Looks up the stored JTI in Redis at `refresh:{userId}:{deviceId}`.
   - If the key is missing → session expired → `403`, login again
   - If the JTI does not match → token reuse detected → deletes the Redis key (invalidates the session) → `403`
6. Issues new access and refresh tokens (new JTI each time).
7. Writes the new JTI to Redis, replacing the old one.
8. Sets both new tokens as cookies.

### Token Reuse Detection Explained

Every refresh token has a unique JTI (JWT ID). When a refresh token is used, it is immediately replaced. The new JTI overwrites the old one in Redis.

If a stolen token is replayed after the legitimate client has already rotated it, the JTI in the request will not match what is stored in Redis. The service detects this mismatch, deletes the Redis key, and forces a re-login — protecting the account even if a token was intercepted.

> **Security:** One active session is allowed per user per device. A user logging in from a new device or browser gets an independent Redis key and does not affect other sessions.

---

## 7. Security Design Decisions

| Decision                                    | Why                                                                                           |
| ------------------------------------------- | --------------------------------------------------------------------------------------------- |
| OTP stored as HMAC, not plaintext           | Redis compromise cannot expose OTPs. Attacker needs HMAC secret + email to reverse.           |
| `crypto.timingSafeEqual` for OTP comparison | Prevents timing attacks where an attacker measures response time to guess OTP bytes.          |
| Tokens in httpOnly cookies                  | XSS cannot steal tokens via `document.cookie`. CSRF is mitigated by `sameSite=strict`.        |
| JTI-based refresh token rotation            | Each refresh token is single-use. Reuse triggers immediate session invalidation.              |
| Device-scoped sessions                      | `refresh:{userId}:{deviceId}` allows independent sessions per device and targeted revocation. |
| bcrypt cost 12                              | Expensive enough to prevent brute force, reasonable for server-side login latency.            |
| OTP rate limiting (5/hour)                  | Prevents email flooding and OTP brute force via repeated `/send-otp` calls.                   |
| OTP attempt limiting (5 attempts)           | Locks out brute force on `/verify-otp` after 5 wrong guesses.                                 |
| No user object in DB until OTP verified     | Prevents account enumeration via failed registrations and keeps DB clean.                     |

---

## 8. Error Handling

All errors extend a base `AppError` class with a `statusCode` and a machine-readable `code` string. `asyncHandler` wraps every controller so unhandled promise rejections are forwarded to the Express error middleware via `next(err)`.

| Class                  | HTTP Status | Default Code        | When thrown                                             |
| ---------------------- | ----------- | ------------------- | ------------------------------------------------------- |
| `BadRequestError`      | 400         | `BAD_REQUEST`       | Invalid input, wrong password, email not found, bad OTP |
| `UnauthorizedError`    | 401         | `UNAUTHORIZED`      | Missing access/refresh token                            |
| `ForbiddenError`       | 403         | `FORBIDDEN`         | Session expired, token reuse detected                   |
| `NotFoundError`        | 404         | `NOT_FOUND`         | Resource does not exist                                 |
| `ConflictError`        | 409         | `CONFLICT`          | Email already registered                                |
| `TooManyRequestsError` | 429         | `TOO_MANY_REQUESTS` | OTP rate limit or attempt limit exceeded                |
| `InternalServerError`  | 500         | `SERVER_ERROR`      | Unexpected server failure                               |

### Zod Validation Errors

When Zod validation fails, the first issue's message is formatted as `field: message` and returned as the response message.

```json
{
  "success": false,
  "message": "password: Password must contain at least one uppercase letter"
}
```

---

## 9. Redis Key Reference

| Key Pattern                   | Value                      | TTL                               | Purpose                                      |
| ----------------------------- | -------------------------- | --------------------------------- | -------------------------------------------- |
| `otp:session:{otpSessionId}`  | JSON `{ hashedOtp, meta }` | `OTP_TTL` (e.g. 300s)             | Stores HMAC + user meta during registration  |
| `otp:rate:{email}`            | Integer (count)            | 3600s (1 hour)                    | Tracks OTP send frequency per email          |
| `otp:attempt:{email}`         | Integer (count)            | `OTP_TTL`                         | Tracks failed OTP verification attempts      |
| `refresh:{userId}:{deviceId}` | JTI string                 | `REFRESH_TOKEN_EXP_SEC` (604800s) | Active refresh token JTI per user per device |
| `user:{userId}`               | JSON (safe user object)    | `REDIS_USER_TTL` (86400s)         | Cached user object for fast lookups          |

---

## 10. File Structure

```
src/
├── routes/
│   └── auth.routes.ts          # Route definitions → controller mapping
├── controllers/
│   └── auth.controller.ts      # HTTP layer: parse request, call service, set cookies
├── services/
│   └── auth.service.ts         # Business logic: DB, Redis, token operations
├── utils/
│   ├── auth.ts                 # JWT sign/verify, token hashing
│   ├── otp.ts                  # OTP generation, HMAC, Redis storage
│   ├── email.ts                # Resend email service with retry logic
│   ├── device-fingerprint.ts   # SHA-256 fingerprint from request headers
│   ├── error.ts                # Custom error classes (AppError hierarchy)
│   ├── api-response.ts         # SuccessResponse / ErrorResponse helpers
│   ├── zod.formatter.ts        # Formats first Zod issue as a readable string
│   └── asyncHandler.ts         # Wraps async controllers to catch unhandled rejections
├── types/
│   └── zod.ts                  # Zod schemas and inferred TypeScript types
└── config/
    ├── index.ts                # Typed config object from process.env
    ├── prisma.ts               # Prisma client singleton
    └── redis.ts                # ioredis client singleton
```

---

## 11. Known Bugs Fixed

| File                      | Bug                                                                                                                                                        | Fix                                                                          |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `otp.ts`                  | Typo: `{ hasedOtp }` on JSON.parse destructure — `storedOtp` was always `undefined`, causing `timingSafeEqual` to throw and all OTP verifications to fail. | Changed `hasedOtp` → `hashedOtp` to match the key written during generation. |
| `zod.ts`                  | `VerifyOtpBodyType` was inferred from `zSendOtp` instead of `zVerifyOtp`, giving it the full registration shape instead of `{ otp: string }`.              | Changed `z.infer<typeof zSendOtp>` → `z.infer<typeof zVerifyOtp>`.           |
| `auth.service.ts` (login) | `loggedInUser` was returning the full `existingUser` object including the hashed password field.                                                           | Return `safeUser` (password-stripped object) instead of `existingUser`.      |
| `utils/auth.ts`           | `ZodError` imported from `zod/v3` subpath, causing a type mismatch with the v4 `ZodError` returned by `safeParse`.                                         | Changed import from `zod/v3` → `zod`.                                        |

---
