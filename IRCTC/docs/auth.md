# Auth Flow — Technical Reference

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

This is the entry point to registration. Its only job is to validate the signup payload, make sure the email isn't already taken, generate a one-time code, and get that code to the user's inbox — without ever writing anything to the permanent database yet.

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

`lastName` is optional — everything else is required.

#### Step-by-step breakdown

**1. Request validation (Zod — `zSendOtp` schema)**

| Field       | Rules                                                                    | Notes                                                                                                                          |
| ----------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `firstName` | required, string, min 4, max 40 chars, trimmed                           | Rejects `undefined` with a custom "First name is required" message                                                             |
| `lastName`  | optional, string, max 40 chars, trimmed                                  | Defaults to `""` in the controller if omitted                                                                                  |
| `email`     | required, valid email format, trimmed, lowercased                        | Lowercasing happens _before_ the uniqueness check, so `Alice@Example.com` and `alice@example.com` are treated as the same user |
| `password`  | required, min 8 chars, must contain ≥1 uppercase, ≥1 lowercase, ≥1 digit | No special-character requirement currently enforced                                                                            |

If any rule fails, `zSendOtp.safeParse()` returns `success: false`. The controller short-circuits immediately and responds `400` with the formatted Zod message — the request never reaches the service layer, Postgres, or Redis.

**2. Duplicate email check**

```ts
const existingUser = await prisma.user.findUnique({ where: { email } });
if (existingUser) throw new ConflictError("User already exists");
```

This is a hard stop — `409 CONFLICT`. It happens _before_ any OTP is generated, so a duplicate signup attempt never triggers an email send or touches the rate limiter.

> This does mean an attacker can determine whether an email is already registered (a form of account enumeration). If that's a concern for your product, consider returning a generic "if this email is available you'll receive an OTP" response instead — see Security Design Decisions for the trade-off this codebase currently makes.

**3. Password hashing**

```ts
const hashedPassword = await bcrypt.hash(password, 12);
```

The plaintext password from the request body is hashed immediately, before anything is persisted anywhere — including Redis. Cost factor 12 is used consistently across the app (see `login` for the matching `bcrypt.compare`).

**4. OTP generation & storage (`generateAndStoreOtp` in `otp.ts`)**

This is the most involved step. It runs as a single function but does five distinct things in order:

a. **Rate limit check** — reads `otp:rate:{email}` from Redis.

```ts
if (sentCount >= RATE_MAX) throw new TooManyRequestsError(...)  // 429
```

`RATE_MAX` comes from `OTP_RATE_MAX_PER_HOUR` (default 5 if unset). This check happens _before_ a new OTP is generated, so hitting the limit costs no extra OTP-generation work.

b. **OTP generation** — `otp-generator` produces a 6-digit numeric string (no letters, no symbols):

```ts
otpGenerator.generate(6, {
  upperCaseAlphabets: false,
  lowerCaseAlphabets: false,
  specialChars: false,
});
```

c. **Session ID creation** — `crypto.randomUUID()` generates a unique `otpSessionId`. This ID (not the OTP, not the email) is what the client will hold onto via cookie between requests.

d. **HMAC hashing** — the OTP is never stored raw:

```ts
const hashed = crypto
  .createHmac("sha256", OTP_HMAC_SECRET)
  .update(`${email}:${otp}`)
  .digest("hex");
```

Binding the email into the HMAC input means the same 6-digit OTP hashes differently per user, so there's no shared lookup table risk across accounts.

e. **Redis write** — the registration payload is packaged and stored under the session ID:

```ts
await redis.set(
  `otp:session:${otpSessionId}`,
  JSON.stringify({
    hashedOtp: hashed,
    meta: { firstName, lastName, email, password: hashedPassword },
  }),
  "EX",
  OTP_TTL,
);
```

This is the **only** place the user's registration data lives until they verify — it's not in Postgres yet. If they never verify, this key simply expires (`OTP_TTL`, default 300s) and the signup attempt disappears with no trace.

f. **Rate counter increment** — `otp:rate:{email}` is incremented and given (or refreshed to) a 1-hour expiry, so the 5-per-hour window is a rolling one hour from the _first_ send in that window, not a fixed clock hour.

**5. Sending the email**

```ts
await emailService.sendOtpEmail(email, otp, 5);
```

This is where the plaintext OTP is used for the only time in the entire flow — to put it in the email body. The email service (`EmailService.sendWithRetry`) wraps the Resend API call with up to 3 attempts and exponential backoff (1s, 2s, 4s) if delivery fails. If all 3 attempts fail, the error propagates up and the whole request fails with a 500 — the OTP session was already written to Redis at this point, so a client could theoretically retry `/send-otp` again once the transient email issue clears (subject to the rate limit).

**6. Cookie assignment**

Back in the controller, the returned `otpSessionId` is set as a cookie — this is what ties the _next_ request (`/verify-otp`) back to this Redis session, without the client ever seeing or handling the OTP session data directly:

```ts
res.cookie("otp_session", otpSessionId, {
  httpOnly: true,
  secure: true,
  sameSite: "strict",
  maxAge: config.OTP_TTL * 1000,
});
```

The cookie's `maxAge` matches `OTP_TTL`, so the browser naturally drops the cookie at roughly the same time Redis expires the session — they're not linked mechanically, just configured to agree.

**What the client receives**

```http
HTTP 200
Set-Cookie: otp_session=<uuid>; HttpOnly; Secure; SameSite=Strict; Max-Age=300

{ "success": true, "message": "OTP sent successfully" }
```

#### Failure modes at a glance

| Condition                                        | Status | Thrown by                    |
| ------------------------------------------------ | ------ | ---------------------------- |
| Missing/invalid field in body                    | 400    | Zod validation in controller |
| Email already registered                         | 409    | `auth.service.sendOtp`       |
| > 5 OTP requests for this email in the last hour | 429    | `generateAndStoreOtp`        |
| Email provider fails after 3 retries             | 500    | `EmailService.sendWithRetry` |

> **Security:** The OTP is never stored in Redis. Only its HMAC is. Even if Redis is compromised, an attacker cannot recover the OTP from the hash without knowing `OTP_HMAC_SECRET`.

### 4.2 Step 2 — `POST /auth/verify-otp`

This endpoint is the only place in the entire registration flow where a row actually gets written to Postgres. Everything before this point lived in Redis and could vanish on TTL expiry with no trace.

**What the client sends**

```http
POST /auth/verify-otp
Content-Type: application/json
Cookie: otp_session=<uuid>

{ "otp": "482910" }
```

Note that the client never sends the email or session ID in the body — the session is entirely carried by the `otp_session` cookie set during step 1. This is deliberate: it means a captured request body alone (e.g. logged by a proxy) is useless without also having the cookie.

#### Step-by-step breakdown

**1. Request validation (Zod — `zVerifyOtp` schema)**

| Field | Rules                                              | Notes                                                                                                                                  |
| ----- | -------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `otp` | required, string, exactly 6 chars, regex `^\d{6}$` | Both `.length(6)` and the regex are enforced — length alone would pass `"12ab56"`, so the regex catches non-numeric input specifically |

**2. Cookie presence check**

```ts
const otpSessionId = req.cookies.otp_session;
if (!otpSessionId) throw new BadRequestError("OTP session is missing");
```

This fires a `400` if the cookie was never set, already expired client-side, or the client is calling this endpoint without having gone through `/send-otp` first. It's a distinct failure from "OTP is wrong" — there's no Redis lookup at all yet at this point.

**3. OTP verification (`verifyOtpViaUnHashing` in `otp.ts`)**

This is the core of the endpoint and runs through several checks in sequence, each of which can short-circuit the request:

a. **Session lookup** — fetches `otp:session:{otpSessionId}` from Redis.

```ts
const rawData = await redis.get(`otp:session:${otpSessionId}`);
if (!rawData) return null; // → 400 Invalid or expired OTP
```

A `null` here means either the TTL expired (default 5 minutes) or the session was already consumed by a prior successful verification — Redis keys are deleted on success (see step e below), so this doubles as replay protection for the OTP itself.

b. **Attempt counter check** — before even looking at the submitted OTP, it checks how many times this email has already gotten it wrong:

```ts
const attemptsCount = parseInt((await redis.get(`otp:attempt:${meta.email}`)) || "0", 10);
if (attemptsCount >= config.OTP_MAX_VERIFY_ATTEMPTS) throw new TooManyRequestsError(...); // 429
```

Default limit is 5. This is scoped by **email**, not by session ID — so even if someone requests a fresh OTP (new session ID) for the same email mid-lockout, the attempt counter still applies. It only clears on a successful verification or natural TTL expiry.

c. **HMAC recomputation** — the submitted OTP is hashed the same way it was during generation:

```ts
const hashedOtp = hmacFor({ email: meta.email, otp });
```

Note `meta.email` is used here (from the stored session), not anything from the request — the client has no way to influence which email the OTP is checked against.

d. **Timing-safe comparison**:

```ts
crypto.timingSafeEqual(
  Buffer.from(hashedOtp, "hex"),
  Buffer.from(storedOtp, "hex"),
);
```

A regular `===` comparison would return faster on an early mismatched byte than a late one, which — over enough requests — leaks information about how close a guess is. `timingSafeEqual` always takes the same amount of time regardless of where the mismatch occurs.

e. **On success** — three keys are cleaned up together:

```ts
await redis.del(`otp:session:${otpSessionId}`, attemptsKey);
await redis.del(`otp:rate:${meta.email}`);
```

This means a successful verification also resets the _rate limit_ counter, not just the attempt counter — a user who eventually gets it right isn't penalized on a future signup attempt by leftover rate-limit state.

f. **On failure** — the attempt counter is bumped and its TTL is (re)set to `OTP_TTL`:

```ts
await redis.incr(attemptsKey);
await redis.expire(attemptsKey, config.OTP_TTL);
```

The function returns `null` rather than throwing, so the controller treats a wrong OTP the same way as an expired session — both come back as `400 Invalid or expired OTP`. This is intentional: the client can't distinguish "your code was wrong" from "your code expired," which avoids leaking whether a session still exists.

**4. User creation**

Only after all of the above succeeds does anything touch Postgres:

```ts
const user = await prisma.user.create({
  data: {
    firstName: meta.firstName,
    lastName: meta.lastName,
    email: meta.email,
    password: meta.hashedPassword, // already bcrypt-hashed back in step 1
    emailVerified: true,
  },
});
```

The password stored here was hashed during `/send-otp`, not re-hashed here — `meta.hashedPassword` is carried through Redis exactly as it was written.

**5. Response shaping**

The created Prisma record includes the hashed password field by default. The service does not currently strip it before returning — worth checking against the "Known Bugs Fixed" table if you're auditing this endpoint, since the login flow has the equivalent fix applied but this path should be checked for the same pattern.

**What the client receives**

```http
HTTP 201

{
  "message": "Account is created",
  "data": { "id": "...", "firstName": "Alice", "email": "alice@example.com", ... }
}
```

#### Failure modes at a glance

| Condition                                      | Status | Thrown by                              |
| ---------------------------------------------- | ------ | -------------------------------------- |
| `otp` not exactly 6 digits                     | 400    | Zod validation in controller           |
| `otp_session` cookie missing                   | 400    | Controller                             |
| Session expired / already used / never existed | 400    | `verifyOtpViaUnHashing` returns `null` |
| ≥ 5 failed verify attempts for this email      | 429    | `verifyOtpViaUnHashing`                |
| OTP does not match                             | 400    | `verifyOtpViaUnHashing` returns `null` |

> **Note:** The password field is returned in the user object here — double check whether that's intended before shipping to production; the login endpoint deliberately strips it (see section 5) and this endpoint arguably should too.

---

## 5. Login Flow

`POST /auth/login`

This is a standard credential check, but it does meaningfully more work than "check password" — it also establishes a device-scoped session in Redis that the refresh-rotation logic (section 6) depends on.

**What the client sends**

```http
POST /auth/login
Content-Type: application/json

{
  "email":    "alice@example.com",
  "password": "SecurePass1"
}
```

#### Step-by-step breakdown

**1. Request validation (Zod — `zLogin` schema)**

| Field      | Rules                                                                    | Notes                                                                                                                                      |
| ---------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `email`    | required, valid email format, trimmed, lowercased                        | Same normalization as registration, so case differences never cause a false "not found"                                                    |
| `password` | required, min 8 chars, must contain ≥1 uppercase, ≥1 lowercase, ≥1 digit | This is validating _shape_, not correctness — a syntactically valid but wrong password still passes Zod and fails at the bcrypt step below |

**2. Device fingerprint generation**

```ts
const deviceId = getDeviceFingerprint(req);
// SHA-256(userAgent + "|" + ip + "|" + accept) → first 16 hex chars
```

This runs _before_ the credentials are even checked against the database. It doesn't depend on anything about the user — it's purely a hash of request metadata, so the same physical browser/device will produce the same `deviceId` across login attempts, sessions, and even different accounts. This is what allows one refresh-token session per device per user, rather than per user globally.

**3. User lookup**

```ts
const existingUser = await prisma.user.findUnique({ where: { email } });
if (!existingUser || !existingUser.password)
  throw new BadRequestError("Email not found");
```

The `!existingUser.password` check matters because it implies this schema supports users without a password set (e.g. an OAuth-only account, given the unused `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET` env vars). Someone who signed up via Google and never set a password would correctly get "Email not found" rather than crashing on `bcrypt.compare(password, null)`.

**4. Password comparison**

```ts
const doesPasswordMatch = await bcrypt.compare(password, existingUser.password);
if (!doesPasswordMatch) throw new BadRequestError("Incorrect password");
```

Note this is a _different_ error message than "Email not found" above — which technically allows an attacker to enumerate which emails are registered (a valid email + wrong password returns a different message than a nonexistent email). Compare this to the account-enumeration trade-off flagged in `/send-otp` — this codebase currently accepts that risk in both places.

**5. Token generation**

Two independent JWTs are signed, using two different secrets:

```ts
const accessToken = generateAccessToken(existingUser.id);
// payload: { id: userId }, secret: JWT_ACCESS_SECRET, expiresIn: 15m

const refreshToken = generateRefreshToken(existingUser.id);
// payload: { id: userId, jti: randomUUID() }, secret: JWT_REFRESH_SECRET, expiresIn: 7d
```

Using separate secrets means a leaked access-token secret can't be used to forge refresh tokens, and vice versa. The `jti` on the refresh token is the linchpin of the entire rotation/reuse-detection system covered in section 6 — the access token has no equivalent because it's never rotated or checked against Redis, it just expires naturally.

**6. Redis session write**

```ts
const data = jwt.decode(refreshToken) as { jti: string };
await redis.set(
  `refresh:${existingUser.id}:${deviceId}`,
  data.jti,
  "EX",
  config.REFRESH_TOKEN_EXP_SEC,
);
```

Note this uses `jwt.decode`, not `jwt.verify` — decoding just reads the payload without checking the signature, which is fine here because the token was _just signed_ by this same process a line above; there's nothing to verify against an attacker.

The key is composed of both `userId` and `deviceId`, so logging in from a second browser/device does not overwrite or invalidate the first session — each device gets its own independent refresh lineage.

**7. User cache write**

```ts
const { password: _password, ...safeUser } = existingUser;
await redis.set(
  `user:${existingUser.id}`,
  JSON.stringify(safeUser),
  "EX",
  config.REDIS_USER_TTL,
);
```

This is a read-through cache for other parts of the app (e.g. an auth middleware that resolves `req.user` from the access token) to avoid a Postgres round-trip on every authenticated request. Destructuring `password` out before serializing is what keeps the hash out of this cache entirely.

**8. Cookie assignment & response**

```ts
res.cookie("accessToken", accessToken, {
  httpOnly: true,
  secure: true,
  sameSite: "strict",
  maxAge: config.ACCESS_TOKEN_EXP_SEC * 1000,
});
res.cookie("refreshToken", refreshToken, {
  httpOnly: true,
  secure: true,
  sameSite: "strict",
  maxAge: config.REFRESH_TOKEN_EXP_SEC * 1000,
});
```

Both cookies are set with identical flags — only the name, value, and `maxAge` differ. `loggedInUser` returned in the JSON body is `safeUser`, the same password-stripped object written to the Redis cache — not the raw Prisma record.

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

#### Failure modes at a glance

| Condition                                            | Status | Thrown by                    |
| ---------------------------------------------------- | ------ | ---------------------------- |
| Invalid email format or weak password shape          | 400    | Zod validation in controller |
| No user with this email, or user has no password set | 400    | `auth.service.login`         |
| Password does not match stored hash                  | 400    | `auth.service.login`         |

> **Security:** Tokens are set as httpOnly cookies so they are never accessible via `document.cookie` in the browser. This eliminates XSS-based token theft.

---

## 6. Token Rotation (Silent Refresh)

`POST /auth/refresh`

When the access token expires (15m), the client silently calls `/auth/refresh`. No user interaction is needed — the browser automatically sends the httpOnly cookies. This endpoint has no request body at all; everything it needs comes from cookies and headers.

#### Step-by-step breakdown

**1. Refresh token presence check**

```ts
const refreshToken = req.cookies.refreshToken;
if (!refreshToken)
  throw new UnauthorizedError("Refresh token is missing", "LOGIN_AGAIN");
```

This fires `401` if the cookie is absent entirely — e.g. it expired client-side after 7 days, or the user is in a fresh browser with no session at all. The `LOGIN_AGAIN` code is meant to be a signal the frontend can key off of to redirect straight to the login screen rather than showing a generic error.

**2. Device fingerprint recomputation**

```ts
const deviceId = getDeviceFingerprint(req);
```

This must produce the _same_ value it did at login time for the lookup in step 4 to succeed. Since the fingerprint is derived purely from `user-agent` + `ip` + `accept` headers, anything that changes those between login and refresh (switching networks, some browser updates, certain proxies rewriting headers) can change the fingerprint and effectively invalidate the session — this is a deliberate trade-off for tying sessions to devices without requiring a client-generated identifier.

**3. Signature & expiry verification**

```ts
const payload = verifyRefreshToken(refreshToken); // jwt.verify — throws if invalid or expired
const { id: userId, jti } = payload;
```

Unlike the `jwt.decode` used during login (section 5, step 6), this is a full `jwt.verify` — it checks the signature against `JWT_REFRESH_SECRET` and rejects the token outright if it's expired or has been tampered with in any way. If this throws, `asyncHandler` catches it and it surfaces as an error response before any Redis lookup happens.

**4. JTI comparison against Redis — the reuse-detection core**

```ts
const storedJti = await redis.get(`refresh:${userId}:${deviceId}`);

if (!storedJti) {
  throw new ForbiddenError("Session expired", "LOGIN_AGAIN"); // 403
}

if (storedJti !== jti) {
  await redis.del(`refresh:${userId}:${deviceId}`);
  throw new ForbiddenError("Refresh token reused", "LOGIN_AGAIN"); // 403
}
```

This is two separate failure branches with different meanings:

- **No key found at all** — the session was never established for this device, or it already expired/was deleted (including by the reuse branch below on a _previous_ request). Treated as a normal "please log in again."
- **Key exists but doesn't match** — the token presented is _valid_ (it passed signature verification in step 3) but it's an _old_ one that has already been rotated out. This is the signature of a replay: either the legitimate client rotated already and this is a stale copy, or a second party is holding a stolen copy and using it after the real owner already refreshed. Either way, the response is the same — the Redis key is deleted, killing the session for _both_ parties, and the caller must log in again.

**5. Issuing new tokens**

```ts
const newAccessToken = generateAccessToken(payload.id);
const newRefreshToken = generateRefreshToken(payload.id); // new random jti
```

Identical generation logic to login (section 5, step 5) — a fresh `jti` is created every single rotation, which is what makes the next refresh cycle's comparison in step 4 meaningful.

**6. Redis key replacement**

```ts
const response = jwt.decode(newRefreshToken) as { jti: string };
await redis.set(
  `refresh:${payload.id}:${deviceId}`,
  response.jti,
  "EX",
  config.REFRESH_TOKEN_EXP_SEC,
);
```

This **overwrites** the same key from login rather than creating a new one — there is only ever one active JTI per `userId:deviceId` pair at a time. The TTL is also reset to the full `REFRESH_TOKEN_EXP_SEC` on every rotation, so an actively-used session can stay alive indefinitely past the original 7-day window, while an abandoned one still expires 7 days after its last refresh.

**7. Cookie reassignment**

```ts
res.cookie("accessToken", newAccessToken, { ... });
res.cookie("refreshToken", newRefreshToken, { ... });
```

Same cookie flags as login. The old refresh token cookie is simply overwritten — the browser has no way to "remember" the old one, so once this response is received, only the new token pair is usable going forward.

**What the client receives**

```http
HTTP 200
Set-Cookie: accessToken=<new-jwt>; HttpOnly; Secure; SameSite=Strict; Max-Age=900
Set-Cookie: refreshToken=<new-jwt>; HttpOnly; Secure; SameSite=Strict; Max-Age=604800

{
  "success": true,
  "message": "Access and refresh tokens reissued"
}
```

Note there's no `data` field here — unlike login, this endpoint doesn't re-fetch or return the user object. If the frontend needs fresh user data after a refresh, it should hit a separate `/me`-style endpoint (backed by the `user:{userId}` Redis cache written at login) rather than expecting it from this response.

#### Failure modes at a glance

| Condition                             | Status                     | Meaning                                           |
| ------------------------------------- | -------------------------- | ------------------------------------------------- |
| `refreshToken` cookie missing         | 401                        | No session cookie present at all                  |
| Signature invalid or token expired    | — (thrown by `jwt.verify`) | Tampered or past its 7-day expiry                 |
| No Redis key for `userId:deviceId`    | 403                        | Session was never established, or already cleared |
| Redis JTI doesn't match token's `jti` | 403                        | Reuse detected — stale or stolen token replayed   |

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
