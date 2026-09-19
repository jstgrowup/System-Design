# Learning Progress — Auth System (user-service)

Tracks exactly where the code walkthrough (see `docs/learning-mode.md`) left
off, so the next session can resume mid-function instead of restarting. Also
see `docs/playlist-guide.md` for how this maps to the companion YouTube
playlist — video 5 (Signup using OTP) and video 6 (Login + Refresh Token
Rotation) cover this same code.

## Current position

**Service:** `user-service` → `notification-service` (Kafka boundary)
**Flow:** Playlist video 8 — Kafka Integration in user-service for OTP and
Email Notifications.
**Currently on:** `user-service/src/kafka/producer/notification-producer.ts`
— covered the `NotificationProducer` class shell and its lazy-connect
`initialize()` method. User is new to Kafka, so a plain-English primer was
given (topic/producer/message/consumer/broker/partition-key, all mapped to
this exact file — see "Kafka primer given" note below) before continuing
into the code.

**Open question, not yet answered:** why does this file export
`new NotificationProducer()` (a pre-built instance) instead of a
`getInstance()` static method like `RedisClient` uses (video 3)? Both solve
"one shared thing" but with different patterns — worth revisiting before
moving past this file.

**Not yet covered in this file:** `sendMessage<T>` (the generic send
helper — try/catch, logging, the `key || `${topic}-${Date.now()}``fallback),
and the two public methods`sendOtpEmail`/`sendWelcomeEmail`.

**Not yet opened:** `notification-service/` entirely (the consumer side —
`kafka/email-consumer.ts`, `services/email-service.ts`, `templates/index.ts`).

### Covered so far

- **`/send-otp`** — fully walked end to end (playlist video 5):
  - `auth.route.ts` — plain route registration, no middleware.
  - `auth.controller.ts::sendOtp` — zod validation, delegate to service, set
    `otp_session` httpOnly/secure/sameSite cookie, 200 response.
  - `auth.service.ts::sendOtp` — duplicate-user check, bcrypt hash password,
    build `meta`, call `generateAndStoreOtp`, publish to Kafka via
    `notificationProducer.sendOtpEmail` (awaited directly, no try/catch —
    unlike the welcome email later, which is fire-and-forget).
  - `otp.ts::hmacFor` — HMAC-SHA256(email + ":" + otp, secret).
  - `otp.ts::generateAndStoreOtp` — fully covered line by line: rate-limit
    check, generate 6-digit OTP, random `otpSessionId` (UUID), HMAC it,
    store `{hashedOtp, meta}` in Redis keyed by session (not email) with
    TTL, bump + refresh the rate counter.

- **`/verify-otp`** — in progress (playlist video 5, open thread):
  - `auth.route.ts` — plain route, no middleware. ✅
  - `auth.controller.ts::verifyOtp`:
    - zod validation (`zVerifyOtp.safeParse`) — ✅
    - read `otp_session` cookie, 400 if missing — ✅ (noted: this uses
      `throw`, while the zod check above uses `return ErrorResponse(...)` —
      two different error-handling styles in the same function)
    - `authservice.verifyOtp(...)` call + `201` response — ✅
  - `auth.service.ts::verifyOtp`:
    - call `verifyOtpViaUnHashing`, null check → `BadRequestError` — ✅
      (noted: "expired" and "wrong OTP" are collapsed into the same error,
      deliberately, so an attacker can't distinguish the two cases)
    - **not yet covered:** the `prisma.user.create(...)` block, the
      fire-and-forget welcome email, and stripping the password before
      returning `safeUser`.
  - `otp.ts::verifyOtpViaUnHashing` — in progress, line by line:
    - fetch `otp:session:<otpSessionId>` from Redis, null check — ✅
    - parse `{hashedOtp, meta}`, check per-email `otp:attempt:*` rate
      limit (separate from the per-email `otp:rate:*` send-limit in
      `generateAndStoreOtp`) — ✅
    - the comparison itself — ✅
      ```ts
      const hashedOtp = hmacFor({ email: meta.email, otp });
      if (crypto.timingSafeEqual(
        Buffer.from(hashedOtp, "hex"),
        Buffer.from(storedOtp, "hex"),
      )) {
      ```
      Covered: why `timingSafeEqual` instead of `===` (constant-time
      comparison to avoid a timing side-channel on early-exit string
      comparison), and what `Buffer.from(str, "hex")` does (hex string →
      raw bytes, needed because `timingSafeEqual` operates on buffers).
    - **not yet covered:** the success branch (deleting Redis keys on
      success to prevent replay) and the failure branch (incrementing the
      attempts counter).

- **`/login`** — fully walked end to end (playlist video 6):
  - `auth.route.ts` — plain route, no middleware. ✅
  - `auth.controller.ts::login` — zod validation (`zLogin.safeParse`,
    `return ErrorResponse` style, same as `sendOtp`/`verifyOtp`'s zod
    checks), `getDeviceFingerprint(req)`, delegate to
    `authservice.login`, set `accessToken` (15 min) and `refreshToken`
    (7 day) httpOnly/secure/sameSite cookies. ✅
  - `utils/device-fingerprint.ts::getDeviceFingerprint` — SHA-256 hash of
    `user-agent|ip|accept`, truncated to first 16 hex chars. Covered: why
    IP-in-the-hash makes this unstable (wifi switch/VPN/carrier handoff
    silently changes the fingerprint and forces a re-login on next
    `/refresh`); why truncating to 16 chars is an acceptable tradeoff here
    (this is a soft device-binding tag, not a cryptographic identity — a
    collision doesn't leak anyone's tokens). ✅
  - `auth.service.ts::login`: - `prisma.user.findUnique` + `!existingUser.password` check → throws
    `"Email not found"` — ✅ (noted: unlike `/verify-otp`'s deliberately
    collapsed "expired vs. wrong OTP" error, `/login` throws two
    _distinguishable_ errors — `"Email not found"` vs.
    `"Incorrect password"` — which lets an attacker enumerate valid
    emails; discussed as a real inconsistency, left as-is per
    learning-mode convention of not fixing while documenting/learning) - `bcrypt.compare(password, existingUser.password)` → throws
    `"Incorrect password"` — ✅ - `generateAccessToken` / `generateRefreshToken` — ✅. Covered: why
    `generateRefreshToken` returns `{ token, jti }` together instead of
    making the caller `jwt.decode()` the token back out afterward (the
    JTI already exists in memory at signing time; returning it avoids a
    redundant decode). Also covered: `jti` is a server-side-only
    tracking value, never sent to the client — only `accessToken`/
    `refreshToken` are. - `{ password: _password, ...safeUser }` strip — ✅ (same pattern as
    `verifyOtp`'s `safeUser`) - `Promise.all([...])` writing `refresh:<userId>:<deviceId>` → `jti`
    (TTL = `REFRESH_TOKEN_EXP_SEC`) and `user:<userId>` → cached
    `safeUser` JSON (TTL = `REDIS_USER_TTL`) concurrently, since neither
    write dsave
    iOS Developer
    Vtechfamily Solution India logo
    Vtechfamily Solution Indias
    3.4
    3 Reviews
    2-4 YrsNepends on the other — ✅ - returns `{ accessToken, refreshToken, loggedInUser: safeUser }` — ✅

- **`/refresh`** — fully walked end to end (playlist video 6):
  - `auth.route.ts` — plain route, no middleware. ✅
  - `auth.controller.ts::rotateRefreshToken` — reads `refreshToken` from
    cookies, `throw`s (not `return ErrorResponse`) if missing, recomputes
    `deviceId` via the same `getDeviceFingerprint(req)` used at login,
    delegates to `authservice.rotateRefreshToken`, reissues both cookies.
    ✅ (discussed: this recompute is exactly why a wifi/IP change forces a
    "Session expired" logout even with a perfectly valid refresh token)
  - `auth.service.ts::rotateRefreshToken`:
    - `verifyRefreshToken(refreshToken)` → `{ id: userId, jti }` — ✅
    - `redis.get('refresh:<userId>:<deviceId>')`, throws
      `ForbiddenError("Session expired")` if key missing — ✅
    - `storedJti !== jti` reuse-detection branch: deletes the Redis key
      and throws `"Refresh token reused"` — ✅. Covered: why this kills
      the _whole_ device session rather than just rejecting the one call
      (server can't tell attacker from legitimate caller when JTIs
      mismatch, since refresh tokens are single-use and Redis only ever
      holds the latest JTI).
    - new `generateAccessToken`/`generateRefreshToken` call using
      `payload.id` — ✅ (confirmed: yes, `/refresh` rotates _both_ tokens,
      not just the access token — that's what makes reuse of an old
      refresh token always detectable)
    - `redis.set` overwriting the JTI at the same key — ✅
    - returns `{ newAccessToken, newRefreshToken }` — ✅

- **Kafka primer given (video 8 kickoff):** user is new to Kafka. Covered,
  all mapped to `notification-producer.ts` specifically: topic (
  `KAFKA_TOPICS.OTP_EMAIL`) = named channel; producer = `user-service`
  writing messages; message = the JSON payload (`{ email, otp, ttlMinutes }`);
  consumer = `notification-service` reading them; broker = the Kafka
  container in `docker-compose.yml`; and why Kafka over direct HTTP
  (decoupling — `user-service` doesn't block on/depend on
  `notification-service` being up). Also covered: partition keys —
  `otp-${email}` keeps all of one user's OTP messages on the same
  partition, preserving order. Consumer groups not yet covered — flagged
  to explain when `notification-service`'s consumer is opened.

### Not yet started

- Rest of `verifyOtpViaUnHashing` (success/failure branches) — video 5.
- Rest of `authservice.verifyOtp` (user creation, welcome email, safeUser)
  — video 5.
- `utils/auth.ts` (`generateAccessToken`/`generateRefreshToken`/
  `verifyRefreshToken` internals) — referenced several times in `/login`
  and `/refresh` but never opened directly.
- Video 7 (Google Auth) — flagged in `docs/playlist-guide.md` as not
  actually built in this repo; optional/reference-only.
- Rest of video 8: `sendMessage<T>`/`sendOtpEmail`/`sendWelcomeEmail` in
  `notification-producer.ts`, then all of `notification-service/`.

## Resume instructions

Paste the prompt from `docs/learning-mode.md`, then say "resume where we
left off." Pick up at `notification-producer.ts`'s `sendMessage<T>` method
(the generic send helper), one block at a time. The singleton-pattern
question above (`new NotificationProducer()` vs. `RedisClient.getInstance()`)
is still open and worth circling back to before moving past this file.

Separately still open, lower priority: the video-5 thread (`verifyOtpViaUnHashing`
success/failure branches + rest of `authservice.verifyOtp`).
