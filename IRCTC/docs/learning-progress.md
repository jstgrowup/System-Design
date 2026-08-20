# Learning Progress — Auth System (user-service)

Tracks exactly where the code walkthrough (see `docs/learning-mode.md`) left
off, so the next session can resume mid-function instead of restarting.

## Current position

**Service:** `user-service`
**Flow:** Registration — `/send-otp` → `/verify-otp` → `/login` → `/refresh`
**Currently on:** `POST /verify-otp`

### Covered so far

- **`/send-otp`** — fully walked end to end:
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

- **`/verify-otp`** — in progress:
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
    - **currently here:** the comparison itself —
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

### Not yet started

- Rest of `verifyOtpViaUnHashing` (success/failure branches).
- Rest of `authservice.verifyOtp` (user creation, welcome email, safeUser).
- `/login` API (route → controller → service), including
  `generateAccessToken` / `generateRefreshToken` / JTI storage in Redis.
- `/refresh` API (rotateRefreshToken), including reuse-detection logic.

## Resume instructions

Paste the prompt from `docs/learning-mode.md`, then say: "resume where we
left off" — pick up at the `timingSafeEqual` block's success/failure
branches in `verifyOtpViaUnHashing`, one block at a time, waiting for a
green light after each.
