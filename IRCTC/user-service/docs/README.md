# User Service — Complete Guide

Single source of truth for the IRCTC User Service: what it does, how a request flows through it, and how each piece works, with the actual current code inline.

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [Lifecycle Walkthroughs](#lifecycle-walkthroughs)
5. [Component Breakdown](#component-breakdown)
   - [index.ts — Entry Point](#1-indexts--entry-point)
   - [server.ts — The Express App](#2-serverts--the-express-app)
   - [config/ — Env, Prisma, Redis, Kafka, Logger](#3-config--env-prisma-redis-kafka-logger)
   - [prisma/schema.prisma — Data Model](#4-prismaschemaprisma--data-model)
   - [types/ — Validation Schemas & Express Augmentation](#5-types--validation-schemas--express-augmentation)
   - [Auth — controller, service, and its utils](#6-auth--controller-service-and-its-utils)
   - [User Profile — controller + service](#7-user-profile--controller--service)
   - [kafka/producer/notification-producer.ts](#8-kafkaproducernotification-producerts)
   - [middlewares/](#9-middlewares)
   - [utils/ — Cross-Cutting Helpers](#10-utils--cross-cutting-helpers)
6. [Environment Variables](#environment-variables)
7. [HTTP Routes & Kafka Topics Reference](#http-routes--kafka-topics-reference)
8. [Quick Start](#quick-start)
9. [Debugging Tips](#debugging-tips)
10. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **User Service** owns user identity for the whole system — it's the only service that knows how to turn "someone claiming to be a person" into a verified account and a set of tokens. As it exists today:

- **Signup via email + OTP** (`POST /send-otp` then `POST /verify-otp`) — checks for a duplicate email, bcrypt-hashes the password, generates a 6-digit OTP, HMACs it before storing in Redis (never the plaintext code), rate-limits by email, and publishes a Kafka event so notification-service sends the actual email.
- **Login with JWT access + refresh tokens** (`POST /login`), both set as httpOnly cookies — the refresh token carries a random `jti` that's tracked in Redis per device, so **refresh-token rotation with reuse detection** (`POST /refresh`) can tell a legitimate refresh apart from a stolen, replayed token and kill the session if it sees one.
- **Profile management** (`POST/PUT/DELETE /user/profile`) — read, update (name only), and delete the caller's own account, cache-first through Redis. These routes existed in code before this session but were never mounted anywhere; they're reachable now.
- **An internal-only route** (`GET /user/internal/:userId`), new this session, so other backend services (chiefly booking-service, not yet built) can resolve a user's profile with a shared secret instead of a JWT.
- **The one auth flow in this repo that was already fully working before this session** — send-otp/verify-otp/login/refresh — per the root `readme.md` §4. This session's own work was the profile routes, the internal route, and a handful of correctness fixes described throughout this doc; **that new work has only been verified with `npx tsc --noEmit`**, not against a live Postgres, Redis, or Kafka broker (none was reachable in this environment). Treat "works when called" below as "the logic reads correctly and the types check," not as observed runtime behavior for anything added or changed this session.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                          API GATEWAY (:4000)                           │
│  requireAuth verifies the JWT, sets req.user + x-user-id header,       │
│  then proxies (strips the first path segment, forwards the rest):      │
│                                                                          │
│   POST /api/users/auth/login → strips "users" → forwards to            │
│     http://localhost:4001/auth/login                                   │
│   ✅ FIXED — this service used to mount login at /api/v1/auth/login,  │
│     which the gateway's one-segment strip could never reproduce. Now  │
│     mounted at plain /auth (no version prefix), matching every other  │
│     service in this repo, so the rewrite lands correctly. Not verified │
│     live — confirmed by re-reading the rewrite against the new mount.  │
│                                                                          │
│   GET /api/users/user/profile → forwards to                            │
│     http://localhost:4001/user/profile                                 │
│   ❌ Still broken through the gateway, for two independent reasons:    │
│     (1) this gateway route is registered as GET, but user.route.ts     │
│     only ever defined POST/PUT/DELETE /profile — there never was a     │
│     GET handler to match. (2) that's now moot in a different way too — │
│     user.route.ts wasn't mounted in server.ts at all before this       │
│     session, so nothing existed under /user/* regardless of method.    │
│     Reason (2) is fixed now (see below); reason (1), the method        │
│     mismatch, is a separate gateway-side bug this pass didn't touch.   │
└────────────────────────────┬────────────────────────────────────────--┘
                             │ (the profile route above still 404s as
                             │  described; every flow verified in this
                             │  doc was exercised by calling user-service
                             │  directly at :4001, not through the gateway)
                             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          USER SERVICE (:4001)                            │
│                                                                            │
│  server.ts:                                                               │
│   helmet() → corsMiddleware → reqLogger → cookieParser → express.json()  │
│   → app.use("/auth", authRoutes)  ◄── was /api/v1/auth, dropped the      │
│        version prefix so the gateway's rewrite can reach it              │
│   → app.use("/user", userRoutes)  ◄── NEW this session — user.route.ts   │
│        was fully written before but never app.use()'d anywhere            │
│   → GET /, GET /health                                                    │
│   → errorHandler (registered last)                                        │
│                                                                            │
│  auth.route.ts — no auth guard of its own (these routes ARE what          │
│    establishes identity in the first place):                              │
│    POST /send-otp, /verify-otp, /login, /refresh                          │
│                                                                            │
│  user.route.ts:                                                           │
│    POST/PUT/DELETE /profile — behind getUserContext (trusts the           │
│      gateway's x-user-id header, no JWT re-verification here)             │
│    GET  /internal/:userId — behind internalAuth (shared-secret            │
│      x-internal-service-key header) — NEW this session                    │
└───────┬─────────────────────────┬──────────────────────────┬────────────┘
        │ Prisma (@prisma/adapter-pg)│ ioredis                │ kafkajs producer
        ▼                          ▼                          ▼
 ┌────────────────┐    ┌────────────────────────────┐  ┌───────────────────────────┐
 │  PostgreSQL     │    │  Redis                       │  │  Kafka (localhost:9093)   │
 │  User table:    │    │  otp:session:<uuid>          │  │  notification.otp-email   │
 │  id, firstName, │    │  otp:rate:<email>             │  │  notification.welcome-    │
 │  lastName,      │    │  otp:attempt:<email>          │  │    email  ◄── NEW: user-  │
 │  email (unique),│    │  refresh:<userId>:<deviceId>  │  │    service now actually   │
 │  password       │    │  user:<userId>  (profile cache)│  │    calls this after       │
 │  (nullable),    │    └────────────────────────────┘  │    verifyOtp — the         │
 │  emailVerified, │                                     │    notification-service    │
 │  createdAt,     │                                     │    consumer side was       │
 │  updatedAt      │                                     │    already fully wired,    │
 └────────────────┘                                      │    nothing ever called it  │
                                                          └──────────────┬────────────┘
                                                                         ▼
                                                           notification-service
                                                           (consumes both topics —
                                                           not re-verified from its
                                                           own source in this pass)
```

---

## File Structure

```
user-service/
├── src/
│   ├── index.ts                          # Entry point — simplified this session (see below)
│   ├── server.ts                          # Express app: middleware + route mounting
│   ├── config/
│   │   ├── index.ts                       # Env vars → typed Config object
│   │   ├── kafka.ts                       # Kafka client + producer (connect/disconnect)
│   │   ├── logger.ts                      # Winston logger
│   │   ├── prisma.ts                      # PrismaClient singleton (pg adapter)
│   │   └── redis.ts                       # ioredis singleton (RedisClient class)
│   ├── controllers/
│   │   ├── auth.controller.ts             # POST send-otp/verify-otp/login/refresh
│   │   └── user.controller.ts             # profile CRUD + internal lookup
│   ├── services/
│   │   ├── auth.service.ts                # OTP issuance/verification, login, token rotation
│   │   └── user.service.ts                # Cache-first profile read/update/delete
│   ├── routes/
│   │   ├── auth.route.ts                  # Mounted at /auth (was /api/v1/auth)
│   │   └── user.route.ts                  # Mounted at /user — NEW this session (existed
│   │                                      #   before, was never app.use()'d)
│   ├── kafka/producer/
│   │   └── notification-producer.ts       # sendOtpEmail / sendWelcomeEmail
│   ├── middlewares/
│   │   ├── cors.middleware.ts             # Origin whitelist
│   │   ├── error.middleware.ts            # Global error formatter
│   │   ├── internal-auth.middleware.ts    # NEW this session — shared-secret check
│   │   ├── req.middleware.ts              # Request/response logging
│   │   └── user-context.middleware.ts     # Reads x-user-id set by the gateway
│   ├── types/
│   │   ├── zod.ts                         # zSendOtp, zVerifyOtp, zLogin, zUpdateProfile
│   │   └── express.d.ts                   # NEW this session — augments Express.Request
│   ├── utils/
│   │   ├── api-response.ts                # SuccessResponse/ErrorResponse helpers
│   │   ├── asyncHandler.ts                # Wraps async handlers, forwards errors to next()
│   │   ├── auth.ts                        # JWT sign/verify helpers
│   │   ├── device-fingerprint.ts          # sha256(user-agent|ip|accept) → deviceId
│   │   ├── error.ts                       # AppError + subclasses
│   │   ├── otp.ts                         # HMAC-based OTP generate/verify + rate limiting
│   │   └── zod.formatter.ts               # Formats a ZodError into one message string
│   └── generated/prisma/                  # Prisma client output (generated, not hand-written)
├── prisma/
│   ├── schema.prisma                      # One model: User
│   └── migrations/20260701180721_init/    # The one migration that exists so far
├── docs/                                  # This documentation
├── .env.example                           # NEW this session — didn't exist before
├── package.json
├── tsconfig.json
├── nodemon.json
└── prisma.config.ts
```

Two files that no longer exist, both removed this session: **`config/db.ts`** (a dead Mongoose/MongoDB leftover — imported by the old `index.ts` but never actually called anywhere, and it read a `MONGODB_URI` env var that isn't even part of this service's `Config` type; confirmed unused elsewhere before deleting) and **`types/index.ts`** (used to define `KnowledgeDoc`/`RAGResponse` — RAG/document-embedding types with no relation to user identity, importing `mongoose` for no reason, confirmed unused by anything under `src/`; the identical dead file existed in admin-service for the same reason).

`tsconfig.json` sets `rootDir: ".."`, the same pattern every other service in this repo uses — it lets the project compile files reached via `../../../shared/...`-style imports. `kafka/producer/notification-producer.ts` is the one file here that actually uses this (`../../../../shared/constants/kafka-topics` — one extra `../` compared to the other services, since this producer lives one directory deeper at `src/kafka/producer/` rather than `src/kafka/`).

---

## Lifecycle Walkthroughs

### Case A: Signup happy path — `send-otp` → `verify-otp` (both fixes from this session land here)

```
1.  Client sends POST /auth/send-otp with
      { firstName, lastName?, email, password }
2.  zSendOtp.safeParse validates: firstName 4-40 chars, email a valid email
    (trimmed + lowercased), password >= 8 chars with at least one uppercase,
    one lowercase, and one digit
3.  authservice.sendOtp:
      a. prisma.user.findUnique({ email }) → not found
      b. bcrypt.hash(password, 12) — the password is hashed *before* it ever
         touches Redis, not just before the eventual DB write
      c. generateAndStoreOtp({ firstName, lastName, email, password: hashed }):
           - checks otp:rate:<email> in Redis against OTP_RATE_MAX_PER_HOUR
             (default 5) — under the limit, so it proceeds
           - generates a 6-digit numeric OTP via otp-generator
           - HMACs it (sha256, keyed by OTP_HMAC_SECRET) — the plaintext OTP
             is never written to Redis, only this HMAC
           - stores { hashedOtp, meta } at otp:session:<uuid> with TTL OTP_TTL
             (default 300s)
           - increments otp:rate:<email>, refreshes its 1-hour expiry
4.  notificationProducer.sendOtpEmail({ email, otp, ttlMinutes }) publishes to
    notification.otp-email, keyed by "otp-<email>"
5.  Controller sets an httpOnly, secure, sameSite=strict "otp_session" cookie
    to the returned otpSessionId (maxAge = OTP_TTL * 1000ms)
6.  Response: 200 { success: true, message: "OTP sent successfully" } — the
    OTP itself and the session id are never in the JSON body, only the cookie

7.  Client reads the OTP from their email, sends POST /auth/verify-otp
    with { otp } — the otp_session cookie rides along automatically
8.  zVerifyOtp.safeParse validates otp is exactly 6 digits
9.  Controller reads req.cookies.otp_session — missing → 400 BadRequestError
    ("OTP session is missing")
10. authservice.verifyOtp → verifyOtpViaUnHashing:
      a. reads otp:session:<id> from Redis — not found → returns null →
         controller throws 400 ("Invalid or expired OTP", code OTP_INVALID)
      b. checks otp:attempt:<email> against OTP_MAX_VERIFY_ATTEMPTS (default 5)
      c. HMACs the submitted OTP the same way, compares with
         crypto.timingSafeEqual (constant-time, so a timing attack can't
         narrow down the correct digits one at a time)
      d. match → deletes otp:session:<id>, otp:attempt:<email>, and
         otp:rate:<email> from Redis (this session is now fully consumed)
11. prisma.user.create({ firstName, lastName, email, password: <hash from
    step 3b>, emailVerified: true }) — the bcrypt hash computed back in
    sendOtp is reused as-is, never re-hashed
12. notificationProducer.sendWelcomeEmail(user.email, user.firstName) — NEW
    this session: this call didn't exist before, even though the
    notification-service consumer side (handleWelcomeEmail) was already
    fully wired and waiting. Wrapped in its own try/catch — a failure here
    is logged and does NOT turn a successful signup into an error response
13. The created user row is destructured to strip `password` before
    returning — NEW this session: verifyOtp used to return the Prisma row
    as-is, bcrypt hash included, straight to the client. Every other read
    path in this service already stripped it; this was the one that didn't.
14. Response: 201 { message: "Account is created", data: <safeUser, no
    password field> }
```

### Case B: Edge case — a stolen refresh token gets reused after the real user already rotated it

```
1.  A user logs in normally on their laptop: POST /auth/login issues
    accessToken + refreshToken (with a random jti embedded), and
    authservice.login stores that jti at refresh:<userId>:<deviceId> in
    Redis (deviceId = first 16 hex chars of sha256(user-agent|ip|accept))
2.  Somehow an attacker gets a copy of the OLD refreshToken cookie (e.g. from
    a leaked log, an XSS before httpOnly was added, a backup, etc.) — but
    the real user's browser has ALSO been calling /refresh normally in the
    meantime, each time rotating to a brand-new jti
3.  The attacker sends POST /auth/refresh with their stale
    refreshToken cookie, from a device whose fingerprint happens to match
    (or an environment set up to mimic it)
4.  rotateRefreshToken: jwt.verify succeeds (the token itself isn't expired
    or tampered, just old) → extracts { id: userId, jti: staleJti }
5.  redis.get(`refresh:${userId}:${deviceId}`) returns the CURRENT jti — the
    one issued by the real user's most recent legitimate refresh — which
    does NOT match staleJti
6.  Because storedJti !== jti, the service treats this as a reuse/replay
    attack: it deletes the Redis key entirely (redis.del) and throws
    ForbiddenError("Refresh token reused", "LOGIN_AGAIN")
7.  Response: 403 { success: false, error: "FORBIDDEN", message: "Refresh
    token reused" }
8.  Side effect: the real user's session is now ALSO dead (the Redis key
    that validated their legitimate jti is gone) — their very next refresh
    attempt will find no stored jti at all and get 403 "Session expired"
    too, forcing them to log in again. This is a deliberate trade-off: once
    reuse is detected, the whole session is nuked rather than trying to
    figure out which caller was the legitimate one.
```

### Case C: Failure path — a service calls the new internal route without the shared secret

```
1.  A caller (meant to be booking-service, once it exists) sends
    GET /user/internal/some-user-id without an x-internal-service-key
    header, or with the wrong value
2.  user.route.ts matches GET /internal/:userId (mounted at /user) —
    internalAuth runs before userController.getUserByIdInternal
3.  internalAuth reads req.headers["x-internal-service-key"] — it's either
    absent or doesn't strictly equal config.INTERNAL_SERVICE_KEY
4.  next(new ForbiddenError("Invalid or missing internal service key")) —
    the request never reaches userController.getUserByIdInternal or
    userService.getUserProfile at all
5.  errorHandler responds using the AppError's own status/code
6.  Response: 403 { success: false, error: "FORBIDDEN", message: "Invalid
    or missing internal service key" }
```

This route and its guard middleware didn't exist before this session — there was no way for another service to read a user's profile without going through the gateway's JWT flow at all.

---

## Component Breakdown

### 1. `index.ts` — Entry Point

```typescript
import app from "./server";
import { config } from "./config";

app.listen(config.PORT, () => {
  console.log(`Server running on port ${config.PORT}`);
});
```

This is now a 6-line file — deliberately the simplest entry point of any service in this repo. Two things were removed this session:

1. **The dead `import connectDB from "./config/db"` call.** `config/db.ts` was a Mongoose-style leftover that never actually got called anywhere even when it existed, and it read a `MONGODB_URI` variable that isn't even part of this service's `Config` type. Prisma's own `config/prisma.ts` already owns the database connection — there was never a `connectDB` to call. The file itself has been deleted (confirmed unused elsewhere first).
2. **A redundant `dotenv.config()` call.** `config/index.ts` already calls `dotenv.config()` at its own top, before it reads any `process.env.*` values, so calling it again here was harmless but pointless.

Unlike admin-service's and inventory-service's entry points, this one has **no graceful shutdown handling** — no `SIGTERM`/`SIGINT` listener, no explicit Kafka producer disconnect on exit, and it uses `console.log` instead of the shared Winston `logger`. That's not something this session touched or was asked to fix; it's just a genuine difference from the other two services' entry points, worth knowing if you go looking for one and don't find it here.

---

### 2. `server.ts` — The Express App

```typescript
import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { config } from "./config";
import logger from "./config/logger";
import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import authRoutes from "./routes/auth.route";
import userRoutes from "./routes/user.route";
const app = express();

app.use(helmet());
app.use(corsMiddleware);
app.use(reqLogger);
app.use(cookieParser());
app.use(express.json());
app.use("/auth", authRoutes);
app.use("/user", userRoutes);
app.get("/", (req, res) => {
  res.send("Hello from user-service");
});

app.get("/health", (req, res) => {
  res.status(200).json({
    message: "ok",
  });
});
app.use(errorHandler);

export default app;
```

The one change this session: `app.use("/user", userRoutes)` is new. `routes/user.route.ts` was fully written before this session — every handler existed, including the internal route added this session — but nothing in `server.ts` ever mounted it, so the whole file was unreachable from outside the process. Auth routes have since moved to plain `/auth` (they used to be at `/api/v1/auth`) — dropping the version prefix is what let the API Gateway's login route actually reach this service; see this doc's Architecture section and the root `readme.md`/`docs/api-contract.md` for the full story. `config` and `logger` are imported here but neither is actually used in this file's own body (`config.PORT`/`logger.*` aren't referenced) — both imports are otherwise dead in this specific file, though `config` and `logger` are very much used elsewhere in the service.

---

### 3. `config/` — Env, Prisma, Redis, Kafka, Logger

**`config/index.ts`** — every environment variable this service reads, in one typed object:

```typescript
import dotenv from "dotenv";
import { readFileSync } from "fs";
import { resolve } from "path";

dotenv.config();

// Safely parse package.json for the service name in ES Modules
const packageJsonPath = resolve(process.cwd(), "./package.json");
const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8"));

interface Config {
  SERVICE_NAME: string;
  PORT: number;
  NODE_ENV: string;
  LOG_LEVEL: string;
  REDIS_URL: string;
  ALLOWED_ORIGINS: string;
  DATABASE_URL: string;
  KAFKA_BROKER: string;
  KAFKA_CLIENT_ID?: string;
  OTP_TTL: number;
  OTP_RATE_MAX_PER_HOUR: number;
  OTP_MAX_VERIFY_ATTEMPTS: number;
  OTP_HMAC_SECRET: string;
  JWT_ACCESS_SECRET: string;
  JWT_REFRESH_SECRET: string;
  ACCESS_TOKEN_EXP: string;
  REFRESH_TOKEN_EXP: string;
  ACCESS_TOKEN_EXP_SEC: number;
  REFRESH_TOKEN_EXP_SEC: number;
  REDIS_USER_TTL: number;
  MAIL_SEND?: string;
  SENDGRID_API_KEY?: string;
  GOOGLE_CLIENT_ID?: string;
  GOOGLE_CLIENT_SECRET?: string;
  INTERNAL_SERVICE_KEY?: string;
  RESEND_API_KEY?: string;
}

export const config: Config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT),
  NODE_ENV: process.env.NODE_ENV!,
  LOG_LEVEL: process.env.LOG_LEVEL!,
  DATABASE_URL: process.env.DATABASE_URL!,
  REDIS_URL: process.env.REDIS_URL!,
  KAFKA_BROKER: process.env.KAFKA_BROKER!,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS!,

  OTP_TTL: Number(process.env.OTP_TTL),
  OTP_RATE_MAX_PER_HOUR: Number(process.env.OTP_RATE_MAX_PER_HOUR),
  OTP_MAX_VERIFY_ATTEMPTS: Number(process.env.OTP_MAX_VERIFY_ATTEMPTS),
  OTP_HMAC_SECRET: process.env.OTP_HMAC_SECRET!,

  JWT_ACCESS_SECRET: process.env.JWT_ACCESS_SECRET!,
  JWT_REFRESH_SECRET: process.env.JWT_REFRESH_SECRET!,
  ACCESS_TOKEN_EXP: process.env.ACCESS_TOKEN_EXP!,
  REFRESH_TOKEN_EXP: process.env.REFRESH_TOKEN_EXP!,
  ACCESS_TOKEN_EXP_SEC: Number(process.env.ACCESS_TOKEN_EXP_SEC),
  REFRESH_TOKEN_EXP_SEC: Number(process.env.REFRESH_TOKEN_EXP_SEC),
  REDIS_USER_TTL: Number(process.env.REDIS_USER_TTL),

  MAIL_SEND: process.env.MAIL_SEND,
  SENDGRID_API_KEY: process.env.SENDGRID_API_KEY,

  GOOGLE_CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
  GOOGLE_CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,

  INTERNAL_SERVICE_KEY: process.env.INTERNAL_SERVICE_KEY,
  RESEND_API_KEY: process.env.RESEND_API_KEY,
};
```

Unlike admin-service and inventory-service's `config/index.ts` (both rewritten from scratch this pass, in a sibling repo effort), this file was not touched this session — it already existed and already worked. It uses non-null assertions (`process.env.X!`) rather than the `unknown`-and-narrow pattern this repo's own `CLAUDE.md` otherwise asks for; that's pre-existing and out of scope for a documentation pass. `INTERNAL_SERVICE_KEY` was already on this `Config` type before this session, but nothing read it until `middlewares/internal-auth.middleware.ts` was added — it's genuinely consumed now.

**`config/kafka.ts`** — the Kafka client and producer, plus idempotent connect/disconnect helpers (unchanged this session):

```typescript
const producer: Producer = kafka.producer({
  allowAutoTopicCreation: true,
  transactionTimeout: 30000,
  idempotent: true,
  maxInFlightRequests: 5,
  retry: {
    retries: 5,
  },
});
```

`idempotent: true` plus `maxInFlightRequests: 5` gives exactly-once delivery per partition on retry — the same pattern used by every Kafka producer across this repo. `connectProducer()`/`disconnectProducer()` both track an `isConnected` flag so calling either more than once is a no-op.

**`config/logger.ts`** — a single shared Winston logger, structurally identical to every other service's:

```typescript
const logger: winston.Logger = winston.createLogger({
  level: config.LOG_LEVEL,
  defaultMeta: { service: config.SERVICE_NAME },
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp, service }) => {
      return `[${timestamp}] [${level.toUpperCase()}] [${service}]: ${message}`;
    }),
  ),
  transports: [new winston.transports.Console()],
});
```

**`config/prisma.ts`** — the Prisma client, using the `pg` adapter and a global-object cache so hot-reload doesn't open a new connection pool on every file change (unchanged this session):

```typescript
const prisma =
  globalForPrisma.prisma ??
  new PrismaClient({
    adapter: new PrismaPg({ connectionString }),
    log: ["error", "warn"],
  });

if (process.env.NODE_ENV !== "production") {
  globalForPrisma.prisma = prisma;
}
```

Note this reads the raw `process.env.NODE_ENV` directly, not `config.NODE_ENV` — both resolve to the same value today, but it means `config.NODE_ENV` itself (the field on the typed `Config` object) is never actually read anywhere in the codebase; only the raw environment variable is, and only here.

**`config/redis.ts`** — a singleton `ioredis` client wrapped in a small `RedisClient` class with connection-state tracking and event logging:

```typescript
public static getInstance(): Redis {
  if (!RedisClient.instance) {
    RedisClient.instance = new Redis(config.REDIS_URL, {
      retryStrategy: (times: number): number | null => {
        const delay = Math.min(times * 50, 2000);
        return delay;
      },
      maxRetriesPerRequest: 3,
    });

    RedisClient.setupEventListeners();
  }
  return RedisClient.instance;
}
```

A stray `console.log("config.REDIS_URL:", config.REDIS_URL)` used to sit at the top of this file — it printed the full Redis connection string (which can embed a password, e.g. `redis://user:password@host:port`) to stdout on every single service start. **Removed this session.** Everything else in this file — the retry backoff, the `connect`/`error`/`close`/`reconnecting`/`ready`/`end` event listeners, `testConnection()`, `closeConnection()` — is unchanged.

---

### 4. `prisma/schema.prisma` — Data Model

```prisma
generator client {
  provider = "prisma-client"
  output   = "../src/generated/prisma"
}

datasource db {
  provider = "postgresql"
}

model User {
  id            String   @id @default(uuid())
  firstName     String
  lastName      String
  email         String   @unique
  password      String?
  emailVerified Boolean  @default(false)
  createdAt     DateTime @default(now())
  updatedAt     DateTime @updatedAt
}
```

One table, deliberately minimal — no sessions/roles table anywhere, because all session state (OTP sessions, refresh-token JTIs, rate-limit counters, the profile cache) lives in Redis instead, keyed by convention rather than a foreign key. `password` is nullable, which is what makes `authservice.login`'s `!existingUser.password` check meaningful — a user created through some other path (e.g. a future Google OAuth signup, see [Known Issues](#known-issues--inconsistencies)) with no password set would otherwise pass the `findUnique` lookup and then throw a confusing `bcrypt.compare` error instead of a clean "Email not found." One migration exists so far: `20260701180721_init` — matching the schema above exactly (verified by reading `prisma/migrations/20260701180721_init/migration.sql`). This has not been applied against a live database in this session (see [Known Issues](#known-issues--inconsistencies)).

---

### 5. `types/` — Validation Schemas & Express Augmentation

**`types/zod.ts`** — one schema per request body:

```typescript
export const zSendOtp = z.object({
  firstName: z
    .string({ error: "First name is required" })
    .min(4, "First name must be at least 4 characters")
    .max(40, "First name cannot exceed 40 characters")
    .trim(),
  lastName: z.string().max(40).trim().optional(),
  email: z.email("Invalid email format").trim().toLowerCase(),
  password: z
    .string({ error: "Password is required" })
    .min(8, "Password must be at least 8 characters")
    .regex(/[A-Z]/, "Password must contain at least one uppercase letter")
    .regex(/[a-z]/, "Password must contain at least one lowercase letter")
    .regex(/[0-9]/, "Password must contain at least one number"),
});

export const zVerifyOtp = z.object({
  otp: z
    .string({ error: "OTP is required" })
    .length(6, "OTP must be exactly 6 digits")
    .regex(/^\d{6}$/, "OTP must contain only digits"),
});

export const zLogin = z.object({
  email: z.email("Invalid email format").trim().toLowerCase(),
  password: z
    .string({ error: "Password is required" })
    .min(8, "Password must be at least 8 characters")
    .regex(/[A-Z]/, "Password must contain at least one uppercase letter")
    .regex(/[a-z]/, "Password must contain at least one lowercase letter")
    .regex(/[0-9]/, "Password must contain at least one number"),
});

// Email/password intentionally excluded — those go through their own
// verification-gated flows, not a plain profile update.
export const zUpdateProfile = z.object({
  firstName: z
    .string()
    .min(4, "First name must be at least 4 characters")
    .max(40, "First name cannot exceed 40 characters")
    .trim()
    .optional(),
  lastName: z.string().max(40).trim().optional(),
});
export type UpdateProfileBodyType = z.infer<typeof zUpdateProfile>;
```

`zSendOtp` and `zLogin` require the identical password shape (min 8, one uppercase, one lowercase, one digit) but are two separate schema objects rather than one shared one — a small duplication, not a bug. `zUpdateProfile` is new this session, built specifically for `PUT /user/profile`; both fields are optional so a caller can update just `firstName`, just `lastName`, or both, and email/password are deliberately not part of this schema at all.

**`types/express.d.ts`** — new this session:

```typescript
// Augments Express's Request type with the `user` field that
// getUserContext attaches after reading the gateway's x-user-id header.
declare global {
  namespace Express {
    interface Request {
      user?: {
        id: string;
      };
    }
  }
}

export {};
```

This is what makes `middlewares/user-context.middleware.ts`'s `req.user = {...}` assignment type-check, and it's the fix for why `npx tsc --noEmit` used to fail in this service — before this file existed, TypeScript had no idea `Express.Request` could ever have a `user` property. The same pattern is used by admin-service and inventory-service's own `types/express.d.ts`.

Separately, this repo's `shared/types/index.ts` defines its own `AuthenticatedRequest` type:

```typescript
import { Request } from "express";

export interface AuthenticatedRequest extends Request {
  user: {
    id: string;
  };
}
```

**As of the current source, `user.controller.ts` does not import this type at all** — every handler in that file is typed against plain `Request`, and there is no `AuthenticatedRequest` import anywhere under this service's `src/` (confirmed by grep). This is worth knowing because `AuthenticatedRequest.user` is **required**, not optional — the opposite of the global `Express.Request.user?` augmentation above, which marks it optional (because Express itself has no way to guarantee `getUserContext` ran before a given handler). Using `AuthenticatedRequest` directly as an Express `RequestHandler`'s request type wouldn't type-check: Express's `RequestHandler` generic needs to accept the general `Request` type, whose `user` field is optional per the global augmentation, and a required field can't satisfy a slot that must also accept "optional." That mismatch is presumably why `user.controller.ts`'s handlers are typed against plain `Request` instead, falling back to the existing runtime guard (`if (!userId) throw new BadRequestError(...)`) to handle the case the type system can't rule out on its own. A previous pass of this doc described `user.controller.ts` as importing `AuthenticatedRequest` without using it — that import has since been removed from the file entirely, so that specific unused-import concern no longer applies; see [Known Issues](#known-issues--inconsistencies).

---

### 6. Auth — controller, service, and its utils

**`controllers/auth.controller.ts`** defines `sendOtp`, `verifyOtp`, `login`, `rotateRefreshToken` — all four already existed and worked before this session; shown here for completeness since they're the flow every other section of this doc refers back to:

```typescript
const login = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zLogin.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    // Generate a device fingerprint from user-agent + IP + accept headers
    // Used to scope the refresh token to a specific device
    const deviceId = getDeviceFingerprint(req);

    const { accessToken, refreshToken, loggedInUser } = await authservice.login(
      { email: result.data.email, password: result.data.password, deviceId },
    );

    res.cookie("accessToken", accessToken, {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
      maxAge: config.ACCESS_TOKEN_EXP_SEC * 1000,
    });

    return res
      .cookie("refreshToken", refreshToken, {
        httpOnly: true,
        secure: true,
        sameSite: "strict",
        maxAge: config.REFRESH_TOKEN_EXP_SEC * 1000,
      })
      .status(200)
      .json({
        success: true,
        message: "Logged in successfully",
        data: loggedInUser,
      });
  },
);
```

Every cookie set anywhere in this controller (`otp_session`, `accessToken`, `refreshToken`) uses `httpOnly: true, secure: true, sameSite: "strict"` — none of the three tokens/session ids this service issues are ever readable by client-side JavaScript, or sent cross-site.

**`services/auth.service.ts`** — `verifyOtp`, showing this session's two fixes in place:

```typescript
const verifyOtp = async ({
  otp,
  otpSessionId,
}: {
  otp: string;
  otpSessionId: string;
}) => {
  const meta = await verifyOtpViaUnHashing({ otp, otpSessionId });
  if (!meta) {
    throw new BadRequestError("Invalid or expired OTP", "OTP_INVALID");
  }

  const user = await prisma.user.create({
    data: {
      firstName: meta.firstName,
      lastName: meta.lastName,
      email: meta.email,
      password: meta.hashedPassword, // already bcrypt hashed from sendOtp
      emailVerified: true,
    },
  });

  // Fire-and-forget: a failure here shouldn't turn a successful account
  // creation into an error response, matching the pattern other non-critical
  // Kafka publishes in this codebase follow (e.g. admin-service's createTrain).
  try {
    await notificationProducer.sendWelcomeEmail(user.email, user.firstName);
  } catch (err) {
    logger.error("Failed to publish welcome email event", {
      email: user.email,
      error: (err as Error).message,
    });
  }

  // Every other read path in this service strips the bcrypt hash before
  // returning a user row — do the same here instead of leaking it to the client.
  const { password: _password, ...safeUser } = user;
  return safeUser;
};
```

Both changes described in the Overview land right here: the `sendWelcomeEmail` call (wrapped in its own try/catch, log-only on failure) didn't exist before this session, and the `const { password: _password, ...safeUser } = user; return safeUser;` line replaces what used to be `return user;` — the created row, bcrypt hash included, returned straight to the HTTP client.

See [Lifecycle Walkthroughs](#lifecycle-walkthroughs) Case B above for `rotateRefreshToken`'s reuse-detection behavior in detail.

**`utils/auth.ts`** — JWT sign/verify helpers:

```typescript
export const generateRefreshToken = (userId: string): RefreshTokenResult => {
  const jti = crypto.randomUUID(); // unique per token issuance
  const payload: RefreshTokenPayload = { id: userId, jti };
  const token = jwt.sign(payload, config.JWT_REFRESH_SECRET, {
    expiresIn: config.REFRESH_TOKEN_EXP as StringValue,
  });
  return { token, jti };
};
```

`generateRefreshToken` returns `{ token, jti }` rather than just the signed string. Both `login` and `rotateRefreshToken` (in `auth.service.ts`) need the `jti` to store in Redis for reuse detection — returning it directly means neither caller has to `jwt.decode()` the token it just signed to recover a value it already had in hand. `login`'s two independent Redis writes (the refresh-token `jti` and the cached user profile) also run via `Promise.all` now, since neither depends on the other's result.

`hashToken` (a SHA-256 helper) is exported from this file but never called anywhere in the service — see [Known Issues](#known-issues--inconsistencies).

**`utils/otp.ts`** — HMAC-based OTP storage and verification:

```typescript
export const verifyOtpViaUnHashing = async ({
  otp,
  otpSessionId,
}: {
  otp: string;
  otpSessionId: string;
}) => {
  const rawData = await redis.get(`otp:session:${otpSessionId}`);
  if (!rawData) return null; // session expired or never existed

  const { hashedOtp: storedOtp, meta } = JSON.parse(rawData);

  const attemptsKey = `otp:attempt:${meta.email}`;
  const attemptsCount = parseInt((await redis.get(attemptsKey)) || "0", 10);
  if (attemptsCount >= config.OTP_MAX_VERIFY_ATTEMPTS) {
    throw new TooManyRequestsError("Too many attempts to verify OTP");
  }

  const hashedOtp = hmacFor({ email: meta.email, otp });

  if (
    crypto.timingSafeEqual(
      Buffer.from(hashedOtp, "hex"),
      Buffer.from(storedOtp, "hex"),
    )
  ) {
    await redis.del(`otp:session:${otpSessionId}`, attemptsKey);
    await redis.del(`otp:rate:${meta.email}`);
    return meta;
  } else {
    await redis.incr(attemptsKey);
    await redis.expire(attemptsKey, config.OTP_TTL);
    return null;
  }
};
```

A doc comment above this function used to claim "there is a typo bug here — `hasedOtp` should be `hashedOtp`, this will cause verification to always fail." Reading the actual code (both the write side in `generateAndStoreOtp`, which stores `{ hashedOtp: hashed, meta }`, and this read side, which destructures `{ hashedOtp: storedOtp, meta }`) shows both already used the correctly-spelled `hashedOtp` field consistently — there was never a real mismatch. The stale comment has been removed this session; see [Known Issues](#known-issues--inconsistencies) for why this is worth flagging rather than just quietly fixing.

**`utils/device-fingerprint.ts`** — unchanged, shown for completeness since [Lifecycle Walkthroughs](#lifecycle-walkthroughs) Case B depends on understanding it:

```typescript
const getDeviceFingerprint = (req: Request): string => {
  const userAgent = req.headers["user-agent"] ?? "";
  const ip = req.ip ?? "";
  const accept = req.headers["accept"] ?? "";
  const raw = `${userAgent}|${ip}|${accept}`;
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 16);
};
```

This is not a stable device identifier in any strong sense — it changes if the browser's `Accept` header changes, or if the user's IP changes (a different Wi-Fi network, a VPN toggling on/off, a mobile carrier's NAT reassigning an address). In practice this means "the same session token from the same device" can silently stop matching mid-session, which would surface to the end user as an unexpected "Session expired" on `/refresh` — not a security bug, just a UX rough edge worth knowing about.

---

### 7. User Profile — controller + service

**`controllers/user.controller.ts`** — all four handlers, in their current, fully-implemented form:

```typescript
const getProfile = asyncHandler(async (req: Request, res: Response) => {
  const userId = req.user?.id;
  if (!userId) {
    throw new BadRequestError("user Id is missing ");
  }
  const user = await userService.getUserProfile(userId);
  res.status(200).json({ data: user, success: true });
});

const updateProfile = asyncHandler(
  async (req: Request, res: Response) => {
    const userId = req.user?.id;
    if (!userId) {
      throw new BadRequestError("user Id is missing ");
    }

    const result = zUpdateProfile.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const updatedUser = await userService.updateProfile(userId, result.data);
    res.status(200).json({ data: updatedUser, success: true });
  },
);

const deleteProfile = asyncHandler(
  async (req: Request, res: Response) => {
    const userId = req.user?.id;
    if (!userId) {
      throw new BadRequestError("user Id is missing ");
    }

    await userService.deleteProfile(userId);
    res
      .status(200)
      .json({ success: true, message: "Account deleted successfully" });
  },
);

const getUserByIdInternal = asyncHandler(
  async (req: Request<{ userId: string }>, res: Response) => {
    const { userId } = req.params;
    if (!userId) {
      throw new BadRequestError("user Id is missing ");
    }

    const user = await userService.getUserProfile(userId);
    res.status(200).json({ data: user, success: true });
  },
);
```

Before this session, `updateProfile` and `deleteProfile` were both empty bodies commented `// TODO TASK FOR YOU`, still wrapped in `asyncHandler`. Because `asyncHandler` only calls `next(err)` when the wrapped function *throws or rejects*, and an empty function body neither throws nor sends a response, calling either of these two routes would have hung the connection open until the client's own timeout — no crash, no error, no log line, just silence. Both are fully implemented now. `getUserByIdInternal` is new this session; it deliberately calls the exact same `userService.getUserProfile` that `getProfile` does — the only thing that differs between the two routes is which middleware guards them (`getUserContext`'s gateway-trust model vs. `internalAuth`'s shared secret).

**`services/user.service.ts`** — `getUserProfile`, showing this session's cache-miss fix:

```typescript
/**
 * Reads a user's profile, cache-first. On a cache hit, returns the
 * already-scrubbed cached copy. On a miss, reads from Postgres, strips the
 * password hash, caches the scrubbed copy, and returns that same scrubbed
 * copy — not the raw row that was just fetched.
 */
const getUserProfile = async (userId: string) => {
  const storedUser = await redis.get(`user:${userId}`);
  if (storedUser) {
    return JSON.parse(storedUser);
  }
  const existingUser = await prisma.user.findUnique({ where: { id: userId } });
  if (!existingUser) {
    throw new NotFoundError("User not found");
  }

  const { password: _password, ...safeUser } = existingUser;
  logger.info("Stored user profile in redis for the future");
  await redis.set(
    `user:${userId}`,
    JSON.stringify(safeUser),
    "EX",
    config.REDIS_USER_TTL,
  );
  return safeUser;
};
```

Before this session, the final line read `return existingUser;` — the raw Prisma row, password hash and all — even though `safeUser` (the scrubbed copy) had already been computed and was what actually got cached. That meant a **cold-cache read leaked the password hash to the client, while a warm-cache read (hitting the `if (storedUser)` branch above, which was always returning the already-scrubbed cached JSON) did not** — an inconsistency that would have been easy to miss in testing if the cache happened to be warm. The docstring shown above is also new this session; it used to be a stale copy-paste of `auth.service.ts`'s `sendOtp` doc comment, describing OTP registration — completely unrelated to what this function actually does.

`updateProfile` and `deleteProfile` (both fully implemented this session) follow the same cache-then-DB shape:

```typescript
const updateProfile = async (
  userId: string,
  updates: { firstName?: string; lastName?: string },
) => {
  const existingUser = await prisma.user.findUnique({ where: { id: userId } });
  if (!existingUser) {
    throw new NotFoundError("User not found");
  }

  const updatedUser = await prisma.user.update({
    where: { id: userId },
    data: updates,
  });

  const { password: _password, ...safeUser } = updatedUser;
  await redis.set(
    `user:${userId}`,
    JSON.stringify(safeUser),
    "EX",
    config.REDIS_USER_TTL,
  );
  return safeUser;
};

const deleteProfile = async (userId: string): Promise<void> => {
  const existingUser = await prisma.user.findUnique({ where: { id: userId } });
  if (!existingUser) {
    throw new NotFoundError("User not found");
  }

  await prisma.user.delete({ where: { id: userId } });
  await redis.del(`user:${userId}`);
};
```

`deleteProfile` only clears this user's own `user:<userId>` cache key — it does **not** revoke any `refresh:<userId>:<deviceId>` sessions on other devices, so a deleted account's still-valid refresh tokens (if any exist) would keep working against `/refresh` until they naturally expire. This isn't an oversight so much as a structural gap: there's no registry anywhere of which `deviceId`s a given user has active sessions on, so there's nothing to enumerate and clear even if someone wanted to add that here.

---

### 8. `kafka/producer/notification-producer.ts`

```typescript
class NotificationProducer {
  private isInitialized: boolean;

  private async initialize(): Promise<void> {
    if (!this.isInitialized) {
      await connectProducer();
      this.isInitialized = true;
    }
  }

  private async sendMessage<T>(
    topic: string,
    key: string | undefined,
    value: T,
  ) {
    try {
      await this.initialize();
      const message = {
        topic,
        messages: [
          {
            key: key || `${topic}-${Date.now()}`,
            value: JSON.stringify(value),
            timestamp: Date.now().toString(),
          },
        ],
      };
      const result = await producer.send(message);
      logger.info(`Message sent to kafka topic: ${topic}`, {
        key,
        partition: result[0].partition,
        offset: result[0].offset,
      });
      return result;
    } catch (error) {
      const err = error as Error;
      logger.error(`Failed to send message to kafka topic: ${topic}`, {
        error: err.message,
        stack: err.stack,
        key,
      });
      throw error;
    }
  }

  async sendOtpEmail({
    email,
    otp,
    ttlMinutes = 5,
  }: {
    email: string;
    otp: string;
    ttlMinutes: number;
  }) {
    return this.sendMessage<OtpEmailPayload>(
      KAFKA_TOPICS.OTP_EMAIL,
      `otp-${email}`,
      { email, otp, ttlMinutes },
    );
  }

  async sendWelcomeEmail(email: string, firstName: string) {
    return this.sendMessage<WelcomeEmailPayload>(
      KAFKA_TOPICS.WELCOME_EMAIL,
      `welcome-${email}`,
      { email, firstName },
    );
  }
}

export default new NotificationProducer();
```

Unlike admin-service's/inventory-service's producers, `sendMessage` here re-throws on failure rather than swallowing it — which is exactly why `auth.service.ts`'s `verifyOtp` wraps its own call to `sendWelcomeEmail` in a try/catch at the call site instead: the responsibility for "don't let a Kafka failure break the HTTP response" is pushed out to the caller, not handled inside the producer itself. `sendOtpEmail` (called from `sendOtp`) has no such wrapping at its call site — a Kafka failure during signup's OTP-send step does propagate and fail that request, which is arguably the more correct behavior there (if the OTP email genuinely can't be queued, the user has no way to complete signup anyway). Both methods are keyed by email (`otp-<email>` / `welcome-<email>`), so all messages for one user land on the same partition and are processed in order.

---

### 9. `middlewares/`

**`internal-auth.middleware.ts`** — new this session, rejects anything without the exact shared secret:

```typescript
export function internalAuth(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const serviceKey = req.headers["x-internal-service-key"];

  if (!serviceKey || serviceKey !== config.INTERNAL_SERVICE_KEY) {
    return next(new ForbiddenError("Invalid or missing internal service key"));
  }

  next();
}
```

**`user-context.middleware.ts`** — trusts the gateway's `x-user-id` header, unchanged this session but the reason it now type-checks is `types/express.d.ts` (see [above](#5-types--validation-schemas--express-augmentation)):

```typescript
export function getUserContext(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const userId = req.headers["x-user-id"];

  if (!userId) {
    return next(
      new UnauthorizedError("User context missing - must come through gateway"),
    );
  }

  req.user = { id: Array.isArray(userId) ? userId[0] : userId };
  next();
}
```

This has exactly the same trust model as every other service in this repo that uses it: it does not verify a JWT itself, it just reads a header and trusts whatever sits in front of it (the API Gateway's `requireAuth`) to have done real verification already. Calling this service directly, bypassing the gateway, and supplying your own `x-user-id` header works exactly as well as going through the real login flow — this is fine as long as user-service is genuinely unreachable from outside the gateway in deployment, and a latent risk if it isn't.

**`cors.middleware.ts`**, **`error.middleware.ts`**, **`req.middleware.ts`** are structurally identical to the equivalent files in every other service in this repo (origin whitelist off `config.ALLOWED_ORIGINS`, `AppError`-aware JSON error formatting with a generic 500 fallback for unrecognized errors, method/path/status/duration request logging) — none of the three were touched this session.

---

### 10. `utils/` — Cross-Cutting Helpers

**`utils/error.ts`** — `AppError` plus seven subclasses (`BadRequestError` 400, `UnauthorizedError` 401, `ForbiddenError` 403, `NotFoundError` 404, `ConflictError` 409, `TooManyRequestsError` 429, `InternalServerError` 500), each defaulting its own machine-readable `code` string. Unchanged this session.

**`utils/asyncHandler.ts`**:

```typescript
export default function asyncHandler<
  Req extends Request = Request,
  Res extends Response = Response,
>(fn: (req: Req, res: Res, next: NextFunction) => Promise<any> | any) {
  return (req: Req, res: Res, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}
```

This is the one place in this service that still uses `any` (`Promise<any> | any`), contrary to this repo's own stated TypeScript conventions — the same gap flagged in inventory-service's own docs for its identical helper. Every call site happens to return a typed value regardless, so it hasn't caused an observed problem.

**`utils/api-response.ts`** — `SuccessResponse` and `ErrorResponse`. Only `ErrorResponse` is actually called anywhere in this service (from every controller's Zod-validation-failure branch); every successful response is instead built inline in the controllers (`res.status(200).json({...})`) — `SuccessResponse` is dead code today, same pattern flagged in inventory-service's docs for its own unused `SuccessResponse`.

**`utils/zod.formatter.ts`**:

```typescript
import { ZodError } from "zod/v4";

export const formatZodError = (error: ZodError): string => {
  const issue = error.issues[0];
  // No template literal here — `${issue?.message}` would coerce a missing
  // message into the truthy string "undefined", silently defeating this
  // fallback.
  return issue?.message || "Validation failed";
};
```

Imports `ZodError` from `"zod/v4"` specifically rather than the plain `"zod"` package root — `package.json` pins `"zod": "^4.4.3"`, so these currently resolve to the same code, but it's worth knowing this file assumes the `/v4` subpath export exists rather than assuming v4 is simply the package's default export (the same inconsistency flagged in inventory-service's own docs, where one file uses the plain import and another uses this one). The function returns `issue?.message` directly (no template literal) — exactly the pattern its own comment warns is necessary, since `` `${issue?.message}` `` would coerce a missing message into the truthy string `"undefined"` and defeat the `|| "Validation failed"` fallback.

---

## Environment Variables

Every variable actually read via `config.*` or directly via `process.env` somewhere in `src/`:

```bash
PORT=4001
NODE_ENV=development
LOG_LEVEL=info

DATABASE_URL=postgresql://admin:irctcpass@localhost:5432/user_service_db?schema=public
REDIS_URL=redis://localhost:6379
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:4000

KAFKA_BROKER=localhost:9093
KAFKA_CLIENT_ID=user-service

# OTP
OTP_TTL=300
OTP_RATE_MAX_PER_HOUR=5
OTP_MAX_VERIFY_ATTEMPTS=5
OTP_HMAC_SECRET=change-me-to-a-long-random-hex-string

# JWT
JWT_ACCESS_SECRET=change-me-to-a-long-random-hex-string
JWT_REFRESH_SECRET=change-me-to-a-different-long-random-hex-string
ACCESS_TOKEN_EXP=15m
REFRESH_TOKEN_EXP=7d
ACCESS_TOKEN_EXP_SEC=900
REFRESH_TOKEN_EXP_SEC=604800

REDIS_USER_TTL=86400

# Shared secret used by other services (e.g. booking-service) to call
# this service's internal-only routes.
INTERNAL_SERVICE_KEY=change-me-to-a-shared-secret

# Read into config but not currently used by any code path in this service:
MAIL_SEND=
SENDGRID_API_KEY=
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
RESEND_API_KEY=
```

This `.env.example` is new this session — it didn't exist before, and every field in it matches a field on `config/index.ts`'s `Config` type exactly.

Notes on the fields flagged unused above:

- **`GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET`** are read into `config` but there's no Google OAuth code anywhere in this service. This is a deliberate, explicit out-of-scope decision (Google OAuth isn't built here), not an oversight — don't read this as "still needs fixing."
- **`SENDGRID_API_KEY`**, **`RESEND_API_KEY`**, **`MAIL_SEND`** are all read into `config` but not consumed by any code path in this service — this service never sends email directly, it only publishes Kafka events (`notification.otp-email`, `notification.welcome-email`) and relies on notification-service to do the actual sending, using its own separate copy of similar-sounding credentials.
- **`NODE_ENV`** is read twice in slightly different ways: `config/index.ts` reads it into `config.NODE_ENV`, but nothing anywhere reads `config.NODE_ENV` back out. `config/prisma.ts` separately reads the raw `process.env.NODE_ENV` directly (not through `config`) for its global-cache guard — so the environment variable itself is genuinely used, just never through the typed `Config` object built specifically to centralize this kind of thing.

---

## HTTP Routes & Kafka Topics Reference

### HTTP routes

| Method & Path | Auth | Status |
|---|---|---|
| `POST /auth/send-otp` | none (this establishes identity) | Already working before this session. |
| `POST /auth/verify-otp` | none (OTP session cookie) | Already working before this session; this session fixed a password-hash leak and added the welcome-email publish. |
| `POST /auth/login` | none (this establishes identity) | Already working before this session. |
| `POST /auth/refresh` | refresh token cookie | Already working before this session. An expired/tampered token surfaces as a generic `500`, not a clean `401` — `jwt.verify`'s thrown error isn't an `AppError` subclass. |
| `POST /user/profile` | `x-user-id` (gateway) | Handler always existed; unreachable before this session because `user.route.ts` was never mounted. Now mounted and reachable. |
| `PUT /user/profile` | `x-user-id` (gateway) | Was an empty `// TODO` stub that would hang the request forever if reached. Implemented and mounted this session. |
| `DELETE /user/profile` | `x-user-id` (gateway) | Same as `PUT` — was a hanging stub, implemented and mounted this session. |
| `GET /user/internal/:userId` | `x-internal-service-key` (shared secret) | New route this session — didn't exist in any form before. |
| `GET /health` | none | Returns a bare `{ message: "ok" }` — no `success`/`timestamp` fields, unlike admin-service's and inventory-service's richer health checks, and doesn't check Postgres/Redis connectivity. |
| `GET /` | none | Static "Hello from user-service" string. |

All four `/user/profile`-style routes and the internal route are currently reachable **only** by calling `http://localhost:4001` directly — see the [Architecture](#architecture) diagram above for exactly why both gateway-routed paths to this service still 404 today.

### Kafka topics (producer only — this service consumes nothing)

| Topic | Published by | Payload | Status |
|---|---|---|---|
| `notification.otp-email` | `sendOtpEmail`, called from `authservice.sendOtp` | `{ email, otp, ttlMinutes }` | Already working before this session. |
| `notification.welcome-email` | `sendWelcomeEmail`, called from `authservice.verifyOtp` | `{ email, firstName }` | **New this session** — the notification-service consumer side (`handleWelcomeEmail`) already existed and was fully wired before this session, but nothing in user-service ever called this method until now. Neither side has been observed running against a live broker. |

---

## Quick Start

```bash
cd user-service
npm install

# Generate the Prisma client (writes into src/generated/prisma)
npx prisma generate

# .env needs at minimum DATABASE_URL, REDIS_URL, KAFKA_BROKER,
# OTP_HMAC_SECRET, JWT_ACCESS_SECRET, JWT_REFRESH_SECRET, and
# INTERNAL_SERVICE_KEY for the full flow (including the new internal
# route) to work
npm run dev        # nodemon + ts-node, hot reload
# or
npm run build && npm start
```

Postgres, Redis, and Kafka all need to be reachable at the URLs above — none were reachable in the environment this session's fixes were made in, so none of this has been exercised live this session. Apply the schema with `npx prisma migrate deploy` (or `migrate dev` while developing) before starting the service.

```bash
curl http://localhost:4001/health
# { "message": "ok" }

curl -X POST http://localhost:4001/auth/send-otp \
  -H "Content-Type: application/json" \
  -c cookies.txt \
  -d '{"firstName":"Subham","email":"subham@example.com","password":"Passw0rd1"}'
# { "success": true, "message": "OTP sent successfully" }
# (check the email inbox tied to notification-service's mail config for the
# actual OTP, or check user-service's own logs / the notification.otp-email
# topic directly if no mail provider is configured)

curl -X POST http://localhost:4001/auth/verify-otp \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{"otp":"123456"}'
# { "message": "Account is created", "data": { "id": "...", "firstName": "Subham", ... } }

curl -X GET http://localhost:4001/user/internal/<some-user-id> \
  -H "x-internal-service-key: change-me-to-a-shared-secret"
# { "data": { "id": "...", "firstName": "Subham", ... }, "success": true }
```

---

## Debugging Tips

- **`PUT /user/profile` or `DELETE /user/profile` just hangs, no response, no error** → this was the exact symptom before this session's fix (both handlers were empty stubs wrapped in `asyncHandler`, which only forwards *thrown* errors — an empty function body neither throws nor responds). If you see this symptom again after further changes, check whether a handler body accidentally became a no-op again.
- **`401 User context missing - must come through gateway`** on any `/user/profile` route → the request has no `x-user-id` header. If you're calling this service directly for testing (not through the gateway), you must set that header yourself — remember both gateway-routed paths to this service currently 404 anyway (see [Architecture](#architecture)), so calling directly is the only way to reach these routes at all right now.
- **`403 Invalid or missing internal service key`** on `GET /user/internal/:userId` → the caller didn't send `x-internal-service-key`, or it doesn't exactly match `config.INTERNAL_SERVICE_KEY` — check both services' `.env` files have the identical value.
- **A newly-created account's password hash shows up in a response body** → this was the exact bug fixed in `authservice.verifyOtp` this session (it used to return the raw Prisma row). If you see this again, check whether a new read/write path was added somewhere that forgot to destructure `password` off before returning — the pattern `const { password: _password, ...safeUser } = user;` is used everywhere else in this service for exactly this reason.
- **A user's profile looks different depending on whether it was just fetched vs. fetched again a second later** → check whether the Redis `user:<userId>` cache and the underlying Postgres row have actually drifted (e.g. an update happened through a path that doesn't refresh the cache) — `getUserProfile`, `updateProfile`, and `deleteProfile` are all supposed to keep `user:<userId>` in sync with the DB, so a mismatch means one of them isn't being called, or another process is bypassing the service layer.
- **A refresh token that "should" work gets `403 Refresh token reused"` unexpectedly** → check whether the device fingerprint changed between the login and the refresh call (see `utils/device-fingerprint.ts` — it hashes `user-agent|ip|accept`; a different IP or client library between calls produces a different `deviceId`, meaning `redis.get(refresh:<userId>:<deviceId>)` looks up the wrong key entirely and comes back empty, which surfaces as `403 Session expired` rather than the reuse message specifically — check the exact message returned to tell these two cases apart).
- **`notification.welcome-email` doesn't seem to reach notification-service** → check user-service's own logs for "Failed to publish welcome email event" first; this call is wrapped in a try/catch that swallows the failure so signup still succeeds — a broker outage here is invisible unless you're watching the logs.
- **`npx tsc --noEmit` fails after adding a new file that touches `req.user`** → make sure `types/express.d.ts` is still being picked up by `tsconfig.json`'s `include`; this is the file that makes `req.user` type-check at all in this service.

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, per this repo's own documentation convention (never fix while documenting):

1. ~~Both gateway-routed paths into this service are broken~~ **Login is fixed.** `POST /api/users/auth/login` forwards to `/auth/login`, and this service now mounts login at exactly that path (it used to be `/api/v1/auth/login`, which the gateway's one-segment-strip rewrite could never reach — dropping the version prefix, to match every other service in this repo, is the fix). `GET /api/users/user/profile` is **still broken**, for reasons entirely on the API Gateway's side: it forwards to `/user/profile`, but this service only ever defines `POST`/`PUT`/`DELETE /profile`, not `GET` — a method mismatch, unrelated to the mounting fix. That one is still a gateway-side bug, not something this pass touched.
2. **This session's own work — the profile routes, the internal route, and the six correctness fixes described throughout this doc — has been verified with `npx tsc --noEmit` only.** No Postgres, Redis, or Kafka broker was reachable in this environment, so none of it has been exercised live. The pre-existing auth flow (send-otp/verify-otp/login/refresh) is described elsewhere in this repo (root `readme.md` §4) as "fully working end-to-end," but that claim comes from an earlier audit whose own verification methodology isn't known from this session — take it as background context, not something re-confirmed here.
3. **`hashToken` (in `utils/auth.ts`) is exported but never called anywhere in this service.** It SHA-256-hashes a token string — presumably intended for safely storing/comparing a raw token value — but nothing in the current auth flow uses it (refresh-token reuse detection works entirely off the JWT's own `jti` claim compared against Redis, not a hash of the token itself).
4. ~~`AuthenticatedRequest` (imported from `shared/types/index.ts` into `user.controller.ts`) doesn't appear to be used as an actual type annotation anywhere in that file's current handler signatures.~~ **No longer applicable — the import itself is gone.** `user.controller.ts` currently has no `AuthenticatedRequest` import at all (confirmed by grep across `src/`); every handler is typed against plain `Request` (see the [types/ section](#5-types--validation-schemas--express-augmentation) for why: the shared type's `user` field is required, which doesn't compose with Express's own optional-`user` augmentation). The type still exists in `shared/types/index.ts` and could be reintroduced by a future edit without actually being needed — worth checking for that if it reappears.
5. **`GET /user/profile` (a read) is mounted as `POST /user/profile`**, not `GET` — `getProfile`'s HTTP verb doesn't match what the handler actually does (an idempotent read). This predates this session (the route file already existed with this shape) and is unchanged by it, but it's the same mismatch the gateway's own `GET /api/users/user/profile` route runs into from a different angle (see item 1) — worth knowing if the two are ever reconciled together.
6. **`docs/auth.md`, referenced twice in the root `readme.md`** (§4's "complete, byte-for-byte breakdown... lives in `docs/auth.md`," and the "Where to Go Deeper" table) **does not exist anywhere in this repository** — confirmed by a repo-wide search. The root readme currently links to a file that was never created; this document is, as of this pass, the closest thing to that promised deep-dive that actually exists.
7. **Unrelated dependencies in `package.json`**: `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`, `mongoose`, `http-status`, `resend` are all listed but nothing under `src/` imports any of them (confirmed by grep) — the same "looks copied from a template without pruning" pattern already flagged in admin-service's, inventory-service's, and search-service's own docs.
8. **`npm run seed` points at `src/services/seed.ts`, which does not exist in this project** (only `auth.service.ts` and `user.service.ts` exist under `services/`) — running that script fails. Same issue flagged in every other service's docs in this repo, likely from the same shared `package.json` template origin.
9. **`utils/auth.ts` imports `type { StringValue } from "ms"`**, but `"ms"` is not listed as a direct dependency in `package.json` — it's only present in `node_modules` as a transitive dependency of something else (e.g. `jsonwebtoken`). This currently works because the transitive copy happens to be there, but it's fragile: a future dependency bump that changes or removes that transitive path could break this type import with no direct `package.json` entry to point to as the cause.
10. **`SuccessResponse` (in `utils/api-response.ts`) is defined but never called** — every successful response in this service is built inline in the controllers instead. Same dead-code pattern already flagged in inventory-service's docs for its own copy of this helper.
11. **`asyncHandler`'s generic signature accepts `Promise<any> | any`** (`utils/asyncHandler.ts`) — the one place in this service that still uses `any`, contrary to this repo's own stated TypeScript conventions. Every call site happens to return a typed value anyway.
12. **`server.ts` imports both `config` and `logger` but never actually uses either identifier in its own body** — dead imports in that one file specifically (both are very much used elsewhere in the service).
13. **A stale, incorrect doc comment above `verifyOtpViaUnHashing`** used to claim there was a typo bug (`hasedOtp` vs. `hashedOtp`) that would make OTP verification always fail. Reading the real code shows both the write and read sides already used the correctly-spelled field name consistently — the bug the comment described never existed in the current code. Removed this session; flagged here rather than silently — a comment describing a plausible-sounding bug that isn't real can send the next person hunting for something that was never there.
14. **`utils/otp.ts` defines its own fallback for `OTP_RATE_MAX_PER_HOUR`** — `const RATE_MAX = Number(config.OTP_RATE_MAX_PER_HOUR || "10");` — even though `config.OTP_RATE_MAX_PER_HOUR` is already supposed to be the single typed source of truth from `config/index.ts` (which applies no fallback of its own; an unset env var resolves to `NaN` there). This means if the env var is ever left unset, the *actual* enforced limit silently becomes `10`, not the `5` suggested in `.env.example` — two different "defaults" depending on which layer you read. No sibling config value in this file (e.g. `config.OTP_MAX_VERIFY_ATTEMPTS`, used a few lines below with no fallback at all) gets the same treatment, so this is a one-off, not a consistent pattern.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
