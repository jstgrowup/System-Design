# API Gateway — Complete Guide

Single source of truth for the IRCTC API Gateway: what it does, how requests flow through it, and how each piece works, with the actual current code inline.

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [Request Lifecycle](#request-lifecycle)
5. [Component Breakdown](#component-breakdown)
   - [index.ts — Entry Point](#1-indexts--entry-point)
   - [config/ — Configuration, Redis, Logger](#2-config--configuration-redis-logger)
   - [routes/index.ts — Routing](#3-routesindexts--routing)
   - [auth.middleware.ts — Authentication](#4-authmiddlewarets--authentication)
   - [rate-limiting.middleware.ts — Rate Limiting](#5-rate-limitingmiddlewarets--rate-limiting)
   - [services/proxy.ts — Proxy & Circuit Breaker](#6-servicesproxyts--proxy--circuit-breaker)
   - [cors.middleware.ts / error.middleware.ts / req.middleware.ts / not-found.middleware.ts](#7-remaining-middlewares)
   - [utils/error.ts — Error Classes](#8-utilserrorts--error-classes)
6. [Environment Variables](#environment-variables)
7. [Error Codes Reference](#error-codes-reference)
8. [Quick Start](#quick-start)
9. [Debugging Tips](#debugging-tips)
10. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **API Gateway** is the single entry point for client requests into the IRCTC microservices system. It's an Express app that:

- **Authenticates** requests via JWT (Authorization header or cookie)
- **Rate limits** by IP, by user, and per-endpoint, backed by Redis
- **Routes** requests to the correct downstream microservice
- **Protects** those services with a circuit breaker
- **Normalizes** error responses and logs every request

It does not implement business logic itself — every real operation (login, booking, payment, etc.) lives in a downstream service; the gateway's job is auth, rate limiting, routing, and resilience.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                          │
└───────────────────────────┬───────────────────────────────────┘
                            │ HTTP/REST
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                  API GATEWAY (Port 4000)                      │
│                                                                │
│  Global middleware (index.ts, applied to every request):      │
│   1. CORS            → origin whitelist check                 │
│   2. Helmet          → security headers                       │
│   3. Request logger  → logs method/path/status/duration       │
│   4. Body parser     → JSON (raw bytes for Razorpay webhook)  │
│   5. Cookie parser                                             │
│                                                                │
│  Per-route middleware (routes/index.ts):                      │
│   6. requireAuth          → JWT verification (where listed)   │
│   7. rate limiting        → endpoint / combined IP+user       │
│   8. createProxy(...)     → forwards to downstream service    │
│        └─ circuit breaker → fails fast if service is down     │
│                                                                │
│  Tail middleware (index.ts, registered last):                 │
│   9. notFound            → unmatched routes → 404              │
│  10. errorMiddleware     → formats all thrown errors           │
└───────────────────────────┬───────────────────────────────────┘
              ┌─────────────┼─────────────────┬───────────────┐
              ▼             ▼                 ▼               ▼
        User Service   Search Service   Booking Service  Payment Service
         :4001            :4002             :4005            :4006
                                                          (+ Admin :4003,
                                                       Notification :4004,
                                                          Inventory :4007)

                    ┌───────────────────────────┐
                    │   Redis (rate limiting)  │
                    │   localhost:6379         │
                    └───────────────────────────┘
```

Only **2 business routes** are currently wired up (`src/routes/index.ts`): user login and get-profile, both proxied to the user service. The other six service URLs are configured (`src/config/index.ts`) but have no routes defined yet — the gateway is set up to support them, but doesn't proxy to them today.

---

## File Structure

```
api-gateway/
├── src/
│   ├── index.ts                          # Express app bootstrap
│   ├── config/
│   │   ├── index.ts                      # Env vars → typed Config object
│   │   ├── redis.ts                      # Redis singleton connection
│   │   └── logger.ts                     # Winston logger
│   ├── routes/
│   │   └── index.ts                      # Route definitions + proxies
│   ├── middlewares/
│   │   ├── auth.middleware.ts            # JWT authentication
│   │   ├── rate-limiting.middleware.ts   # IP/user/endpoint rate limiting
│   │   ├── cors.middleware.ts            # CORS whitelist
│   │   ├── error.middleware.ts           # Global error formatter
│   │   ├── req.middleware.ts             # Request/response logging
│   │   └── not-found.middleware.ts       # 404 handler
│   ├── services/
│   │   └── proxy.ts                      # Request forwarding + circuit breaker
│   ├── utils/
│   │   └── error.ts                      # AppError + subclasses
│   └── types/
│       └── index.ts                      # Empty — no shared types defined yet
├── docs/                                 # This documentation
├── package.json
├── tsconfig.json
└── .env
```

> `src/kafka/email-consumer.ts` was present earlier but has since been removed from this project — it isn't referenced anywhere below.

---

## Request Lifecycle

### Case A: `POST /api/users/auth/login` (public, endpoint-rate-limited)

```
1.  Request arrives → CORS check → Helmet → reqLogger logs "[POST] /api/users/auth/login"
2.  Body parsed as JSON
3.  Route matched: POST /users/auth/login
4.  endpointRateLimit(10, 900000) — key: ratelimit:endpoint:POST:/users/auth/login:<ip>
      Redis: remove old entries, add this request, count, set TTL
      count ≤ 10 → allowed, headers set (X-RateLimit-*)
5.  userServiceProxy:
      path /api/users/auth/login → strip "/api" (Express router already did) → strip
      first segment "users" → forward path "/auth/login"
      circuit breaker for "userService": CLOSED → request allowed through
6.  axios POST http://localhost:4001/auth/login  (timeout: SERVICE_TIMEOUT_MS)
7.  Service responds 200 {token, expiresIn} → circuit breaker onSuccess() → failureCount reset
8.  Response headers copied (except connection/keep-alive/transfer-encoding/host)
9.  res.status(200).json(data) sent to client
10. res "finish" event → reqLogger logs "[POST] ... - status: 200 - 48ms"
```

### Case B: `GET /api/users/user/profile` (authenticated, combined rate limit)

```
1.  Request arrives with Authorization: Bearer <token>
2.  Route matched: GET /users/user/profile → middleware chain: requireAuth, combinedRateLimit(), userServiceProxy
3.  requireAuth:
      - extract token from Authorization header (or accessToken cookie)
      - jwt.verify(token, JWT_ACCESS_SECRET) → payload.id
      - req.user = { id }; req.headers["x-user-id"] = id
4.  combinedRateLimit():
      - ipRateLimit(): key ratelimit:ip:<ip>, default 100/15min
      - userRateLimit(): key ratelimit:user:<id>, default 1000/15min (10x IP limit)
      - both must pass or a 429 (TooManyRequestsError) is thrown
5.  userServiceProxy forwards to http://localhost:4001/user/profile
      with x-user-id header attached for the downstream service
6.  Circuit breaker (userService, CLOSED) executes the request
7.  Service responds 200 {id, name, email, ...}
8.  Response sent to client with X-RateLimit-Remaining header set
```

### Case C: Downstream service is down (circuit breaker OPEN)

```
1.  Request passes auth + rate limiting
2.  Proxy checks circuit breaker for the target service
3.  State is OPEN and Date.now() < nextAttempt
      → throws ServiceUnavailableError immediately (no network call attempted)
4.  errorMiddleware catches it → 503 { error: "SERVICE_UNAVAILABLE", message: "..." }
```

The circuit only opens after 5 consecutive failures (`CIRCUIT_BREAKER_THRESHOLD`) and stays open for 60 seconds (`CIRCUIT_BREAKER_TIMEOUT`) before allowing one test request through (`HALF_OPEN`).

---

## Component Breakdown

### 1. `index.ts` — Entry Point

Registers middleware in order, mounts routes under `/api`, starts the server, and wires up graceful shutdown.

```typescript
const app = express();

app.use(corsMiddleware);                 // 1. Origin check first — reject before doing any other work
app.use(helmet({ ... }));                // 2. Security headers
app.use(reqLogger);                      // 3. Log every request (and its outcome on "finish")

// 4. Body parsing — special-cased for the Razorpay webhook, which needs the
//    raw request bytes (not parsed JSON) to verify its signature.
app.use((req, res, next) => {
  if (req.path === "/api/payments/webhooks/razorpay") {
    return express.raw({ type: "application/json", limit: "10mb" })(req, res, next);
  }
  express.json({ limit: "10mb" })(req, res, next);
});

app.use(express.urlencoded({ extended: true, limit: "10mb" }));
app.use(cookieParser());

if (config.NODE_ENV === "development") app.use(morgan("dev"));

app.get("/health", (req, res) => res.status(200).json({ success: true, ... }));

app.use("/api", gatewayRouter);           // All business routes
app.use(notFound);                        // Catch unmatched routes → 404
app.use(errorMiddleware);                 // Must be registered last

const server = app.listen(config.PORT, () => logger.info(`running on ${config.PORT}`));

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);
process.on("unhandledRejection", (err) => { logger.error(err); server.close(() => process.exit(1)); });
```

**Note on the Razorpay webhook route:** there's no `/api/payments/*` route actually registered in `routes/index.ts` today — this raw-body branch is dead code until a payments route is added, but it's harmless to leave in place.

**Graceful shutdown** (`gracefulShutdown()`) calls `server.close()` and force-exits after 30 seconds if it hangs. It does **not** call `RedisClient.closeConnection()` — the Redis connection is left open until process exit rather than being closed explicitly.

---

### 2. `config/` — Configuration, Redis, Logger

**`config/index.ts`** loads everything from environment variables into one typed object, with defaults for everything except the two JWT secrets:

```typescript
const config: Config = {
  PORT: process.env.PORT || 4000,
  NODE_ENV: process.env.NODE_ENV || "development",
  JWT_ACCESS_SECRET: process.env.JWT_ACCESS_SECRET as string,   // required, no default
  JWT_REFRESH_SECRET: process.env.JWT_REFRESH_SECRET as string, // required, no default
  RATE_LIMIT_WINDOW_MS: parseInt(process.env.RATE_LIMIT_WINDOW_MS || "900000", 10),  // 15 min
  RATE_LIMIT_MAX_REQUESTS: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || "100", 10),
  SERVICES: {
    USER_SERVICE_URL: process.env.USER_SERVICE_URL || "http://localhost:4001",
    // ...6 more service URLs, all with localhost defaults
  },
  SERVICE_TIMEOUT_MS: parseInt(process.env.SERVICE_TIMEOUT_MS || "60000", 10),
  CIRCUIT_BREAKER_THRESHOLD: parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD || "5", 10),
  CIRCUIT_BREAKER_TIMEOUT: parseInt(process.env.CIRCUIT_BREAKER_TIMEOUT || "60000", 10),
};

// App refuses to start if either JWT secret is missing:
["JWT_ACCESS_SECRET", "JWT_REFRESH_SECRET"].forEach((key) => {
  if (!config[key]) throw new Error(`Missing required environment variable: ${key}`);
});
```

**`config/redis.ts`** is a singleton wrapper around `ioredis`:

```typescript
class RedisClient {
  static getInstance(): Redis {
    if (!RedisClient.instance) {
      RedisClient.instance = new Redis(config.REDIS_URL, {
        retryStrategy: (times) => Math.min(times * 50, 2000), // exponential-ish backoff, capped at 2s
        maxRetriesPerRequest: 3,
      });
      RedisClient.setupEventListeners(); // logs connect/error/close/reconnecting/ready/end
    }
    return RedisClient.instance;
  }
}
export const redis = RedisClient.getInstance(); // created at import time
```

`RedisClient.closeConnection()`, `isReady()`, and `testConnection()` exist but aren't called from anywhere in the app currently (no `/health` endpoint reports Redis status, and shutdown doesn't call `closeConnection()`).

**`config/logger.ts`** sets up a single shared Winston logger:

```typescript
const logger = winston.createLogger({
  level: config.LOG_LEVEL,   // hardcoded to "4" — see Known Issues below
  defaultMeta: { service: config.SERVICE_NAME },
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp, service }) =>
      `[${timestamp}] [${level}] [${service}]: ${message}`),
  ),
  transports: [new winston.transports.Console()],
});
```

---

### 3. `routes/index.ts` — Routing

```typescript
const userServiceProxy = createProxy("userService", config.SERVICES.USER_SERVICE_URL);

// Public — but rate limited to 10 requests / 15 min per IP+endpoint (brute-force guard)
gatewayRouter.post("/users/auth/login", endpointRateLimit(10, 900000), userServiceProxy);

// Authenticated — requireAuth runs first, then combined IP+user rate limiting
gatewayRouter.get("/users/user/profile", requireAuth, combinedRateLimit(), userServiceProxy);

// Gateway's own health check — answered directly, never proxied
gatewayRouter.get("/gateway/health", (req, res) =>
  res.status(200).json({ success: true, message: "Gateway is healthy", timestamp: new Date().toString() }));
```

This is the entire route table today. Adding a new proxied route means: create a proxy with `createProxy(serviceName, serviceUrl)` (the `serviceName` must match a key already in `circuitBreakers` inside `services/proxy.ts`), then register a route with whatever combination of `requireAuth` / rate-limit middleware fits.

---

### 4. `auth.middleware.ts` — Authentication

```typescript
export function requireAuth(req, res, next) {
  try {
    let accessToken;
    // 1. Prefer Authorization: Bearer <token> (mobile / service-to-service)
    const authHeader = req.headers.authorization;
    if (authHeader?.startsWith("Bearer ")) accessToken = authHeader.split(" ")[1];

    // 2. Fall back to the httpOnly `accessToken` cookie (browser clients)
    if (!accessToken && req.cookies) accessToken = req.cookies.accessToken;

    if (!accessToken) throw new UnauthorizedError("Authorization token missing");

    // 3. Verify signature + expiry against JWT_ACCESS_SECRET
    const payload = jwt.verify(accessToken, config.JWT_ACCESS_SECRET);
    if (!payload.id) throw new UnauthorizedError("Invalid token payload");

    // 4. Attach identity for later middleware and for the downstream service
    req.user = { id: payload.id };
    req.headers["x-user-id"] = payload.id.toString();

    next();
  } catch (err) {
    if (err.name === "TokenExpiredError") return next(new UnauthorizedError("Access token expired", "TOKEN_EXPIRED"));
    if (err.name === "JsonWebTokenError") return next(new UnauthorizedError("Invalid access token", "TOKEN_INVALID"));
    return next(err);
  }
}
```

Downstream services trust the `x-user-id` header rather than re-verifying the JWT themselves — this only holds as long as those services are unreachable from outside the gateway (i.e. the header can't be spoofed by an external caller).

---

### 5. `rate-limiting.middleware.ts` — Rate Limiting

Core algorithm — a Redis sorted-set sliding window, shared by all three strategies below:

```typescript
async function rateLimiter(key, maxRequests, windowMs) {
  const now = Date.now();
  const windowStart = now - windowMs;

  const pipeline = redis.pipeline();
  pipeline.zremrangebyscore(key, 0, windowStart);   // drop entries older than the window
  pipeline.zadd(key, now, `${now}-${Math.random()}`); // record this request
  pipeline.zcard(key);                               // count requests still in window
  pipeline.expire(key, Math.ceil(windowMs / 1000));   // auto-cleanup if key goes idle

  const results = await pipeline.exec();
  const requestCount = results[2][1];

  if (requestCount > maxRequests) {
    const [, oldestScore] = await redis.zrange(key, 0, 0, "WITHSCORES");
    const resetTime = parseInt(oldestScore, 10) + windowMs;
    return { allowed: false, remaining: 0, resetTime, retryAfter: Math.ceil((resetTime - now) / 1000) };
  }
  return { allowed: true, remaining: maxRequests - requestCount, resetTime: windowStart + windowMs };
  // On any Redis error: fails OPEN (returns allowed: true) rather than blocking traffic
}
```

Three call sites, keyed differently:

| Function | Key | Default limit | Used for |
|---|---|---|---|
| `ipRateLimit()` | `ratelimit:ip:<ip>` | 100 / 15min | Any request, keyed by caller IP |
| `userRateLimit()` | `ratelimit:user:<id>` | 1000 / 15min (10×) | Skipped entirely if `req.user` isn't set |
| `endpointRateLimit(max, windowMs)` | `ratelimit:endpoint:<method>:<path>:<ip>` | caller-specified | Sensitive endpoints, e.g. login |

`combinedRateLimit()` chains `ipRateLimit()` then `userRateLimit()` — both headers get set, and either one failing produces a 429.

---

### 6. `services/proxy.ts` — Proxy & Circuit Breaker

**Circuit breaker** — one instance per service, tracked in a fixed map:

```typescript
const circuitBreakers = {
  userService: new CircuitBreaker("user-service"),
  searchService: new CircuitBreaker("search-service"),
  adminService: new CircuitBreaker("admin-service"),
  notificationService: new CircuitBreaker("notification-service"),
  bookingService: new CircuitBreaker("booking-service"),
  paymentService: new CircuitBreaker("payment-service"),
  inventoryService: new CircuitBreaker("inventory-service"),
};
```

Only `userService`'s breaker is ever exercised today, since it's the only one with a route pointing at it.

State machine:

```
CLOSED  --(≥5 consecutive failures)-->  OPEN
OPEN    --(60s elapsed)-->               HALF_OPEN  (one test request allowed)
HALF_OPEN --(success)-->  CLOSED     |   HALF_OPEN --(failure)--> OPEN (timer restarts)
```

**Request forwarding + path rewrite:**

```typescript
function createProxy(serviceName, serviceUrl) {
  const circuitBreaker = circuitBreakers[serviceName];
  return async (req, res, next) => {
    try {
      // /api/users/auth/login → req.path (relative to gatewayRouter) is /users/auth/login
      const pathParts = req.path.split("/").filter(Boolean);   // ["users", "auth", "login"]
      const servicePath = "/" + pathParts.slice(1).join("/");  // drop "users" → "/auth/login"

      const result = await forwardRequest(
        serviceUrl,
        servicePath + (query string if present),
        req.method, req.body, req.headers,
        circuitBreaker,
      );

      // Copy response headers except connection/keep-alive/transfer-encoding/host
      Object.keys(result.headers).forEach((key) => {
        if (!excludeHeaders.includes(key.toLowerCase())) res.setHeader(key, result.headers[key]);
      });
      res.status(result.status).json(result.data);
    } catch (err) {
      next(err);
    }
  };
}
```

`forwardRequest()` uses axios with `validateStatus: () => true` (so 4xx/5xx from the downstream service are returned as-is, not thrown), and maps connection-level failures to gateway-specific errors:

- `ECONNABORTED` / `ETIMEDOUT` → `GatewayTimeoutError` (504)
- `ECONNREFUSED` → `ServiceUnavailableError` (503, "service may be down")
- any other network error → `ServiceUnavailableError` (503)
- a response *was* received (even a 4xx/5xx) → forwarded through unchanged

`getCircuitBreakerStatus()` is exported for inspecting breaker state (`state`, `failureCount`, `nextAttempt` per service) but nothing currently calls it — there's no `/gateway/circuit-status` route wired up.

---

### 7. Remaining Middlewares

**`cors.middleware.ts`** — whitelist check against `config.ALLOWED_ORIGINS` (comma-separated env var), allows credentials, restricts methods to `GET/POST/PUT/DELETE/OPTIONS`.

**`error.middleware.ts`** — registered last in `index.ts`. `AppError` instances (and subclasses) are returned with their own status/code; anything else logs (non-production only) and returns a generic `500 SERVER_ERROR` without leaking internals.

**`req.middleware.ts`** — logs the request at `debug` on arrival, then logs method/path/status/duration at `info` once the response's `"finish"` event fires.

**`not-found.middleware.ts`** — registered right after the gateway's routes; anything that didn't match becomes a `NotFoundError` (404), routed through the same error middleware as everything else.

---

### 8. `utils/error.ts` — Error Classes

All extend `AppError` (message + `statusCode` + machine-readable `code`), so `errorMiddleware` can format them consistently:

| Class | Status | Thrown by |
|---|---|---|
| `BadRequestError` | 400 | — (defined, not currently thrown anywhere in this codebase) |
| `UnauthorizedError` | 401 | `auth.middleware.ts` |
| `ForbiddenError` | 403 | — (defined, not currently thrown) |
| `NotFoundError` | 404 | `not-found.middleware.ts` |
| `ConflictError` | 409 | — (defined, not currently thrown) |
| `TooManyRequestsError` | 429 | `rate-limiting.middleware.ts` |
| `InternalServerError` | 500 | — (defined, not currently thrown — `errorMiddleware` builds its own 500 response inline instead) |
| `ServiceUnavailableError` | 503 | `services/proxy.ts` (circuit open, connection refused, network error) |
| `GatewayTimeoutError` | 504 | `services/proxy.ts` (timeout) |

---

## Environment Variables

```bash
PORT=4000
NODE_ENV=development

# Required — app throws on startup if either is missing
JWT_ACCESS_SECRET=<32+ char secret>
JWT_REFRESH_SECRET=<32+ char secret>

ACCESS_TOKEN_EXP_SEC=900        # 15 min
REFRESH_TOKEN_EXP_SEC=604800    # 7 days

ALLOWED_ORIGINS=http://localhost:3000,http://localhost:3001

REDIS_URL=redis://localhost:6379

RATE_LIMIT_WINDOW_MS=900000     # 15 min
RATE_LIMIT_MAX_REQUESTS=100

USER_SERVICE_URL=http://localhost:4001
SEARCH_SERVICE_URL=http://localhost:4002
ADMIN_SERVICE_URL=http://localhost:4003
NOTIFICATION_SERVICE_URL=http://localhost:4004
BOOKING_SERVICE_URL=http://localhost:4005
PAYMENT_SERVICE_URL=http://localhost:4006
INVENTORY_SERVICE_URL=http://localhost:4007

SERVICE_TIMEOUT_MS=60000
CIRCUIT_BREAKER_THRESHOLD=5
CIRCUIT_BREAKER_TIMEOUT=60000
```

`LOG_LEVEL` is **not** read from the environment despite appearing in the `Config` interface — see [Known Issues](#known-issues--inconsistencies).

---

## Error Codes Reference

| Code | HTTP | Meaning |
|---|---|---|
| `UNAUTHORIZED` | 401 | Auth token missing |
| `TOKEN_EXPIRED` | 401 | Token past expiry |
| `TOKEN_INVALID` | 401 | Bad signature / malformed token |
| `NOT_FOUND` | 404 | No matching route |
| `TOO_MANY_REQUESTS` | 429 | Rate limit exceeded — see `Retry-After` header |
| `SERVER_ERROR` | 500 | Unexpected/unhandled error |
| `SERVICE_UNAVAILABLE` | 503 | Circuit breaker OPEN, or `ECONNREFUSED`, or other network error |
| `GATEWAY_TIMEOUT` | 504 | Downstream service exceeded `SERVICE_TIMEOUT_MS` |

`BAD_REQUEST`, `FORBIDDEN`, and `CONFLICT` error classes exist in `utils/error.ts` but nothing in this codebase currently throws them.

---

## Quick Start

```bash
cd api-gateway
npm install

# .env needs at minimum JWT_ACCESS_SECRET and JWT_REFRESH_SECRET (app won't boot without them)
npm run dev        # nodemon, hot reload
# or
npm run build && npm start
```

Redis must be reachable at `REDIS_URL` for rate limiting to function (if it's down, rate limiting fails open — requests are allowed rather than blocked).

```bash
curl http://localhost:4000/health
# { "success": true, "message": "API Gateway is running", "timestamp": "...", "environment": "development" }

curl -X POST http://localhost:4000/api/users/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"..."}'

curl http://localhost:4000/api/users/user/profile \
  -H "Authorization: Bearer <token>"
```

---

## Debugging Tips

- **`ECONNREFUSED` from the gateway** → the target service (e.g. user-service on :4001) isn't running, or its circuit breaker has opened. Check `getCircuitBreakerStatus()` (not currently exposed via a route — would need to be called from a debug script or a temporary route).
- **429 on every request** → check `X-RateLimit-Remaining` / `Retry-After` response headers; inspect the relevant key directly: `redis-cli ZCARD ratelimit:ip:<ip>` / `TTL ratelimit:ip:<ip>`.
- **401 on a token you just issued** → confirm `JWT_ACCESS_SECRET` is identical between whatever service issues the token and this gateway's `.env`.
- **CORS error in the browser console** → the request's `Origin` isn't in `ALLOWED_ORIGINS` (comma-separated, exact string match, no wildcards).
- **Nothing gets logged at debug level** → see the `LOG_LEVEL` note below; it's hardcoded and won't respond to an env var.

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, since these are informational:

1. **`LOG_LEVEL` is hardcoded to `"4"`** in `config/index.ts` (`LOG_LEVEL: "4"`), not read from `process.env`. Winston expects level strings like `"debug" | "info" | "warn" | "error"`, so `"4"` isn't a recognized level — setting `LOG_LEVEL` in `.env` currently has no effect at all.
2. **`package.json` name is `"Notification Service"`**, not something referencing the API Gateway — likely left over from copying `package.json` from the notification service.
3. **Several dependencies look unrelated to a gateway** — `@langchain/*`, `mongoose`, `resend` are present in `package.json` but nothing under `src/` imports them. Also carried over, most likely.
4. **`npm run seed` points at `src/services/seed.ts`**, which doesn't exist in this project — running that script will fail.
5. **The Razorpay-webhook raw-body branch in `index.ts`** checks for `req.path === "/api/payments/webhooks/razorpay"`, but no `/payments/*` route exists in `routes/index.ts` yet — currently unreachable code, harmless but dead until a payments route is added.
6. **`RedisClient.closeConnection()`, `isReady()`, `testConnection()`** are defined in `config/redis.ts` but never called anywhere — the Redis connection isn't closed during `gracefulShutdown()` in `index.ts`, and there's no health endpoint reporting Redis status.
7. **`getCircuitBreakerStatus()`** (in `services/proxy.ts`) is exported but not called by any route — there's no way to inspect circuit breaker state over HTTP today.
8. **Six of seven configured service URLs have no routes** — `SEARCH_SERVICE_URL`, `ADMIN_SERVICE_URL`, `NOTIFICATION_SERVICE_URL`, `BOOKING_SERVICE_URL`, `PAYMENT_SERVICE_URL`, `INVENTORY_SERVICE_URL` are all configured and have circuit breakers pre-created, but only `userService` is ever proxied to from `routes/index.ts`.
9. **`src/types/index.ts` is empty** — no shared types are defined there despite the file existing.
10. **`BadRequestError`, `ForbiddenError`, `ConflictError`, `InternalServerError`** are defined in `utils/error.ts` but nothing in the current codebase throws them.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
