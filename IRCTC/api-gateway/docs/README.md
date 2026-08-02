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

`src/routes/index.ts` today defines 11 routes across four proxies:
`userService` (login, profile), `adminService` (two `GET /admins/*` routes),
`bookingService` (five `POST`/`GET /bookings/bookings*` routes), and
`paymentService` (one `POST /payments/webhooks/razorpay` route for the
Razorpay webhook), plus the gateway's own `/gateway/health`. `searchService`,
`notificationService`, and `inventoryService` have circuit breakers
pre-created in `services/proxy.ts` but no route proxies to them yet. See
[routes/index.ts — Routing](#3-routesindexts--routing) below for the full
table.

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

**Note on the Razorpay webhook route:** `routes/index.ts` now registers `POST /payments/webhooks/razorpay` (proxying to `paymentService`), so this raw-body branch is live — Razorpay's webhook calls hit exactly this path and need the raw bytes for signature verification. (Earlier revisions of this doc described this branch as dead code, back when no `/payments/*` route existed yet.)

**Graceful shutdown** (`gracefulShutdown()`) calls `server.close()` and force-exits after 30 seconds if it hangs. It does **not** call `RedisClient.closeConnection()` — the Redis connection is left open until process exit rather than being closed explicitly.

---

### 2. `config/` — Configuration, Redis, Logger

**`config/index.ts`** loads environment variables into one typed object. Most fields have a fallback default; the notable exceptions are `JWT_ACCESS_SECRET` and `JWT_REFRESH_SECRET` (validated below — the app refuses to boot without them) and `REDIS_URL`, which has no fallback string at all — if unset, `undefined` is passed straight to `ioredis`, which happens to interpret that as "connect to `localhost:6379`" on its own. `LOG_LEVEL` isn't read from `process.env` at all — see [Known Issues](#known-issues--inconsistencies):

```typescript
const config: Config = {
  // Server configuration
  PORT: process.env.PORT || 4000,
  LOG_LEVEL: "4",  // NOTE: hardcoded, ignores process.env.LOG_LEVEL — see logger.ts for the resulting behavior
  SERVICE_NAME: packageJson.name,
  NODE_ENV: process.env.NODE_ENV || "development",

  // Cache & storage
  REDIS_URL: process.env.REDIS_URL,

  // CORS security - comma-separated list of allowed origins
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS || "http://localhost:3000",

  // JWT Secrets (REQUIRED - no defaults!)
  JWT_ACCESS_SECRET: process.env.JWT_ACCESS_SECRET as string,
  JWT_REFRESH_SECRET: process.env.JWT_REFRESH_SECRET as string,
  ACCESS_TOKEN_EXP: process.env.ACCESS_TOKEN_EXP,
  REFRESH_TOKEN_EXP: process.env.REFRESH_TOKEN_EXP,

  // Token expiry times in seconds
  ACCESS_TOKEN_EXP_SEC: parseInt(process.env.ACCESS_TOKEN_EXP_SEC || "900", 10),        // 15 minutes
  REFRESH_TOKEN_EXP_SEC: parseInt(process.env.REFRESH_TOKEN_EXP_SEC || "604800", 10),  // 7 days

  // Rate Limiting Configuration
  RATE_LIMIT_WINDOW_MS: parseInt(process.env.RATE_LIMIT_WINDOW_MS || "900000", 10),  // 15 minutes
  RATE_LIMIT_MAX_REQUESTS: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || "100", 10), // per IP/user

  // Downstream Microservices URLs
  SERVICES: {
    USER_SERVICE_URL: process.env.USER_SERVICE_URL || "http://localhost:4001",
    // ...6 more service URLs, all with localhost defaults
  },

  // Circuit Breaker Configuration
  SERVICE_TIMEOUT_MS: parseInt(process.env.SERVICE_TIMEOUT_MS || "60000", 10),              // 60 seconds
  CIRCUIT_BREAKER_THRESHOLD: parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD || "5", 10),  // Open after 5 failures
  CIRCUIT_BREAKER_TIMEOUT: parseInt(process.env.CIRCUIT_BREAKER_TIMEOUT || "60000", 10),  // Wait 60 seconds
};

// App refuses to start if either JWT secret is missing:
const requiredConfig: (keyof Config)[] = ["JWT_ACCESS_SECRET", "JWT_REFRESH_SECRET"];
requiredConfig.forEach((key) => {
  if (!config[key]) throw new Error(`Missing required environment variable: ${key}`);
});
```

`ACCESS_TOKEN_EXP`, `REFRESH_TOKEN_EXP`, `ACCESS_TOKEN_EXP_SEC`, and `REFRESH_TOKEN_EXP_SEC` are loaded here but nothing else in `api-gateway/src` reads `config.ACCESS_TOKEN_EXP*` or `config.REFRESH_TOKEN_EXP*` — the gateway doesn't issue tokens itself (that's user-service's job) — see [Known Issues](#known-issues--inconsistencies).

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
const userServiceProxy = createProxy(
  "userService",
  config.SERVICES.USER_SERVICE_URL,
);

// ROUTE 1: User Login (Unauthenticated)
// POST /api/users/auth/login — no auth required (login creates the token)
// Rate limited: 10 requests per 15 minutes per IP (prevents brute force)
gatewayRouter.post(
  "/users/auth/login",
  endpointRateLimit(10, 900000), // 10 requests, 900000ms = 15 minutes
  userServiceProxy,
);

// ROUTE 2: Get User Profile (Authenticated)
// GET /api/users/user/profile — requires a valid JWT
// Rate limited: Combined IP + user-based (IP: 100/15min, User: 1000/15min)
gatewayRouter.get(
  "/users/user/profile",
  requireAuth,
  combinedRateLimit(),
  userServiceProxy,
);

const adminServiceProxy = createProxy(
  "adminService",
  config.SERVICES.ADMIN_SERVICE_URL,
);

gatewayRouter.get(
  "/admins/stations/station",
  requireAuth,
  combinedRateLimit(),
  adminServiceProxy,
);
gatewayRouter.get(
  "/admins/trains/train",
  requireAuth,
  combinedRateLimit(),
  adminServiceProxy,
);

// BOOKING SERVICE ROUTES (authenticated)
const bookingServiceProxy = createProxy(
  "bookingService",
  config.SERVICES.BOOKING_SERVICE_URL,
);

gatewayRouter.post(
  "/bookings/bookings",
  requireAuth,
  endpointRateLimit(5, 60000), // 5 booking attempts per minute — booking creation is expensive/sensitive
  bookingServiceProxy,
);
gatewayRouter.get(
  "/bookings/bookings",
  requireAuth,
  combinedRateLimit(),
  bookingServiceProxy,
);
gatewayRouter.get(
  "/bookings/bookings/:bookingId",
  requireAuth,
  combinedRateLimit(),
  bookingServiceProxy,
);
gatewayRouter.post(
  "/bookings/bookings/:bookingId/verify-payment",
  requireAuth,
  combinedRateLimit(),
  bookingServiceProxy,
);
gatewayRouter.post(
  "/bookings/bookings/:bookingId/cancel",
  requireAuth,
  combinedRateLimit(),
  bookingServiceProxy,
);

// PAYMENT SERVICE ROUTES
const paymentServiceProxy = createProxy(
  "paymentService",
  config.SERVICES.PAYMENT_SERVICE_URL,
);

// Public — no auth. Razorpay calls this directly; payment-service verifies
// the request itself via its own webhook signature, not a JWT. This also
// activates the raw-body middleware branch in index.ts, which was written
// for this exact path before payment-service existed.
gatewayRouter.post("/payments/webhooks/razorpay", paymentServiceProxy);

// ROUTE 3: Gateway Health Check
// GET /api/gateway/health — returns gateway status (no proxying needed)
gatewayRouter.get("/gateway/health", (req, res) => {
  return res.status(200).json({
    success: true,
    message: "Gateway is healthy",
    timestamp: new Date().toString(),
  });
});
```

This is the full route table today — 11 routes across four proxies, plus the gateway's own health check:

| Method | Path | Middleware | Forwards to |
|---|---|---|---|
| POST | `/users/auth/login` | `endpointRateLimit(10, 15min)` | userService `/auth/login` |
| GET | `/users/user/profile` | `requireAuth`, `combinedRateLimit()` | userService `/user/profile` |
| GET | `/admins/stations/station` | `requireAuth`, `combinedRateLimit()` | adminService `/stations/station` |
| GET | `/admins/trains/train` | `requireAuth`, `combinedRateLimit()` | adminService `/trains/train` |
| POST | `/bookings/bookings` | `requireAuth`, `endpointRateLimit(5, 1min)` | bookingService `/bookings` |
| GET | `/bookings/bookings` | `requireAuth`, `combinedRateLimit()` | bookingService `/bookings` |
| GET | `/bookings/bookings/:bookingId` | `requireAuth`, `combinedRateLimit()` | bookingService `/bookings/:bookingId` |
| POST | `/bookings/bookings/:bookingId/verify-payment` | `requireAuth`, `combinedRateLimit()` | bookingService `/bookings/:bookingId/verify-payment` |
| POST | `/bookings/bookings/:bookingId/cancel` | `requireAuth`, `combinedRateLimit()` | bookingService `/bookings/:bookingId/cancel` |
| POST | `/payments/webhooks/razorpay` | *(none — public)* | paymentService `/webhooks/razorpay` |
| GET | `/gateway/health` | *(none)* | answered directly, never proxied |

Cross-checked against each downstream service's own route files: user-service mounts `/auth` and `/user` directly (`user-service/src/server.ts`), admin-service mounts `/stations` and `/trains` (`admin-service/src/server.ts`), booking-service mounts its router at root (`booking-service/src/server.ts`), and payment-service's webhook router is mounted at root too (`payment-service/src/server.ts`) — so the gateway's one-segment path stripping (see [services/proxy.ts](#6-servicesproxyts--proxy--circuit-breaker) below) lines up correctly with all four proxied services today.

`searchService`, `notificationService`, and `inventoryService` have circuit breakers pre-created in `services/proxy.ts` but no route here proxies to any of them yet.

Adding a new proxied route means: create a proxy with `createProxy(serviceName, serviceUrl)` (the `serviceName` must match a key already in `circuitBreakers` inside `services/proxy.ts`), then register a route with whatever combination of `requireAuth` / rate-limit middleware fits.

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

`userService`, `adminService`, `bookingService`, and `paymentService`'s breakers are all exercised today via the routes registered in `routes/index.ts`. `searchService`, `notificationService`, and `inventoryService` still have breakers pre-created here but no route ever points at them.

State machine:

```
CLOSED  --(≥5 consecutive failures)-->  OPEN
OPEN    --(60s elapsed)-->               HALF_OPEN  (one test request allowed)
HALF_OPEN --(success)-->  CLOSED     |   HALF_OPEN --(failure)--> OPEN (timer restarts)
```

**Request forwarding + path rewrite:**

```typescript
function createProxy(serviceName: string, serviceUrl: string) {
  const circuitBreaker = circuitBreakers[serviceName];

  if (!circuitBreaker) {
    throw new Error(`No circuit breaker found for service: ${serviceName}`);
  }

  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      // Extract path (remove /api prefix only)
      // Gateway: /api/users/auth/login -> Service: /auth/login
      // Gateway: /api/users/user/profile -> Service: /user/profile
      logger.info(req.path);
      const pathParts = req.path.split("/").filter(Boolean);
      logger.info(pathParts);

      // Remove 'users' (first part), keep the rest
      // ['users', 'auth', 'login'] -> ['auth', 'login'] -> '/auth/login'
      // Only ever strips exactly one segment — can't reproduce a multi-segment
      // prefix (e.g. user-service mounts under /api/v1), which is why some
      // proxied routes 404 downstream despite matching here.
      const servicePath = "/" + pathParts.slice(1).join("/");
      logger.info(servicePath);

      const result = await forwardRequest(
        serviceUrl,
        servicePath +
          (req.url.includes("?")
            ? req.url.substring(req.url.indexOf("?"))
            : ""),
        req.method,
        req.body,
        req.headers,
        circuitBreaker,
      );

      // Forward response headers (except some)
      const excludeHeaders = [
        "connection",
        "keep-alive",
        "transfer-encoding",
        "host",
      ];
      Object.keys(result.headers).forEach((key) => {
        if (!excludeHeaders.includes(key.toLowerCase())) {
          res.setHeader(key, result.headers[key] as string);
        }
      });

      res.status(result.status).json(result.data);
    } catch (err) {
      next(err);
    }
  };
}
```

The "can't reproduce a multi-segment prefix" comment in the code is itself a little stale: it was written when user-service was mounted under `/api/v1`, which — per [Known Issue #7](#known-issues--inconsistencies) below — is no longer the case. It's left as-is in the source; this doc isn't changing it, just flagging that the specific example it gives no longer applies, even though the one-segment-only limitation it describes is still real.

Before forwarding, the incoming request headers are run through `normalizeHeaders()`, which drops `host` and `content-length` (both would be wrong for the downstream request) and flattens any multi-value header into a comma-joined string. For `GET`/`DELETE` requests, `req.body` (if present) is sent as axios `params` (i.e. a query string) rather than a request body; for every other method it's sent as `data`.

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
ACCESS_TOKEN_EXP=              # read into config, but nothing in this codebase reads it back out — see Known Issues
REFRESH_TOKEN_EXP=             # same as above

ALLOWED_ORIGINS=http://localhost:3000,http://localhost:3001
# code default if unset is a single origin, "http://localhost:3000" (not a
# comma list) — the two-origin example above is just illustrating the format

REDIS_URL=redis://localhost:6379
# note: config/index.ts has no fallback string for this one — if REDIS_URL is
# unset, `undefined` is passed to ioredis directly, which happens to default
# to localhost:6379 on its own

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
2. **`package.json` name is `"Notification Service"`**, not something referencing the API Gateway — likely left over from copying `package.json` from the notification service. This isn't just cosmetic: `config.SERVICE_NAME` is set to `packageJson.name`, and `config/logger.ts` attaches `SERVICE_NAME` to every log line as the `service` field — so every log this gateway emits is currently tagged `[Notification Service]` instead of something identifying it as the gateway.
3. **Several dependencies look unrelated to a gateway** — `@langchain/*`, `mongoose`, `resend`, and `kafkajs` are all present in `package.json` but nothing under `src/` imports any of them. Also carried over, most likely.
4. **`npm run seed` points at `src/services/seed.ts`**, which doesn't exist in this project — running that script will fail.
5. **`RedisClient.closeConnection()`, `isReady()`, `testConnection()`** are defined in `config/redis.ts` but never called anywhere — the Redis connection isn't closed during `gracefulShutdown()` in `index.ts`, and there's no health endpoint reporting Redis status.
6. **`getCircuitBreakerStatus()`** (in `services/proxy.ts`) is exported but not called by any route — there's no way to inspect circuit breaker state over HTTP today.
7. **Some configured service URLs still have no routes** — `SEARCH_SERVICE_URL`, `NOTIFICATION_SERVICE_URL`, and `INVENTORY_SERVICE_URL` are all configured and have circuit breakers pre-created, but no route in `routes/index.ts` proxies to any of them. `userService` and `adminService` were already proxied to before this pass; `bookingService` and `paymentService` (webhook only) were added across two later passes (see booking-service's and payment-service's own docs) — four of seven downstream services are now reachable in principle. The login-routing bug this doc used to describe elsewhere is fixed now (user-service dropped its `/api/v1` prefix), so `requireAuth`-gated routes like booking's can actually obtain a JWT; the payment webhook route never needed one in the first place (Razorpay calls it directly, verified by its own signature) — it's blocked only by the lack of a real Razorpay account to send one.
8. **`src/types/index.ts` is empty** — no shared types are defined there despite the file existing.
9. **`BadRequestError`, `ForbiddenError`, `ConflictError`, `InternalServerError`** are defined in `utils/error.ts` but nothing in the current codebase throws them.
10. **`config.ACCESS_TOKEN_EXP`, `REFRESH_TOKEN_EXP`, `ACCESS_TOKEN_EXP_SEC`, and `REFRESH_TOKEN_EXP_SEC`** are all loaded from `process.env` in `config/index.ts`, but nothing else in `api-gateway/src` reads any of them back out. The gateway doesn't issue or refresh tokens itself — that's user-service's job — so these look like leftovers from copying `config/index.ts` from a service that does.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
