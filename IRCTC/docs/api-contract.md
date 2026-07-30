# IRCTC Backend — API & Kafka Contract (As-Is)

This is a status-annotated reference for every HTTP route and Kafka topic that
exists in this repo's code, across all six services. It documents **current
behavior, including bugs** — not the contract as it was intended to work. For the
narrative "how does this all fit together and why is it broken" version, see
[`implementation-plan.md`](./implementation-plan.md) in this same folder.

### Status legend

| Tag | Meaning |
|---|---|
| ✅ WORKING | Reachable and behaves as the code intends |
| ❌ BROKEN | Reachable, but produces wrong/failing behavior — reason given inline |
| ⛔ UNREACHABLE | The route/handler exists in code but nothing mounts/calls it |
| 🚧 STUBBED | Mounted-reachable in principle, but the handler body is an empty placeholder |
| ⏳ NEVER FIRES / NEVER TRIGGERED | Kafka topic with no live producer, or a producer call site that can't execute |
| 🔇 SILENTLY NO-OPS | Executes but deliberately does nothing observable (e.g. logs a warning and returns) |

All response bodies below are quoted from the actual code, not paraphrased.

---

## 1. API Gateway (port 4000) — the only public entry point

Global middleware (every request): `corsMiddleware → helmet → reqLogger → [raw-body
parser for POST /api/payments/webhooks/razorpay, no route registered for it] →
express.json/urlencoded → cookieParser → morgan(dev only) → GET /health → /api/*
(gatewayRouter) → notFound → errorMiddleware`.

Path-rewrite rule used by every proxied route (`services/proxy.ts`): strip the
first path segment after `/api`, forward the remainder (plus query string)
verbatim to the target service.

```
servicePath = "/" + req.path.split("/").filter(Boolean).slice(1).join("/")
forwardedUrl = `${serviceBaseUrl}${servicePath}${queryString}`
```

### `POST /api/users/auth/login` — ❌ BROKEN
- Middleware: `endpointRateLimit(10, 900_000)` (10 req / 15 min per IP+endpoint) → proxy
- No auth required (this route issues the token)
- Rewrite: `/users/auth/login` → strips `users` → forwards to `userService/auth/login`
  → `http://localhost:4001/auth/login`
- **Why broken**: user-service actually mounts login at `/api/v1/auth/login`, not
  `/auth/login`. The one-segment strip can never reproduce the `/api/v1` prefix.
  Every login attempt through the gateway 404s against user-service.

### `GET /api/users/user/profile` — ❌ BROKEN (doubly)
- Middleware: `requireAuth` → `combinedRateLimit()` (100/15min per IP + 1000/15min
  per user) → proxy
- Rewrite: `/users/user/profile` → strips `users` → forwards to
  `http://localhost:4001/user/profile`
- **Why broken**: (1) user-service never mounts `routes/user.route.ts` in
  `server.ts` at all, so nothing exists under `/user/*` on that service regardless
  of path; (2) even if it were mounted, that file only defines `POST`/`PUT`/`DELETE
  /profile` — there is no `GET /profile` handler to match this route's method.

### `GET /api/admins/stations/station` — ❌ BROKEN (method mismatch)
- Middleware: `requireAuth` → `combinedRateLimit()` → proxy
- Rewrite: `/admins/stations/station` → strips `admins` → forwards to
  `http://localhost:4003/stations/station`
- **Why broken**: admin-service mounts `POST /stations/station` only. This gateway
  route is registered as `GET`. (Moot in the most literal sense today since
  admin-service doesn't build at all — see `implementation-plan.md` §6 — but this
  is an independent bug that would still block the route even once that's fixed.)

### `GET /api/admins/trains/train` — ❌ BROKEN (method mismatch)
- Middleware: `requireAuth` → `combinedRateLimit()` → proxy
- Rewrite: `/admins/trains/train` → strips `admins` → forwards to
  `http://localhost:4003/trains/train`
- **Why broken**: admin-service mounts `POST /trains/train` only; this route is `GET`.

### `GET /api/gateway/health` — ✅ WORKING
- No middleware, no proxy — self-contained.
- Response: `200 { success: true, message: "Gateway is healthy", timestamp: new Date().toString() }`

### Configured but never wired to a route
5 of the gateway's 7 known downstream services have a `config.SERVICES.*` URL and a
pre-built circuit breaker, but **no `createProxy()` call anywhere references them** —
`searchService` (4002), `notificationService` (4004), `bookingService` (4005 in
`.env`, code default 4005),`paymentService` (4006), `inventoryService` (4007,
code default only — not in `.env`). Only `userService` and `adminService` are ever
proxied to.

### Circuit breaker
Per-service `CLOSED → OPEN → HALF_OPEN` state machine. `CIRCUIT_BREAKER_THRESHOLD`
(default 5 failures) trips it to `OPEN` for `CIRCUIT_BREAKER_TIMEOUT` (default
60,000ms), during which every call fails fast with `503 ServiceUnavailableError`
without attempting the request. `SERVICE_TIMEOUT_MS` (default 60,000ms) is the
axios timeout that, on expiry, counts as a breaker failure and surfaces as
`504 GatewayTimeoutError`.

### Auth (`requireAuth`)
1. Reads `Authorization: Bearer <token>`, else falls back to the `accessToken` cookie.
2. Missing → `401 UnauthorizedError("Authorization token missing")`.
3. `jwt.verify(token, config.JWT_ACCESS_SECRET)` → payload `{ id }`.
4. Missing `id` on payload → `401 UnauthorizedError("Invalid token payload")`.
5. On success: `req.user = { id }`, and `req.headers["x-user-id"] = id` — this header
   is how every downstream service is meant to learn caller identity; none of them
   re-verify the JWT themselves.
6. `TokenExpiredError` → `401 { error: "TOKEN_EXPIRED" }`; `JsonWebTokenError` →
   `401 { error: "TOKEN_INVALID" }`.

### Rate limiting
Redis sorted-set sliding window, **fails open** (Redis error → request allowed).
Defaults: `RATE_LIMIT_MAX_REQUESTS=100` / `RATE_LIMIT_WINDOW_MS=900000` (15 min) per
IP; user-based limiting is 10x that (1000/15min) and only applies once `req.user`
is set. `endpointRateLimit(max, windowMs)` (used only on login, 10/15min) requires
explicit args. All three set `X-RateLimit-*` response headers; rejection adds
`Retry-After` and throws `429 TooManyRequestsError`.

---

## 2. User Service (port 4001)

**Builds and typechecks now** (`tsc --noEmit` passes clean, previously failed —
`middlewares/user-context.middleware.ts` accessed `req.user` with no
`Express.Request` augmentation anywhere in the service; a `types/express.d.ts`
now provides it) — **not verified live**, no reachable Postgres/Redis/Kafka in
the environment this was fixed in.

Mounted in `server.ts`: `app.use("/api/v1/auth", authRoutes)` and
`app.use("/user", userRoutes)` (this second one was never mounted before — every
route in `routes/user.route.ts` was ⛔ UNREACHABLE). Global middleware:
`helmet → corsMiddleware → reqLogger → cookieParser → express.json → routes →
errorHandler`.

### `POST /api/v1/auth/send-otp` — ✅ WORKING
**Body** (`zSendOtp`):
```ts
firstName: string, min 4, max 40, trimmed
lastName?: string, max 40, trimmed
email: string, valid email, trimmed, lowercased
password: string, min 8, must contain [A-Z], [a-z], [0-9]
```
**Flow**: reject if email already exists (`409 ConflictError "User already exists"`)
→ bcrypt-hash password (cost 12) → generate 6-digit OTP → HMAC it with
`OTP_HMAC_SECRET` → store `{ hashedOtp, meta }` in Redis at `otp:session:<uuid>`
(TTL `OTP_TTL`, default 300s) → set httpOnly `otp_session` cookie to that uuid →
rate-limit check via `otp:rate:<email>` (max `OTP_RATE_MAX_PER_HOUR`, default 5,
1hr window) → publish `notification.otp-email` with `{ email, otp, ttlMinutes }`.
**Success**: `200 { success: true, message: "OTP sent successfully" }` (OTP session
id is cookie-only, never in the body).
**Errors**: `400` (zod, body shape `{success:false, message}` — no `error` code
field on this path, unlike the ones below), `409 { error: "CONFLICT", message:
"User already exists" }`, `429 { error: "OTP_RATE_LIMIT", message: "Too many OTP
requests. Try again later" }`.

### `POST /api/v1/auth/verify-otp` — ✅ WORKING
**Body** (`zVerifyOtp`): `otp: string, exactly 6 digits`.
**Flow**: reads `otp_session` cookie (missing → `400 BadRequestError("OTP session
is missing")`) → looks up `otp:session:<id>` in Redis → checks attempt cap
(`otp:attempt:<email>`, max `OTP_MAX_VERIFY_ATTEMPTS`, default 5) →
`crypto.timingSafeEqual` on the recomputed HMAC vs. stored → on mismatch, increments
attempt counter and returns `400 { error: "OTP_INVALID", message: "Invalid or
expired OTP" }` → on match, clears the Redis session/attempt/rate keys, creates the
`User` row (`emailVerified: true`), fires `sendWelcomeEmail` (fire-and-forget,
log-only on failure), and strips the bcrypt `password` hash before returning —
previously returned the created-user object **as-is, including the password
hash**; every other endpoint in this service already stripped `password` before
responding, this one didn't. `sendWelcomeEmail` also used to exist and be fully
wired on the notification-service side but was never called here — it's called now.
**Success**: `201 { message: "Account is created", data: <safeUser, no password field> }`.

### `POST /api/v1/auth/login` — ✅ WORKING
**Body** (`zLogin`): same email/password shape as send-otp.
**Flow**: look up user by email (not found, or no password set — e.g. an
OAuth-only stub — → `400 BadRequestError("Email not found")`) → `bcrypt.compare`
(mismatch → `400 BadRequestError("Incorrect password")`) → issue access token
(`JWT_ACCESS_SECRET`, `ACCESS_TOKEN_EXP` = 15m) and refresh token
(`JWT_REFRESH_SECRET`, `REFRESH_TOKEN_EXP` = 7d, includes a random `jti`) → store
`jti` at `refresh:<userId>:<deviceId>` in Redis (device fingerprint = sha256 of
`User-Agent|IP|Accept`, first 16 hex chars — so one active refresh session per
device, not per user) → cache the password-stripped user at `user:<userId>` (TTL
`REDIS_USER_TTL`, default 86400s).
**Success**: `200 { success: true, message: "Logged in successfully", data: <safeUser> }`,
plus both tokens set as httpOnly/secure/sameSite=strict cookies (`accessToken`,
`refreshToken`) — neither token is ever in the JSON body.
**Errors**: `400` (zod), `400 { error:"BAD_REQUEST", message:"Email not found" }`,
`400 { error:"BAD_REQUEST", message:"Incorrect password" }`.

### `POST /api/v1/auth/refresh` — ✅ WORKING (mostly)
No body — reads the `refreshToken` cookie.
**Flow**: missing cookie → `401 UnauthorizedError("Refresh token is missing",
"LOGIN_AGAIN")` → `jwt.verify` against `JWT_REFRESH_SECRET` → look up
`refresh:<userId>:<deviceId>` in Redis (missing → `403 ForbiddenError("Session
expired", "LOGIN_AGAIN")`) → if the stored `jti` doesn't match the token's `jti`
(reuse/replay), delete the Redis entry and `403 ForbiddenError("Refresh token
reused", "LOGIN_AGAIN")` → otherwise issue a new access+refresh pair and overwrite
the Redis entry with the new `jti` (rotation).
**Gap**: an expired/tampered refresh token makes `jwt.verify` throw a plain
`JsonWebTokenError`/`TokenExpiredError` — neither is an `AppError`, so it falls
through to the generic `500` handler instead of a clean `401`.
**Success**: `200 { success: true, message: "Access and refresh tokens reissued" }`
(new tokens are cookie-only, not in the body).

### `POST /user/profile` — ✅ WORKING
Behind `getUserContext` (reads `x-user-id` header, no JWT re-verification —
trusts the gateway already did it). No body validation, no Zod schema. Reads
cache-first (`user:<userId>` in Redis), else DB. Previously mounted nowhere —
now mounted at `/user`. The cache-miss bug (`userService.getUserProfile`
computed and cached the password-stripped `safeUser`, but returned the original
unscrubbed row instead — a cold-cache read leaked the password hash, a warm-cache
read didn't) is fixed: both paths now return the scrubbed copy.
**Success**: `200 { data: <safeUser>, success: true }`.
**Errors**: `400 { error:"BAD_REQUEST", message:"user Id is missing " }`,
`404 { error:"NOT_FOUND", message:"User not found" }`.

### `PUT /user/profile` — ✅ WORKING
Was an empty `// TODO` handler that would hang forever (no response, `next()`
never called) even once mounted. Now implemented: validates `{firstName?,
lastName?}` via `zUpdateProfile` (email/password intentionally excluded — those
need their own verification-gated flows), updates the row, refreshes the Redis
cache.
**Success**: `200 { data: <safeUser>, success: true }`.
**Errors**: `400` (zod), `404 { error:"NOT_FOUND", message:"User not found" }`.

### `DELETE /user/profile` — ✅ WORKING
Was the same empty-stub hang as `PUT`. Now implemented: deletes the row, clears
the `user:<userId>` Redis cache entry. Doesn't revoke outstanding refresh-token
sessions on other devices — there's no registry of a user's active device
sessions to enumerate and clear from here.
**Success**: `200 { success: true, message: "Account deleted successfully" }`.
**Errors**: `404 { error:"NOT_FOUND", message:"User not found" }`.

### `GET /user/internal/:userId` — ✅ WORKING (new)
Didn't exist before this pass. Behind `internalAuth` (shared-secret
`x-internal-service-key` header, not a JWT) — for other services, chiefly
booking-service, to resolve a user's profile without going through the
gateway's auth flow. Reuses `userService.getUserProfile` (same cache-first
read, same scrubbed shape).
**Success**: `200 { data: <safeUser>, success: true }`.
**Errors**: `403 { error:"FORBIDDEN", message:"Invalid or missing internal service key" }`,
`404 { error:"NOT_FOUND", message:"User not found" }`.

### Kafka — produces only (`kafka/producer/notification-producer.ts`)
| Method | Topic | Payload | Called from? |
|---|---|---|---|
| `sendOtpEmail({email, otp, ttlMinutes})` | `notification.otp-email` | `{ email, otp, ttlMinutes }` | ✅ `auth.service.ts` → `sendOtp` |
| `sendWelcomeEmail(email, firstName)` | `notification.welcome-email` | `{ email, firstName }` | ✅ `auth.service.ts` → `verifyOtp` (previously never called anywhere in this service) |

### Data model (`User`)
`id (uuid) · firstName · lastName · email (unique) · password (nullable) ·
emailVerified (bool, default false) · createdAt · updatedAt`. No sessions/roles
table — all session state lives in Redis.

---

## 3. Admin Service (port 4003)

**Builds and typechecks now** (`tsc --noEmit` passes clean) — **not verified
live**, no reachable Postgres/Kafka in the environment this was fixed in.
Previously the whole service failed to build: `config/index.ts` existed but was
**empty**, and `index.ts` imported a `config/db.ts` that never existed anywhere in
the project (a dead Mongoose-style leftover — Prisma's own `config/prisma.ts`
already owns the DB connection, no separate connect call was ever needed). Fixed
by populating `config/index.ts` and dropping the dead import.

Mounted in `server.ts`: `app.use("/stations", stationRoutes)`,
`app.use("/trains", trainRoutes)`, `app.use("/schedules", scheduleRoutes)` (this
last one was previously never mounted). Every route across all three routers is
now behind `getUserContext` (previously zero auth was wired up anywhere in this
service).

### `POST /stations/station` — ✅ WORKING
**Body** (`zStation`):
```ts
name: string, min 4, max 40, trimmed
code: string, min 2, max 10, trimmed, uppercased
city: string, min 2, max 40, trimmed
state?: string, max 40, trimmed
```
**Flow**: duplicate `code` → `409 ConflictError("Station already exists")` →
`prisma.station.create` → `adminProducer.publishStationCreated(station)`.
The controller now `await`s `stationService.createStation(...)`, so a duplicate
correctly surfaces as a `409` instead of an unhandled promise rejection.
**Success**: `200 { success: true, message: "Station created successfully", data: <Station row> }`
(previously `"OTP sent successfully"` — a copy-pasted, unrelated message).

### `POST /trains/train` — ✅ WORKING
**Body** (`zTrain` + `zSeat`):
```ts
trainNumber: string, 1-10 chars, trimmed
trainName: string, 4-40 chars, trimmed
coachName?: string, max 20, trimmed  // defaults to "AC" if omitted
seats: array of { seatNumber: positive int, seatType: LOWER|MIDDLE|UPPER|SIDE_LOWER|SIDE_UPPER, price: positive number }, min 1 seat
```
**Flow**: duplicate `trainNumber` → `409 ConflictError`; duplicate `seatNumber`s in
payload → `400 BadRequestError("Duplicate seat numbers found")`; else
`prisma.train.create` with nested seats (one transaction) → publish
`admin.train-created` (failure here is caught and logged, not thrown — a Kafka
outage won't turn a successful train creation into a 500, but also won't surface to
the caller).
**Success**: `200 { success: true, message: "Train created successfully" }`.

### `POST /trains/route` — ✅ WORKING
**Body** (`zRoute` + `zRouteStation`):
```ts
trainId: uuid
stations: array of { stationId: uuid, sequenceNumber: positive int, arrivalTime?: "HH:mm", departureTime?: "HH:mm", distanceFromOrigin?: non-negative number }, min 2 stations
```
**Flow**: train not found → `404 NotFoundError("Train Not found")`; route already
exists for this train → `409 ConflictError("Route already exists for this train")`
(previously this check was inverted — `if (!existingRoute) throw
"already exists"` — which blocked every train's *first* route and let a genuine
duplicate fall through to an uncaught Prisma error instead; also fixed the
"existis" typo). Validates all `stationId`s exist (`400 "One or more station Ids
are invalid"`) and that `sequenceNumber`s are contiguous from 1 (`400 "Sequence
numbers must be contiguous starting from 1"` — previously read "continous
starting free"). On success, publishes `admin.route-created` (previously
commented out; failure now caught+logged, matching `createTrain`'s pattern).
**Success**: `200 { success: true, message: "Route created successfully" }`.

### `GET /trains/train/:trainId` — ✅ WORKING
Fetches a train with its seats (ordered by seatNumber) and its route (ordered by
sequenceNumber, each stop including the full station record). Previously mounted
as `POST /trains/route/:id` with a route param named `:id` while the controller
read `req.params.trainId` — always `undefined`, always a 400. Now a `GET` at
`/trains/train/:trainId`, matching the controller's own param name.
**Success**: `200 { success: true, data: <train with seats + route + stations> }`.
`404 { error: "NOT_FOUND", message: "Train not found" }` if the id doesn't exist.

### `POST /schedules/schedule` — ✅ WORKING
Previously defined in `schedule.route.ts` but never mounted in `server.ts` — now
mounted at `/schedules`.
**Body** (`zSchedule`): `trainId: uuid`, `departureDate: coerced date`,
`status?: ACTIVE|CANCELLED` (accepted by the schema but silently dropped — the
controller only destructures `{trainId, departureDate}`).
**Flow**: train not found → `409 ConflictError("Train not found")`; train has no
seats → `400 BadRequestError("Train not found")` (same message text, different
status code as the previous branch); no route defined → `400 "Train has no route
defined. Create a route first "`; duplicate `(trainId, departureDate)` →
`409 "Schedule already exists for this train on this date "`; else creates the
`Schedule` row and publishes `admin.schedule-created` with a fully denormalized
payload (train + seats + route inlined, for inventory-service and search-service).
**Success**: `200 { success: true, message: "Schedule created successfully" }`
(previously `"Train created successfully"` — copy-pasted from `train.controller.ts`).

### Kafka — produces only, no consumer at all
| Topic | Payload | Call site | Status |
|---|---|---|---|
| `admin.station-created` | `{ eventType: "STATION_CREATED", data: <Station row>, timestamp }` | `station.service.ts`, keyed `station-<id>` | ✅ fires |
| `admin.train-created` | raw `Train` row incl. nested `seats` | `train.service.ts`, publish failures caught+logged | ✅ fires |
| `admin.route-created` | raw `Route` row (incl. `routeStations`) | `train.service.ts`'s `createRoute`, publish failures caught+logged | ✅ fires (previously commented out) |
| `admin.schedule-created` | `ScheduleCreatedPayload` — schedule + train + seats + route, fully denormalized | `schedule.service.ts` | ✅ fires (previously unreachable — route was never mounted) |
| `admin.schedule-cancelled` | `{ eventType: "SCHEDULE_CANCELLED", data: <Schedule row>, timestamp }` | defined, zero call sites — no cancel feature exists at all | ⏳ never fires — building a cancel-schedule feature was out of scope for this fix pass |
| `admin.train-updated` / `admin.station-updated` | — | no producer method implemented for either | ⏳ n/a |

### Data model
`Station(id, name unique, code unique, city, state?) · Train(id, trainNumber
unique, trainName, coachName default "AC", totalSeats) · Seat(id, trainId,
seatNumber, seatType enum, price; unique[trainId,seatNumber]) · Route(id, trainId
unique — one route per train) · RouteStation(id, routeId, stationId,
sequenceNumber, arrivalTime?, departureTime?, distanceFromOrigin default 0; unique
per [routeId,sequenceNumber] and [routeId,stationId]) · Schedule(id, trainId,
departureDate date, status enum default ACTIVE; unique[trainId,departureDate])`.

---

## 4. Search Service (port 4002)

**Builds and typechecks now** (`tsc --noEmit` passes clean) — **not verified
live**, no reachable Elasticsearch or Kafka in the environment this was fixed
in. The compile failure previously documented here (a singular/plural route
import mismatch, a default/named export mismatch on `errorMiddleware`) was
already stale by the time this was checked — both had apparently been fixed
independently already. The actual blockers: `search.controller.ts` imported
`searchService` from a nonexistent `../services/inventory.service` (fixed to
`../services/search.service`), and three unused api-gateway-style scaffold
files (`config/redis.ts`, `middlewares/auth.middleware.ts`,
`middlewares/rate-limiting.middleware.ts`) referenced config fields
(`REDIS_URL`, `JWT_ACCESS_SECRET`, `RATE_LIMIT_MAX_REQUESTS`,
`RATE_LIMIT_WINDOW_MS`) that don't exist on this service's `Config` type —
`tsc` type-checks every file matched by `tsconfig.json`'s `include` glob
regardless of whether anything actually imports it, so this dead code blocked
the whole build even though nothing outside those three files referenced them
(confirmed via grep before deleting). A `notFound` 404 handler (written but
never mounted) is now registered before the error middleware too.

### `GET /trains` (`search.routes.ts`, intended at `/search/trains`) — ✅ WORKING
**Query params** (`zSearchTrains`): `from: string 1-50 chars`, `to: string 1-50
chars`, `date?: "YYYY-MM-DD"`.
**Flow**: `searchService.searchTrains({from, to, date})` runs a real nested
Elasticsearch query (resolves each station via exact-code → completion-suggester →
fuzzy-match, then finds trains whose route contains both stations in the right
order, attaching the matching date's schedule if `date` was given) and returns a
fully-formed result. The controller now returns that result instead of discarding
it for a hardcoded message.
**Success**: `200 { success: true, data: <SearchTrainsResult> }`.

### `GET /autocomplete` — ✅ WORKING (logic-wise)
Calls `searchService.autocompleteStation(q)` (completion suggester, fuzzy) and
returns `200 { success: true, data: [{name?, code, stationId}, ...] }`.

### `GET /debug/stations` — ✅ WORKING
Now calls `getAllStations()` (previously called `autocompleteStation(q)` —
identical to `/autocomplete` — a copy-paste bug).

### `GET /debug/trains` — ✅ WORKING
Now calls `getAllTrains()` (previously the same copy-paste bug as `/debug/stations`).

### Kafka — consumer (`kafka/search.service.ts`, group `search-service-group-v2`)
| Topic | Handler | Payload expected | Status |
|---|---|---|---|
| `admin.station-created` | `indexStation` | `{eventType, data:{id,name,code,city,state?}, timestamp}` | ⏳ never fires end-to-end — admin-service publishes it, but neither service has been run live |
| `admin.route-created` | `indexTrainRoute` | `{train, routeStations[]}` | ⏳ never fires end-to-end — admin-service now publishes exactly this shape (previously would have published a bare `Route` row with no `train` field, silently no-op'ing against this handler's `if (!train \|\| !routeStations) return` guard even once "fixed"), but neither service has been run live |
| `admin.schedule-created` | `indexSchedule` | `{scheduleId, trainId, departureDate, status, seats?}` | ⏳ never fires end-to-end — admin-service now publishes it, but neither service has been run live |
| `admin.schedule-cancelled` | `cancelSchedule` | `{eventType, data:{id, trainId, status}, timestamp}` | ⏳ never fires — no caller in admin-service |
| `inventory.seat-availability-updated` | `updateSeatAvailability` | `{scheduleId, trainId, available, locked, booked}` | ⏳ never fires end-to-end — inventory-service can publish this now, but only once it receives a schedule to track, and neither service has been run live |

**The DLQ gap is fixed**: `indexStation`/`indexSchedule`/`cancelSchedule`/
`updateSeatAvailability` used to each catch their own Elasticsearch errors and
log-and-swallow them internally, so `withDLQ` (which only retries/forwards
errors that propagate *out* of the wrapped handler) never saw a failure. All
four now let errors propagate. `indexTrainRoute` already didn't swallow its own
errors, so it needed no change.

`indexStation`'s write now includes the `name` field (previously missing —
`code`/`city` were written but `name` wasn't, even though it was right there on
the event).

### Elasticsearch indices
`stations` (autocomplete) and `trains` (nested `route` + `schedules` +
`seatSummary`) are the only two ever created. `ROUTE_INDEX`/`SCHEDULE_INDEX`
constants exist but name indices `initIndices` never creates — route/schedule data
lives inside the `trains` document instead.

---

## 5. Notification Service (port 4004) — no HTTP surface at all

`server.ts` registers **zero routes**, not even a health check — the Express app
exists only so the process has a listener. This is a pure Kafka consumer.

Subscribes to **every** topic in `KAFKA_TOPICS` (`Object.values(KAFKA_TOPICS)`),
including admin/inventory/payment topics that mean nothing to this service and even
its own DLQ topic — anything without a matching `case` falls to
`logger.warn("Unknown topic: ...")` and is dropped.

| Topic | Handler | Expected payload | Producer exists? | Status |
|---|---|---|---|---|
| `notification.otp-email` | `handleOtpEmail` | `{email, otp, ttlMinutes?}` | ✅ user-service, fields match exactly | ✅ WORKS END-TO-END |
| `notification.welcome-email` | `handleWelcomeEmail` | `{email, firstName}` | ✅ user-service, fields match | ⏳ never fires end-to-end — user-service now calls the producer, but neither service has been run live |
| `booking.confirmed` | `handleBookingConfirmed` | `BookingConfirmedData` — **no `email` field**; handler does an unsafe cast to pull one out anyway | ❌ no `booking-service` exists in this repo | ⏳ NEVER TRIGGERED, and would 🔇 SILENTLY NO-OP (warn+return) even if triggered, since `email` is never actually present |
| `booking.failed` | `handleBookingFailed` | `BookingFailedData` — same missing-`email` issue | ❌ none | same as above |
| `booking.cancelled` | `handleBookingCancelled` | `BookingCancelledData` — same missing-`email` issue | ❌ none | same as above |
| everything else in `KAFKA_TOPICS` | `default` case | n/a | n/a | logged as "Unknown topic", dropped |

**DLQ path** (`dlq.notification-service`, via `shared/utils/dlqHanlder.ts`,
3 retries): only actually exercised by `handleOtpEmail`/`handleWelcomeEmail`
throwing on missing required fields, or by `emailService`'s own send failing after
its separate 3-attempt retry. The three booking handlers catch their own
"no email" case and `return` instead of throwing, so that failure mode never
reaches the DLQ — it's a log line, nothing more.

### Email templates (`templates/index.ts`)
`getOtpTemplate`, `getWelcomeTemplate`, `getBookingConfirmedTemplate`,
`getBookingFailedTemplate`, `getBookingCancelledTemplate` are all invoked from
`email-service.ts`. `getTicketConfirmationTemplate` is fully defined but **never
called from anywhere** in this service.

Sender is Resend (`RESEND_API_KEY`, `MAIL_SEND` as the `from` address) —
`SENDGRID_API_KEY` is read into config but never used; `FRONTEND_URL` is read into
config but never used either (the welcome template's login link uses
`ALLOWED_ORIGINS` instead, so these two "where's the frontend" vars are
inconsistent with each other).

---

## 6. Inventory Service (port 4007)

Mounted directly at root in `server.ts` (no `/inventory` prefix — the gateway
would strip that segment anyway once a route is wired up there, which it
currently is not, see §1). No global auth middleware; each route picks its own
guard. **Not verified against a live Postgres/Kafka** — confirmed only via
`tsc --noEmit` and `prisma generate` in the environment this was built in.

### `GET /schedules/:scheduleId/availability` — ✅ WORKING (logic-wise)
No auth. Returns `200 { success: true, data: <ScheduleAvailability> }` — aggregate
counts only (`available`/`locked`/`booked`), no per-seat detail. `404
{ error: "NOT_FOUND", message: "Schedule not found in inventory" }` if this
`scheduleId` was never initialized by a `SCHEDULE_CREATED` event.

### `GET /schedules/:scheduleId/seats` — ✅ WORKING (logic-wise)
Behind `userOrInternal` — either a request that already passed the gateway's
`requireAuth` (`x-user-id` header) or a direct call from booking-service bearing
`x-internal-service-key`. Query params (`zSeatFilters`): `status?`, `seatType?`,
`fromSeq?`, `toSeq?` — the latter two narrow per-seat status to a specific
journey segment instead of the whole route. `200 { success: true, data:
{ scheduleId, totalSeats, seats: [...] } }`.

### `POST /seats/lock` — ✅ WORKING (logic-wise)
Behind `internalAuth` (shared-secret header only — booking-service is the only
intended caller). Body (`zLockSeats`): `scheduleId`, `seatIds: string[]`,
`userId`, `ttlSeconds?`, `fromSeq?`, `toSeq?`. Row-level `FOR UPDATE NOWAIT` locks
the requested seats; `409 ConflictError` if any are already
locked/booked (or, for segment locks, overlap an existing segment). Wrapped in
`retryTransaction` (retries on Postgres serialization/lock/deadlock errors, not
on business-logic conflicts). `200 { success: true, message, data: { scheduleId,
lockedSeats, lockExpiresAt } }`.

### `POST /seats/unlock` — ✅ WORKING (logic-wise)
Same guard/shape family as lock. `409` if a seat isn't `LOCKED`, `403
ForbiddenError` if it's locked by a different `userId`.

### `POST /seats/confirm` — ✅ WORKING (logic-wise)
Transitions `LOCKED` → `BOOKED` (or, for segment locks, the matching
`SeatSegmentLock` row) and stamps `bookingId`. `409 { code: "LOCK_EXPIRED" }` if
the lock already expired or was never held by this `userId`.

### `POST /seats/cancel-booking` — ✅ WORKING (logic-wise)
Releases a confirmed booking's seats back to `AVAILABLE` (segment locks are
checked first, since a segment booking never sets `SeatInventory.bookingId` —
only `SeatSegmentLock.bookingId`). `404` if no booked seats match.

### Kafka — consumer (`kafka/consumer/inventory.consumer.ts`, group
`inventory-service-group`)
| Topic | Handler | Status |
|---|---|---|
| `admin.schedule-created` | `initializeInventory` — creates the `ScheduleInventory` + `SeatInventory` rows (+ `RouteStop` rows if the event's `route` array is present) | ✅ admin-service now publishes this (its `POST /schedules/schedule` is mounted) — unverified end-to-end since neither service has been run live in this environment, but no longer structurally blocked |
| `admin.schedule-cancelled` | `cancelScheduleInventory` — marks the schedule and all its seats `CANCELLED` | ⏳ NEVER FIRES — no caller anywhere in admin-service |

Both handlers are idempotent via an `IdempotencyRecord` row keyed by
`eventKey` (`SCHEDULE_CREATED:<scheduleId>` / `SCHEDULE_CANCELLED:<scheduleId>`),
so redelivery of the same Kafka message is a safe no-op rather than a duplicate
row or a double-cancel.

### Kafka — producer (`kafka/producer/inventory.producer.ts`)
| Method | Topic | Called from |
|---|---|---|
| `publishSeatAvailabilityUpdated` | `inventory.seat-availability-updated` | `initializeInventory`, `cancelScheduleInventory`, `lockSeats`, `unlockSeats`, `confirmSeats`, `cancelBooking`, `recountAndPublish` (used by the lock-expiry job) — every mutation that changes aggregate counts |

Publish failures are caught and logged, not thrown — a Kafka outage doesn't turn
a successful seat mutation into a 500, but also doesn't retry the specific
missed publish beyond the producer's own 3-attempt internal retry.

### Background job (`utils/lockExpiry.ts`)
Runs once on startup, then every `LOCK_EXPIRY_INTERVAL_MS` (default 60s).
Uses `pg_try_advisory_lock` for leader election, so running multiple replicas of
this service doesn't run the sweep redundantly. Releases expired
`SeatSegmentLock` rows first (recomputing each affected seat's summary status),
then expired full-journey `SeatInventory` locks, republishing availability after
each schedule it touches.

### Data model
`ScheduleInventory(id, scheduleId unique, trainId, trainNumber, trainName,
departureDate, totalSeats, available, locked, booked, status default "ACTIVE",
version) · SeatInventory(id, scheduleInventoryId, scheduleId, seatId, seatNumber,
seatType, price, status enum AVAILABLE|LOCKED|BOOKED|CANCELLED, lockedBy?,
lockedAt?, lockExpiresAt?, bookingId?, version; unique[scheduleId,seatId] and
[scheduleId,seatNumber]) · SeatSegmentLock(id, scheduleId, seatId, fromSeq, toSeq,
status, lockedBy?, lockedAt?, lockExpiresAt?, bookingId?, version) ·
RouteStop(id, scheduleId, stationId, stationName, stationCode, sequenceNumber;
unique per [scheduleId,stationId] and [scheduleId,sequenceNumber]) ·
IdempotencyRecord(id, eventKey unique, processedAt)`.

---

## 7. Full Kafka topic matrix

| Topic | Producer(s) | Consumer(s) | End-to-end status |
|---|---|---|---|
| `notification.otp-email` | user-service (`sendOtp`) | notification-service | ✅ works |
| `notification.welcome-email` | user-service (`verifyOtp`, previously never called) | notification-service (handles correctly) | ⏳ never fires end-to-end — user-service now calls it, but neither service has been run live |
| `notification.booking-email` | none | notification-service (falls to `default`) | ⏳ n/a — no producer, no handler case either |
| `notification.payment-email` | none | notification-service (falls to `default`) | ⏳ n/a |
| `admin.station-created` | admin-service (`createStation`) | search-service (`indexStation`) | ⏳ never fires end-to-end — both services now build and publish/consume this correctly, but neither has been run live |
| `admin.train-created` | admin-service (`createTrain`) | none | produced, nothing consumes it |
| `admin.route-created` | admin-service (`createRoute`, now publishes `{...route, train}` matching the consumer's expected shape) | search-service (`indexTrainRoute`) | ⏳ never fires end-to-end — same as `admin.station-created` above |
| `admin.schedule-created` | admin-service (`createSchedule`, route now mounted) | search-service (`indexSchedule`), inventory-service (`initializeInventory`) | ⏳ never fires end-to-end — all three services now build, but none has been run live |
| `admin.train-updated` / `admin.station-updated` / `admin.route-updated` | no producer implemented for any | none | ⏳ n/a |
| `admin.schedule-cancelled` | admin-service — zero call sites, no cancel-schedule feature built | search-service (`cancelSchedule`), inventory-service (`cancelScheduleInventory`) | ⏳ never fires |
| `inventory.seat-availability-updated` | inventory-service (every seat-mutating operation) | search-service (`updateSeatAvailability`) | ⏳ never fires end-to-end — inventory-service can now publish it, but only once it receives a `SCHEDULE_CREATED` event to seed a schedule first, and neither service has been run live |
| `booking.confirmed` / `booking.failed` / `booking.cancelled` | no booking-service exists | notification-service (would silently no-op — missing `email` field) | ⏳ never fires |
| `payment.success` / `payment.failed` | no payment-service exists | notification-service (falls to `default`) | ⏳ n/a |
| `dlq.booking-service` | n/a (would-be DLQ target) | notification-service (falls to `default`) | inert |
| `dlq.inventory-service` | inventory-service, on 3 failed retries | none | effectively inert (nothing consumes this service's own DLQ) |
| `dlq.search-service` | search-service, on 3 failed retries — internal catch blocks that used to make this rarely reached are now fixed, so it triggers on a genuine Elasticsearch outage | none | fires when it should now, still nothing consumes it |
| `dlq.notification-service` | notification-service, on 3 failed retries | notification-service itself (subscribed to `Object.values(KAFKA_TOPICS)`, falls to `default`) | fires occasionally, consumed as "unknown topic" by the same service that produced it |

`DLQ_MAX_RETRIES = 3` (`shared/constants/kafka-topics.ts`), shared by every
service's `withDLQ` wrapper (`shared/utils/dlqHanlder.ts`).

---

## 8. Shared error-response shapes

Every service's `errorMiddleware` produces one of two shapes:
- Thrown `AppError` subclass → `{ success: false, error: <code>, message: <message> }`
  at `err.statusCode`.
- Anything else (uncaught) → generic `500 { success: false, error: "SERVER_ERROR" (or "INTERNAL_SERVER_ERROR", varies by service), message: "Internal Server Error" }`.

Manual `ErrorResponse(res, status, {message})` calls (used for Zod-validation
failures, across every service) produce a **third**, slightly different shape:
`{ success: false, message }` — no `error` code field at all. A client cannot
assume `error` is always present; it depends on whether the failure came from a
thrown `AppError` or a hand-called `ErrorResponse`.

| Class | HTTP status | Default `code` |
|---|---|---|
| `BadRequestError` | 400 | `BAD_REQUEST` |
| `UnauthorizedError` | 401 | `UNAUTHORIZED` (or `TOKEN_EXPIRED` / `TOKEN_INVALID` / `LOGIN_AGAIN`, set per call site) |
| `ForbiddenError` | 403 | `FORBIDDEN` (or `LOGIN_AGAIN`, set per call site) |
| `NotFoundError` | 404 | `NOT_FOUND` |
| `ConflictError` | 409 | `CONFLICT` |
| `TooManyRequestsError` | 429 | `TOO_MANY_REQUESTS` (or `OTP_RATE_LIMIT`) |
| `InternalServerError` | 500 | `SERVER_ERROR` |
| `ServiceUnavailableError` (gateway only) | 503 | `SERVICE_UNAVAILABLE` |
| `GatewayTimeoutError` (gateway only) | 504 | `GATEWAY_TIMEOUT` |

---

## 9. Services referenced but not implemented in this repo

`booking-service` and `payment-service` each have a `config.SERVICES.*` URL in
api-gateway and/or Kafka topics named for them (`booking.*`, `payment.*`), but
**no directory, code, or `docs/` folder for either exists anywhere in this
repo.** Any reference to them elsewhere in this document describes what other
services *expect* to talk to, not something that can currently be exercised.
(`inventory-service` used to be listed here too — it's now built, see §6.)
