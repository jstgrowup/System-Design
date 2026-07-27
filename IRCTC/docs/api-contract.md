# IRCTC Backend — API & Kafka Contract (As-Is)

This is a status-annotated reference for every HTTP route and Kafka topic that
exists in this repo's code, across all five services. It documents **current
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

## 2. User Service (port 4001 per `.env`; gateway assumes 4001 too)

Mounted in `server.ts`: `app.use("/api/v1/auth", authRoutes)` only.
`routes/user.route.ts` is never imported/mounted — every route in it is ⛔ UNREACHABLE.
Global middleware: `helmet → corsMiddleware → reqLogger → cookieParser →
express.json → routes → errorHandler`.

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

### `POST /api/v1/auth/verify-otp` — ❌ BROKEN (data leak)
**Body** (`zVerifyOtp`): `otp: string, exactly 6 digits`.
**Flow**: reads `otp_session` cookie (missing → `400 BadRequestError("OTP session
is missing")`) → looks up `otp:session:<id>` in Redis → checks attempt cap
(`otp:attempt:<email>`, max `OTP_MAX_VERIFY_ATTEMPTS`, default 5) →
`crypto.timingSafeEqual` on the recomputed HMAC vs. stored → on mismatch, increments
attempt counter and returns `400 { error: "OTP_INVALID", message: "Invalid or
expired OTP" }` → on match, clears the Redis session/attempt/rate keys, creates the
`User` row (`emailVerified: true`).
**Why broken**: the created-user object is returned **as-is, including the bcrypt
`password` hash**, in the response body — every other endpoint in this service
strips `password` before responding; this one doesn't.
**Also note**: `notificationProducer.sendWelcomeEmail` exists and is fully wired on
the notification-service side, but is never called here (or anywhere) — no welcome
email is ever sent despite the pipe being ready end to end.
**Success**: `201 { message: "Account is created", data: <full User row incl. password hash> }`.

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

### `POST /profile` — ⛔ UNREACHABLE (would be ✅ if mounted)
`user.route.ts`, behind `getUserContext` (reads `x-user-id` header, no JWT
re-verification — trusts the gateway already did it). No body validation, no
Zod schema. Reads cache-first (`user:<userId>` in Redis), else DB.
**Bug if it were reachable**: on a cache miss, `userService.getUserProfile`
computes and caches the password-stripped `safeUser`, but then **returns the
original unscrubbed row** instead — so a cold-cache read leaks the password hash,
a warm-cache read doesn't.
**Success** (as coded): `200 { data: <safeUser or unscrubbed row>, success: true }`.
**Errors**: `400 { error:"BAD_REQUEST", message:"user Id is missing " }`,
`404 { error:"NOT_FOUND", message:"User not found" }`.

### `PUT /profile` — ⛔ UNREACHABLE + 🚧 STUBBED
Empty handler body (`// TODO TASK FOR YOU`) — even once mounted, a request here
would hang forever (no response sent, `next()` never called).

### `DELETE /profile` — ⛔ UNREACHABLE + 🚧 STUBBED
Same as above — empty handler, would hang.

### Kafka — produces only (`kafka/producer/notification-producer.ts`)
| Method | Topic | Payload | Called from? |
|---|---|---|---|
| `sendOtpEmail({email, otp, ttlMinutes})` | `notification.otp-email` | `{ email, otp, ttlMinutes }` | ✅ `auth.service.ts` → `sendOtp` |
| `sendWelcomeEmail(email, firstName)` | `notification.welcome-email` | `{ email, firstName }` | ⏳ never called anywhere in this service |

### Data model (`User`)
`id (uuid) · firstName · lastName · email (unique) · password (nullable) ·
emailVerified (bool, default false) · createdAt · updatedAt`. No sessions/roles
table — all session state lives in Redis.

---

## 3. Admin Service (`.env` says port 4001; gateway assumes 4003)

**The whole service fails to build.** `src/index.ts` imports `./config` and
`./config/db`; `config/kafka.ts`, `config/logger.ts`, `config/prisma.ts`, and
`middlewares/cors.middleware.ts` all import `config` from `../config`/`.` —
**neither `config/index.ts` nor `config/db.ts` exists anywhere in this project.**
Everything below describes the code as written, on the assumption this were fixed;
in the checked-in state, none of it can actually run.

Mounted in `server.ts`: `app.use("/stations", stationRoutes)`,
`app.use("/trains", trainRoutes)`. `schedule.route.ts` is never imported — its one
route is ⛔ UNREACHABLE. No auth/user-context middleware anywhere in this service.

### `POST /stations/station` — ❌ BROKEN
**Body** (`zStation`):
```ts
name: string, min 4, max 40, trimmed
code: string, min 2, max 10, trimmed, uppercased
city: string, min 2, max 40, trimmed
state?: string, max 40, trimmed
```
**Flow**: duplicate `code` → `409 ConflictError("Station already exists")` →
`prisma.station.create` → `adminProducer.publishStationCreated(station)`.
**Why broken**: the controller calls `stationService.createStation(...)` **without
`await`** — the `200` response fires before the DB write/Kafka publish settle, and
the `ConflictError` on a duplicate becomes an unhandled promise rejection instead of
reaching the client as a `409`.
**Success**: `200 { success: true, message: "OTP sent successfully" }` — copy-pasted,
unrelated message.

### `POST /trains/train` — ✅ WORKING (logic-wise)
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

### `POST /trains/route` — ❌ BROKEN (inverted existence check)
**Body** (`zRoute` + `zRouteStation`):
```ts
trainId: uuid
stations: array of { stationId: uuid, sequenceNumber: positive int, arrivalTime?: "HH:mm", departureTime?: "HH:mm", distanceFromOrigin?: non-negative number }, min 2 stations
```
**Why broken**: `train.service.ts`'s existence check is written backwards —
`if (!existingRoute) throw new NotFoundError("Route already existis for this train")`
(note the typo "existis") fires exactly when **no** route exists yet, so **no train
can ever get its first route created** through this endpoint. If a route *does*
already exist, execution instead falls through to `prisma.route.create`, which then
throws a raw (uncaught) Prisma unique-constraint error — surfaced as a generic `500`,
not a clean `409`.
Also validates all `stationId`s exist (`400 "One or more station Ids are invalid"`)
and that `sequenceNumber`s are contiguous from 1 (`400 "Sequence Numbers must be
continous starting free"` — typo "free" for "from").
**Success** (unreachable in practice): `200 { success: true, message: "Route created successfully" }`.

### `POST /trains/route/:id` (intended as "get train by id") — ❌ BROKEN
**Two independent bugs**: (1) mounted as `POST` for what is semantically a read;
(2) the route param is named `:id`, but the controller reads
`req.params.trainId` — always `undefined` — so every call immediately
`400`s with `{ message: "Train Id is missing" }` before the service layer ever runs.
The intended success shape (never reached) would be
`200 { success: true, data: <train with seats + route + stations> }`.

### `POST /schedule` — ⛔ UNREACHABLE
Defined in `schedule.route.ts`, never mounted in `server.ts`. Documented anyway
since the code is fully written:
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
**Success** (unreachable): `200 { success: true, message: "Train created successfully" }`
— copy-pasted from `train.controller.ts`, describes the wrong resource.

### Kafka — produces only, no consumer at all
| Topic | Payload | Call site | Status |
|---|---|---|---|
| `admin.station-created` | `{ eventType: "STATION_CREATED", data: <Station row>, timestamp }` | `station.service.ts`, keyed `station-<id>` | ✅ fires (route reachable, modulo the missing-`await` bug and the global build failure) |
| `admin.train-created` | raw `Train` row incl. nested `seats` | `train.service.ts`, publish failures caught+logged | ✅ fires |
| `admin.route-created` | raw `Route` row | commented out in `createRoute` | ⏳ never fires |
| `admin.schedule-created` | `ScheduleCreatedPayload` — schedule + train + seats + route, fully denormalized | `schedule.service.ts` | ⏳ never fires (route unmounted) |
| `admin.schedule-cancelled` | `{ eventType: "SCHEDULE_CANCELLED", data: <Schedule row>, timestamp }` | defined, zero call sites — no cancel feature exists at all | ⏳ never fires |
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

## 4. Search Service (port 4002) — does not compile

`src/index.ts` fails `npx tsc --noEmit` for two independent reasons: it imports
`./routes/search.route` (singular) but the actual file is `routes/search.routes.ts`
(plural, added after the rest of the service was documented) — the import still
doesn't resolve; and it default-imports `errorHandler` from `error.middleware.ts`,
which only exports `errorMiddleware` as a named export. Everything below describes
the code as written.

### `GET /trains` (`search.routes.ts`, intended at `/search/trains`) — ❌ BROKEN
**Query params** (`zSearchTrains`): `from: string 1-50 chars`, `to: string 1-50
chars`, `date?: "YYYY-MM-DD"`.
**Flow**: `searchService.searchTrains({from, to, date})` runs a real nested
Elasticsearch query (resolves each station via exact-code → completion-suggester →
fuzzy-match, then finds trains whose route contains both stations in the right
order, attaching the matching date's schedule if `date` was given) and returns a
fully-formed result — **which the controller then throws away.**
**Why broken**: the controller computes `response` from the real search call,
never uses it, and responds with a hardcoded, copy-pasted
`{ success: true, message: "Train created successfully" }` instead. The actual
search results never reach the client.

### `GET /autocomplete` — ✅ WORKING (logic-wise)
Calls `searchService.autocompleteStation(q)` (completion suggester, fuzzy) and
returns `200 { success: true, data: [{name?, code, stationId}, ...] }`.

### `GET /debug/stations` — ❌ BROKEN (copy-paste bug)
Wired to call `autocompleteStation(q)` — identical to `/autocomplete` — instead of
`getAllStations()`, which exists in `services/search.service.ts` for exactly this
purpose.

### `GET /debug/trains` — ❌ BROKEN (copy-paste bug)
Same bug, also calls `autocompleteStation(q)` instead of `getAllTrains()`.

### Kafka — consumer (`kafka/search.service.ts`, group `search-service-group-v2`)
| Topic | Handler | Payload expected | Status |
|---|---|---|---|
| `admin.station-created` | `indexStation` | `{eventType, data:{id,name,code,city,state?}, timestamp}` | ✅ fires — the only one that reliably does |
| `admin.route-created` | `indexTrainRoute` | `{train, routeStations[]}` | ⏳ never fires — admin-service's publish call is commented out |
| `admin.schedule-created` | `indexSchedule` | `{scheduleId, trainId, departureDate, status, seats?}` | ⏳ never fires — admin-service's route is unmounted |
| `admin.schedule-cancelled` | `cancelSchedule` | `{eventType, data:{id, trainId, status}, timestamp}` | ⏳ never fires — no caller in admin-service |
| `inventory.seat-availability-updated` | `updateSeatAvailability` | `{scheduleId, trainId, available, locked, booked}` | ⏳ never fires — no inventory-service exists in this repo |

**A subtle DLQ gap**: every one of these handlers catches its own Elasticsearch
errors and logs-and-swallows them internally. `withDLQ` only retries/forwards
errors that propagate *out* of the wrapped handler — since these never throw, an
Elasticsearch outage silently drops the write instead of ever reaching
`dlq.search-service`.

`indexStation`'s write is missing a `name` field on the stored document (present in
the event, just not written) — the same station gets a `name` filled in later, but
only once/if `indexTrainRoute`'s per-station reindex touches it.

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
| `notification.welcome-email` | `handleWelcomeEmail` | `{email, firstName}` | ✅ user-service (method exists), fields match | ⏳ NEVER TRIGGERED — the producer method is never called |
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

## 6. Full Kafka topic matrix

| Topic | Producer(s) | Consumer(s) | End-to-end status |
|---|---|---|---|
| `notification.otp-email` | user-service (`sendOtp`) | notification-service | ✅ works |
| `notification.welcome-email` | user-service (method exists, never called) | notification-service (handles correctly) | ⏳ never fires |
| `notification.booking-email` | none | notification-service (falls to `default`) | ⏳ n/a — no producer, no handler case either |
| `notification.payment-email` | none | notification-service (falls to `default`) | ⏳ n/a |
| `admin.station-created` | admin-service (`createStation`) | search-service (`indexStation`) | ✅ works (once admin-service's build is fixed) |
| `admin.train-created` | admin-service (`createTrain`) | none | produced, nothing consumes it |
| `admin.route-created` | admin-service — commented out | search-service (`indexTrainRoute`) | ⏳ never fires |
| `admin.schedule-created` | admin-service — route unmounted | search-service (`indexSchedule`) | ⏳ never fires |
| `admin.train-updated` / `admin.station-updated` / `admin.route-updated` | no producer implemented for any | none | ⏳ n/a |
| `admin.schedule-cancelled` | admin-service — zero call sites | search-service (`cancelSchedule`) | ⏳ never fires |
| `inventory.seat-availability-updated` | no inventory-service exists | search-service (`updateSeatAvailability`) | ⏳ never fires |
| `booking.confirmed` / `booking.failed` / `booking.cancelled` | no booking-service exists | notification-service (would silently no-op — missing `email` field) | ⏳ never fires |
| `payment.success` / `payment.failed` | no payment-service exists | notification-service (falls to `default`) | ⏳ n/a |
| `dlq.booking-service` / `dlq.inventory-service` | n/a (would-be DLQ targets) | notification-service (falls to `default`, including its own unrelated DLQ topics) | inert |
| `dlq.search-service` | search-service, on 3 failed retries — but internal catch blocks mean this path is rarely reached in practice | none | effectively inert |
| `dlq.notification-service` | notification-service, on 3 failed retries | notification-service itself (subscribed to `Object.values(KAFKA_TOPICS)`, falls to `default`) | fires occasionally, consumed as "unknown topic" by the same service that produced it |

`DLQ_MAX_RETRIES = 3` (`shared/constants/kafka-topics.ts`), shared by every
service's `withDLQ` wrapper (`shared/utils/dlqHanlder.ts`).

---

## 7. Shared error-response shapes

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

## 8. Services referenced but not implemented in this repo

`booking-service`, `payment-service`, and `inventory-service` each have a
`config.SERVICES.*` URL in api-gateway and/or Kafka topics named for them
(`booking.*`, `payment.*`, `inventory.seat-availability-updated`), but **no
directory, code, or `docs/` folder for any of the three exists anywhere in this
repo.** Any reference to them elsewhere in this document describes what other
services *expect* to talk to, not something that can currently be exercised.
