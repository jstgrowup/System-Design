# Booking Service — Complete Guide

Single source of truth for the IRCTC Booking Service: what it does, how a request or event flows through it, and how each piece works — written in plain English, with the actual current code inline.

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [Lifecycle Walkthroughs](#lifecycle-walkthroughs)
5. [Component Breakdown](#component-breakdown)
   - [index.ts / server.ts — Entry Point & Express App](#1-indexts--serverts--entry-point--express-app)
   - [config/ — Configuration, Prisma, Redis, Kafka, Logger](#2-config--configuration-prisma-redis-kafka-logger)
   - [prisma/schema.prisma — Data Model](#3-prismaschemaprisma--data-model)
   - [utils/distributedLock.ts — Redis Seat Locking](#4-utilsdistributedlockts--redis-seat-locking)
   - [services/saga.service.ts — Saga Steps](#5-servicessagaservicets--saga-steps)
   - [services/booking.service.ts — Core Orchestration](#6-servicesbookingservicets--core-orchestration)
   - [services/*Client.ts — Downstream Service Clients](#7-servicesclientts--downstream-service-clients)
   - [utils/bookingExpiry.ts — The Background Sweep](#8-utilsbookingexpiryts--the-background-sweep)
   - [controllers/ and routes/ — The HTTP Surface](#9-controllers-and-routes--the-http-surface)
   - [kafka/ — Producer and Consumer](#10-kafka--producer-and-consumer)
6. [Environment Variables](#environment-variables)
7. [Kafka Topics & HTTP Routes Reference](#kafka-topics--http-routes-reference)
8. [Quick Start](#quick-start)
9. [Debugging Tips](#debugging-tips)
10. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **Booking Service** orchestrates the whole ticket-purchase flow — it's the one service that talks to every other service in a single request. It never owns seat state or payment state itself; it coordinates them via a **saga** (a sequence of steps with explicit compensation on failure), because a booking spans two other services' databases (inventory-service's seats, payment-service's payment orders) with no shared transaction across them.

- **Creates a booking** (`POST /bookings`) — validates the request, checks seat availability with inventory-service, acquires a Redis distributed lock on the requested seats, holds the seats in inventory-service, opens a payment order with payment-service, and returns the payment details the client needs to complete checkout.
- **Confirms a booking** on `payment.success` (a Kafka event from payment-service) — moves the held seats to `BOOKED` in inventory-service and publishes `booking.confirmed` so notification-service can email the user.
- **Compensates on any failure** — if a later saga step fails, every already-completed step is undone in reverse order (confirmed seats are released, a payment is refunded, held seats are unlocked), so a booking never ends up half-committed.
- **Expires stale bookings** — a background job releases seats and marks a booking `EXPIRED` if payment isn't completed within `BOOKING_TTL_SECONDS`.
- **Supports partial-journey (segment) bookings** — a passenger can book a seat for just part of a train's route (`fromStationId`/`toStationId`/`fromSeq`/`toSeq`), matching inventory-service's segment-lock model, so two passengers can hold the same physical seat for non-overlapping legs.
- **Cancels a booking** (`POST /bookings/:id/cancel`) — releases seats (and refunds payment, if one was captured), at any point before the booking reaches a terminal state.

Every state transition uses **optimistic concurrency control (CAS — compare-and-swap on a `version` column)**, not row locks, so the payment webhook, the user's cancel request, and the expiry job can never double-process the same booking even if they race.

**Ported from a reference JavaScript implementation** (`irctc-backend-main/booking-service`) into TypeScript, following this repo's conventions: Zod validation on every request body (the reference validated manually), no `any`, and every downstream HTTP/Kafka contract typed locally rather than imported across services. The business logic (the saga, the CAS pattern, the segment-locking scheme) is unchanged from the reference. **Not verified live** — no reachable Postgres/Redis/Kafka in the environment this was built in, and its two hardest dependencies (inventory-service, payment-service) are each independently only "code complete, not verified live" or, in payment-service's case, not built yet at all (see [Known Issues](#known-issues--inconsistencies)).

---

## Architecture

```
┌─────────────────────┐   ┌──────────────────────┐   ┌─────────────────────┐
│   User Service :4001  │   │  Admin Service :4003   │   │  Inventory Service :4007│
│  GET /user/internal/  │   │  GET /stations/station/│   │  seat lock/unlock/     │
│      :userId          │   │      internal/:id      │   │  confirm/cancel-booking│
└──────────┬───────────┘   └──────────┬───────────┘   └──────────┬──────────┘
           │ x-internal-service-key       │ x-internal-service-key       │ x-internal-service-key
           ▼                              ▼                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        BOOKING SERVICE (Port 4005)                          │
│                                                                               │
│  HTTP surface (routes/booking.routes.ts, behind the gateway's requireAuth):  │
│    POST /bookings                          — start the saga                 │
│    GET  /bookings                          — list the caller's bookings     │
│    GET  /bookings/:bookingId               — read one booking               │
│    POST /bookings/:bookingId/verify-payment— client-side payment verify      │
│    POST /bookings/:bookingId/cancel        — cancel at any non-terminal state│
│                                                                               │
│  Saga (services/saga.service.ts):                                           │
│    HOLD_SEATS -> CREATE_PAYMENT -> CONFIRM_SEATS -> COMPLETE                 │
│    every step logged to SagaLog; compensateAll() unwinds in reverse order    │
│                                                                               │
│  Redis distributed lock (utils/distributedLock.ts):                         │
│    booking:lock:seat:{scheduleId}:{seatId}[:fromSeq:toSeq]                   │
│    all-or-nothing acquisition via a Lua script                              │
│                                                                               │
│  Kafka consumer (kafka/consumer/booking.consumer.ts):                       │
│    payment.success       -> handlePaymentSuccess  (confirm seats)            │
│    payment.failed        -> handlePaymentFailure  (release seats)            │
│    admin.schedule-cancelled -> handleScheduleCancelled (cancel + refund all) │
│                                                                               │
│  Kafka producer (kafka/producer/booking.producer.ts):                       │
│    booking.confirmed / booking.cancelled / booking.failed                   │
│    — each enriched with the user's email/firstName (and station names for   │
│    confirmed) so notification-service's handlers, which read `.email` off   │
│    the event, actually have something to read                              │
│                                                                               │
│  Background job (utils/bookingExpiry.ts):                                   │
│    every BOOKING_EXPIRY_CHECK_INTERVAL_MS, expire bookings whose            │
│    lockExpiresAt has passed and compensate their saga steps                  │
└──────────────────────────┬───────────────────────────┬──────────────────────┘
                           │ Postgres                    │ Redis
                           ▼                              ▼
                  bookings, booking_seats,        distributed seat locks,
                  passengers, saga_logs,          expiry-job leader election
                  idempotency_records

                           │ HTTP (no gateway route reaches this yet)
                           ▼
                 ┌───────────────────────┐
                 │  Payment Service :4006  │  ⚠️ does not exist in this repo yet
                 │  POST /orders           │     (see Known Issues)
                 │  POST /orders/:id/verify│
                 │  POST /refunds          │
                 └───────────────────────┘

                           │ Kafka
                           ▼
                 notification-service (booking.confirmed/cancelled/failed)
```

---

## File Structure

```
booking-service/
├── src/
│   ├── index.ts                           # Starts Kafka consumer, expiry job, then the HTTP server
│   ├── server.ts                          # Express app: helmet, cors, logging, routes, error handler
│   ├── config/
│   │   ├── index.ts                       # Env vars -> typed Config object
│   │   ├── prisma.ts                      # PrismaClient singleton (via @prisma/adapter-pg)
│   │   ├── redis.ts                       # ioredis singleton (RedisClient class) — locks + leader election
│   │   ├── kafka.ts                       # Kafka client, producer, consumer, connect/disconnect helpers
│   │   └── logger.ts                      # Winston logger
│   ├── controllers/
│   │   └── booking.controller.ts          # Validates request bodies (Zod), calls the service, shapes the response
│   ├── routes/
│   │   └── booking.routes.ts              # Route table — every route behind getUserContext
│   ├── services/
│   │   ├── booking.service.ts             # All business logic: saga orchestration + REST handlers + Kafka handlers
│   │   ├── saga.service.ts                # The 3 forward steps and their 3 compensations
│   │   ├── inventoryClient.ts             # HTTP client for inventory-service (seat lock/unlock/confirm/cancel)
│   │   ├── paymentClient.ts               # HTTP client for payment-service (order/verify/refund)
│   │   ├── userClient.ts                  # HTTP client for user-service (internal profile lookup)
│   │   └── stationClient.ts               # HTTP client for admin-service (internal station lookup, in-memory cached)
│   ├── kafka/
│   │   ├── consumer/booking.consumer.ts   # Subscribes to payment.success/failed, admin.schedule-cancelled
│   │   └── producer/booking.producer.ts   # Publishes booking.confirmed/cancelled/failed
│   ├── middlewares/
│   │   ├── cors.middleware.ts             # Origin whitelist
│   │   ├── error.middleware.ts            # Global error formatter
│   │   ├── req.middleware.ts              # Request/response logging
│   │   └── user-context.middleware.ts     # Reads x-user-id set by the gateway
│   ├── utils/
│   │   ├── distributedLock.ts             # Redis Lua-script locking, all-or-nothing across a seat set
│   │   ├── bookingExpiry.ts               # Background sweep + Redis leader election
│   │   ├── error.ts                       # AppError + subclasses, including StaleStateError (CAS conflict)
│   │   ├── api-response.ts                # SuccessResponse / ErrorResponse helpers
│   │   ├── asyncHandler.ts                # Wraps async route handlers, forwards errors to next()
│   │   └── zod.formatter.ts               # Turns the first ZodError issue into a plain message
│   ├── types/
│   │   ├── index.ts                       # Every downstream response shape, Kafka event payload, and DTO
│   │   ├── zod.ts                         # Zod schemas for every request body/query this service accepts
│   │   └── express.d.ts                   # Augments Express's Request with `user`
│   └── generated/prisma/                  # Prisma Client output (gitignored, regenerated by `prisma generate`)
├── prisma/
│   └── schema.prisma                      # Booking, BookingSeat, Passenger, SagaLog, IdempotencyRecord
├── docs/                                  # This documentation
├── package.json
├── tsconfig.json
├── prisma.config.ts
├── nodemon.json
└── .env.example
```

It also reaches outside its own folder into the repo-wide `shared/` package, exactly like every other service:

```
shared/
├── constants/kafka-topics.ts   # KAFKA_TOPICS — every topic name used across all services
└── utils/dlqHanlder.ts         # withDLQ() — retry + dead-letter-queue wrapper (filename typo, shared repo-wide)
```

`tsconfig.json` sets `rootDir: ".."`, the same pattern every other service in this repo uses, so it can compile the `../../../../shared/...` imports from inside `src/kafka/consumer/` and `src/kafka/producer/`.

---

## Lifecycle Walkthroughs

### Case A: A booking is created, paid for, and confirmed (happy path)

```
1.  Client calls POST /bookings with { scheduleId, seatIds, passengers,
    idempotencyKey, fromStationId?, toStationId?, fromSeq?, toSeq? }
2.  zCreateBooking validates the body — seatIds.length must equal
    passengers.length, and if fromSeq/toSeq are both given, fromSeq < toSeq
3.  bookingService.createBooking:
      a. Checks the idempotencyKey against IdempotencyRecord — a retried
         request with the same key returns the original response, not a
         second booking
      b. inventoryClient.getAvailability(scheduleId) — rejects if the
         schedule isn't ACTIVE or has already departed
      c. inventoryClient.getSeats(scheduleId, {fromSeq, toSeq}) — resolves
         each requested seatId to its price/type/current status (segment-aware
         if fromSeq/toSeq were given)
      d. acquireSeatLocks (Redis Lua script) — all seats lock atomically, or
         none do; a temporary lock value is used since the booking row
         doesn't exist yet
      e. prisma.booking.create — one row, plus its BookingSeat and Passenger
         children, status PENDING
      f. saga.executeHoldSeats — calls inventory-service's POST /seats/lock,
         logs the attempt to SagaLog, sets status SEATS_HELD on success
      g. saga.executeCreatePayment — calls payment-service's POST /orders,
         logs to SagaLog, sets status PAYMENT_PENDING and stores paymentOrderId
      h. Saves the response under the idempotency key and returns it —
         { bookingId, status, totalAmount, lockExpiresAt, seats, passengers,
         paymentOrder: { paymentOrderId, gatewayOrderId, amount, currency, keyId } }
4.  Client uses paymentOrder to complete checkout with the payment gateway
    (e.g. Razorpay's own checkout widget) directly
5.  payment-service publishes "payment.success" once the gateway confirms —
    booking.consumer.ts routes it to bookingService.handlePaymentSuccess
6.  handlePaymentSuccess:
      a. Looks up the booking by paymentOrderId — if already CONFIRMED,
         returns immediately (idempotent)
      b. casUpdateBooking(bookingId, expectedVersion, {status: CONFIRMING}) —
         if another process already changed the version, StaleStateError is
         thrown and this handler bails out silently
      c. saga.executeConfirmSeats — calls inventory-service's
         POST /seats/confirm, transitioning the held seats to BOOKED
      d. Final status update to CONFIRMED (version already bumped by the CAS)
      e. forceReleaseSeatLocks — the Redis lock is no longer needed once
         inventory-service itself owns the BOOKED state
      f. Enriches and publishes booking.confirmed — looks up the user's
         email/firstName (userClient) and both station names (stationClient,
         only if this was a segment booking), then publishes. A publish
         failure here is logged as CRITICAL but does not fail the booking —
         the seats are already confirmed at this point.
```

### Case B: Payment never completes — the booking expires (failure path)

```
1.  A client calls POST /bookings and gets back a paymentOrder, but never
    completes checkout — no payment.success or payment.failed event ever
    arrives
2.  The booking sits in PAYMENT_PENDING (or SEATS_HELD, if payment-service's
    order creation itself failed) with lockExpiresAt = createdAt + BOOKING_TTL_SECONDS
3.  utils/bookingExpiry.ts's cleanExpiredBookings runs on its interval
    (default 30s):
      a. tryAcquireLeadership() — a Redis SET NX EX lock so only one running
         instance of this service does the sweep, even with multiple replicas
      b. Finds every booking in PENDING/SEATS_HELD/PAYMENT_PENDING whose
         lockExpiresAt has passed
      c. For each: an optimistic CAS claim (matching on the row's own
         `version`) — if the payment webhook raced in and already claimed
         it, this booking is skipped
      d. saga.compensateAll — walks SagaLog's COMPLETED steps in reverse
         order and undoes each one (CONFIRM_SEATS -> cancel the inventory
         booking; CREATE_PAYMENT -> refund; HOLD_SEATS -> release the seats)
      e. forceReleaseSeatLocks — clears the Redis lock regardless of who
         held it
      f. Publishes booking.failed with reason "booking_timeout"
```

### Case C: A user cancels a confirmed booking (edge case — refund path)

```
1.  Client calls POST /bookings/:bookingId/cancel while status is CONFIRMED
2.  casUpdateBooking claims the booking into CANCELLING — if the expiry job
    or a payment webhook already changed its version, this throws a 409
    telling the user to refresh
3.  Since status was CONFIRMED: inventoryClient.cancelBooking releases the
    seats in inventory-service. If that call fails, the booking is rolled
    back from CANCELLING to CONFIRMED (so the user can retry) and the error
    is re-thrown — cancellation never leaves the booking stuck mid-transition.
4.  If a paymentOrderId exists, paymentClient.initiateRefund is called
    (failure here is logged but does not block cancellation — a booking can
    end up CANCELLED with a refund that needs manual follow-up)
5.  Final status CANCELLING -> CANCELLED, Redis locks force-released,
    booking.cancelled published with refundAmount set only if the refund
    call actually succeeded
```

---

## Component Breakdown

### 1. `index.ts` / `server.ts` — Entry Point & Express App

```typescript
const startServer = async (): Promise<void> => {
  try {
    await bookingConsumer.start();
    startBookingExpiryJob();

    const server = app.listen(config.PORT, () => {
      logger.info(`${config.SERVICE_NAME} is running on port ${config.PORT}`);
    });

    const shutdown = async (): Promise<void> => {
      logger.info("Shutting down gracefully...");
      stopBookingExpiryJob();

      server.close(async () => {
        await disconnectAll();
        await RedisClient.closeConnection();
        logger.info("Server closed");
        process.exit(0);
      });
    };

    process.on("SIGTERM", () => void shutdown());
    process.on("SIGINT", () => void shutdown());
  } catch (error) {
    logger.error("Failed to start server", { error: (error as Error).message });
    process.exit(1);
  }
};
```

Startup order mirrors inventory-service: connect the Kafka consumer, start the expiry-job timer, then start listening on HTTP — so the process only reports itself "running" once it can actually react to `payment.success`/`payment.failed` events. Shutdown is the reverse, plus an explicit Redis `quit()` (this is the only service in the repo besides api-gateway/user-service that holds a Redis connection needing explicit closing).

`server.ts`'s `/health` endpoint checks both Postgres (`SELECT 1`) and Redis (`RedisClient.isReady()`) — a booking genuinely can't be created without either, so both are load-bearing for this service's health, not just Postgres.

---

### 2. `config/` — Configuration, Prisma, Redis, Kafka, Logger

`config/index.ts` reads four downstream service URLs (`INVENTORY_SERVICE_URL`, `PAYMENT_SERVICE_URL`, `USER_SERVICE_URL`, `ADMIN_SERVICE_URL`) plus `INTERNAL_SERVICE_KEY` — this is the first service in the repo whose config is built primarily around *calling* other services rather than being called. `BOOKING_TTL_SECONDS` (default 600) is both the Redis lock TTL passed to `acquireSeatLocks` and the inventory-service hold TTL passed to `holdSeats` — they're kept equal so a Redis lock never outlives (or falls short of) the inventory-service hold it's protecting.

`config/redis.ts` and `config/kafka.ts` are structurally identical to every other service's — see inventory-service's or user-service's own docs for the line-by-line breakdown; nothing about the connection/retry/idempotency setup changed here.

---

### 3. `prisma/schema.prisma` — Data Model

```prisma
model Booking {
  id             String        @id @default(uuid())
  userId         String
  scheduleId     String
  trainId        String
  trainNumber    String
  trainName      String
  departureDate  DateTime      @db.Date
  status         BookingStatus @default(PENDING)
  totalAmount    Float         @default(0)
  seatCount      Int
  fromStationId  String?
  toStationId    String?
  fromSeq        Int?
  toSeq          Int?
  idempotencyKey String        @unique
  paymentOrderId String?       @unique
  lockExpiresAt  DateTime?
  failureReason  String?
  version        Int           @default(0)
  createdAt      DateTime      @default(now())
  updatedAt      DateTime      @updatedAt

  seats      BookingSeat[]
  passengers Passenger[]
  sagaLog    SagaLog[]
}
```

`version` is the CAS column every optimistic-lock update reads and increments — `casUpdateBooking` (in `booking.service.ts`) is a thin wrapper around `prisma.booking.updateMany({ where: { id, version: expectedVersion }, data: { ...data, version: { increment: 1 } } })` and throws `StaleStateError` if `updateMany`'s count comes back 0, meaning someone else's write already moved the version forward.

`SagaLog` is the audit trail every saga step writes to before and after attempting its downstream call — `compensateAll` reads it back (`status: "COMPLETED"`, newest-first) to know exactly which steps need undoing, rather than guessing from the booking's current status alone.

`IdempotencyRecord` guards `createBooking` specifically — keyed by the caller-supplied `idempotencyKey`, not a Kafka `eventKey` like inventory-service/search-service use their idempotency tables for.

One migration would need to be generated (`npx prisma migrate dev`) before this schema has ever been applied to a real database — no migration exists yet in this port (see [Known Issues](#known-issues--inconsistencies)).

---

### 4. `utils/distributedLock.ts` — Redis Seat Locking

```typescript
const ACQUIRE_SCRIPT = `
local lockValue = ARGV[1]
local ttl = tonumber(ARGV[2])
local acquired = {}

for i, key in ipairs(KEYS) do
     local result = redis.call('SET', key, lockValue, 'NX', 'EX', ttl)
     if not result then
          for j = 1, #acquired do
               redis.call('DEL', acquired[j])
          end
          return 0
     end
     table.insert(acquired, key)
end

return 1
`;
```

This Lua script is why seat locking is all-or-nothing: Redis runs the whole script atomically (no other command can interleave), so if seat 2 of 3 is already locked, seats already claimed in this same call get rolled back before the script returns `0`. `buildLockKeys` sorts `seatIds` before building keys — two concurrent requests for seats `[A, B]` and `[B, A]` always attempt to lock them in the same order, which is what prevents a classic two-lock deadlock. Segment bookings suffix the key with `:fromSeq:toSeq`, so two non-overlapping segment holds on the same physical seat get different Redis keys entirely (the DB-side overlap check that actually matters lives in inventory-service's `FOR UPDATE NOWAIT` transaction, not here).

---

### 5. `services/saga.service.ts` — Saga Steps

Three forward functions (`executeHoldSeats`, `executeCreatePayment`, `executeConfirmSeats`) and three compensations (`compensateHoldSeats`, `compensateCreatePayment`, `compensateConfirmSeats`), plus `compensateAll` which walks a booking's `SagaLog` rows newest-first and calls whichever compensation matches each `COMPLETED` step:

```typescript
export async function compensateAll(booking: Booking, seatIds: string[]): Promise<void> {
  const completedSteps = await prisma.sagaLog.findMany({
    where: { bookingId: booking.id, status: "COMPLETED" },
    orderBy: { createdAt: "desc" },
  });

  for (const step of completedSteps) {
    switch (step.step) {
      case "CONFIRM_SEATS":
        await compensateConfirmSeats(booking);
        break;
      case "CREATE_PAYMENT":
        await compensateCreatePayment(booking);
        break;
      case "HOLD_SEATS":
        await compensateHoldSeats(booking, seatIds);
        break;
    }
  }
}
```

Every compensation function catches and logs its own errors rather than throwing — a failed compensation shouldn't crash whatever caller invoked `compensateAll` (the create-booking failure path, the expiry job, a failed payment confirmation), since the alternative (an unhandled rejection) would be strictly worse than "this one cleanup step needs a human to look at it later." `compensateHoldSeats`'s own comment is explicit about this: if releasing the inventory hold fails, "Inventory lock expiry will eventually clean this up" — there's a second safety net on the inventory-service side.

---

### 6. `services/booking.service.ts` — Core Orchestration

The largest file in the service — every REST handler, both Kafka event handlers, and the CAS/idempotency/notification-enrichment helpers they all share. The CAS helper:

```typescript
const casUpdateBooking = async (
  bookingId: string,
  expectedVersion: number,
  data: Record<string, unknown>,
): Promise<void> => {
  const result = await prisma.booking.updateMany({
    where: { id: bookingId, version: expectedVersion },
    data: { ...data, version: { increment: 1 } },
  });

  if (result.count === 0) {
    throw new StaleStateError(
      `Booking ${bookingId} was modified by another process (expected version ${expectedVersion})`,
    );
  }
};
```

`handlePaymentSuccess`, `handlePaymentFailure`, `cancelBooking`, and the expiry job's `cleanExpiredBookings` all call this before touching a booking's status — it's the single mechanism preventing any two of "user cancels," "payment webhook confirms," and "expiry job times it out" from double-processing the same booking, without needing a database-level row lock held across a slow downstream HTTP call.

`createBooking` is the one function that reads request-shaped input directly (already Zod-validated by the controller) rather than a Kafka event — see [Lifecycle Walkthroughs](#lifecycle-walkthroughs) Case A for its full flow.

---

### 7. `services/*Client.ts` — Downstream Service Clients

Four thin axios wrappers, each with the same retry shape: exponential backoff (200ms × 2^attempt), up to 3 attempts, but **only for server/network errors** — a 4xx response means the request itself was wrong and retrying won't help:

```typescript
async function withRetry<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const status = (error as AxiosError).response?.status;
      if (status && status >= 400 && status < 500) throw error;
      if (attempt < maxRetries) {
        const delay = 200 * Math.pow(2, attempt - 1);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}
```

- `inventoryClient.ts` — every route inventory-service exposes for booking-service's saga (`getAvailability`, `getSeats`, `holdSeats`, `releaseSeats`, `confirmSeats`, `cancelBooking`), each sending `x-internal-service-key`.
- `paymentClient.ts` — the four routes a not-yet-built payment-service is expected to expose (`createPaymentOrder`, `getPaymentStatus`, `verifyPayment`, `initiateRefund`) — see [Known Issues](#known-issues--inconsistencies).
- `userClient.ts` — a single call, `GET /user/internal/:userId` on user-service, used only to enrich Kafka events with an email/firstName.
- `stationClient.ts` — `GET /stations/station/internal/:stationId` on admin-service, with a 10-minute in-memory cache (`Map<stationId, {value, expiresAt}>`) since station names rarely change and this is called on every confirmed segment booking.

---

### 8. `utils/bookingExpiry.ts` — The Background Sweep

```typescript
async function tryAcquireLeadership(): Promise<boolean> {
  try {
    const result = await redis.set(
      LEADER_KEY,
      process.pid.toString(),
      "NX",
      "EX",
      LEADER_TTL_SECONDS,
    );
    return result === "OK";
  } catch (err) {
    logger.error("Failed to acquire expiry job leadership", { error: (err as Error).message });
    return false;
  }
}
```

`SET key value NX EX seconds` is a single atomic Redis command — either this instance is the first to call it this cycle (and becomes leader) or it isn't (another replica already holds the key), with no race window between "check" and "set." `LEADER_TTL_SECONDS` (25s) is deliberately shorter than the sweep interval (30s default) so a crashed leader's lock expires before the next cycle would otherwise be blocked waiting for it. This is the same pattern inventory-service uses for its own lock-expiry job, just backed by Redis instead of a Postgres advisory lock (this service already holds a Redis connection for seat locks, so it was the natural choice here).

---

### 9. `controllers/` and `routes/` — The HTTP Surface

Every route is behind `getUserContext` (the gateway's `x-user-id` header) — unlike inventory-service, there is no internal-only or public route here; every operation is something an end user does directly. Every controller validates its input with a Zod schema from `types/zod.ts` before calling into `booking.service.ts` — this is a deliberate departure from the reference implementation, which validated manually (`if (!scheduleId || ...) throw new BadRequestError(...)`); the Zod schemas cover the exact same required-field checks the reference did, plus type coercion (e.g. `page`/`limit` query params) that the reference handled with manual `parseInt` calls in the controller.

---

### 10. `kafka/` — Producer and Consumer

The consumer subscribes to exactly three topics it handles (`payment.success`, `payment.failed`, `admin.schedule-cancelled`) — unlike notification-service, there's no "subscribe to everything" pattern here. The producer's three publish methods (`publishBookingConfirmed`/`Cancelled`/`Failed`) each stamp on a timestamp field (`confirmedAt`/`cancelledAt`/`failedAt`) and are the reason notification-service's `booking.confirmed`/`failed`/`cancelled` handlers can actually send an email today — those handlers read an `email` field off the event that the reference notification-service code already expected but that, before this service existed, no producer ever populated.

---

## Environment Variables

```bash
PORT=4005
NODE_ENV=development
LOG_LEVEL=info

DATABASE_URL=postgresql://admin:irctcpass@localhost:5432/booking_service_db?schema=public
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:4000

KAFKA_BROKER=localhost:9093
KAFKA_CLIENT_ID=booking-service

REDIS_URL=redis://localhost:6379

INVENTORY_SERVICE_URL=http://localhost:4007
PAYMENT_SERVICE_URL=http://localhost:4006
USER_SERVICE_URL=http://localhost:4001
ADMIN_SERVICE_URL=http://localhost:4003
INTERNAL_SERVICE_KEY=change-me-to-a-shared-secret

BOOKING_TTL_SECONDS=600
LOCK_TTL_SECONDS=600
BOOKING_EXPIRY_CHECK_INTERVAL_MS=30000
```

Every variable here is actually read by `config/index.ts` — no unused vars, matching inventory-service's pattern rather than notification-service's (`SENDGRID_API_KEY`, `FRONTEND_URL`).

---

## Kafka Topics & HTTP Routes Reference

### Kafka topics

| Topic | Direction | Handled by |
|---|---|---|
| `payment.success` | consumed | `handlePaymentSuccess` — confirms seats, marks the booking CONFIRMED. **Never fires today** — payment-service, the only producer, doesn't exist in this repo yet. |
| `payment.failed` | consumed | `handlePaymentFailure` — releases held seats, marks the booking FAILED. Same caveat as above. |
| `admin.schedule-cancelled` | consumed | `handleScheduleCancelled` — cancels every active booking on that schedule. **Never fires today** — admin-service has no cancel-schedule feature built (see admin-service's own docs and the root `missing.md`). |
| `booking.confirmed` | published | Consumed by notification-service's `handleBookingConfirmed`. Only reachable once `payment.success` starts firing. |
| `booking.cancelled` | published | Consumed by notification-service's `handleBookingCancelled`. Fires from `cancelBooking`, `handleScheduleCancelled`, and the expiry job's compensations reach `booking.failed` instead — see next row. |
| `booking.failed` | published | Consumed by notification-service's `handleBookingFailed`. Fires from `handlePaymentFailure` and the expiry job. |
| `dlq.booking-service` | published (rare) | Only reached if a `payment.success`/`payment.failed`/`admin.schedule-cancelled` message fails processing 3 times in a row (`withDLQ`, `DLQ_MAX_RETRIES = 3`). |

### HTTP routes

| Method & Path | Auth | Status |
|---|---|---|
| `POST /bookings` | `x-user-id` (gateway) | Starts the saga; see Lifecycle Case A. Depends on inventory-service and payment-service both being reachable. |
| `GET /bookings` | `x-user-id` (gateway) | Paginated list of the caller's own bookings (`?status=&page=&limit=`). |
| `GET /bookings/:bookingId` | `x-user-id` (gateway) | 404s if the booking doesn't exist or belongs to a different user (never leaks "exists but not yours"). |
| `POST /bookings/:bookingId/verify-payment` | `x-user-id` (gateway) | Client-side verification after checkout completes in the browser — separate from the `payment.success` Kafka path, for a client that wants an immediate synchronous confirmation. |
| `POST /bookings/:bookingId/cancel` | `x-user-id` (gateway) | Works from any non-terminal status; see Lifecycle Case C. |
| `GET /health` | none | Checks both Postgres (`SELECT 1`) and Redis (`RedisClient.isReady()`); `503` if either is down. |
| `GET /` | none | Static "Hello from booking-service" string. |

None of these routes are reachable through the API Gateway's login-only routing yet in the *user-facing* sense described by the root README's known Tier-1/2 gateway bugs — but this pass **did** add gateway proxy routes for all five (`/api/bookings/bookings*`, see `api-gateway/src/routes/index.ts`), so once the gateway's existing login-routing bugs are fixed, booking-service's routes are already wired up correctly on the gateway side.

---

## Quick Start

```bash
cd booking-service
npm install

# Generate the Prisma client (writes into src/generated/prisma, gitignored)
npx prisma generate

# .env needs at minimum DATABASE_URL, KAFKA_BROKER, REDIS_URL, and
# INTERNAL_SERVICE_KEY (must match inventory-service's/user-service's/
# admin-service's own INTERNAL_SERVICE_KEY exactly)
npm run dev        # nodemon, hot reload
```

Postgres, Redis, and Kafka must all be reachable — from the IRCTC root, `docker-compose up -d postgres redis kafka zookeeper` brings up the infrastructure this service expects. Apply the schema with `npx prisma migrate dev` before starting the service (no migration exists yet in this port — see [Known Issues](#known-issues--inconsistencies)).

```bash
curl http://localhost:4005/health
# { "success": true, "message": "Booking Service is healthy", "redis": true, "database": true, "timestamp": "..." }

# Creating a real booking end-to-end requires a live schedule in inventory-service
# (itself requires admin-service's schedule-creation route, which is mounted —
# see admin-service's docs) and a running payment-service, which does not exist
# in this repo yet — so POST /bookings cannot be exercised end-to-end today.
curl -X POST http://localhost:4005/bookings \
  -H "Content-Type: application/json" \
  -H "x-user-id: <a real user id>" \
  -d '{"scheduleId":"<uuid>","seatIds":["<seat-uuid>"],"passengers":[{"name":"Alice","age":30,"gender":"FEMALE"}],"idempotencyKey":"test-1"}'
```

---

## Debugging Tips

- **`POST /bookings` hangs or times out on the payment step** → payment-service doesn't exist in this repo yet (see [Known Issues](#known-issues--inconsistencies)); `paymentClient.createPaymentOrder` will fail with `ECONNREFUSED` after 3 retries, and `createBooking`'s catch block will compensate (release the seat hold) and re-throw. This is expected until payment-service is built.
- **A booking is stuck in `SEATS_HELD` or `PAYMENT_PENDING` forever** → check the expiry job's logs for "Skipping expiry job — another instance is the leader" (expected if running multiple replicas) or check `BOOKING_EXPIRY_CHECK_INTERVAL_MS`/the booking's `lockExpiresAt` — it only gets cleaned up once that timestamp is in the past.
- **`409 Booking status changed to ... while cancelling`** → the payment webhook or the expiry job claimed the booking via CAS between the user's read and their cancel request — this is the optimistic-lock conflict working as intended, not a bug; the user should refresh and retry.
- **`booking.confirmed`/`failed`/`cancelled` never reach notification-service** → check this service's own logs for "Failed to publish ... after retries" — publish failures are logged, not thrown (the booking's own state change already succeeded), so a Kafka outage here silently leaves notification-service without an email to send.
- **Seat locks never seem to release** → check `booking:lock:seat:*` keys directly in Redis; `forceReleaseSeatLocks` is called on every terminal transition (confirm, cancel, expire, schedule-cancel), so a lingering key usually means the Redis TTL itself hasn't hit yet, not a bug in the release call.
- **`FOR UPDATE NOWAIT`-style conflicts surfacing as `409 SEATS_LOCKED`** → this is the Redis Lua script correctly refusing a second concurrent booking attempt on an overlapping seat set — check inventory-service's own seat status via `GET /schedules/:id/seats` to see who currently holds it.

---

## Known Issues & Inconsistencies

Observed while porting this service — documented here rather than fixed, since these are informational (same approach as every other service's docs in this repo):

1. **payment-service does not exist in this repo.** `paymentClient.ts` is written against the reference implementation's expected contract (`POST /orders`, `GET /orders/:id`, `POST /orders/:id/verify`, `POST /refunds`) and this service's own `PAYMENT_SERVICE_URL` config, but there is nothing listening on that URL. Every saga step that calls `paymentClient` will fail with a connection error until payment-service is built — this was a deliberate scope decision (build booking-service's saga logic fully now, following the same pattern already used when inventory-service was built ahead of admin-service's schedule route), not an oversight.
2. **`admin.schedule-cancelled` never fires.** admin-service has no cancel-schedule feature built at all (route, controller, or service) — `handleScheduleCancelled` is fully implemented and subscribed to, but structurally unreachable until that feature exists. Same situation as inventory-service's identical dependency on this event.
3. **No Prisma migration exists yet.** `prisma/schema.prisma` was authored for this port but `npx prisma migrate dev` has not been run in the environment this was built in (no reachable Postgres) — there is no `prisma/migrations/` directory yet, unlike every other service in this repo which has at least one `..._init` migration checked in.
4. **Not verified against live infrastructure.** No Postgres, Redis, or Kafka broker was reachable while this was built — `npx tsc --noEmit` passing clean is the only verification performed. Treat every "works when called" description above as "the logic reads correctly and the types check," not observed runtime behavior.
5. **Gender is validated as an enum (`MALE`/`FEMALE`/`OTHER`)**, tighter than the reference implementation, which stored it as a free-form string with no validation at all. This is a deliberate value-add consistent with this repo's stricter Zod-everywhere convention, not a behavior port — a client sending any other string now gets a 400 instead of having it silently stored.
6. **The gateway's proxy routes added for this service** (`/api/bookings/bookings*`) are wired up correctly on the gateway side, but every request that reaches booking-service through them still needs `requireAuth` to have set `x-user-id` — and the gateway's own pre-existing login-route bug (see the root README/`missing.md`) means no token can currently be minted through the gateway to test this end-to-end. Calling booking-service directly on `:4005` with a hand-set `x-user-id` header is the only way to exercise these routes today, same as every other service in this repo.
7. **`verifyPayment`'s return shape differs slightly from the reference.** The reference returned `{ bookingId, status: 'CONFIRMED', message: 'Already confirmed' }` for the already-confirmed case and `{ bookingId, paymentStatus }` otherwise — two different shapes from one function. This port normalizes both to `{ bookingId, paymentStatus }` (using `"CONFIRMED"` as the `paymentStatus` value in the already-confirmed case) for a single consistent response shape, matching this repo's preference for one predictable DTO per endpoint.
