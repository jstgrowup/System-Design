# Inventory Service — Complete Guide

Single source of truth for the IRCTC Inventory Service: what it does, how a request or event flows through it, and how each piece works — written in plain English, with the actual current code inline.

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [Lifecycle Walkthroughs](#lifecycle-walkthroughs)
5. [Component Breakdown](#component-breakdown)
   - [index.ts — Entry Point](#1-indexts--entry-point)
   - [server.ts — Express App](#2-serverts--express-app)
   - [config/ — Configuration, Prisma, Kafka, Logger](#3-config--configuration-prisma-kafka-logger)
   - [prisma/schema.prisma — Data Model](#4-prismaschemaprisma--data-model)
   - [kafka/consumer/inventory.consumer.ts — Reading Events](#5-kafkaconsumerinventoryconsumerts--reading-events)
   - [kafka/producer/inventory.producer.ts — Publishing Events](#6-kafkaproducerinventoryproducerts--publishing-events)
   - [services/inventory.service.ts — The Core Logic](#7-servicesinventoryservicets--the-core-logic)
   - [controllers/ and routes/ — The HTTP Surface](#8-controllers-and-routes--the-http-surface)
   - [middlewares/](#9-middlewares)
   - [utils/lockExpiry.ts — The Background Sweep](#10-utilslockexpiryts--the-background-sweep)
   - [utils/retryTransaction.ts](#11-utilsretrytransactionts)
   - [types/ and utils/ — Everything Else](#12-types-and-utils--everything-else)
6. [Environment Variables](#environment-variables)
7. [Kafka Topics & HTTP Routes Reference](#kafka-topics--http-routes-reference)
8. [Quick Start](#quick-start)
9. [Debugging Tips](#debugging-tips)
10. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **Inventory Service** is the system of record for seat availability on every scheduled train run. Its job is to answer, and safely change, "is this seat free right now":

- **Tracks per-schedule seat state** — for every `(trainId, departureDate)` schedule, it stores one row per physical seat plus rolled-up counters (available / locked / booked).
- **Initializes inventory from Kafka**, not from an HTTP call — when admin-service creates a schedule, this service listens for that event and creates the seat rows itself.
- **Locks, unlocks, confirms, and cancels seats over HTTP**, called internally by booking-service as it runs its create-booking / cancel-booking saga.
- **Supports partial-journey (segment) locking** — two different passengers can hold the same physical seat for two non-overlapping legs of one train's route, which is why there's a separate `SeatSegmentLock` table alongside the seat's own summary status.
- **Sweeps expired locks itself**, on a timer, using a Postgres advisory lock so only one running instance does the sweep even if this service is scaled to multiple replicas.

It is not a public-facing service — every route either requires the caller to already be authenticated by the gateway (`x-user-id` header) or to know a shared internal secret (`x-internal-service-key`).

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                            ADMIN SERVICE                              │
│   scheduleController.createSchedule → adminProducer                   │
│   publishes admin.schedule-created (schedule-cancelled has no caller) │
└─────────────────────────────┬───────────────────────────────────────--┘
                              │ Kafka (localhost:9093)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     INVENTORY SERVICE (Port 4007)                    │
│                                                                       │
│  Kafka consumer (kafka/consumer/inventory.consumer.ts):               │
│    admin.schedule-created   → initializeInventory()                  │
│    admin.schedule-cancelled → cancelScheduleInventory()               │
│    (wrapped in withDLQ → dlq.inventory-service on repeated failure)   │
│                                                                       │
│  HTTP surface (routes/inventory.routes.ts, no gateway route today):   │
│    GET  /schedules/:scheduleId/availability   (public)                │
│    GET  /schedules/:scheduleId/seats          (user OR internal)      │
│    POST /seats/lock                           (internal only)         │
│    POST /seats/unlock                         (internal only)         │
│    POST /seats/confirm                        (internal only)         │
│    POST /seats/cancel-booking                 (internal only)         │
│                                                                       │
│  Background job (utils/lockExpiry.ts):                                │
│    every LOCK_EXPIRY_INTERVAL_MS, try a Postgres advisory lock;       │
│    if acquired, release seats/segments whose lockExpiresAt has        │
│    passed, then recompute + publish fresh counts                     │
│                                                                       │
│  Producer (kafka/producer/inventory.producer.ts):                     │
│    publishes inventory.seat-availability-updated after every          │
│    state change (init, cancel, lock, unlock, confirm, cancel-booking, │
│    and every expiry sweep)                                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ inventory.seat-availability-updated
                              ▼
                     ┌──────────────────────┐
                     │  search-service       │  (intended consumer — not
                     │  (not verified here)  │   verified as part of this doc)
                     └──────────────────────┘

                ▲ HTTP calls (internal secret)        ▲ HTTP calls (x-user-id)
                │                                      │
      ┌──────────────────┐                    ┌──────────────────────┐
      │  booking-service   │                    │  API Gateway          │
      │  (lock/unlock/     │                    │  — NO inventory       │
      │   confirm/cancel)  │                    │  route wired up       │
      └──────────────────┘                    └──────────────────────┘
```

Two things worth checking on your own before trusting this diagram at face value (see [Known Issues](#known-issues--inconsistencies) for detail):

- **`admin.schedule-created` now actually fires — this used to be broken, but no longer is.** admin-service's `server.ts` mounts `scheduleRoutes` at `/schedules` (`app.use("/schedules", scheduleRoutes)`), so `POST /schedules/schedule` reaches `scheduleController.createSchedule`, which calls `scheduleService.createSchedule` → `adminProducer.publishScheduleCreated`. Calling that endpoint directly against admin-service (there's still no gateway route for it, see below) does publish a real `admin.schedule-created` event that this service's consumer picks up and processes.
- **`admin.schedule-cancelled` still never fires, but for a different reason.** `adminProducer.publishScheduleCancelled` is fully implemented and would work if called, but nothing in admin-service ever calls it — there's no schedule-cancellation route/controller/service built yet (admin-service's own producer file has a comment saying as much). So this service's `cancelScheduleInventory` handler remains unreachable in practice today.
- **The API Gateway doesn't proxy to this service.** `api-gateway/src/routes/index.ts` has no route mentioning inventory at all — an external client (or even booking-service, if it were forced to go through the gateway) has no path into this service except by calling `http://localhost:4007` directly.

---

## File Structure

```
inventory-service/
├── src/
│   ├── index.ts                          # Starts Kafka consumer, lock-expiry job, then the HTTP server
│   ├── server.ts                          # Express app: helmet, cors, logging, routes, error handler
│   ├── config/
│   │   ├── index.ts                       # Env vars → typed Config object
│   │   ├── prisma.ts                      # PrismaClient singleton (via @prisma/adapter-pg)
│   │   ├── kafka.ts                       # Kafka client, producer, consumer, connect/disconnect helpers
│   │   └── logger.ts                      # Winston logger
│   ├── kafka/
│   │   ├── consumer/inventory.consumer.ts # Subscribes to admin.schedule-created/cancelled
│   │   └── producer/inventory.producer.ts # Publishes inventory.seat-availability-updated
│   ├── controllers/
│   │   └── inventory.controller.ts        # Validates request bodies, calls the service, shapes the response
│   ├── routes/
│   │   └── inventory.routes.ts            # Route table + the userOrInternal auth chooser
│   ├── services/
│   │   └── inventory.service.ts           # All business logic: Kafka handlers + REST handlers + segment logic
│   ├── middlewares/
│   │   ├── cors.middleware.ts             # Origin whitelist
│   │   ├── error.middleware.ts            # Global error formatter
│   │   ├── req.middleware.ts              # Request/response logging
│   │   ├── internal-auth.middleware.ts    # Shared-secret check for service-to-service routes
│   │   └── user-context.middleware.ts     # Reads x-user-id set by the gateway
│   ├── utils/
│   │   ├── lockExpiry.ts                  # Background sweep + Postgres advisory-lock leader election
│   │   ├── retryTransaction.ts            # Retries a transaction on serialization/lock-timeout/deadlock
│   │   ├── error.ts                       # AppError + subclasses
│   │   ├── api-response.ts                # SuccessResponse / ErrorResponse helpers
│   │   ├── asyncHandler.ts                # Wraps async route handlers, forwards errors to next()
│   │   └── zod.formatter.ts               # Turns the first ZodError issue into a plain message
│   ├── types/
│   │   ├── index.ts                       # Event payload shapes, transaction/query row types, result types
│   │   ├── zod.ts                         # Zod schemas for every request body/query this service accepts
│   │   └── express.d.ts                   # Augments Express's Request with `user`
│   └── generated/prisma/                  # Prisma Client output (gitignored, regenerated by `prisma generate`)
├── prisma/
│   ├── schema.prisma                      # ScheduleInventory, SeatInventory, RouteStop, SeatSegmentLock, IdempotencyRecord
│   └── migrations/20260730000000_init/    # The one migration that exists so far
├── docs/                                  # This documentation
├── package.json
├── tsconfig.json
├── prisma.config.ts
├── nodemon.json
└── .env.example
```

It also reaches outside its own folder into the repo-wide `shared/` package, exactly like notification-service does:

```
shared/
├── constants/kafka-topics.ts   # KAFKA_TOPICS — every topic name used across all services
└── utils/dlqHanlder.ts         # withDLQ() — retry + dead-letter-queue wrapper (filename typo, see notification-service's docs)
```

`tsconfig.json` sets `rootDir: ".."`, the same trick notification-service uses, so it can compile the `../../../shared/...` imports from inside `src/kafka/consumer/`.

---

## Lifecycle Walkthroughs

### Case A: A schedule is created (happy path)

```
0.  Prerequisite: something calls admin-service's POST /schedules/schedule
    directly (there's no API Gateway route for it, so this has to be a
    curl/Postman/script call straight to admin-service, not a normal user
    action) — this route is mounted and does now publish the event below.
1.  admin-service publishes a fully denormalized payload (train info + every
    seat + the full route) to "admin.schedule-created"
2.  inventoryConsumer's eachMessage (wrapped by withDLQ) parses the JSON and
    routes it to inventoryService.initializeInventory(eventData)
3.  initializeInventory checks an IdempotencyRecord keyed
    "SCHEDULE_CREATED:<scheduleId>" — if it already exists, this is a
    redelivery, log and return without doing anything
4.  Otherwise, in one Prisma transaction:
      - create one ScheduleInventory row (available = totalSeats, locked = 0, booked = 0)
      - createMany one SeatInventory row per seat, all status AVAILABLE
      - if the event carried route stops, createMany one RouteStop row per stop
      - create the IdempotencyRecord row so this exact event can't be replayed
5.  Log "Inventory initialized for schedule <id> with <n> seats"
6.  Publish inventory.seat-availability-updated (available=totalSeats, locked=0,
    booked=0) so search-service's index picks up the new schedule
7.  If that publish fails after its own internal retries, the error is caught
    and logged — the Kafka message is still considered successfully processed
    (the DB write already committed), so it is NOT retried or sent to the DLQ
```

### Case B: Two passengers lock the same physical seat for different legs (segment locking, edge case)

```
1.  A schedule runs Delhi(seq 1) → Agra(seq 2) → Jhansi(seq 3) → Bhopal(seq 4)
2.  Passenger A calls POST /seats/lock with seatIds: ["seat-12"], fromSeq: 1,
    toSeq: 2 (Delhi → Agra) — because fromSeq/toSeq are both present, lockSeats
    takes the segment-aware branch:
      - row-locks seat-12 with FOR UPDATE NOWAIT
      - checks seat_segment_locks for any LOCKED/BOOKED row on seat-12 whose
        [fromSeq, toSeq) overlaps [1, 2) — none exists yet
      - creates a new SeatSegmentLock row: seat-12, fromSeq 1, toSeq 2, LOCKED
      - recomputeSegmentSeatStatuses sets SeatInventory.status = LOCKED for
        seat-12 (since it now has one active segment lock)
      - recountScheduleAggregates recounts available/locked/booked from the
        real seat_inventories rows and writes them back
3.  Passenger B calls POST /seats/lock for the SAME seatId "seat-12", fromSeq: 2,
    toSeq: 4 (Agra → Bhopal) — the overlap check is
    "existing.fromSeq < requested.toSeq AND existing.toSeq > requested.fromSeq":
    existing is [1,2), requested is [2,4) → 1 < 4 is true, but 2 > 2 is false
    → no overlap → the lock succeeds, a second SeatSegmentLock row is created
4.  seat-12 now has two active segment locks (by two different users), and its
    SeatInventory.status is still just LOCKED — the per-segment detail only
    lives in seat_segment_locks, not on the seat's own summary row
5.  If Passenger A's booking is later cancelled, only their segment lock row
    is deleted; recomputeSegmentSeatStatuses re-derives seat-12's status from
    whatever segment locks remain (Passenger B's), so the seat doesn't
    incorrectly flip to AVAILABLE while B still holds it
```

### Case C: A lock is never confirmed and expires (failure path)

```
1.  A user locks seat-7 (full-journey, no fromSeq/toSeq) via POST /seats/lock
    with the default ttlSeconds — lockSeats clamps this to LOCK_TTL_SECONDS
    (300s from .env.example), sets lockExpiresAt = now + 300s, and sets
    SeatInventory.status = LOCKED
2.  The user closes their browser tab; nobody ever calls /seats/confirm or
    /seats/unlock for seat-7
3.  Up to LOCK_EXPIRY_INTERVAL_MS (60s default) later, cleanExpiredLocks() runs
      - tryAcquireLeadership() calls pg_try_advisory_lock(800001); if this
        instance gets it, it proceeds (if another replica already holds it,
        this instance logs "Skipping lock expiry job" and returns immediately)
      - it queries SeatInventory for status = LOCKED AND lockExpiresAt < now()
        — seat-7 matches
      - it flips seat-7 back to AVAILABLE, clears lockedBy/lockedAt/lockExpiresAt
      - it calls inventoryService.recountAndPublish(scheduleId), which
        recounts available/locked/booked directly from seat_inventories and
        publishes the corrected counts to inventory.seat-availability-updated
      - pg_advisory_unlock(800001) releases leadership in the `finally` block,
        whether or not the sweep succeeded
4.  Segment locks are swept the same way, in a separate step that runs first
    within the same cleanExpiredLocks() call, using
    recomputeSegmentSeatStatuses() instead of a flat "set to AVAILABLE"
    (since a seat with segment locks might still be held by a different leg)
```

---

## Component Breakdown

### 1. `index.ts` — Entry Point

```typescript
import dotenv from "dotenv";
dotenv.config();

import app from "./server";
import { config } from "./config";
import logger from "./config/logger";
import { disconnectAll } from "./config/kafka";
import inventoryConsumer from "./kafka/consumer/inventory.consumer";
import { startLockExpiryJob, stopLockExpiryJob } from "./utils/lockExpiry";

const startServer = async (): Promise<void> => {
  try {
    await inventoryConsumer.start();
    startLockExpiryJob();

    const server = app.listen(config.PORT, () => {
      logger.info(`${config.SERVICE_NAME} is running on port ${config.PORT}`);
    });

    const shutdown = async (): Promise<void> => {
      logger.info("Shutting down gracefully...");
      stopLockExpiryJob();

      server.close(async () => {
        await disconnectAll();
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

void startServer();
```

In plain English: connect the Kafka consumer first, start the lock-expiry timer, then start listening on HTTP — in that order, so the process only reports itself as "running" once it can actually react to events. `SIGTERM`/`SIGINT` both stop the timer, close the HTTP server, then disconnect Kafka before exiting.

---

### 2. `server.ts` — Express App

```typescript
import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import inventoryRoutes from "./routes/inventory.routes";
import prisma from "./config/prisma";
import logger from "./config/logger";

const app = express();

app.use(helmet());
app.use(corsMiddleware);
app.use(reqLogger);
app.use(cookieParser());
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Hello from inventory-service");
});

app.get("/health", async (req, res) => {
  let dbHealthy = false;
  try {
    await prisma.$queryRaw`SELECT 1`;
    dbHealthy = true;
  } catch (err) {
    logger.error("Health check: DB unreachable", {
      error: (err as Error).message,
    });
  }

  res.status(dbHealthy ? 200 : 503).json({
    success: dbHealthy,
    message: dbHealthy
      ? "Inventory Service is healthy"
      : "Inventory Service is degraded",
    database: dbHealthy,
    timestamp: new Date().toISOString(),
  });
});

app.use(inventoryRoutes);

app.use(errorHandler);

export default app;
```

Unlike notification-service's mostly-empty Express app, this one's `/health` endpoint actually round-trips to Postgres (`SELECT 1`) and reports `503` if the database is unreachable — a genuinely useful health check, not just "process is alive."

---

### 3. `config/` — Configuration, Prisma, Kafka, Logger

**`config/index.ts`** — every environment variable this service reads, in one typed object:

```typescript
interface Config {
  SERVICE_NAME: string;
  PORT: number;
  NODE_ENV: string;
  LOG_LEVEL: string;
  DATABASE_URL: string | undefined;
  ALLOWED_ORIGINS: string | undefined;
  KAFKA_BROKER: string | undefined;
  KAFKA_CLIENT_ID: string | undefined;
  LOCK_TTL_SECONDS: number;
  LOCK_EXPIRY_INTERVAL_MS: number;
  INTERNAL_SERVICE_KEY: string | undefined;
}

export const config: Config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT) || 4007,
  NODE_ENV: process.env.NODE_ENV || "development",
  LOG_LEVEL: process.env.LOG_LEVEL || "info",
  DATABASE_URL: process.env.DATABASE_URL,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS,
  KAFKA_BROKER: process.env.KAFKA_BROKER,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  LOCK_TTL_SECONDS: Number(process.env.LOCK_TTL_SECONDS) || 300,
  LOCK_EXPIRY_INTERVAL_MS: Number(process.env.LOCK_EXPIRY_INTERVAL_MS) || 60000,
  INTERNAL_SERVICE_KEY: process.env.INTERNAL_SERVICE_KEY,
};
```

Unlike the API Gateway, `LOG_LEVEL` is genuinely read from `process.env` here (`winston`'s `config/logger.ts` uses `config.LOG_LEVEL` directly), so setting it in `.env` actually changes verbosity. There's no "throw if missing" startup check for anything, including `DATABASE_URL` and `INTERNAL_SERVICE_KEY` — if either is unset, the service still starts, and only fails the first time it actually needs them (a DB query, or the first internal-auth check rejecting every caller because `config.INTERNAL_SERVICE_KEY` is `undefined`).

**`config/prisma.ts`** — a singleton `PrismaClient`, using the `pg` adapter directly instead of Prisma's default connection handling:

```typescript
import { PrismaClient } from "../generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { config } from "./index";

const connectionString = config.DATABASE_URL;

const globalForPrisma = global as unknown as { prisma: PrismaClient };

const prisma =
  globalForPrisma.prisma ??
  new PrismaClient({
    adapter: new PrismaPg({ connectionString }),
    log: ["error", "warn"],
  });

if (process.env.NODE_ENV !== "production") {
  globalForPrisma.prisma = prisma;
}

export default prisma;
```

Caching the client on `global` in non-production avoids creating a fresh connection pool on every `nodemon` reload.

**`config/kafka.ts`** — one Kafka client, one idempotent producer, one consumer, and connect/disconnect helpers:

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

`idempotent: true` plus `maxInFlightRequests: 5` gives exactly-once delivery per partition on retry — a duplicate send from a retried request won't double-publish. The consumer uses its own group id, `"inventory-service-group"`, so its partition assignment and committed offsets are isolated from every other service's consumer group.

**`config/logger.ts`** — a single shared Winston logger, same shape as the other two services' loggers, with `LOG_LEVEL` actually wired up correctly (see above).

---

### 4. `prisma/schema.prisma` — Data Model

```prisma
enum SeatStatus {
  AVAILABLE
  LOCKED
  BOOKED
  CANCELLED
}

// One row per (trainId, departureDate) schedule fanned out from
// admin-service's admin.schedule-created event. Aggregate seat counters
// (available/locked/booked) are recomputed from SeatInventory rather than
// trusted as a running total, to avoid counter drift under concurrent writes.
model ScheduleInventory {
  id            String   @id @default(uuid())
  scheduleId    String   @unique
  trainId       String
  trainNumber   String
  trainName     String
  departureDate DateTime @db.Date
  totalSeats    Int
  available     Int
  locked        Int      @default(0)
  booked        Int      @default(0)
  status        String   @default("ACTIVE")
  version       Int      @default(0)
  createdAt     DateTime @default(now())
  updatedAt     DateTime @updatedAt

  seats SeatInventory[]

  @@index([trainId])
  @@index([scheduleId, status])
  @@index([departureDate])
  @@map("schedule_inventories")
}

// One row per physical seat per schedule. `status` is the seat's summary
// status across the whole journey; segment-level (partial-journey) locks
// live in SeatSegmentLock instead and get reconciled into this row's status
// by recomputeSegmentSeatStatuses.
model SeatInventory {
  id                  String     @id @default(uuid())
  scheduleInventoryId String
  scheduleId          String
  seatId              String
  seatNumber          Int
  seatType            String
  price               Float
  status              SeatStatus @default(AVAILABLE)
  lockedBy            String?
  lockedAt            DateTime?
  lockExpiresAt       DateTime?
  bookingId           String?
  version             Int        @default(0)
  createdAt           DateTime   @default(now())
  updatedAt           DateTime   @updatedAt

  scheduleInventory ScheduleInventory @relation(fields: [scheduleInventoryId], references: [id], onDelete: Cascade)

  @@unique([scheduleId, seatId])
  @@unique([scheduleId, seatNumber])
  @@index([scheduleId, status])
  @@index([lockExpiresAt, status])
  @@index([bookingId])
  @@map("seat_inventories")
}

// Ordered station list per schedule, populated from the same
// SCHEDULE_CREATED event's denormalized route. Used to resolve a station ID
// to its sequence number for segment-overlap checks (see SeatSegmentLock).
model RouteStop {
  id             String @id @default(uuid())
  scheduleId     String
  stationId      String
  stationName    String
  stationCode    String
  sequenceNumber Int

  @@unique([scheduleId, stationId])
  @@unique([scheduleId, sequenceNumber])
  @@index([scheduleId])
  @@map("route_stops")
}

// One row per seat locked/booked for a specific journey segment
// [fromSeq, toSeq). Two segments overlap when a.fromSeq < b.toSeq AND
// b.fromSeq < a.toSeq — this is what lets two passengers hold the same
// physical seat for non-overlapping parts of a train's route.
model SeatSegmentLock {
  id            String     @id @default(uuid())
  scheduleId    String
  seatId        String
  fromSeq       Int
  toSeq         Int
  status        SeatStatus @default(LOCKED)
  lockedBy      String?
  lockedAt      DateTime?
  lockExpiresAt DateTime?
  bookingId     String?
  version       Int        @default(0)
  createdAt     DateTime   @default(now())
  updatedAt     DateTime   @updatedAt

  @@index([scheduleId, seatId])
  @@index([scheduleId, seatId, status])
  @@index([lockExpiresAt, status])
  @@index([bookingId])
  @@map("seat_segment_locks")
}

// Guards Kafka consumer handlers (initializeInventory, cancelScheduleInventory)
// against reprocessing the same event twice on redelivery.
model IdempotencyRecord {
  id          String   @id @default(uuid())
  eventKey    String   @unique
  processedAt DateTime @default(now())

  @@index([eventKey])
  @@map("idempotency_records")
}
```

The schema's own comments explain the two trickiest design choices well:

- `ScheduleInventory`'s `available`/`locked`/`booked` are **recomputed from `SeatInventory` rows, never trusted as a running total** — this is why `recountScheduleAggregates()` exists in the service layer, and why a `+1`/`-1` arithmetic bug anywhere would eventually get corrected on the next recount rather than drifting forever.
- `SeatSegmentLock` models one row per seat **per locked/booked journey leg**, and "two segments overlap when `a.fromSeq < b.toSeq AND b.fromSeq < a.toSeq`" is the exact overlap test used everywhere segment locks are checked.

One migration exists so far: `20260730000000_init` — the schema has not yet been applied against a live database as part of this session (see [Known Issues](#known-issues--inconsistencies)).

---

### 5. `kafka/consumer/inventory.consumer.ts` — Reading Events

```typescript
class InventoryConsumer {
  async start(): Promise<void> {
    await consumer.connect();
    await connectProducer(); // needed for DLQ publishing
    logger.info("Inventory consumer connected");

    await consumer.subscribe({
      topics: [KAFKA_TOPICS.SCHEDULE_CREATED, KAFKA_TOPICS.SCHEDULE_CANCELLED],
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: withDLQ<unknown>(
        producer,
        KAFKA_TOPICS.DLQ_INVENTORY,
        logger,
        async ({ topic, partition, message, parsedValue }) => {
          logger.info(`Processing ${topic}`, {
            partition,
            offset: message.offset,
          });

          switch (topic) {
            case KAFKA_TOPICS.SCHEDULE_CREATED:
              await inventoryService.initializeInventory(
                parsedValue as ScheduleCreatedEventData,
              );
              break;
            case KAFKA_TOPICS.SCHEDULE_CANCELLED:
              await inventoryService.cancelScheduleInventory(
                parsedValue as ScheduleCancelledEventData,
              );
              break;
            default:
              logger.warn(`Unknown topic: ${topic}`);
          }
        },
      ),
    });

    logger.info("Inventory consumer running...");
  }
}

export default new InventoryConsumer();
```

Unlike notification-service, this consumer only subscribes to the **two topics it actually handles**, not every topic in the shared registry — so there's no "Unknown topic" noise problem here. `fromBeginning: true` means a brand-new deployment (or one that lost its committed offsets) will replay every historical `admin.schedule-created`/`admin.schedule-cancelled` event from the start of the topic, relying on the `IdempotencyRecord` check inside each handler to avoid double-processing anything already handled.

---

### 6. `kafka/producer/inventory.producer.ts` — Publishing Events

```typescript
class InventoryProducer {
  private isInitialized: boolean;

  constructor() {
    this.isInitialized = false;
  }

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
    await this.initialize();

    let lastError: Error | undefined;
    for (let attempt = 1; attempt <= MAX_PUBLISH_RETRIES; attempt++) {
      try {
        const result = await producer.send({
          topic,
          messages: [
            {
              key: key || `${topic}-${Date.now()}`,
              value: JSON.stringify(value),
              timestamp: Date.now().toString(),
            },
          ],
        });
        logger.info(`Message sent to topic: ${topic}`, {
          key,
          partition: result[0]?.partition,
          offset: result[0]?.offset,
        });
        return result;
      } catch (error) {
        lastError = error as Error;
        logger.error(
          `Failed to send message to ${topic} (attempt ${attempt}/${MAX_PUBLISH_RETRIES})`,
          { error: lastError.message, key },
        );
        if (attempt < MAX_PUBLISH_RETRIES) {
          await new Promise((r) => setTimeout(r, RETRY_DELAY_MS * attempt));
        }
      }
    }

    logger.error(
      `All ${MAX_PUBLISH_RETRIES} publish attempts failed for ${topic}`,
      { key },
    );
    throw lastError;
  }

  async publishSeatAvailabilityUpdated(
    scheduleId: string,
    trainId: string,
    available: number,
    locked: number,
    booked: number,
  ) {
    return this.sendMessage<SeatAvailabilityUpdatedPayload>(
      KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED,
      `schedule-${scheduleId}`,
      { scheduleId, trainId, available, locked, booked },
    );
  }
}

export default new InventoryProducer();
```

Every call site in `inventory.service.ts` wraps `publishSeatAvailabilityUpdated` in its own `try/catch` that only logs on failure — meaning a publish failure **never** rolls back or fails the HTTP request/Kafka handler that triggered it. The database write (the actual seat state change) always wins; the availability broadcast is best-effort on top of it. That's a deliberate tradeoff (seat state is the source of truth, not the event), but it does mean search-service's index can silently drift out of sync if publishing keeps failing.

---

### 7. `services/inventory.service.ts` — The Core Logic

This is the single largest file in the service (over 1000 lines) and holds every piece of business logic: the two Kafka handlers, the four REST handlers, and the shared segment/aggregate-recompute helpers they all call into.

**The idempotency guard**, used identically by both Kafka handlers:

```typescript
const eventKey = `SCHEDULE_CREATED:${scheduleId}`;

const existing = await prisma.idempotencyRecord.findUnique({
  where: { eventKey },
});
if (existing) {
  logger.info(`Duplicate event skipped: ${eventKey}`);
  return;
}
```

**Recomputing a seat's summary status from its segment locks** — this is the piece that makes segment locking safe to mix with the seat's single `status` column:

```typescript
async function recomputeSegmentSeatStatuses(
  tx: TransactionClient,
  scheduleId: string,
  seatIds: string[],
): Promise<SegmentStatusChanges> {
  const statusChanges: SegmentStatusChanges = {
    nowAvailable: 0,
    nowOccupied: 0,
    lockedToBooked: 0,
    bookedToLocked: 0,
  };

  for (const seatId of seatIds) {
    const locks = await tx.seatSegmentLock.findMany({
      where: { scheduleId, seatId, status: { in: ["LOCKED", "BOOKED"] } },
      select: { status: true },
    });

    let newStatus: "AVAILABLE" | "LOCKED" | "BOOKED";
    if (locks.length === 0) {
      newStatus = "AVAILABLE";
    } else if (locks.some((l) => l.status === "LOCKED")) {
      newStatus = "LOCKED";
    } else {
      newStatus = "BOOKED";
    }
    // ... reads current status via `FOR UPDATE NOWAIT`, skips if unchanged,
    // tracks the transition, then writes the new status + clears
    // lockedBy/lockedAt/lockExpiresAt/bookingId if the new status is AVAILABLE
  }

  return statusChanges;
}
```

In plain English: a seat with at least one `LOCKED` segment is `LOCKED` overall; a seat with only `BOOKED` segments (no `LOCKED` ones left) is `BOOKED` overall; a seat with no active segment locks at all is `AVAILABLE`. This function is called after every segment lock/unlock/confirm/cancel, and again by the lock-expiry sweep.

**Recomputing the schedule's aggregate counters from the real seat rows** — the schema comment's "recomputed, not trusted" promise, implemented:

```typescript
async function recountScheduleAggregates(
  tx: TransactionClient,
  scheduleId: string,
): Promise<AvailabilityCounts> {
  const counts = await tx.$queryRaw<CountsRow[]>`
    SELECT
      COUNT(*) FILTER (WHERE status = 'AVAILABLE')::int AS available,
      COUNT(*) FILTER (WHERE status = 'LOCKED')::int AS locked,
      COUNT(*) FILTER (WHERE status = 'BOOKED')::int AS booked
    FROM seat_inventories
    WHERE "scheduleId" = ${scheduleId}
  `;

  const { available, locked, booked } = counts[0];

  await tx.$executeRaw`
    UPDATE schedule_inventories
    SET available = ${available}, locked = ${locked}, booked = ${booked},
        version = version + 1, "updatedAt" = NOW()
    WHERE "scheduleId" = ${scheduleId}
  `;

  return { available, locked, booked };
}
```

**Every mutating handler follows the same shape**: acquire row locks with `SELECT ... FOR UPDATE NOWAIT`, branch on whether `fromSeq`/`toSeq` were supplied (segment path) or not (full-journey fallback path), make the state change, recompute aggregates, then publish availability — wrapped in `retryTransaction()` (below) so a `FOR UPDATE NOWAIT` conflict from a genuinely concurrent request gets retried a few times before giving up. `lockSeats`, `unlockSeats`, `confirmSeats`, and `cancelBooking` all follow this exact pattern; see the full file for each one's specific checks (e.g. `unlockSeats` requires the caller to be the same `lockedBy` user in the full-journey path; `cancelBooking` checks `SeatSegmentLock` for a matching `bookingId` first, and only falls back to the flat `SeatInventory.bookingId` field if no segment locks are found).

---

### 8. `controllers/` and `routes/` — The HTTP Surface

**`routes/inventory.routes.ts`** defines the auth strategy per route:

```typescript
function userOrInternal(req: Request, res: Response, next: NextFunction) {
  const serviceKey = req.headers["x-internal-service-key"];
  if (serviceKey && serviceKey === config.INTERNAL_SERVICE_KEY) {
    req.user = { id: "internal-service" };
    return next();
  }
  return getUserContext(req, res, next);
}

// Public: aggregate availability (used by search results)
router.get(
  "/schedules/:scheduleId/availability",
  inventoryController.getScheduleAvailability,
);

// Authenticated OR internal: individual seat statuses
router.get(
  "/schedules/:scheduleId/seats",
  userOrInternal,
  inventoryController.getScheduleSeats,
);

// Internal only: called by booking-service during the create/cancel-booking saga
router.post("/seats/lock", internalAuth, inventoryController.lockSeats);
router.post("/seats/unlock", internalAuth, inventoryController.unlockSeats);
router.post("/seats/confirm", internalAuth, inventoryController.confirmSeats);
router.post(
  "/seats/cancel-booking",
  internalAuth,
  inventoryController.cancelBooking,
);
```

**`controllers/inventory.controller.ts`** does request validation (via the Zod schemas in `types/zod.ts`) and response shaping only — all the actual logic lives in the service layer. Every handler is wrapped in `asyncHandler` so a thrown error reaches `errorHandler` instead of crashing the process. One representative handler:

```typescript
const lockSeats = asyncHandler(async (req: Request, res: Response) => {
  const result = zLockSeats.safeParse(req.body);
  if (!result.success) {
    return ErrorResponse(res, 400, { message: formatZodError(result.error) });
  }

  const { scheduleId, seatIds, userId, ttlSeconds, fromSeq, toSeq } =
    result.data;

  const lockResult = await inventoryService.lockSeats(
    scheduleId,
    seatIds,
    userId,
    ttlSeconds ?? 0,
    fromSeq,
    toSeq,
  );

  res.status(200).json({
    success: true,
    message: `${lockResult.lockedSeats.length} seat(s) locked successfully`,
    data: {
      scheduleId: lockResult.scheduleId,
      lockedSeats: lockResult.lockedSeats,
      lockExpiresAt: lockResult.lockExpiresAt,
    },
  });
});
```

The other four handlers (`getScheduleAvailability`, `getScheduleSeats`, `unlockSeats`, `confirmSeats`, `cancelBooking`) follow the identical validate → call service → shape response pattern.

---

### 9. `middlewares/`

**`internal-auth.middleware.ts`** — rejects anything without the exact shared secret:

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

**`user-context.middleware.ts`** — trusts the gateway's `x-user-id` header (same trust model as the API Gateway's own docs describe — this only holds if the service is unreachable from outside the gateway, which today it very much is, since nothing proxies to it, but also nothing firewalls it either):

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

**`cors.middleware.ts`**, **`error.middleware.ts`**, **`req.middleware.ts`** are structurally identical to the equivalent files in api-gateway/notification-service (origin whitelist, `AppError`-aware JSON error formatting, method/path/status/duration request logging).

---

### 10. `utils/lockExpiry.ts` — The Background Sweep

```typescript
const ADVISORY_LOCK_ID = 800001;

async function tryAcquireLeadership(): Promise<boolean> {
  try {
    const result = await prisma.$queryRaw<
      AdvisoryLockRow[]
    >`SELECT pg_try_advisory_lock(${ADVISORY_LOCK_ID}) AS acquired`;
    return result[0]?.acquired === true;
  } catch (err) {
    logger.error("Failed to acquire lock expiry leadership", {
      error: (err as Error).message,
    });
    return false;
  }
}

async function releaseLeadership(): Promise<void> {
  try {
    await prisma.$queryRaw`SELECT pg_advisory_unlock(${ADVISORY_LOCK_ID})`;
  } catch (err) {
    logger.error("Failed to release lock expiry leadership", {
      error: (err as Error).message,
    });
  }
}
```

`pg_try_advisory_lock` is Postgres's non-blocking session-level advisory lock — if instance A already holds lock id `800001`, instance B's call returns `false` immediately rather than waiting. That's the entire leader-election mechanism: whichever replica calls it first this tick wins, does the sweep, and releases it in a `finally` block so a crash mid-sweep still eventually frees the lock (Postgres also auto-releases session-level advisory locks if the holding connection dies).

```typescript
export function startLockExpiryJob(): void {
  void cleanExpiredLocks();

  intervalHandle = setInterval(
    () => void cleanExpiredLocks(),
    config.LOCK_EXPIRY_INTERVAL_MS,
  );
  logger.info(
    `Lock expiry job started (interval: ${config.LOCK_EXPIRY_INTERVAL_MS}ms)`,
  );
}
```

It runs once immediately on startup (so locks don't sit expired for up to a full interval right after a deploy), then every `LOCK_EXPIRY_INTERVAL_MS` (60s default) after that. Segment locks are cleaned in one pass, then full-journey seat locks in a second pass — both call back into `inventoryService.recountAndPublish()` per affected schedule afterward.

---

### 11. `utils/retryTransaction.ts`

```typescript
export async function retryTransaction<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
): Promise<T> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const err = error as RetryableError;
      const isRetryable =
        err.code === "P2034" ||
        !!err.message?.includes("could not serialize") ||
        !!err.message?.includes("could not obtain lock") ||
        !!err.message?.includes("deadlock detected");

      if (isRetryable && attempt < maxRetries) {
        const delay = 50 * attempt;
        logger.warn(
          `Transaction attempt ${attempt} failed (retryable), retrying in ${delay}ms...`,
        );
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error("retryTransaction: exhausted retries without a result");
}
```

Every mutating service function (`lockSeats`, `unlockSeats`, `confirmSeats`, `cancelBooking`) wraps its `prisma.$transaction(...)` call in this. It only retries on genuinely transient conflicts (Prisma's `P2034` serialization-failure code, or Postgres's own "could not serialize"/"could not obtain lock"/"deadlock detected" messages) — an application-level error like `ConflictError("Seats not available")` is not retryable and passes straight through on the first attempt, so retrying never masks a real "seat is actually taken" failure as a transient one.

---

### 12. `types/` and `utils/` — Everything Else

**`types/index.ts`** defines every shape used across the service: the two Kafka event payloads (`ScheduleCreatedEventData`, `ScheduleCancelledEventData`), raw-query row types (`SeatInventoryRow`, `CountsRow`, etc.), and every REST handler's result type (`LockSeatsResult`, `UnlockSeatsResult`, `ConfirmSeatsResult`, `CancelBookingResult`). Note `ScheduleCancelledEventData` is modeled as a union (`{ data: {...} } | {...}`) because admin-service's producer wraps the payload in an envelope, and the service's `cancelScheduleInventory` explicitly unwraps whichever shape it receives (`"data" in eventData ? eventData.data : eventData`).

**`types/zod.ts`** has one schema per request body/query — `zLockSeats`, `zUnlockSeats`, `zConfirmSeats`, `zCancelBooking`, `zSeatFilters` — all requiring `scheduleId`/`userId`/etc. as strings and treating `fromSeq`/`toSeq` as optional positive integers.

**`utils/error.ts`** — the same `AppError` + subclasses pattern as api-gateway and notification-service (`BadRequestError`, `UnauthorizedError`, `ForbiddenError`, `NotFoundError`, `ConflictError`, `TooManyRequestsError`, `InternalServerError`), each carrying its own HTTP status and machine-readable code.

**`utils/api-response.ts`** exports both `SuccessResponse` and `ErrorResponse`, but only `ErrorResponse` is actually called anywhere (from the controller, on Zod validation failure) — every successful response is built inline in the controller instead (`res.status(200).json({ success: true, ... })`), so `SuccessResponse` is currently dead code.

**`utils/asyncHandler.ts`** and **`utils/zod.formatter.ts`** are small, single-purpose helpers described inline above wherever they're used.

---

## Environment Variables

```bash
PORT=4007
NODE_ENV=development
LOG_LEVEL=info

DATABASE_URL=postgresql://admin:irctcpass@localhost:5432/inventory_service_db?schema=public
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:4000

KAFKA_BROKER=localhost:9093
KAFKA_CLIENT_ID=inventory-service

LOCK_TTL_SECONDS=300
LOCK_EXPIRY_INTERVAL_MS=60000

INTERNAL_SERVICE_KEY=change-me-to-a-shared-secret
```

Every variable in `.env.example` is actually read by `config/index.ts` — unlike notification-service (`SENDGRID_API_KEY`, `FRONTEND_URL`), there are no unused env vars here. None of them are validated as "required" at startup, though (see [Known Issues](#known-issues--inconsistencies)) — the service will boot even with `DATABASE_URL` or `INTERNAL_SERVICE_KEY` unset, and only fail once something actually tries to use them.

---

## Kafka Topics & HTTP Routes Reference

### Kafka topics

| Topic | Direction | Handled by |
|---|---|---|
| `admin.schedule-created` | consumed | `initializeInventory` — creates `ScheduleInventory` + `SeatInventory` rows. **Now actually fires** — admin-service's `server.ts` mounts `scheduleRoutes` at `/schedules`, so `POST /schedules/schedule` reaches `scheduleController.createSchedule`, which publishes this event via `adminProducer.publishScheduleCreated`. (Still no API Gateway route for it, so it's only reachable by calling admin-service directly.) |
| `admin.schedule-cancelled` | consumed | `cancelScheduleInventory` — marks the schedule and all its seats `CANCELLED`. **Still never fires, for a different reason than schedule-created's old one** — `adminProducer.publishScheduleCancelled` exists and is wired correctly, but nothing in admin-service calls it; there's no schedule-cancellation route/controller/service built yet. |
| `inventory.seat-availability-updated` | published | Emitted after every state change (init, cancel, lock, unlock, confirm, cancel-booking, expiry sweep). Intended for search-service to keep its index current — not verified as part of this documentation pass. |
| `dlq.inventory-service` | published (rare) | Only reached if a `admin.schedule-created`/`admin.schedule-cancelled` message fails processing 3 times in a row (`withDLQ`, `DLQ_MAX_RETRIES = 3`). |

### HTTP routes

| Method & Path | Auth | Status |
|---|---|---|
| `GET /schedules/:scheduleId/availability` | none (public) | Reads `ScheduleInventory` directly; `404` if the schedule isn't in inventory yet. |
| `GET /schedules/:scheduleId/seats` | `x-user-id` (gateway) OR `x-internal-service-key` | Optional `?status=`, `?seatType=`, `?fromSeq=`, `?toSeq=` query filters; segment params add a computed `segmentStatus` per seat. |
| `POST /seats/lock` | `x-internal-service-key` only | Body validated by `zLockSeats`. Segment-aware if `fromSeq`/`toSeq` given, else full-journey. |
| `POST /seats/unlock` | `x-internal-service-key` only | Body validated by `zUnlockSeats`. |
| `POST /seats/confirm` | `x-internal-service-key` only | Body validated by `zConfirmSeats`. |
| `POST /seats/cancel-booking` | `x-internal-service-key` only | Body validated by `zCancelBooking`. |
| `GET /health` | none | Actually checks the database (`SELECT 1`); `503` if unreachable. |
| `GET /` | none | Static "Hello from inventory-service" string. |

All four `x-internal-service-key`-only routes, and the `/seats` route's internal branch, currently have **no route through the API Gateway** — they're only reachable by calling `http://localhost:4007` directly (which is exactly how booking-service is expected to call them, per the code comments; see [Known Issues](#known-issues--inconsistencies)).

---

## Quick Start

```bash
cd inventory-service
npm install

# Generate the Prisma client (writes into src/generated/prisma, gitignored)
npx prisma generate

# .env needs at minimum DATABASE_URL, KAFKA_BROKER, and INTERNAL_SERVICE_KEY
# for the internal routes and the lock-expiry advisory lock to work correctly
npm run dev        # nodemon, hot reload
# or
npm run build && npm start
```

Postgres must be reachable at `DATABASE_URL`, and Kafka at `KAFKA_BROKER` (default `localhost:9093`) — from the IRCTC root, `docker-compose up -d postgres kafka zookeeper` brings up the infrastructure this service expects. Apply the schema with `npx prisma migrate deploy` (or `migrate dev` while developing) before starting the service.

```bash
curl http://localhost:4007/health
# { "success": true, "message": "Inventory Service is healthy", "database": true, "timestamp": "..." }

# admin.schedule-created now fires for real (see Known Issues) — call
# admin-service's POST /schedules/schedule directly to get a
# ScheduleInventory row created here via Kafka. There's no API Gateway
# route for it yet, so this has to go straight to admin-service (default
# port 4003), e.g.:
#   curl -X POST http://localhost:4003/schedules/schedule \
#     -H "Content-Type: application/json" -H "x-user-id: <uuid>" \
#     -d '{"trainId":"<uuid>","departureDate":"2026-09-01"}'

curl -X POST http://localhost:4007/seats/lock \
  -H "Content-Type: application/json" \
  -H "x-internal-service-key: change-me-to-a-shared-secret" \
  -d '{"scheduleId":"<uuid>","seatIds":["seat-1","seat-2"],"userId":"user-123","ttlSeconds":300}'
```

---

## Debugging Tips

- **Database shows no schedules at all, even though admin-service is running** → the route is mounted and does publish `admin.schedule-created` now, so first check whether anything has actually called `POST /schedules/schedule` on admin-service — there's no gateway route or UI that calls it automatically, so an idle system will still show an empty inventory database until something calls it directly (curl/Postman/script). Only suspect this service's Kafka consumer once you've confirmed the event was actually published.
- **`403 Invalid or missing internal service key`** on `/seats/lock` etc. → the caller didn't send `x-internal-service-key`, or it doesn't exactly match `config.INTERNAL_SERVICE_KEY` — check both services' `.env` have the identical value.
- **`401 User context missing - must come through gateway`** on `GET /schedules/:scheduleId/seats` → the request has neither a valid `x-internal-service-key` nor an `x-user-id` header. If you're calling this service directly (not through the gateway) for testing, you must set `x-user-id` yourself — there's no gateway in this path to set it for you (there's no gateway route for this service at all today, see Known Issues).
- **A lock never seems to expire** → check the process logs for "Skipping lock expiry job — another instance is the leader" — if you're running multiple instances locally, only one of them will ever log the actual cleanup. Also check `LOCK_EXPIRY_INTERVAL_MS` and that `lockExpiresAt` on the seat row is actually in the past.
- **`FOR UPDATE NOWAIT` errors surfacing as request failures** → this is Postgres raising a lock-not-available error on truly concurrent requests for the same seat(s); `retryTransaction()` should absorb most of these, but if you see one bubble up to the client, check whether the error message matches one of the three retryable patterns in `retryTransaction.ts` — a Postgres error message that doesn't match `could not serialize` / `could not obtain lock` / `deadlock detected` will not be retried.
- **`inventory.seat-availability-updated` isn't reaching search-service** → check this service's own logs for "Failed to publish availability after ..." — publishing failures here are swallowed (logged, not thrown), so the HTTP/Kafka request that triggered the change will still report success even if the broadcast never went out.
- **Health check returns 503** → `prisma.$queryRaw\`SELECT 1\`` failed, meaning Postgres is unreachable at `DATABASE_URL` or the connection pool is exhausted; check the logged error message under "Health check: DB unreachable".

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, since these are informational (same approach as the API Gateway's and Notification Service's docs):

1. **This service has not been run against a live Postgres or Kafka broker as part of this session.** Verification so far consisted of `npx tsc --noEmit` (passes clean) and `npx prisma generate` (schema compiles) — there has been no live boot, no HTTP round-trip, and no Kafka round-trip, because this environment has no reachable database or broker. Treat the request/response shapes and lifecycle walkthroughs above as "what the code says it does," not as "observed behavior."
2. **Not reachable through the API Gateway.** `api-gateway/src/routes/index.ts` has no route mentioning inventory at all (confirmed by grep) — there is no proxy configured for this service today, even though `INVENTORY_SERVICE_URL` and an `inventoryService` circuit breaker already exist in the gateway's config (see the gateway's own docs). Wiring that up is a separate, not-yet-done task.
3. **`admin.schedule-created` now fires — this entry is corrected from a previous version of this doc, which said it never did.** `admin-service/src/server.ts` mounts `scheduleRoutes` at `/schedules` (`app.use("/schedules", scheduleRoutes)`), so `POST /schedules/schedule` reaches `scheduleController.createSchedule` → `scheduleService.createSchedule` → `adminProducer.publishScheduleCreated`. Calling that endpoint directly against admin-service (there is still no API Gateway route for it) does publish a real event that this service's consumer picks up and processes correctly. **`admin.schedule-cancelled` still never fires, though** — `adminProducer.publishScheduleCancelled` is implemented and would work if called, but nothing in admin-service ever calls it (its own file has a comment confirming there's no schedule-cancellation route/controller/service yet). So `cancelScheduleInventory` remains unreachable in practice, just not for the reason previously documented here.
4. **`SuccessResponse` (in `utils/api-response.ts`) is defined but never called.** Every successful response is built inline in the controller (`res.status(200).json({ success: true, ... })`) instead of going through this helper — dead code today.
5. **No startup validation of required env vars.** Unlike notification-service (which calls `process.exit(1)` if `RESEND_API_KEY`/`MAIL_SEND`/`KAFKA_BROKER` are missing) or the API Gateway (which throws if JWT secrets are missing), this service will start successfully even with `DATABASE_URL` or `INTERNAL_SERVICE_KEY` unset — it just fails the first time something actually needs them (every DB call; every internal-auth check, which then rejects all callers since `undefined !== ` any real header value sent).
6. **Eleven dependencies in `package.json` are unrelated to what this service does and are never imported anywhere under `src/`** (confirmed by grep): `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`, `mongoose`, `bcrypt`, `jsonwebtoken`, `otp-generator`, `ioredis`, `http-status`, `resend`. This is the same pattern seen in api-gateway and notification-service's docs — `package.json` looks copied from a template (likely user-service) without pruning.
7. **`npm run seed` points at `src/services/seed.ts`**, which does not exist in this project (only `services/inventory.service.ts` exists) — running that script fails. Same issue flagged in both other services' docs, likely from the same shared `package.json` origin.
8. **Two different import paths for the same Zod version.** `types/zod.ts` imports from `"zod"`, while `utils/zod.formatter.ts` imports `ZodError` from `"zod/v4"`. Since `package.json` pins `"zod": "^4.4.3"`, these currently resolve to the same code, but it's an inconsistent style within one small service — one file assumes it might be running under Zod v3 with the v4 compat import, the other assumes v4 is already the default export.
9. **`ScheduleCancelledEventData`'s union-envelope handling is a workaround for an upstream inconsistency**, not a bug in this service: `cancelScheduleInventory` explicitly checks `"data" in eventData ? eventData.data : eventData` because admin-service's producer wraps the payload (`{ eventType, data, timestamp }`) while `admin.schedule-created`'s payload is not wrapped the same way. Worth knowing if a payload shape ever needs to change on the admin-service side.
10. **`asyncHandler`'s generic signature accepts `Promise<any> | any`** (`utils/asyncHandler.ts`) — the one place in this codebase that still uses `any`, contrary to this repo's own stated TypeScript conventions. Every call site happens to return a typed value anyway, so this hasn't caused an observed problem, but it is the one type-erasure gap in an otherwise strictly-typed service.
11. **Publishing `inventory.seat-availability-updated` failures are silently absorbed everywhere they occur** (`initializeInventory`, `cancelScheduleInventory`, `lockSeats`, `unlockSeats`, `confirmSeats`, `cancelBooking`, `recountAndPublish`) — each site catches the error, logs it, and moves on. The underlying database change always succeeds or fails on its own; the Kafka broadcast is best-effort. This is a deliberate and reasonable tradeoff (seat state shouldn't be held hostage by a flaky broker), but it does mean a run of publish failures would leave search-service's cached availability numbers stale with no automatic alarm beyond the log line.
12. **Only one Prisma migration exists** (`20260730000000_init`) and it has not been applied against any real database in this environment — there is no way to confirm from this session alone that `prisma migrate deploy` succeeds cleanly against a live Postgres instance.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
