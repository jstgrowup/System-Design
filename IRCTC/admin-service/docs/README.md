# Admin Service — Complete Guide

Single source of truth for the IRCTC Admin Service: what it does, how a request flows through it, and how each piece works, with the actual current code inline.

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [Request Lifecycle](#request-lifecycle)
5. [Component Breakdown](#component-breakdown)
   - [index.ts — Entry Point](#1-indexts--entry-point)
   - [server.ts — The Express App](#2-serverts--the-express-app)
   - [config/ — Kafka, Logger, Prisma](#3-config--kafka-logger-prisma)
   - [types/ — Validation Schemas & a Stray Type File](#4-types--validation-schemas--a-stray-type-file)
   - [Station Creation — controller + service](#5-station-creation--controller--service)
   - [Train Creation — controller + service](#6-train-creation--controller--service)
   - [kafka/producer/admin.producer.ts — Event Publishing](#7-kafkaproduceradminproducerts--event-publishing)
   - [middlewares/ & utils/ — Cross-Cutting Helpers](#8-middlewares--utils--cross-cutting-helpers)
6. [Environment Variables](#environment-variables)
7. [Error Codes Reference](#error-codes-reference)
8. [Kafka Topics Reference](#kafka-topics-reference)
9. [Quick Start](#quick-start)
10. [Debugging Tips](#debugging-tips)
11. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **Admin Service** is meant to be the internal API for setting up the data the rest of IRCTC runs on — stations and trains. As it exists today:

- **Creates stations** (`POST /stations/station`) — name, code, city, optional state
- **Creates trains** (`POST /trains/train`) — train number, name, coach, and a full seat map in one call
- **Validates** every request body with Zod before touching the database
- **Persists** through Prisma into Postgres (`stations`, `trains`, `seats`, plus `routes`/`route_stations`/`schedules` tables that nothing in this service's code populates yet)
- **Publishes Kafka events** (`admin.station-created`, `admin.train-created`) so other services (inventory, search) can react to new stations/trains

There's no read, update, or delete endpoint for either resource — just the two creation routes above.

**Important:** as currently checked in, this service does not compile. `src/index.ts` and every file under `src/config/` import a `config` module (`./config`, `../config`, `.`, `./index` — all pointing at `src/config/index.ts`) that doesn't exist in this project, and `index.ts` also imports a `./config/db` that doesn't exist either. Running `npx tsc --noEmit` from `admin-service/` confirms this with five "Cannot find module" errors. Everything described below is what the code is written to do — see [Known Issues #1](#known-issues--inconsistencies) for the specifics of why it can't do it yet.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                        API GATEWAY (:4000)                     │
│  Proxies (per api-gateway/src/routes/index.ts):                │
│   GET /admins/stations/station  → adminServiceProxy            │
│   GET /admins/trains/train      → adminServiceProxy            │
│  (both registered as GET — admin-service only defines POST     │
│  for these paths, so requests routed through the gateway to    │
│  either endpoint 404 today — see Known Issue #12)              │
└───────────────────────────┬───────────────────────────────────┘
                            │ HTTP → ADMIN_SERVICE_URL (default
                            │ http://localhost:4003, per the
                            │ gateway's own config)
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                       ADMIN SERVICE                            │
│                                                                 │
│  server.ts (Express app):                                      │
│   1. helmet()          → security headers                      │
│   2. corsMiddleware    → origin whitelist (config.ALLOWED_...) │
│   3. reqLogger         → logs method/path/status/duration      │
│   4. cookieParser()                                             │
│   5. express.json()                                             │
│   6. /stations → station.route.ts → stationController          │
│   7. /trains    → train.routes.ts  → trainController            │
│   8. errorHandler (registered last)                             │
│                                                                 │
│  No auth/user-context middleware is mounted anywhere — see      │
│  Known Issue #4.                                                │
└───────────┬─────────────────────────────────────┬─────────────┘
            │ Prisma (@prisma/adapter-pg)          │ kafkajs producer
            ▼                                     ▼
   ┌──────────────────────┐            ┌─────────────────────────────┐
   │  PostgreSQL           │            │  Kafka (localhost:9093)     │
   │  stations, trains,    │            │  admin.station-created  ✅  │
   │  seats, routes,       │            │  admin.train-created    ✅  │
   │  route_stations,      │            │  admin.route-created    —  │
   │  schedules            │            │  admin.schedule-created —  │
   │  (only stations/      │            │  admin.schedule-cancelled— │
   │  trains/seats are     │            │  (producer methods exist,  │
   │  ever written to)     │            │  nothing calls them yet)   │
   └──────────────────────┘            └──────────────┬──────────────┘
                                                       │ consumed by
                                                       ▼
                                        inventory-service, search-service
                                        (per shared/constants/kafka-topics.ts
                                        comments — not verified from their
                                        own source in this pass)
```

---

## File Structure

```
admin-service/
├── src/
│   ├── index.ts                          # Entry point — imports two modules that don't exist (see below)
│   ├── server.ts                         # Express app: middleware + route mounting
│   ├── config/
│   │   ├── kafka.ts                      # Kafka client + producer (connect/disconnect)
│   │   ├── logger.ts                     # Winston logger
│   │   └── prisma.ts                     # PrismaClient singleton (pg adapter)
│   │   # NOTE: index.ts and every file above import a "config" object from
│   │   # "./config" / "../config" / "." / "./index" — none of which resolve
│   │   # to a real file. There is no config/index.ts or config/db.ts here.
│   ├── controllers/
│   │   ├── station.controller.ts         # POST /stations/station
│   │   └── train.controller.ts           # POST /trains/train
│   ├── services/
│   │   ├── station.service.ts            # Station creation + Kafka publish
│   │   └── train.service.ts              # Train + seats creation + Kafka publish
│   ├── routes/
│   │   ├── station.route.ts
│   │   └── train.routes.ts
│   ├── kafka/producer/
│   │   └── admin.producer.ts             # publishStationCreated/TrainCreated/RouteCreated/...
│   ├── middlewares/
│   │   ├── cors.middleware.ts
│   │   ├── error.middleware.ts
│   │   ├── req.middleware.ts
│   │   └── user-context.middleware.ts    # Defined, but never mounted anywhere
│   ├── types/
│   │   ├── zod.ts                        # zStation, zSeat, zTrain schemas
│   │   └── index.ts                      # KnowledgeDoc/RAGResponse — unrelated to this service
│   ├── utils/
│   │   ├── api-response.ts               # SuccessResponse/ErrorResponse helpers
│   │   ├── asyncHandler.ts               # Wraps async route handlers, forwards errors to next()
│   │   ├── error.ts                      # AppError + subclasses
│   │   └── zod.formatter.ts              # Formats a ZodError into one message string
│   └── generated/prisma/                 # Prisma client output (generated, not hand-written)
├── prisma/
│   └── schema.prisma                     # Station, Train, Seat, Route, RouteStation, Schedule
├── docs/                                  # This documentation
├── package.json
├── tsconfig.json
└── .env
```

`tsconfig.json` sets `rootDir: ".."`, mirroring the other services in this repo — it points one level above `admin-service/` so the project can compile files it reaches via `../../shared/...`-style imports, though nothing under `src/` currently uses one.

---

## Request Lifecycle

### Case A: `POST /trains/train` (happy path — the one fully-correct flow in this service)

```
1.  Client sends POST /trains/train with:
      { trainNumber, trainName, coachName?, seats: [{ seatNumber, seatType, price }, ...] }
2.  server.ts middleware runs: helmet → corsMiddleware → reqLogger → cookieParser →
    express.json()
3.  train.routes.ts matches POST /train (mounted at /trains) → trainController.createTrain
4.  zTrain.safeParse validates the body — trims strings, requires seats.length >= 1,
    validates each seat's seatType against the SeatType enum
5.  The controller's own `if (seats.length === 0)` check passes trivially — zTrain
    already guarantees this
6.  await trainService.createTrain({ trainName, trainNumber, coachName, seats }):
      a. prisma.train.findUnique({ trainNumber }) → not found
      b. seatNumbers checked for in-payload duplicates via a Set — none found
      c. prisma.train.create(...) inserts the train and all its seats in a single
         nested write (one transaction), coachName defaults to "AC" if omitted
      d. adminProducer.publishTrainCreated(train) → Kafka topic admin.train-created,
         keyed by train-<id>; failures here are caught and logged, not thrown
7.  Controller responds 200 { success: true, message: "Train created successfully" }
```

### Case B: `POST /stations/station` with an invalid body (validation failure)

```
1.  Client sends POST /stations/station with { name: "NY", code: "NYC", city: "New York" }
2.  stationController.createStation: zStation.safeParse fails — "Station name must be
    at least 4 characters" ("NY" is 2 characters)
3.  ErrorResponse(res, 400, { message: "Station name must be at least 4 characters" })
    is returned directly inside the asyncHandler callback — no error is thrown, so
    errorHandler never runs for this case
4.  Response: 400 { success: false, message: "Station name must be at least 4 characters" }
```

### Case C: `POST /stations/station` with a code that already exists (illustrates the missing `await`)

```
1.  Client sends POST /stations/station with a code that's already in the database,
    e.g. "NDLS"
2.  zStation validation passes
3.  stationController.createStation calls stationService.createStation({...}) but does
    NOT await or return the promise it returns
4.  Because nothing is awaiting it, execution falls straight through to the next line:
    res.status(200).json({ success: true, message: "OTP sent successfully" }) — the
    client gets a 200 OK before the service call has even resolved
5.  Meanwhile, inside stationService.createStation: prisma.station.findUnique finds the
    existing row and throws ConflictError("Station already exists")
6.  That rejection has nowhere to go. asyncHandler's `.catch(next)` only wraps the
    promise returned by the outer async callback, and that callback already returned
    (at step 4) without ever awaiting the inner call. The rejection becomes an
    unhandled promise rejection instead of reaching errorHandler.
7.  Net effect: a duplicate-station request looks like a success (200, "OTP sent
    successfully") to the caller. No new row is created, but the caller has no way to
    know that from the response — the real error only shows up as an unhandled
    rejection in the server's own logs.
```

---

## Component Breakdown

### 1. `index.ts` — Entry Point

```typescript
import app from "./server";
import { config } from "./config";
import connectDB from "./config/db";
import dotenv from "dotenv";

dotenv.config();
app.listen(config.PORT, () => {
  console.log(`Server running on port ${config.PORT}`);
});
```

In plain English, this is meant to: load `.env`, then start listening on `config.PORT`. Two problems, in order of how badly they'd bite:

1. `./config` and `./config/db` don't exist anywhere in `src/` — this file fails to even type-check or run (`npx tsc --noEmit` reports `Cannot find module './config'` and `Cannot find module './config/db'` for these two lines).
2. Even once those files exist, `import { config } from "./config"` is hoisted and evaluated before `dotenv.config()` runs on line 6 — so if `config/index.ts` builds its object by reading `process.env.X` at module-load time (as every other service's `config/index.ts` in this repo does), any variable that's only set in `.env` (and not already in the shell environment) would still be `undefined` at that point.

`connectDB` is imported but never called anywhere in this file — its only use is the import itself.

---

### 2. `server.ts` — The Express App

```typescript
import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { corsMiddleware } from "./middlewares/cors.middleware";
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import stationRoutes from "./routes/station.route";
import trainRoutes from "./routes/train.routes";

const app = express();

// Order matters: security headers and CORS first, then request logging,
// then body/cookie parsing, before any route handlers run.
app.use(helmet());
app.use(corsMiddleware);
app.use(reqLogger);
app.use(cookieParser());
app.use(express.json());
app.use("/stations", stationRoutes);
app.use("/trains", trainRoutes);

app.get("/", (req, res) => {
  // Response text is a holdover from whatever service this was scaffolded from.
  res.send("Hello from index.js of user-service");
});

app.get("/health", (req, res) => {
  res.status(200).json({
    message: "ok",
  });
});
// Must be registered after all routes — Express only treats a 4-arg
// middleware as an error handler when it's last in the chain.
app.use(errorHandler);

export default app;
```

This is the entire app definition: two route groups (`/stations`, `/trains`), a root `GET /` that still identifies itself as "user-service" in its response text, a `GET /health`, and the error handler registered last. No auth middleware is applied anywhere in this chain.

---

### 3. `config/` — Kafka, Logger, Prisma

**`config/kafka.ts`** — the Kafka client and producer, plus idempotent connect/disconnect helpers:

```typescript
const kafka = new Kafka({
  clientId: config.KAFKA_CLIENT_ID,
  brokers: [config.KAFKA_BROKER || "localhost:9093"],
  logLevel: logLevel.ERROR,
  retry: {
    initialRetryTime: 300,
    retries: 8,
    maxRetryTime: 30000,
  },
});

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

`idempotent: true` guarantees each message is written exactly once per partition on retry, which is why `maxInFlightRequests` is capped at 5 (required for that guarantee to hold). `connectProducer()`/`disconnectProducer()` both track an `isConnected` flag so calling either more than once is a no-op. Like every other file in this directory, `config/kafka.ts` imports `{ config } from "."` — a module that doesn't exist (see Known Issue #1).

**`config/logger.ts`** — a single shared Winston logger:

```typescript
import winston from "winston";
import { config } from "../config";

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

export default logger;
```

**`config/prisma.ts`** — the Prisma client, using the `pg` adapter and a global-object cache so hot-reload (`nodemon`) doesn't open a new connection pool on every file change:

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

---

### 4. `types/` — Validation Schemas & a Stray Type File

**`types/zod.ts`** defines the three schemas every request goes through:

- `zStation` — `name` (4–40 chars), `code` (2–10 chars, trimmed and uppercased by the schema itself), `city` (2–40 chars), optional `state` (≤40 chars)
- `zSeat` — `seatNumber` (positive int), `seatType` (`LOWER | MIDDLE | UPPER | SIDE_LOWER | SIDE_UPPER`), `price` (positive number)
- `zTrain` — `trainNumber` (1–10 chars), `trainName` (4–40 chars), optional `coachName` (≤20 chars), `seats` (array of `zSeat`, minimum 1)

`StationBodyType`, `SeatBodyType`, and `TrainBodyType` are the corresponding `z.infer<...>` types used throughout the controllers and services.

**`types/index.ts`** is unrelated to any of the above:

```typescript
import { Types } from "mongoose";

export interface KnowledgeDoc {
  _id: Types.ObjectId | string;
  title: string;
  content: string;
  tags: string[];
  createdAt: Date;
  embedding: number[];
  sim?: number;
}

export interface RAGResponse {
  answer: string;
  sources: { id: string; title: string }[];
  confidence: "high" | "medium" | "low";
}
```

Nothing under `src/` imports either of these interfaces. They describe a document-embedding / retrieval-augmented-generation feature that has nothing to do with stations or trains — see Known Issues for how this lines up with other leftovers in this project.

---

### 5. Station Creation — controller + service

`controllers/station.controller.ts`:

```typescript
const createStation = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zStation schema
    const result = zStation.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { name, code, city, state } = result.data;

    const station = stationService.createStation({
      code: code.toUpperCase(),
      name,
      city,
      state,
    });

    res.status(200).json({ success: true, message: "OTP sent successfully" });
  },
);
```

`code.toUpperCase()` here is a no-op — `zStation`'s own `code` field already applies `.toUpperCase()` during parsing, so `result.data.code` is already uppercase by the time this line runs. The bigger issue is that `stationService.createStation(...)`'s returned promise is neither awaited nor returned — see [Lifecycle Case C](#case-c-post-stationsstation-with-a-code-that-already-exists-illustrates-the-missing-await) for exactly what that causes.

`services/station.service.ts`:

```typescript
const createStation = async ({ code, name, city, state }: StationBodyType) => {
  // Station code is the unique identifier — reject duplicates before hitting the DB constraint
  const existingStation = await prisma.station.findUnique({ where: { code } });
  if (existingStation) {
    throw new ConflictError("Station already exists");
  }
  const createdStation = await prisma.station.create({
    data: {
      code,
      name,
      city,
      state,
    },
  });
  logger.info("Station Created", { id: createdStation.id });
  await adminProducer.publishStationCreated(createdStation);
  return createdStation;
};
```

Unlike `trainService.createTrain` below, the Kafka publish here isn't wrapped in a `.catch` — a publish failure throws and would normally turn an already-committed station creation into a 500 from the controller. In practice it can't even do that today, because the controller never awaits this function in the first place (previous paragraph).

---

### 6. Train Creation — controller + service

`controllers/train.controller.ts`:

```typescript
const createTrain = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zTrain schema
    const result = zTrain.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { trainName, trainNumber, coachName, seats } = result.data;
    // Redundant with zTrain's own `.min(1, ...)` on `seats`, kept as a defensive check
    if (seats.length === 0) {
      throw new BadRequestError("Atleast one seat must be defined");
    }

    await trainService.createTrain({
      trainName,
      trainNumber,
      coachName,
      seats,
    });

    res
      .status(200)
      .json({ success: true, message: "Train created successfully" });
  },
);
```

This one awaits the service call correctly, so a thrown `ConflictError`/`BadRequestError` reaches `asyncHandler`'s `.catch(next)` and, from there, `errorHandler`, as expected.

`services/train.service.ts`:

```typescript
const createTrain = async ({
  trainName,
  trainNumber,
  coachName,
  seats,
}: TrainBodyType) => {
  const existing = await prisma.train.findUnique({ where: { trainNumber } });
  if (existing) {
    throw new ConflictError("Train with this number already exists");
  }
  const seatNumbers = seats.map((s) => s.seatNumber);
  if (new Set(seatNumbers).size !== seatNumbers.length) {
    throw new BadRequestError("Duplicate seat numbers found");
  }
  const train = await prisma.train.create({
    data: {
      trainNumber,
      trainName,
      coachName: coachName || "AC",
      totalSeats: seats.length,
      seats: {
        create: seats.map((seat) => ({
          seatNumber: seat.seatNumber,
          seatType: seat.seatType,
          price: seat.price,
        })),
      },
    },
    include: { seats: { orderBy: { seatNumber: "asc" } } },
  });
  await adminProducer.publishTrainCreated(train).catch((err) => {
    logger.error("Failed to publish train created event", {
      error: err.message,
    });
  });

  return train;
};
```

The nested `seats: { create: [...] }` write inserts the train row and every seat row in one transaction — `totalSeats` is just `seats.length`, not a separately-counted value. The `.catch` on the Kafka publish means a broker outage is logged but never fails the request — the opposite of what happens in `stationService.createStation`.

---

### 7. `kafka/producer/admin.producer.ts` — Event Publishing

A thin class wrapping the shared Kafka producer with domain-specific publish methods:

```typescript
class AdminProducer {
  private isInitialized: boolean;

  private async initialize(): Promise<void> {
    if (!this.isInitialized) {
      await connectProducer();
      this.isInitialized = true;
    }
  }

  private async sendMessage<T>(topic: string, key: string | undefined, value: T) {
    // ...connects lazily, sends via producer.send(), logs partition/offset on
    // success, logs and re-throws on failure
  }

  async publishStationCreated(station: Station) { /* keyed by station-<id> */ }
  async publishTrainCreated(trainData: Train) { /* keyed by train-<id> */ }
  async publishRouteCreated(routeData: Route) { /* keyed by route-<id> */ }
  async publishScheduleCreated(scheduleData: Schedule) { /* keyed by schedule-<id> */ }
  async publishScheduleCancelled(schedule: Schedule) { /* keyed by schedule-<id> */ }
}

export default new AdminProducer();
```

The producer connects lazily on first use (`initialize()`), not at import time. Every `key` is derived from the entity's own id, so all events about the same entity land on the same Kafka partition and stay in order relative to each other. Only `publishStationCreated` and `publishTrainCreated` are ever called anywhere in this codebase — `publishRouteCreated`, `publishScheduleCreated`, and `publishScheduleCancelled` are fully implemented but currently dead code, since no route or schedule controller/service exists yet.

---

### 8. `middlewares/` & `utils/` — Cross-Cutting Helpers

**`middlewares/cors.middleware.ts`** — whitelist check against `config.ALLOWED_ORIGINS` (comma-separated env var), credentials enabled, methods restricted to `GET/POST/PUT/DELETE/OPTIONS`.

**`middlewares/error.middleware.ts`** — `AppError` instances are returned with their own status/code; anything else is logged to the console and returned as a generic `500 INTERNAL_SERVER_ERROR`.

**`middlewares/req.middleware.ts`** — logs every request at `debug` on arrival, then logs method/path/status/duration at `info` once the response's `"finish"` event fires.

**`middlewares/user-context.middleware.ts`**:

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

Two problems: this is never imported or mounted anywhere in `server.ts` or either route file, so it currently protects nothing; and `req.user = {...}` doesn't type-check, because admin-service has no `declare global { namespace Express { interface Request { user?: ... } } }` augmentation (unlike api-gateway's `auth.middleware.ts`, which has one). `npx tsc --noEmit` reports this as a real error today.

**`utils/api-response.ts`** — `SuccessResponse`/`ErrorResponse` helpers that wrap `res.json()` in a consistent `{ success, message, data? }` shape.

**`utils/asyncHandler.ts`** — wraps an async controller so a rejected promise is forwarded to `next()` instead of crashing the process; only works if the wrapped function's own promise actually rejects (see Lifecycle Case C for the case where it doesn't get the chance to).

**`utils/error.ts`** — `AppError` base class plus `BadRequestError` (400), `UnauthorizedError` (401), `ForbiddenError` (403), `NotFoundError` (404), `ConflictError` (409), `TooManyRequestsError` (429), `InternalServerError` (500) — see the [Error Codes Reference](#error-codes-reference) for which of these are actually thrown anywhere.

**`utils/zod.formatter.ts`** — takes a `ZodError` and returns just the first issue's message as a plain string (not the full list of validation errors).

---

## Environment Variables

Variables actually read via `config.*` somewhere in `src/` (even though `config/index.ts` itself doesn't exist — see Known Issue #1):

```bash
PORT=                  # config/index.ts would need to provide this — no fallback visible in index.ts itself
DATABASE_URL=          # read by config/prisma.ts, passed to the pg adapter
KAFKA_BROKER=          # read by config/kafka.ts (falls back to "localhost:9093" if unset)
KAFKA_CLIENT_ID=       # read by config/kafka.ts
ALLOWED_ORIGINS=       # read by middlewares/cors.middleware.ts (comma-separated)
LOG_LEVEL=             # read by config/logger.ts
# SERVICE_NAME isn't an env var directly — config/logger.ts reads config.SERVICE_NAME,
# which (based on the pattern in every other service in this repo) would normally come
# from package.json's "name" field, not from .env
```

The actual `.env` file in this project also defines a long list of variables that nothing under `admin-service/src` reads: `JWT_ACCESS_SECRET`, `JWT_REFRESH_SECRET`, `ACCESS_TOKEN_EXP`, `ACCESS_TOKEN_EXP_SEC`, `REFRESH_TOKEN_EXP`, `REFRESH_TOKEN_EXP_SEC`, `OTP_HMAC_SECRET`, `OTP_MAX_VERIFY_ATTEMPTS`, `OTP_RATE_MAX_PER_HOUR`, `OTP_TTL`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `MONGODB_URI`, `INTERNAL_SERVICE_KEY`, `REDIS_URL`, `REDIS_USER_TTL`, `SENDGRID_API_KEY`, `MAIL_SEND`, `RESEND_API_KEY` — these look like they belong to an auth/OTP/notification service, not this one (see Known Issues).

---

## Error Codes Reference

| Class | Status | Thrown by, in this codebase |
|---|---|---|
| `BadRequestError` | 400 | `train.controller.ts` (empty seats — unreachable in practice) and `train.service.ts` (duplicate seat numbers) |
| `UnauthorizedError` | 401 | `user-context.middleware.ts` — but that middleware is never mounted, so this never actually fires |
| `ForbiddenError` | 403 | — (defined, not thrown anywhere) |
| `NotFoundError` | 404 | — (defined, not thrown anywhere — there's no 404 handler for unmatched routes either) |
| `ConflictError` | 409 | `station.service.ts` (duplicate code) and `train.service.ts` (duplicate train number) |
| `TooManyRequestsError` | 429 | — (defined, not thrown anywhere — no rate limiting in this service) |
| `InternalServerError` | 500 | — (defined, not thrown — `error.middleware.ts` builds its own 500 response inline instead, same pattern as the API Gateway) |

Validation failures from Zod (`zStation`/`zTrain`) don't go through this hierarchy at all — they're caught with `safeParse` and turned into a 400 via `ErrorResponse` directly in the controller, before any `AppError` would be thrown.

---

## Kafka Topics Reference

All topic names come from `shared/constants/kafka-topics.ts`, shared across every service in this repo.

| Topic | Published by | Wired up in this service? |
|---|---|---|
| `admin.station-created` | `stationService.createStation` | ✅ yes |
| `admin.train-created` | `trainService.createTrain` | ✅ yes |
| `admin.route-created` | — | ❌ `adminProducer.publishRouteCreated` exists but nothing calls it |
| `admin.schedule-created` | — | ❌ `adminProducer.publishScheduleCreated` exists but nothing calls it |
| `admin.schedule-cancelled` | — | ❌ `adminProducer.publishScheduleCancelled` exists but nothing calls it |
| `admin.train-updated`, `admin.station-updated` | — | ❌ defined in the shared constants file, no producer method for either exists here |

---

## Quick Start

```bash
cd admin-service
npm install
npx prisma generate     # regenerates src/generated/prisma from prisma/schema.prisma

# .env needs at least DATABASE_URL, KAFKA_BROKER, KAFKA_CLIENT_ID, ALLOWED_ORIGINS
npm run dev              # nodemon + ts-node, hot reload
```

**As checked in today, this fails immediately** — see [Known Issue #1](#known-issues--inconsistencies). `npx tsc --noEmit` from `admin-service/` will show the missing-module errors without even needing a running database or Kafka broker.

Once the missing `config/index.ts` (and, if still referenced, `config/db.ts`) are restored, the intended requests are:

```bash
curl -X POST http://localhost:<PORT>/stations/station \
  -H "Content-Type: application/json" \
  -d '{"name":"New Delhi","code":"ndls","city":"Delhi","state":"Delhi"}'

curl -X POST http://localhost:<PORT>/trains/train \
  -H "Content-Type: application/json" \
  -d '{"trainNumber":"12301","trainName":"Rajdhani Express","coachName":"AC","seats":[{"seatNumber":1,"seatType":"LOWER","price":1500}]}'
```

---

## Debugging Tips

- **`Cannot find module './config'` (or `'../config'`, `'.'`, `'./index'`) on startup or in `tsc`** → `src/config/index.ts` doesn't exist in this project yet; every config-dependent file (and `index.ts` itself) needs it. See Known Issue #1.
- **A duplicate-station request returns 200 `"OTP sent successfully"` instead of a 409** → `station.controller.ts` doesn't await the service call; check the server's own logs for an unhandled promise rejection mentioning `"Station already exists"` rather than trusting the HTTP response. See Lifecycle Case C.
- **A station's Kafka event never arrives even though the row exists in Postgres** → `stationService.createStation`'s publish isn't wrapped in a `.catch` (unlike the train service's); check for a Kafka connection error in the logs around the time of that request.
- **Nothing ever seems to require authentication** → `getUserContext` exists in `middlewares/user-context.middleware.ts` but isn't mounted in `server.ts` or either route file — there's no auth in this service today.
- **`tsc` complains about `req.user` not existing** → admin-service has no `Express.Request` type augmentation, unlike api-gateway's `auth.middleware.ts`. `user-context.middleware.ts`'s `req.user = {...}` assignment doesn't type-check as a result.
- **A request to `/trains/train` with duplicate seat numbers in the payload returns a 400** → that's `trainService.createTrain`'s own dedup check (`new Set(seatNumbers).size !== seatNumbers.length`), separate from anything Zod validates.

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, since these are informational (same approach as the API Gateway's and Notification Service's docs):

1. **The service doesn't compile.** `src/index.ts` imports `./config` and `./config/db`; `src/config/kafka.ts`, `src/config/logger.ts`, and `src/config/prisma.ts` each import a `config` object from `.`/`../config`/`./index` (all resolving to `src/config/index.ts`). None of these files exist anywhere in this project. Confirmed with `npx tsc --noEmit`, which reports five "Cannot find module" errors plus one unrelated type error (#3 below).
2. **Import/env-load ordering**: even once `config/index.ts` exists, `index.ts` imports it (and everything that reads `config.*` at module-eval time) before calling `dotenv.config()` on line 6. If the restored config object reads `process.env.X` directly at module load (as the equivalent files do in every other service in this repo), `.env`-only values would be `undefined` at that point.
3. **`req.user` doesn't type-check.** `middlewares/user-context.middleware.ts` assigns `req.user = {...}`, but nothing in this project declares the `Express.Request.user` augmentation that api-gateway's `auth.middleware.ts` declares for itself. `npx tsc --noEmit` reports this as `Property 'user' does not exist on type 'Request<...>'`.
4. **No authentication is wired up anywhere.** `getUserContext` (in `user-context.middleware.ts`) is fully implemented but never imported into `server.ts` or either route file. Anything that can reach this service's HTTP port can create stations and trains.
5. **`station.controller.ts`'s `createStation` never awaits (or returns) `stationService.createStation(...)`.** The 200 response is sent before the DB write / Kafka publish settle, and a rejection (e.g. `ConflictError` on a duplicate code) becomes an unhandled promise rejection instead of reaching `errorHandler`. See Lifecycle Case C.
6. **The station-creation success message is `"OTP sent successfully"`** — leftover text from a different (OTP/auth) flow this controller was evidently adapted from.
7. **Inconsistent Kafka failure handling**: `stationService.createStation` lets a publish failure throw (which, combined with #5, currently just becomes an unhandled rejection); `trainService.createTrain` catches and logs the same kind of failure instead. The same class of side effect is handled two different ways.
8. **`src/types/index.ts` defines `KnowledgeDoc` and `RAGResponse`**, referencing `mongoose` — a document-embedding/RAG shape with no relation to stations or trains. Nothing under `src/` imports either interface.
9. **Unrelated dependencies in `package.json`**: `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`, `mongoose`, `otp-generator`, `resend` are all listed, but nothing under `src/` (excluding the unused `types/index.ts` above) imports any of them. Together with #8, this looks like the project was scaffolded from a shared template without trimming unused pieces.
10. **`.env` defines variables nothing in this codebase reads**: `JWT_ACCESS_SECRET`, `JWT_REFRESH_SECRET`, `ACCESS_TOKEN_EXP(_SEC)`, `REFRESH_TOKEN_EXP(_SEC)`, `OTP_HMAC_SECRET`, `OTP_MAX_VERIFY_ATTEMPTS`, `OTP_RATE_MAX_PER_HOUR`, `OTP_TTL`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `MONGODB_URI`, `INTERNAL_SERVICE_KEY`, `REDIS_URL`, `REDIS_USER_TTL`, `SENDGRID_API_KEY`, `MAIL_SEND`, `RESEND_API_KEY` — these read like they belong to a user/auth/notification service rather than this one.
11. **`npm run seed` points at `src/services/seed.ts`**, which doesn't exist in this project — running that script fails. The same issue is already flagged in the API Gateway's and Notification Service's docs, likely from a shared `package.json` origin.
12. **Cross-service routing mismatch**: `api-gateway/src/routes/index.ts` registers `/admins/stations/station` and `/admins/trains/train` as `GET`, but this service only defines `POST` for both paths — requests routed through the gateway to either endpoint 404 today. (Also flagged in the API Gateway's own docs/review.)
13. **Three of five `AdminProducer` publish methods are dead code**: `publishRouteCreated`, `publishScheduleCreated`, and `publishScheduleCancelled` are fully implemented but never called — there's no route or schedule controller/service in this project yet, only station and train creation.
14. **`train.controller.ts`'s manual `if (seats.length === 0)` check** duplicates `zTrain`'s own `.min(1, ...)` on `seats` and is unreachable in practice. The file also imports `zStation`, which it never uses.
15. **No read/list/update/delete endpoints exist** for either stations or trains — only creation. There's no way to look up an existing station or train through this service's API.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
