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
   - [Train & Route — controller + service](#6-train--route--controller--service)
   - [Schedule — controller + service (unmounted)](#7-schedule--controller--service-unmounted)
   - [kafka/producer/admin.producer.ts — Event Publishing](#8-kafkaproduceradminproducerts--event-publishing)
   - [middlewares/ & utils/ — Cross-Cutting Helpers](#9-middlewares--utils--cross-cutting-helpers)
6. [Environment Variables](#environment-variables)
7. [Error Codes Reference](#error-codes-reference)
8. [Kafka Topics Reference](#kafka-topics-reference)
9. [Quick Start](#quick-start)
10. [Debugging Tips](#debugging-tips)
11. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **Admin Service** is the internal API for setting up the data the rest of IRCTC runs on — stations, trains, routes, and schedules. As it exists today:

- **Creates stations** (`POST /stations/station`) — name, code, city, optional state
- **Creates trains** (`POST /trains/train`) — train number, name, coach, and a full seat map in one call
- **Defines a train's route** (`POST /trains/route`) — an ordered list of stations with arrival/departure times and distances
- **Creates schedules** (`schedule.service.ts` / `schedule.controller.ts`) — a specific `departureDate` run of a train that already has a route, denormalized with train + seats + route into a single Kafka event — but this is currently **unreachable over HTTP**, see below
- Has one read endpoint, `getTrainById`, but it's routed and implemented in a way that makes it **always fail** — see below
- **Validates** every request body with Zod before touching the database
- **Persists** through Prisma into Postgres (`stations`, `trains`, `seats`, `routes`, `route_stations`, `schedules`)
- **Publishes Kafka events** so other services (inventory, search) can react — but only two of the five defined publish methods are ever actually reached (see [Kafka Topics Reference](#kafka-topics-reference))

There's no update or delete endpoint for any resource, and no working read endpoint either.

**Important:** as currently checked in, this service does not compile. `src/index.ts` and every file under `src/config/` (plus `middlewares/cors.middleware.ts`) import a `config` module (`./config`, `../config`, `.`, `./index` — all pointing at `src/config/index.ts`) that doesn't exist in this project, and `index.ts` also imports a `./config/db` that doesn't exist either. `middlewares/user-context.middleware.ts` also fails to type-check on its own (see [Known Issues #3](#known-issues--inconsistencies)). Running `npx tsc --noEmit` from `admin-service/` confirms this with the same seven errors documented below. Everything described in this doc is what the code is written to do — see [Known Issues](#known-issues--inconsistencies) for the specifics of why it can't do all of it yet.

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
│         POST /station                                           │
│   7. /trains    → train.routes.ts  → trainController            │
│         POST /train                                             │
│         POST /route                                             │
│         POST /route/:id  (broken — see Known Issue #13)         │
│   8. errorHandler (registered last)                             │
│                                                                 │
│  schedule.route.ts (POST /schedule → scheduleController) exists │
│  but is NEVER app.use()'d here — dead from the outside.          │
│  See Known Issue #16.                                           │
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
   │  (all written to by   │            │  admin.schedule-cancelled— │
   │  at least one path in │            │  (publish methods exist    │
   │  this service, though │            │  for all 5; only station-  │
   │  route/schedule paths │            │  created and train-created │
   │  have bugs — see       │            │  are ever actually called) │
   │  Request Lifecycle)   │            └──────────────┬──────────────┘
   └──────────────────────┘                            │ consumed by
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
│   ├── server.ts                         # Express app: middleware + route mounting (station + train only)
│   ├── config/
│   │   ├── kafka.ts                      # Kafka client + producer (connect/disconnect)
│   │   ├── logger.ts                     # Winston logger
│   │   └── prisma.ts                     # PrismaClient singleton (pg adapter)
│   │   # NOTE: index.ts and every file above import a "config" object from
│   │   # "./config" / "../config" / "." / "./index" — none of which resolve
│   │   # to a real file. There is no config/index.ts or config/db.ts here.
│   ├── controllers/
│   │   ├── station.controller.ts         # POST /stations/station
│   │   ├── train.controller.ts           # POST /trains/train, /trains/route, /trains/route/:id (broken)
│   │   └── schedule.controller.ts        # POST /schedule — defined but never mounted
│   ├── services/
│   │   ├── station.service.ts            # Station creation + Kafka publish
│   │   ├── train.service.ts              # Train+seats creation, route creation (has an inverted bug), getTrainById
│   │   └── schedule.service.ts           # Schedule creation + denormalized Kafka publish (unreachable via HTTP)
│   ├── routes/
│   │   ├── station.route.ts
│   │   ├── train.routes.ts
│   │   └── schedule.route.ts             # Defines POST /schedule but is never app.use()'d in server.ts
│   ├── kafka/producer/
│   │   └── admin.producer.ts             # publishStationCreated/TrainCreated/RouteCreated/ScheduleCreated/ScheduleCancelled
│   ├── middlewares/
│   │   ├── cors.middleware.ts
│   │   ├── error.middleware.ts
│   │   ├── req.middleware.ts
│   │   └── user-context.middleware.ts    # Defined, but never mounted anywhere
│   ├── types/
│   │   ├── zod.ts                        # zStation, zSeat, zTrain, zRouteStation, zRoute, zSchedule schemas
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

`tsconfig.json` sets `rootDir: ".."`, mirroring the other services in this repo — it points one level above `admin-service/` so the project can compile files it reaches via `../../shared/...`-style imports; `admin.producer.ts`'s import of `../../../../shared/constants/kafka-topics` is the one file under `src/` that actually uses this.

---

## Request Lifecycle

### Case A: `POST /trains/train` (happy path — a fully-correct flow in this service)

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

### Case B: `POST /trains/route` on a train that has no route yet (the common case — and it always fails)

```
1.  Client sends POST /trains/route with:
      { trainId, stations: [{ stationId, sequenceNumber, arrivalTime?,
        departureTime?, distanceFromOrigin? }, ...] } (>= 2 stations)
2.  train.routes.ts matches POST /route (mounted at /trains) → trainController.createRoute
3.  zRoute.safeParse validates the body
4.  trainService.createRoute({ trainId, stations }):
      a. prisma.train.findUnique({ id: trainId }) → found
      b. prisma.route.findUnique({ trainId }) → not found (this train has no route yet)
      c. BUG: the code reads `if (!existingRoute) throw new NotFoundError(
         "Route already existis for this train")` — i.e. it throws "already
         exists" precisely when no route exists. This is inverted from what
         was clearly intended.
5.  Response: 404 { success: false, error: "NOT_FOUND", message: "Route already
    existis for this train" } — for a train that has never had a route created.
```

There is, as written, no way to successfully create a *first* route for a train through this endpoint. (If a route already *did* exist, step 4c's condition would be false, and execution would fall through to `prisma.route.create`, which would then throw a raw Prisma unique-constraint error on `Route.trainId` instead of a clean `ConflictError` — see [Known Issue #14](#known-issues--inconsistencies).)

### Case C: `POST /stations/station` with an invalid body (validation failure)

```
1.  Client sends POST /stations/station with { name: "NY", code: "NYC", city: "New York" }
2.  stationController.createStation: zStation.safeParse fails — "Station name must be
    at least 4 characters" ("NY" is 2 characters)
3.  ErrorResponse(res, 400, { message: "Station name must be at least 4 characters" })
    is returned directly inside the asyncHandler callback — no error is thrown, so
    errorHandler never runs for this case
4.  Response: 400 { success: false, message: "Station name must be at least 4 characters" }
```

### Case D: `POST /stations/station` with a code that already exists (illustrates the missing `await`)

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

### Case E: `scheduleService.createSchedule` (hypothetical — no HTTP path reaches it today)

`schedule.route.ts` defines `POST /schedule → scheduleController.createSchedule`, but `server.ts` never imports or `app.use()`s that router — so this flow can currently only be exercised by importing `scheduleService` directly (e.g. from a test), never by an actual HTTP request. Documented here as it's written to work, once/if it gets mounted:

```
1.  Caller invokes scheduleService.createSchedule({ trainId, departureDate })
2.  prisma.train.findUnique({ id: trainId }, include seats + route + routeStations + station)
3.  Throws ConflictError if the train doesn't exist, BadRequestError if it has no seats
    or no route yet (a schedule needs both)
4.  departureDate is re-parsed with `new Date(...)` and checked for NaN
5.  prisma.schedule.findUnique({ trainId_departureDate }) → checked for a duplicate;
    ConflictError if one already exists for that exact date
6.  prisma.schedule.create({ trainId, departureDate }) inserts the schedule row
7.  A denormalized eventPayload is assembled: schedule id/status, train's id/number/
    name/coachName/totalSeats, every seat, and every route stop (with full station
    details) — all inlined so downstream consumers don't need a follow-up call
8.  adminProducer.publishScheduleCreated(eventPayload) → Kafka topic
    admin.schedule-created, keyed by schedule-<scheduleId>
9.  createSchedule returns "" (an empty string, not the created schedule or payload)
10. scheduleController.createSchedule responds 200 with message "Train created
    successfully" — copy-pasted from train.controller.ts, describes the wrong resource
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

This is the entire app definition: two route groups (`/stations`, `/trains`), a root `GET /` that still identifies itself as "user-service" in its response text, a `GET /health`, and the error handler registered last. **`schedule.route.ts` is not imported or mounted here at all** — see [Known Issue #16](#known-issues--inconsistencies). No auth middleware is applied anywhere in this chain.

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

**`types/zod.ts`** defines every schema a request goes through:

- `zStation` — `name` (4–40 chars), `code` (2–10 chars, trimmed and uppercased by the schema itself), `city` (2–40 chars), optional `state` (≤40 chars)
- `zSeat` — `seatNumber` (positive int), `seatType` (`LOWER | MIDDLE | UPPER | SIDE_LOWER | SIDE_UPPER`), `price` (positive number)
- `zTrain` — `trainNumber` (1–10 chars), `trainName` (4–40 chars), optional `coachName` (≤20 chars), `seats` (array of `zSeat`, minimum 1)
- `zRouteStation` — `stationId` (UUID), `sequenceNumber` (positive int), optional `arrivalTime`/`departureTime` (`HH:mm` regex), optional `distanceFromOrigin` (non-negative number)
- `zRoute` — `trainId` (UUID), `stations` (array of `zRouteStation`, minimum 2)
- `zSchedule` — `trainId` (UUID), `departureDate` (coerced to a `Date`), optional `status` (`ACTIVE | CANCELLED`)

`StationBodyType`, `SeatBodyType`, `TrainBodyType`, `RouteStationBodyType`, `RouteBodyType`, and `ScheduleBodyType` are the corresponding `z.infer<...>` types used throughout the controllers and services.

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

Nothing under `src/` imports either of these interfaces. They describe a document-embedding / retrieval-augmented-generation feature that has nothing to do with stations, trains, routes, or schedules — see Known Issues for how this lines up with other leftovers in this project.

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

    // Note: not awaited. zStation's `code` field already applies
    // `.toUpperCase()`, so this second `.toUpperCase()` is a no-op on an
    // already-uppercased value. More importantly, since the returned
    // promise is neither awaited nor returned, the response below fires
    // before the DB write / Kafka publish settle, and a rejection here
    // (e.g. ConflictError on a duplicate code) becomes an unhandled
    // promise rejection instead of reaching errorHandler.
    const station = stationService.createStation({
      code: code.toUpperCase(),
      name,
      city,
      state,
    });

    // Message text is a holdover from a different (OTP-based) flow.
    res.status(200).json({ success: true, message: "OTP sent successfully" });
  },
);
```

The bigger issue is that `stationService.createStation(...)`'s returned promise is neither awaited nor returned — see [Lifecycle Case D](#case-d-post-stationsstation-with-a-code-that-already-exists-illustrates-the-missing-await) for exactly what that causes.

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
  // Unlike trainService.createTrain, this publish isn't wrapped in a .catch —
  // a Kafka failure here throws and would normally turn an already-committed
  // station creation into a 500 response from the controller. In practice
  // it can't even do that today: station.controller.ts's createStation
  // never awaits this function, so the rejection instead becomes an
  // unhandled promise rejection.
  await adminProducer.publishStationCreated(createdStation);
  return createdStation;
};
```

Unlike `trainService.createTrain` below, the Kafka publish here isn't wrapped in a `.catch` — a publish failure throws and would normally turn an already-committed station creation into a 500 from the controller. In practice it can't even do that today, because the controller never awaits this function in the first place.

---

### 6. Train & Route — controller + service

`controllers/train.controller.ts` now defines three handlers: `createTrain`, `createRoute`, and `getTrainById`.

```typescript
const createTrain = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zTrain.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, { message: formatZodError(result.error) });
    }

    const { trainName, trainNumber, coachName, seats } = result.data;
    // Redundant with zTrain's own `.min(1, ...)` on `seats`, kept as a defensive check
    if (seats.length === 0) {
      throw new BadRequestError("Atleast one seat must be defined");
    }

    await trainService.createTrain({ trainName, trainNumber, coachName, seats });

    res.status(200).json({ success: true, message: "Train created successfully" });
  },
);
```

This one awaits the service call correctly, so a thrown `ConflictError`/`BadRequestError` reaches `asyncHandler`'s `.catch(next)` and, from there, `errorHandler`, as expected.

```typescript
const createRoute = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zRoute.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, { message: formatZodError(result.error) });
    }

    const { stations, trainId } = result.data;
    // Redundant with zRoute's own `.min(2, ...)` on `stations`, kept as a defensive check
    if (stations.length === 0) {
      throw new BadRequestError("A route must have at least 2 stations");
    }

    await trainService.createRoute({ stations, trainId });

    res.status(200).json({ success: true, message: "Route created successfully" });
  },
);
```

```typescript
const getTrainById = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const { trainId } = req.params;
    if (!trainId) {
      throw new BadRequestError("Train Id is missing");
    }
    const train = await trainService.getTrainById(trainId as string);
    return res.status(200).json({ success: true, data: train });
  },
);
```

`getTrainById` reads `req.params.trainId`, but the route it's mounted on (`POST /trains/route/:id`, see below) names the param `:id`, not `:trainId` — so `trainId` is always `undefined` and every call to this endpoint 400s with "Train Id is missing" before `trainService.getTrainById` is ever reached. It's also mounted as `POST` despite being a pure read. See [Known Issue #13](#known-issues--inconsistencies).

`routes/train.routes.ts`:

```typescript
router.post("/train", trainController.createTrain); // POST /trains/train — create train + seats
router.post("/route", trainController.createRoute); // POST /trains/route — define a train's route
router.post("/route/:id", trainController.getTrainById); // broken — see above
```

`services/train.service.ts`:

```typescript
const createTrain = async ({ trainName, trainNumber, coachName, seats }: TrainBodyType) => {
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
    logger.error("Failed to publish train created event", { error: err.message });
  });

  return train;
};
```

The nested `seats: { create: [...] }` write inserts the train row and every seat row in one transaction — `totalSeats` is just `seats.length`, not a separately-counted value. The `.catch` on the Kafka publish means a broker outage is logged but never fails the request — the opposite of what happens in `stationService.createStation`.

```typescript
const createRoute = async ({ trainId, stations }: RouteBodyType) => {
  const existingTrain = await prisma.train.findUnique({ where: { id: trainId } });
  if (!existingTrain) {
    throw new NotFoundError("Train Not found");
  }
  const existingRoute = await prisma.route.findUnique({ where: { trainId } });
  if (!existingRoute) {
    throw new NotFoundError("Route already existis for this train");
  }
  const stationIds = stations.map((station) => station.stationId);
  const existingStations = await prisma.station.findMany({ where: { id: { in: stationIds } } });
  if (existingStations.length !== stationIds.length) {
    throw new BadRequestError("One or more station Ids are invalid");
  }
  const sorted = [...stations].sort((a, b) => a.sequenceNumber - b.sequenceNumber);
  for (let i = 0; i < sorted.length; i++) {
    if (sorted[i].sequenceNumber !== i + 1) {
      throw new BadRequestError("Sequence Numbers must be continous starting free");
    }
  }
  const route = await prisma.route.create({
    data: {
      trainId,
      routeStations: {
        create: stations.map((s) => ({
          stationId: s.stationId,
          sequenceNumber: s.sequenceNumber,
          arrivalTime: s.arrivalTime || null,
          departureTime: s.departureTime || null,
          distanceFromOrigin: s.distanceFromOrigin || 0,
        })),
      },
    },
    include: {
      routeStations: { include: { station: true }, orderBy: { sequenceNumber: "asc" } },
    },
  });
  // Commented out in the actual file — a ROUTE_CREATED publish that was
  // started but never finished (and wouldn't type-check as written, since
  // adminProducer.publishRouteCreated expects a plain Prisma Route, not a
  // `{ ...route, train }` shape):
  //
  // const trainWithSeats = await prisma.train.findUnique({ ... });
  // await adminProducer.publishRouteCreated({ ...route, train: trainWithSeats });
  return route;
};
```

The `if (!existingRoute) throw new NotFoundError("Route already existis for this train")` check is inverted — see [Lifecycle Case B](#case-b-post-trainsroute-on-a-train-that-has-no-route-yet-the-common-case--and-it-always-fails) for the exact failure this causes, and [Known Issue #14](#known-issues--inconsistencies).

```typescript
const getTrainById = async (id: string) => {
  const train = await prisma.train.findUnique({
    where: { id },
    include: {
      seats: { orderBy: { seatNumber: "asc" } },
      route: {
        include: {
          routeStations: { include: { station: true }, orderBy: { sequenceNumber: "asc" } },
        },
      },
    },
  });
  if (!train) throw new NotFoundError("Train not found");
  return train;
};
```

This function itself is correct — it's only unreachable because of the routing bug in `getTrainById`'s controller/route pairing described above.

---

### 7. Schedule — controller + service (unmounted)

`controllers/schedule.controller.ts`:

```typescript
const createSchedule = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zSchedule.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, { message: formatZodError(result.error) });
    }

    const { trainId, departureDate } = result.data;
    await scheduleService.createSchedule({ trainId, departureDate });
    return res.status(200).json({ success: true, message: "Train created successfully" });
  },
);
```

The success message ("Train created successfully") is a copy-paste leftover from `train.controller.ts` — it names the wrong resource. The file also imports `zRoute` and `zTrain` from `../types/zod`, neither of which it uses.

`routes/schedule.route.ts`:

```typescript
router.post("/schedule", scheduleController.createSchedule);
```

This defines `POST /schedule`, but **`server.ts` never imports or mounts this router** — unlike `station.route.ts` and `train.routes.ts`, there's no `app.use("/something", scheduleRoutes)` anywhere. The entire schedule-creation feature is unreachable from outside the process. See [Known Issue #16](#known-issues--inconsistencies).

`services/schedule.service.ts`:

```typescript
const createSchedule = async ({ trainId, departureDate }: ScheduleBodyType) => {
  const existingTrain = await prisma.train.findUnique({
    where: { id: trainId },
    include: {
      seats: true,
      route: { include: { routeStations: { include: { station: true } } } },
    },
  });
  if (!existingTrain) {
    throw new ConflictError("Train not found");
  }
  if (existingTrain.seats.length === 0) {
    throw new BadRequestError("Train not found");
  }
  if (!existingTrain.route) {
    throw new BadRequestError("Train has no route defined. Create a route first ");
  }
  const parsedDate = new Date(departureDate);
  if (isNaN(parsedDate.getTime())) {
    throw new BadRequestError("Invalid departure date format ");
  }
  const existingSchedule = await prisma.schedule.findUnique({
    where: { trainId_departureDate: { trainId, departureDate: parsedDate } },
  });
  if (existingSchedule) {
    throw new ConflictError("Schedule already exists for this train on this date ");
  }
  const schedule = await prisma.schedule.create({ data: { trainId, departureDate: parsedDate } });
  const eventPayload = {
    scheduleId: schedule.id,
    trainId: existingTrain.id,
    trainNumber: existingTrain.trainNumber,
    trainName: existingTrain.trainName,
    coachName: existingTrain.coachName,
    totalSeats: existingTrain.totalSeats,
    departureDate: departureDate,
    status: schedule.status,
    seats: existingTrain.seats.map((s) => ({
      seatId: s.id, seatNumber: s.seatNumber, seatType: s.seatType, price: s.price,
    })),
    route: existingTrain.route.routeStations.map((rs) => ({
      stationId: rs.station.id, stationName: rs.station.name, stationCode: rs.station.code,
      city: rs.station.city, sequenceNumber: rs.sequenceNumber, arrivalTime: rs.arrivalTime,
      departureTime: rs.departureTime, distanceFromOrigin: rs.distanceFromOrigin,
    })),
  };

  await adminProducer.publishScheduleCreated(eventPayload);
  return "";
};
```

Note `existingTrain.seats.length === 0` throws `BadRequestError("Train not found")` — a copy-pasted message; the actual condition being checked is "train has no seats", not "train doesn't exist" (that case is the `ConflictError` right above it). `createSchedule` returns an empty string `""`, not the created schedule or the event payload — a caller has no way to get the new schedule's id back from this function's return value alone (it would need to read it off the Kafka event or query separately).

`eventPayload`'s shape is defined by the `ScheduleCreatedPayload` interface in `admin.producer.ts` (see next section) — it's deliberately denormalized (train + seats + route all inlined) so inventory-service and search-service don't need to call back into admin-service to react to a new schedule.

---

### 8. `kafka/producer/admin.producer.ts` — Event Publishing

A thin class wrapping the shared Kafka producer with domain-specific publish methods:

```typescript
export interface ScheduleCreatedPayload {
  scheduleId: string;
  trainId: string;
  trainNumber: string;
  trainName: string;
  coachName: string;
  totalSeats: number;
  departureDate: Date;
  status: ScheduleStatus;
  seats: { seatId: string; seatNumber: number; seatType: SeatType; price: number }[];
  route: {
    stationId: string; stationName: string; stationCode: string; city: string;
    sequenceNumber: number; arrivalTime: string | null; departureTime: string | null;
    distanceFromOrigin: number;
  }[];
}

class AdminProducer {
  private isInitialized: boolean;

  private async initialize(): Promise<void> { /* connects lazily, once */ }

  private async sendMessage<T>(topic: string, key: string | undefined, value: T) {
    // ...connects lazily, sends via producer.send(), logs partition/offset on
    // success, logs and re-throws on failure
  }

  async publishStationCreated(station: Station) { /* keyed by station-<id> */ }
  async publishTrainCreated(trainData: Train) { /* keyed by train-<id> */ }
  async publishRouteCreated(routeData: Route) { /* keyed by route-<id> */ }
  async publishScheduleCreated(scheduleData: ScheduleCreatedPayload) { /* keyed by schedule-<scheduleId> */ }
  async publishScheduleCancelled(schedule: Schedule) { /* keyed by schedule-<id> */ }
}

export default new AdminProducer();
```

The producer connects lazily on first use (`initialize()`), not at import time. Every `key` is derived from the entity's own id, so all events about the same entity land on the same Kafka partition and stay in order relative to each other.

`publishScheduleCreated` takes the `ScheduleCreatedPayload` shape above — not a raw Prisma `Schedule` row — because `schedule.service.ts` needs to ship the denormalized train/seats/route data alongside the schedule itself (this method's signature was corrected in this pass; it previously required a plain `Schedule`, which didn't match what the service actually builds).

Of the five publish methods, only `publishStationCreated` and `publishTrainCreated` are ever actually reached today:
- `publishRouteCreated` is fully implemented, but its only call site (in `train.service.ts`'s `createRoute`) is commented out.
- `publishScheduleCreated` is called correctly from `schedule.service.ts`, but that service is itself unreachable via HTTP (`schedule.route.ts` is never mounted — see Known Issue #16).
- `publishScheduleCancelled` has no caller anywhere in this codebase.

---

### 9. `middlewares/` & `utils/` — Cross-Cutting Helpers

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

Two problems: this is never imported or mounted anywhere in `server.ts` or any route file, so it currently protects nothing; and `req.user = {...}` doesn't type-check, because admin-service has no `declare global { namespace Express { interface Request { user?: ... } } }` augmentation (unlike api-gateway's `auth.middleware.ts`, which has one). `npx tsc --noEmit` reports this as a real error today.

**`utils/api-response.ts`** — `SuccessResponse`/`ErrorResponse` helpers that wrap `res.json()` in a consistent `{ success, message, data? }` shape.

**`utils/asyncHandler.ts`** — wraps an async controller so a rejected promise is forwarded to `next()` instead of crashing the process; only works if the wrapped function's own promise actually rejects (see Lifecycle Case D for the case where it doesn't get the chance to).

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
| `BadRequestError` | 400 | `train.controller.ts` (empty seats/stations — unreachable in practice), `train.service.ts` (duplicate seat numbers; invalid station ids; non-contiguous sequence numbers), and `schedule.service.ts` (no seats, no route, bad date) |
| `UnauthorizedError` | 401 | `user-context.middleware.ts` — but that middleware is never mounted, so this never actually fires |
| `ForbiddenError` | 403 | — (defined, not thrown anywhere) |
| `NotFoundError` | 404 | `train.service.ts` — correctly in `getTrainById` (train doesn't exist), and *incorrectly* in `createRoute`'s inverted "already exists" check (see Known Issue #14) |
| `ConflictError` | 409 | `station.service.ts` (duplicate code), `train.service.ts` (duplicate train number), and `schedule.service.ts` (train not found; duplicate schedule for the same date) |
| `TooManyRequestsError` | 429 | — (defined, not thrown anywhere — no rate limiting in this service) |
| `InternalServerError` | 500 | — (defined, not thrown — `error.middleware.ts` builds its own 500 response inline instead, same pattern as the API Gateway) |

Validation failures from Zod (`zStation`/`zTrain`/`zRoute`/`zSchedule`) don't go through this hierarchy at all — they're caught with `safeParse` and turned into a 400 via `ErrorResponse` directly in the controller, before any `AppError` would be thrown.

---

## Kafka Topics Reference

All topic names come from `shared/constants/kafka-topics.ts`, shared across every service in this repo.

| Topic | Published by | Wired up in this service? |
|---|---|---|
| `admin.station-created` | `stationService.createStation` | ✅ yes |
| `admin.train-created` | `trainService.createTrain` | ✅ yes |
| `admin.route-created` | — | ❌ `adminProducer.publishRouteCreated` exists and is implemented; its only call site in `trainService.createRoute` is commented out |
| `admin.schedule-created` | `scheduleService.createSchedule` | ⚠️ the publish call itself is correct, but nothing can reach `createSchedule` via HTTP — `schedule.route.ts` is never mounted in `server.ts` |
| `admin.schedule-cancelled` | — | ❌ `adminProducer.publishScheduleCancelled` exists but nothing calls it |
| `admin.train-updated`, `admin.station-updated`, `admin.route-updated` | — | ❌ defined in the shared constants file, no producer method for any of them exists here |

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

# This will 404 with "Route already existis for this train" for any train
# that doesn't already have one — see Known Issue #14.
curl -X POST http://localhost:<PORT>/trains/route \
  -H "Content-Type: application/json" \
  -d '{"trainId":"<train-uuid>","stations":[{"stationId":"<station-uuid-1>","sequenceNumber":1,"departureTime":"06:10"},{"stationId":"<station-uuid-2>","sequenceNumber":2,"arrivalTime":"10:00"}]}'

# There is currently no route to this endpoint at all — schedule.route.ts
# is never mounted in server.ts (Known Issue #16). Shown for reference only:
curl -X POST http://localhost:<PORT>/schedule \
  -H "Content-Type: application/json" \
  -d '{"trainId":"<train-uuid>","departureDate":"2026-08-01"}'
```

---

## Debugging Tips

- **`Cannot find module './config'` (or `'../config'`, `'.'`, `'./index'`) on startup or in `tsc`** → `src/config/index.ts` doesn't exist in this project yet; every config-dependent file (and `index.ts` itself) needs it. See Known Issue #1.
- **A duplicate-station request returns 200 `"OTP sent successfully"` instead of a 409** → `station.controller.ts` doesn't await the service call; check the server's own logs for an unhandled promise rejection mentioning `"Station already exists"` rather than trusting the HTTP response. See Lifecycle Case D.
- **A station's Kafka event never arrives even though the row exists in Postgres** → `stationService.createStation`'s publish isn't wrapped in a `.catch` (unlike the train service's); check for a Kafka connection error in the logs around the time of that request.
- **`POST /trains/route` always 404s with `"Route already existis for this train"`, even for a brand-new train** → `trainService.createRoute`'s existing-route check is inverted; see Lifecycle Case B and Known Issue #14. This isn't a data problem — it fails the same way for every train that doesn't already have a route.
- **`POST /trains/route/:id` always 400s with `"Train Id is missing"`** → the route param is `:id`, but `getTrainById`'s controller reads `req.params.trainId`; it's also mounted as `POST` instead of `GET`. See Known Issue #13.
- **`POST /schedule` (or any path you'd expect for schedule creation) 404s no matter what** → `schedule.route.ts` is never imported/mounted in `server.ts`. The feature is fully implemented in `schedule.controller.ts`/`schedule.service.ts` but unreachable from outside the process. See Known Issue #16.
- **Nothing ever seems to require authentication** → `getUserContext` exists in `middlewares/user-context.middleware.ts` but isn't mounted in `server.ts` or any route file — there's no auth in this service today.
- **`tsc` complains about `req.user` not existing** → admin-service has no `Express.Request` type augmentation, unlike api-gateway's `auth.middleware.ts`. `user-context.middleware.ts`'s `req.user = {...}` assignment doesn't type-check as a result.
- **A request to `/trains/train` with duplicate seat numbers in the payload returns a 400** → that's `trainService.createTrain`'s own dedup check (`new Set(seatNumbers).size !== seatNumbers.length`), separate from anything Zod validates.

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, since these are informational (same approach as the API Gateway's and Notification Service's docs):

1. **The service doesn't compile.** `src/index.ts` imports `./config` and `./config/db`; `src/config/kafka.ts`, `src/config/logger.ts`, `src/config/prisma.ts`, and `src/middlewares/cors.middleware.ts` each import a `config` object from `.`/`../config`/`./index` (all resolving to `src/config/index.ts`). None of these files exist anywhere in this project. Confirmed with `npx tsc --noEmit`, which reports six "Cannot find module" errors plus one unrelated type error (#3 below).
2. **Import/env-load ordering**: even once `config/index.ts` exists, `index.ts` imports it (and everything that reads `config.*` at module-eval time) before calling `dotenv.config()` on line 6. If the restored config object reads `process.env.X` directly at module load (as the equivalent files do in every other service in this repo), `.env`-only values would be `undefined` at that point.
3. **`req.user` doesn't type-check.** `middlewares/user-context.middleware.ts` assigns `req.user = {...}`, but nothing in this project declares the `Express.Request.user` augmentation that api-gateway's `auth.middleware.ts` declares for itself. `npx tsc --noEmit` reports this as `Property 'user' does not exist on type 'Request<...>'`.
4. **No authentication is wired up anywhere.** `getUserContext` (in `user-context.middleware.ts`) is fully implemented but never imported into `server.ts` or any route file. Anything that can reach this service's HTTP port can create stations, trains, and routes.
5. **`station.controller.ts`'s `createStation` never awaits (or returns) `stationService.createStation(...)`.** The 200 response is sent before the DB write / Kafka publish settle, and a rejection (e.g. `ConflictError` on a duplicate code) becomes an unhandled promise rejection instead of reaching `errorHandler`. See Lifecycle Case D.
6. **The station-creation success message is `"OTP sent successfully"`** — leftover text from a different (OTP/auth) flow this controller was evidently adapted from.
7. **Inconsistent Kafka failure handling**: `stationService.createStation` lets a publish failure throw (which, combined with #5, currently just becomes an unhandled rejection); `trainService.createTrain` catches and logs the same kind of failure instead. The same class of side effect is handled two different ways.
8. **`src/types/index.ts` defines `KnowledgeDoc` and `RAGResponse`**, referencing `mongoose` — a document-embedding/RAG shape with no relation to stations, trains, routes, or schedules. Nothing under `src/` imports either interface.
9. **Unrelated dependencies in `package.json`**: `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`, `mongoose`, `otp-generator`, `resend` are all listed, but nothing under `src/` (excluding the unused `types/index.ts` above) imports any of them. Together with #8, this looks like the project was scaffolded from a shared template without trimming unused pieces.
10. **`.env` defines variables nothing in this codebase reads**: `JWT_ACCESS_SECRET`, `JWT_REFRESH_SECRET`, `ACCESS_TOKEN_EXP(_SEC)`, `REFRESH_TOKEN_EXP(_SEC)`, `OTP_HMAC_SECRET`, `OTP_MAX_VERIFY_ATTEMPTS`, `OTP_RATE_MAX_PER_HOUR`, `OTP_TTL`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `MONGODB_URI`, `INTERNAL_SERVICE_KEY`, `REDIS_URL`, `REDIS_USER_TTL`, `SENDGRID_API_KEY`, `MAIL_SEND`, `RESEND_API_KEY` — these read like they belong to a user/auth/notification service rather than this one.
11. **`npm run seed` points at `src/services/seed.ts`**, which doesn't exist in this project — running that script fails. The same issue is already flagged in the API Gateway's and Notification Service's docs, likely from a shared `package.json` origin.
12. **Cross-service routing mismatch**: `api-gateway/src/routes/index.ts` registers `/admins/stations/station` and `/admins/trains/train` as `GET`, but this service only defines `POST` for both paths — requests routed through the gateway to either endpoint 404 today. (Also flagged in the API Gateway's own docs/review.)
13. **`trainController.getTrainById` is unreachable as routed.** It's mounted at `POST /trains/route/:id` (wrong verb for a read, and a path that reads like "get a route" rather than "get a train"). Worse, the controller destructures `req.params.trainId`, but the route param is named `:id` — so `trainId` is always `undefined` and the handler always 400s with "Train Id is missing" before `trainService.getTrainById` runs.
14. **`trainService.createRoute`'s existing-route check is inverted.** `if (!existingRoute) throw new NotFoundError("Route already existis for this train")` throws "already exists" exactly when no route exists yet, meaning no train can ever get its first route created through this endpoint. If a route *did* already exist, execution instead falls through to `prisma.route.create`, which would throw a raw Prisma unique-constraint error (`Route.trainId` is `@unique`) rather than a clean `ConflictError`. See Lifecycle Case B.
15. **`trainService.createRoute`'s planned `publishRouteCreated` call is commented out**, and as written wouldn't type-check if uncommented — it builds a `{ ...route, train: trainWithSeats }` object, but `adminProducer.publishRouteCreated` expects a plain Prisma `Route`.
16. **`schedule.route.ts` is never mounted.** `server.ts` only imports and `app.use()`s `station.route.ts` and `train.routes.ts` — there is no `app.use(..., scheduleRoutes)` anywhere. The entire schedule-creation feature (`schedule.controller.ts`, `schedule.service.ts`, and the Kafka publish it triggers) is fully implemented but unreachable via HTTP.
17. **`schedule.controller.ts` imports `zRoute` and `zTrain` from `../types/zod` but uses neither** — likely copied from `train.controller.ts` as a starting point and not trimmed.
18. **`scheduleController.createSchedule`'s success message is `"Train created successfully"`** — copy-pasted from `train.controller.ts`, describes the wrong resource.
19. **`scheduleService.createSchedule` returns `""`** (an empty string) rather than the created schedule or the event payload it just built and published — a caller has no way to get the new schedule back from the return value.
20. **`scheduleService.createSchedule`'s "train has no seats" check throws a copy-pasted message**: `if (existingTrain.seats.length === 0) throw new BadRequestError("Train not found")` — the message says "not found" for a train that clearly *was* found (it's already been fetched); the actual problem is that it has zero seats.
21. **Minor message typos, left as-is**: `train.service.ts`'s `createRoute` says `"Sequence Numbers must be continous starting free"` (should read something like "starting from 1"), and its inverted-check message itself has a typo, `"existis"`.
22. **No read/list/update/delete endpoints exist** for stations, and none work for trains (`getTrainById` is broken per #13) — only creation (and, for routes, a creation endpoint that always fails per #14). There's no way to actually look up an existing station, train, route, or schedule through this service's API today.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
