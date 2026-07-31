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
   - [config/ — Env, Kafka, Logger, Prisma](#3-config--env-kafka-logger-prisma)
   - [types/ — Validation Schemas & Express Augmentation](#4-types--validation-schemas--express-augmentation)
   - [Station Creation — controller + service](#5-station-creation--controller--service)
   - [Train & Route — controller + service](#6-train--route--controller--service)
   - [Schedule — controller + service](#7-schedule--controller--service)
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
- **Resolves a station internally** (`GET /stations/station/internal/:stationId`) — new since booking-service was ported, behind a shared-secret `internalAuth` middleware (the same pattern inventory-service and user-service already use), so booking-service can attach a station's name to a booking-confirmed email without a JWT
- **Creates trains** (`POST /trains/train`) — train number, name, coach, and a full seat map in one call
- **Defines a train's route** (`POST /trains/route`) — an ordered list of stations with arrival/departure times and distances. This now correctly succeeds for a brand-new train (a previously-inverted existence check is fixed — see [Request Lifecycle](#request-lifecycle))
- **Creates schedules** (`POST /schedules/schedule`) — a specific `departureDate` run of a train that already has a route, denormalized with train + seats + route into a single Kafka event. This router is now mounted in `server.ts` and reachable over HTTP (it previously wasn't)
- **Reads a train by id** (`GET /trains/train/:trainId`) — now correctly routed and reachable (it previously always 400'd — see below)
- **Validates** every request body with Zod before touching the database
- **Persists** through Prisma into Postgres (`stations`, `trains`, `seats`, `routes`, `route_stations`, `schedules`)
- **Publishes Kafka events** so other services (inventory, search) can react — all four creation events (`admin.station-created`, `admin.train-created`, `admin.route-created`, `admin.schedule-created`) now actually fire; `admin.schedule-cancelled` still never fires, because there's no cancel-schedule feature built (see [Kafka Topics Reference](#kafka-topics-reference))
- **Requires a user context on every route** — `getUserContext` middleware (reading an `x-user-id` header) is now mounted on all station, train, and schedule routes. This service was previously running with zero auth wired up anywhere. The one exception is the new internal station-lookup route, which is behind `internalAuth` (a shared secret) instead — it's meant for another backend service to call, not a request proxied from the gateway.

There's still no update or delete endpoint for any resource, and no read/list endpoint for stations, routes, or schedules — `getTrainById` is the only working read path in the service.

**Important:** as of this pass, the service type-checks cleanly. `npx tsc --noEmit` from `admin-service/` reports zero errors — the `config` module that used to be missing now exists (`src/config/index.ts`), the dead `./config/db` import is gone, and `middlewares/user-context.middleware.ts`'s `req.user` assignment now type-checks against a new `types/express.d.ts` augmentation. That said, **this has only been verified statically** — no Postgres instance or Kafka broker was reachable in the environment this fix pass ran in, so none of the flows described below have been exercised against a live database or broker. Treat "works when called" claims in this document as "the logic reads correctly and the types check," not as confirmed runtime behavior. See [Known Issues](#known-issues--inconsistencies) for what's still actually broken or missing.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                        API GATEWAY (:4000)                     │
│  Proxies (per api-gateway/src/routes/index.ts):                │
│   GET /admins/stations/station  → adminServiceProxy            │
│   GET /admins/trains/train      → adminServiceProxy            │
│  (both still registered as GET — admin-service only defines    │
│  POST for these paths, so requests routed through the gateway  │
│  to either endpoint still 404 today — unchanged by this pass,  │
│  see Known Issue #4. api-gateway's requireAuth does correctly  │
│  set x-user-id before proxying, though, so once/if the verb    │
│  mismatch is fixed, admin-service's auth requirement below     │
│  would already be satisfied for gateway-routed traffic)        │
└───────────────────────────┬───────────────────────────────────┘
                            │ HTTP → ADMIN_SERVICE_URL (default
                            │ http://localhost:4003, per the
                            │ gateway's own config)
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                       ADMIN SERVICE (:4003)                    │
│                                                                 │
│  server.ts (Express app):                                      │
│   1. helmet()          → security headers                      │
│   2. corsMiddleware    → origin whitelist (config.ALLOWED_...) │
│   3. reqLogger         → logs method/path/status/duration      │
│   4. cookieParser()                                             │
│   5. express.json()                                             │
│   6. /stations → station.route.ts → getUserContext → stationController│
│         POST /station                                           │
│   7. /trains    → train.routes.ts  → getUserContext → trainController │
│         POST /train                                              │
│         POST /route                                              │
│         GET  /train/:trainId  (now correctly wired)              │
│   8. /schedules → schedule.route.ts → getUserContext →           │
│         scheduleController                                       │
│         POST /schedule   (newly mounted — was dead code before)  │
│   9. errorHandler (registered last)                              │
│                                                                 │
│  getUserContext (middlewares/user-context.middleware.ts) is now │
│  mounted on every route above. It doesn't verify a JWT itself — │
│  it just reads x-user-id and 401s if missing, trusting whatever │
│  sits in front of it (api-gateway) to have done real auth.      │
└───────────┬─────────────────────────────────────┬─────────────┘
            │ Prisma (@prisma/adapter-pg)          │ kafkajs producer
            ▼                                     ▼
   ┌──────────────────────┐            ┌─────────────────────────────┐
   │  PostgreSQL           │            │  Kafka (localhost:9093)     │
   │  stations, trains,    │            │  admin.station-created  ✅  │
   │  seats, routes,       │            │  admin.train-created    ✅  │
   │  route_stations,      │            │  admin.route-created    ✅  │
   │  schedules            │            │  admin.schedule-created ✅  │
   │  (all written to by   │            │  admin.schedule-cancelled ❌│
   │  at least one path in │            │  (publish methods exist    │
   │  this service; every  │            │  for all 5; only schedule- │
   │  creation path now    │            │  cancelled has no caller)  │
   │  succeeds for its     │            └──────────────┬──────────────┘
   │  happy path)          │                           │ consumed by
   └──────────────────────┘                            ▼
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
│   ├── index.ts                          # Entry point — dotenv.config() now runs first, then starts
│   │                                      # the server and wires graceful shutdown (SIGTERM/SIGINT)
│   ├── server.ts                         # Express app: middleware + route mounting (station + train + schedule)
│   ├── config/
│   │   ├── index.ts                      # NEW this pass — the Config object every other file imports
│   │   ├── kafka.ts                      # Kafka client + producer (connect/disconnect)
│   │   ├── logger.ts                     # Winston logger
│   │   └── prisma.ts                     # PrismaClient singleton (pg adapter)
│   ├── controllers/
│   │   ├── station.controller.ts         # POST /stations/station, GET /stations/station/internal/:stationId
│   │   ├── train.controller.ts           # POST /trains/train, POST /trains/route, GET /trains/train/:trainId
│   │   └── schedule.controller.ts        # POST /schedules/schedule
│   ├── services/
│   │   ├── station.service.ts            # Station creation + Kafka publish; internal-lookup read
│   │   ├── train.service.ts              # Train+seats creation, route creation (existence-check bug fixed), getTrainById
│   │   └── schedule.service.ts           # Schedule creation + denormalized Kafka publish
│   ├── routes/
│   │   ├── station.route.ts              # getUserContext + createStation; internalAuth + getStationByIdInternal
│   │   ├── train.routes.ts               # getUserContext + trainController's 3 handlers
│   │   └── schedule.route.ts             # getUserContext + scheduleController.createSchedule — now mounted at /schedules in server.ts
│   ├── kafka/producer/
│   │   └── admin.producer.ts             # publishStationCreated/TrainCreated/RouteCreated/ScheduleCreated/ScheduleCancelled
│   ├── middlewares/
│   │   ├── cors.middleware.ts            # ALLOWED_ORIGINS split is now undefined-safe
│   │   ├── error.middleware.ts
│   │   ├── req.middleware.ts
│   │   ├── user-context.middleware.ts    # Now actually mounted on every route
│   │   └── internal-auth.middleware.ts   # NEW — shared-secret check, guards the internal station-lookup route
│   ├── types/
│   │   ├── zod.ts                        # zStation, zSeat, zTrain, zRouteStation, zRoute, zSchedule schemas
│   │   └── express.d.ts                  # NEW this pass — augments Express.Request with `user`
│   ├── utils/
│   │   ├── api-response.ts               # SuccessResponse/ErrorResponse helpers
│   │   ├── asyncHandler.ts               # Wraps async route handlers, forwards errors to next()
│   │   ├── error.ts                      # AppError + subclasses
│   │   └── zod.formatter.ts              # Formats a ZodError into one message string
│   └── generated/prisma/                 # Prisma client output (generated, not hand-written)
├── prisma/
│   └── schema.prisma                     # Station, Train, Seat, Route, RouteStation, Schedule
├── docs/                                  # This documentation
├── .env.example                          # NEW this pass — PORT=4003 and admin-appropriate values (no .env is committed)
├── package.json
├── tsconfig.json
└── prisma.config.ts
```

`src/types/index.ts` — the old `KnowledgeDoc`/`RAGResponse` (RAG/document-embedding) type file that imported `mongoose` for no reason and that nothing under `src/` ever imported — has been deleted entirely in this pass.

`tsconfig.json` sets `rootDir: ".."`, mirroring the other services in this repo — it points one level above `admin-service/` so the project can compile files it reaches via `../../shared/...`-style imports; `admin.producer.ts`'s import of `../../../../shared/constants/kafka-topics` is the one file under `src/` that actually uses this.

---

## Request Lifecycle

### Case A: `POST /trains/train` (happy path)

```
1.  Client sends POST /trains/train with:
      { trainNumber, trainName, coachName?, seats: [{ seatNumber, seatType, price }, ...] }
      and an x-user-id header (set by api-gateway's requireAuth after JWT
      verification, or supplied directly by a caller talking to admin-service
      without going through the gateway)
2.  server.ts middleware runs: helmet → corsMiddleware → reqLogger → cookieParser →
    express.json()
3.  train.routes.ts matches POST /train (mounted at /trains). getUserContext runs
    first: reads req.headers["x-user-id"], sets req.user = { id }, calls next()
4.  trainController.createTrain: zTrain.safeParse validates the body — trims
    strings, requires seats.length >= 1, validates each seat's seatType against
    the SeatType enum
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

### Case B: `POST /trains/route` on a train that has no route yet (happy path — previously always failed here, now fixed)

```
1.  Client sends POST /trains/route with an x-user-id header and:
      { trainId, stations: [{ stationId, sequenceNumber, arrivalTime?,
        departureTime?, distanceFromOrigin? }, ...] } (>= 2 stations)
2.  getUserContext passes the request through → train.routes.ts matches POST
    /route (mounted at /trains) → trainController.createRoute
3.  zRoute.safeParse validates the body
4.  trainService.createRoute({ trainId, stations }):
      a. prisma.train.findUnique({ id: trainId }, include: { seats: ... }) → found.
         Seats are fetched here too, so the same row can be reused for the Kafka
         payload below without a second query
      b. prisma.route.findUnique({ trainId }) → not found (this train has no route
         yet). The existence check now reads the right way round —
         `if (existingRoute) throw new ConflictError("Route already exists for
         this train")` — so it only throws when a route *does* already exist,
         and a brand-new train correctly falls through to route creation
      c. every stationId in the payload is checked to exist; sequenceNumbers are
         checked for being contiguous starting at 1 (the error message now reads
         "Sequence numbers must be contiguous starting from 1" — both the
         "continous starting free" and "existis" typos from before are gone)
      d. prisma.route.create(...) inserts the route and its RouteStation rows in
         one nested Prisma write
      e. adminProducer.publishRouteCreated({ ...route, train: existingTrain }) →
         Kafka topic admin.route-created, keyed by route-<id>. The payload is a
         RouteCreatedPayload — the route plus the train (with its seats) inlined
         — because search-service's indexTrainRoute consumer destructures
         { train, routeStations } directly off the event and would silently
         no-op without a train field. This publish is caught and logged on
         failure, not thrown (same pattern as trainService.createTrain)
5.  Response: 200 { success: true, message: "Route created successfully" }
```

If a route already *did* exist for this train, step 4b's `ConflictError` fires cleanly with a 409 before `prisma.route.create` is ever reached — no raw Prisma unique-constraint error leaks through.

### Case C: any route with no `x-user-id` header (auth failure — new behavior)

```
1.  Client sends POST /stations/station directly to admin-service (bypassing
    api-gateway, or through a misconfigured proxy) without an x-user-id header
2.  server.ts middleware runs (helmet, cors, reqLogger, cookieParser, json) →
    station.route.ts matches POST /station (mounted at /stations) →
    getUserContext runs before stationController.createStation
3.  req.headers["x-user-id"] is undefined, so getUserContext calls
    next(new UnauthorizedError("User context missing - must come through gateway"))
    — the request never reaches Zod validation or the controller at all
4.  errorHandler catches the UnauthorizedError (an AppError) and responds with
    its own status/code
5.  Response: 401 { success: false, error: "UNAUTHORIZED", message: "User
    context missing - must come through gateway" }
```

This is new behavior: `getUserContext` is now mounted on every route in this service (station, train, and schedule). Before this pass, nothing enforced this middleware anywhere, so this header was silently ignored and any caller could reach every endpoint.

### Case D: `POST /stations/station` with a code that already exists (conflict — the missing `await` bug is fixed)

```
1.  Client sends POST /stations/station with an x-user-id header and a code
    that's already in the database, e.g. "NDLS"
2.  getUserContext passes it through → zStation validation passes
3.  stationController.createStation now awaits stationService.createStation({...})
4.  Inside stationService.createStation: prisma.station.findUnique finds the
    existing row and throws ConflictError("Station already exists")
5.  Because createStation is now awaited, the rejection propagates back out of
    the async handler; asyncHandler's `.catch(next)` forwards it to
    errorHandler, which responds using the AppError's own status/code
6.  Response: 409 { success: false, error: "CONFLICT", message: "Station
    already exists" } — no new row is created, and the caller now gets an
    honest error instead of a false 200 (see Known Issues for how this used
    to behave)
```

### Case E: `POST /schedules/schedule` (happy path — now reachable via HTTP)

```
1.  Client sends POST /schedules/schedule with an x-user-id header and
    { trainId, departureDate } (see zSchedule)
2.  schedule.route.ts — now mounted at /schedules in server.ts — matches
    POST /schedule. getUserContext runs, then scheduleController.createSchedule
3.  zSchedule.safeParse validates the body
4.  scheduleService.createSchedule({ trainId, departureDate }):
      a. prisma.train.findUnique({ id: trainId }, include seats + route +
         routeStations + station) — throws ConflictError if the train doesn't
         exist, BadRequestError if it has no seats or no route yet (a schedule
         needs both)
      b. departureDate is re-parsed with `new Date(...)` and checked for NaN
      c. prisma.schedule.findUnique({ trainId_departureDate }) → checked for a
         duplicate; ConflictError if one already exists for that exact date
      d. prisma.schedule.create({ trainId, departureDate }) inserts the schedule row
      e. a denormalized eventPayload is assembled: schedule id/status, the
         train's id/number/name/coachName/totalSeats, every seat, and every
         route stop (with full station details) — all inlined so downstream
         consumers don't need a follow-up call
      f. adminProducer.publishScheduleCreated(eventPayload) → Kafka topic
         admin.schedule-created, keyed by schedule-<scheduleId>. Unlike train
         and route creation, this publish is not wrapped in a .catch — a
         broker failure here throws
      g. createSchedule still returns "" (an empty string, not the created
         schedule or payload) — unchanged by this pass, see Known Issues
5.  scheduleController.createSchedule responds 200 { success: true, message:
    "Schedule created successfully" } — the old copy-pasted "Train created
    successfully" message is fixed
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
import { disconnectProducer } from "./config/kafka";

const startServer = async (): Promise<void> => {
  try {
    const server = app.listen(config.PORT, () => {
      logger.info(`${config.SERVICE_NAME} is running on port ${config.PORT}`);
    });

    const shutdown = async (): Promise<void> => {
      logger.info("Shutting down gracefully...");

      server.close(async () => {
        await disconnectProducer();
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

This is a substantially fuller entry point than before. Two things changed in this pass:

1. `dotenv.config()` now runs on line 2, before `import app from "./server"` and every other import that transitively pulls in `config/index.ts`. Since `config/index.ts` builds its `Config` object by reading `process.env.*` at module-load time, this ordering matters: any variable set only in `.env` (not already in the shell environment) is now guaranteed to be populated by the time `config` is built. Before this fix, `dotenv.config()` ran *after* the `import { config } from "./config"` line — too late to affect anything, since ES module imports are hoisted and evaluated before the rest of the file's own statements run.
2. The dead `import connectDB from "./config/db"` — a leftover Mongoose-style import pointing at a file that never existed in this project, and that was never called anywhere even when it "existed" in intent — has been removed. Prisma's own `config/prisma.ts` already owns the database connection; there was never a `connectDB` to call.

The rest — `startServer`, graceful shutdown on `SIGTERM`/`SIGINT` that closes the HTTP server and disconnects the Kafka producer before exiting — was already structured this way and is unchanged by this pass.

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
import scheduleRoutes from "./routes/schedule.route";

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
app.use("/schedules", scheduleRoutes);

app.get("/", (req, res) => {
  res.send("Hello from admin-service");
});

app.get("/health", (req, res) => {
  res.status(200).json({
    success: true,
    message: "Admin Service is healthy",
    timestamp: new Date().toISOString(),
  });
});

// Must be registered after all routes — Express only treats a 4-arg
// middleware as an error handler when it's last in the chain.
app.use(errorHandler);

export default app;
```

This is the entire app definition: three route groups (`/stations`, `/trains`, `/schedules`), a root `GET /` (now correctly says "admin-service" instead of a leftover "user-service" string), a `GET /health` that now returns a `success`/`message`/`timestamp` body instead of a bare `{ message: "ok" }`, and the error handler registered last. `schedule.route.ts` is now imported and mounted here — previously it wasn't, and the entire schedule-creation feature was unreachable from outside the process. Auth (`getUserContext`) is applied per-route inside each router file (see [File Structure](#file-structure)), not centrally in `server.ts`.

---

### 3. `config/` — Env, Kafka, Logger, Prisma

**`config/index.ts`** — new this pass. This file was empty before; every other file in the project imports a `config` object from it (`"."`, `"./index"`, `"../config"`, `"./config"`), so its absence was the root cause of the service failing to compile. It reads `package.json`'s own `name` field for `SERVICE_NAME` (kept out of `.env` on purpose, matching the pattern used elsewhere in this repo) and everything else from `process.env`:

```typescript
import { readFileSync } from "fs";
import { resolve } from "path";

// require("../../package.json") would type package.json's export as `any`;
// reading + parsing it manually keeps the `any` confined to this one
// external-boundary assertion instead of leaking into Config.SERVICE_NAME.
const packageJsonPath = resolve(process.cwd(), "./package.json");
const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8")) as {
  name: string;
};

interface Config {
  SERVICE_NAME: string;
  PORT: number;
  NODE_ENV: string;
  LOG_LEVEL: string;
  DATABASE_URL: string | undefined;
  ALLOWED_ORIGINS: string | undefined;
  KAFKA_BROKER: string | undefined;
  KAFKA_CLIENT_ID: string | undefined;
  INTERNAL_SERVICE_KEY: string | undefined;
}

export const config: Config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT) || 4003,
  NODE_ENV: process.env.NODE_ENV || "development",
  LOG_LEVEL: process.env.LOG_LEVEL || "info",
  DATABASE_URL: process.env.DATABASE_URL,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS,
  KAFKA_BROKER: process.env.KAFKA_BROKER,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  INTERNAL_SERVICE_KEY: process.env.INTERNAL_SERVICE_KEY,
};
```

Two fields are on this object but effectively unused elsewhere in `src/` — see [Known Issues #6 and #7](#known-issues--inconsistencies): nothing reads `config.NODE_ENV` (the one place that checks `NODE_ENV` reads the raw `process.env.NODE_ENV` instead), and nothing reads `config.INTERNAL_SERVICE_KEY` at all yet.

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

`idempotent: true` guarantees each message is written exactly once per partition on retry, which is why `maxInFlightRequests` is capped at 5 (required for that guarantee to hold). `connectProducer()`/`disconnectProducer()` both track an `isConnected` flag so calling either more than once is a no-op. This file's `import { config } from "."` now resolves correctly, since `config/index.ts` exists.

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

Note the last check reads the raw `process.env.NODE_ENV` rather than `config.NODE_ENV` — both currently resolve to the same value, but it's an inconsistency worth knowing about (see Known Issues).

---

### 4. `types/` — Validation Schemas & Express Augmentation

**`types/zod.ts`** defines every schema a request goes through (unchanged by this pass):

- `zStation` — `name` (4–40 chars), `code` (2–10 chars, trimmed and uppercased by the schema itself), `city` (2–40 chars), optional `state` (≤40 chars)
- `zSeat` — `seatNumber` (positive int), `seatType` (`LOWER | MIDDLE | UPPER | SIDE_LOWER | SIDE_UPPER`), `price` (positive number)
- `zTrain` — `trainNumber` (1–10 chars), `trainName` (4–40 chars), optional `coachName` (≤20 chars), `seats` (array of `zSeat`, minimum 1)
- `zRouteStation` — `stationId` (UUID), `sequenceNumber` (positive int), optional `arrivalTime`/`departureTime` (`HH:mm` regex), optional `distanceFromOrigin` (non-negative number)
- `zRoute` — `trainId` (UUID), `stations` (array of `zRouteStation`, minimum 2)
- `zSchedule` — `trainId` (UUID), `departureDate` (coerced to a `Date`), optional `status` (`ACTIVE | CANCELLED`)

`StationBodyType`, `SeatBodyType`, `TrainBodyType`, `RouteStationBodyType`, `RouteBodyType`, and `ScheduleBodyType` are the corresponding `z.infer<...>` types used throughout the controllers and services.

**`types/express.d.ts`** — new this pass:

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

This is what makes `middlewares/user-context.middleware.ts`'s `req.user = {...}` assignment type-check — before this file existed, `npx tsc --noEmit` reported `Property 'user' does not exist on type 'Request<...>'` here, the same pattern api-gateway's own `auth.middleware.ts` already solved for itself.

`src/types/index.ts` — which previously defined `KnowledgeDoc`/`RAGResponse` (a document-embedding/RAG shape, importing `mongoose`, with no relation to stations/trains/routes/schedules, and that nothing under `src/` ever imported) — has been deleted entirely in this pass.

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

    // zStation's `code` field already applies `.toUpperCase()` via zod, so
    // this call passes it straight through rather than re-uppercasing.
    const station = await stationService.createStation({
      code,
      name,
      city,
      state,
    });

    res.status(200).json({
      success: true,
      message: "Station created successfully",
      data: station,
    });
  },
);

export const stationController = { createStation };
```

Two bugs are fixed here: `stationService.createStation(...)` is now `await`ed (so a rejection — e.g. `ConflictError` on a duplicate code — correctly propagates to `asyncHandler`'s `.catch(next)` and on to `errorHandler`, instead of becoming an unhandled promise rejection while the client gets a false 200), and the response message is now `"Station created successfully"` instead of a leftover `"OTP sent successfully"` from a different (OTP-based) flow. See [Lifecycle Case D](#case-d-post-stationsstation-with-a-code-that-already-exists-conflict--the-missing-await-bug-is-fixed) for the corrected flow.

`services/station.service.ts` (unchanged by this pass):

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

/**
 * Looks up a station by id — used by the internal-only lookup route so
 * other services (currently booking-service, to attach a station's name to
 * a booking-confirmed email) can resolve a station without a JWT.
 */
const getStationById = async (id: string) => {
  const station = await prisma.station.findUnique({ where: { id } });
  if (!station) {
    throw new NotFoundError("Station not found");
  }
  return station;
};

export const stationService = { createStation, getStationById };
```

**New this pass**: `GET /stations/station/internal/:stationId`, behind `internalAuth` rather than `getUserContext` — a shared-secret header check (`x-internal-service-key`), the same pattern already used by inventory-service's and user-service's own internal routes. The controller side:

```typescript
const getStationByIdInternal = asyncHandler(
  async (req: Request<{ stationId: string }>, res: Response) => {
    const { stationId } = req.params;
    const station = await stationService.getStationById(stationId);
    res.status(200).json({ success: true, data: station });
  },
);

export const stationController = { createStation, getStationByIdInternal };
```

And the route mount, in `routes/station.route.ts`:

```typescript
router.get(
  "/station/internal/:stationId",
  internalAuth,
  stationController.getStationByIdInternal,
);
```

This exists specifically to unblock booking-service's `stationClient.ts`, which calls this exact path (`/stations/station/internal/:stationId`) to resolve a station's name for segment-booking confirmation emails. `getStationById` throws the same `NotFoundError` pattern as every other lookup in this service — a nonexistent station id is a clean `404`, not a silent `null`.

The comment above the Kafka publish is itself now slightly stale — it still describes the "never awaits this function" behavior that was true before this pass, but the controller now does await it. The underlying fact that remains true is that this publish still isn't wrapped in a `.catch`, unlike `trainService.createTrain` — a Kafka failure here still throws, and now that the controller awaits the call, that rejection really would surface as a 500 from `errorHandler` if the broker were down.

---

### 6. Train & Route — controller + service

`controllers/train.controller.ts` defines three handlers: `createTrain`, `createRoute`, and `getTrainById`.

```typescript
const createTrain = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
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

```typescript
const createRoute = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zRoute.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { stations, trainId } = result.data;
    // Redundant with zRoute's own `.min(2, ...)` on `stations`, kept as a defensive check
    if (stations.length === 0) {
      throw new BadRequestError("A route must have at least 2 stations");
    }

    await trainService.createRoute({
      stations,
      trainId,
    });

    res
      .status(200)
      .json({ success: true, message: "Route created successfully" });
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
    return res.status(200).json({
      success: true,
      data: train,
    });
  },
);
export const trainController = { getTrainById, createTrain, createRoute };
```

`getTrainById` reads `req.params.trainId`, and it's now mounted as `GET /trains/train/:trainId` (see `train.routes.ts` below) — the param name finally matches, so this endpoint works. Before this pass it was mounted as `POST /trains/route/:id`, which named the param `:id` while the controller read `req.params.trainId`, so `trainId` was always `undefined` and the handler always 400'd with "Train Id is missing" before `trainService.getTrainById` was ever reached.

`routes/train.routes.ts`:

```typescript
import { Router } from "express";
import { trainController } from "../controllers/train.controller";
import { getUserContext } from "../middlewares/user-context.middleware";

const router = Router();

// Mounted at /trains in server.ts.
router.post("/train", getUserContext, trainController.createTrain); // POST /trains/train — create train + seats
router.post("/route", getUserContext, trainController.createRoute); // POST /trains/route — define a train's route
router.get("/train/:trainId", getUserContext, trainController.getTrainById); // GET /trains/train/:trainId — fetch a train with seats + route

export default router;
```

`services/train.service.ts`:

```typescript
const createTrain = async ({
  trainName,
  trainNumber,
  coachName,
  seats,
}: TrainBodyType) => {
  // Train number is the unique identifier — reject duplicates before hitting the DB constraint
  const existing = await prisma.train.findUnique({ where: { trainNumber } });
  if (existing) {
    throw new ConflictError("Train with this number already exists");
  }
  // Guard against two seats in the same payload sharing a seat number
  const seatNumbers = seats.map((s) => s.seatNumber);
  if (new Set(seatNumbers).size !== seatNumbers.length) {
    throw new BadRequestError("Duplicate seat numbers found");
  }
  const train = await prisma.train.create({
    data: {
      trainNumber,
      trainName,
      // Defaults to "AC" when the client omits coachName (zTrain marks it optional)
      coachName: coachName || "AC",
      totalSeats: seats.length,
      // Nested write — train and all of its seats are created in one transaction
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
  // Unlike stationService.createStation, a publish failure here is caught and
  // logged rather than thrown — the train is already committed, so a Kafka
  // outage shouldn't turn a successful creation into a 500.
  await adminProducer.publishTrainCreated(train).catch((err) => {
    logger.error("Failed to publish train created event", {
      error: err.message,
    });
  });

  return train;
};
```

This function is unchanged by this pass. The nested `seats: { create: [...] }` write inserts the train row and every seat row in one transaction — `totalSeats` is just `seats.length`, not a separately-counted value.

```typescript
const createRoute = async ({ trainId, stations }: RouteBodyType) => {
  // Train must already exist — a route can't be attached to a train that isn't there.
  // Seats are fetched here too so the same row can be inlined into the
  // ROUTE_CREATED event below without a second query.
  const existingTrain = await prisma.train.findUnique({
    where: { id: trainId },
    include: { seats: { orderBy: { seatNumber: "asc" } } },
  });
  if (!existingTrain) {
    throw new NotFoundError("Train Not found");
  }
  const existingRoute = await prisma.route.findUnique({ where: { trainId } });
  if (existingRoute) {
    throw new ConflictError("Route already exists for this train");
  }
  const stationIds = stations.map((station) => station.stationId);
  const existingStations = await prisma.station.findMany({
    where: { id: { in: stationIds } },
  });
  if (existingStations.length !== stationIds.length) {
    throw new BadRequestError("One or more station Ids are invalid");
  }
  const sorted = [...stations].sort(
    (a, b) => a.sequenceNumber - b.sequenceNumber,
  );
  for (let i = 0; i < sorted.length; i++) {
    if (sorted[i].sequenceNumber !== i + 1) {
      throw new BadRequestError(
        "Sequence numbers must be contiguous starting from 1",
      );
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
      routeStations: {
        include: { station: true },
        orderBy: { sequenceNumber: "asc" },
      },
    },
  });

  // search-service's indexTrainRoute reads `train` and `routeStations`
  // directly off the event, so the train (with its seats, fetched above) is
  // inlined here rather than published as a bare Route row.
  await adminProducer
    .publishRouteCreated({ ...route, train: existingTrain })
    .catch((err) => {
      logger.error("Failed to publish route created event", {
        error: err.message,
      });
    });

  return route;
};
```

Three things are fixed here relative to before:

1. **The existence check is no longer inverted.** It used to read `if (!existingRoute) throw new NotFoundError("Route already existis for this train")` — throwing "already exists" precisely when no route existed, so no train could ever get its first route created. It now reads `if (existingRoute) throw new ConflictError("Route already exists for this train")` — a clean 409 only when a route genuinely already exists, with the "existis" typo also fixed.
2. **The contiguous-sequence-number message typo is fixed**: `"Sequence Numbers must be continous starting free"` is now `"Sequence numbers must be contiguous starting from 1"`.
3. **The `publishRouteCreated` call is no longer commented out.** It now fires after the route is created, publishing a `RouteCreatedPayload` — the route plus the train (already fetched above, with its seats) inlined — rather than a bare `Route` row, because search-service's `indexTrainRoute` consumer destructures `{ train, routeStations }` straight off the event.

```typescript
const getTrainById = async (id: string) => {
  const train = await prisma.train.findUnique({
    where: { id },
    include: {
      seats: { orderBy: { seatNumber: "asc" } },
      route: {
        include: {
          routeStations: {
            include: { station: true },
            orderBy: { sequenceNumber: "asc" },
          },
        },
      },
    },
  });
  if (!train) throw new NotFoundError("Train not found");
  return train;
};
export const trainService = { getTrainById, createTrain, createRoute };
```

This function itself was always correct — it's only reachable now because of the routing fix in `getTrainById`'s controller/route pairing described above.

---

### 7. Schedule — controller + service

`controllers/schedule.controller.ts`:

```typescript
const createSchedule = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    // Validate incoming body against the zSchedule schema
    const result = zSchedule.safeParse(req.body);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { trainId, departureDate } = result.data;
    await scheduleService.createSchedule({ trainId, departureDate });
    return res
      .status(200)
      .json({ success: true, message: "Schedule created successfully" });
  },
);

export const scheduleController = { createSchedule };
```

The success message is now `"Schedule created successfully"` — it used to say `"Train created successfully"`, a copy-paste leftover from `train.controller.ts`.

`routes/schedule.route.ts`:

```typescript
import { Router } from "express";
import { scheduleController } from "../controllers/schedule.controller";
import { getUserContext } from "../middlewares/user-context.middleware";

const router = Router();

// Mounted at /schedules in server.ts, so this resolves to POST /schedules/schedule.
router.post("/schedule", getUserContext, scheduleController.createSchedule);

export default router;
```

This router itself hasn't changed — what changed is that `server.ts` now actually imports and `app.use("/schedules", scheduleRoutes)`s it. Before this pass, there was no `app.use(..., scheduleRoutes)` anywhere, so the entire schedule-creation feature was fully implemented but completely unreachable from outside the process.

`services/schedule.service.ts` (logic unchanged by this pass):

```typescript
const createSchedule = async ({ trainId, departureDate }: ScheduleBodyType) => {
  // Train number is the unique identifier — reject duplicates before hitting the DB constraint
  const existingTrain = await prisma.train.findUnique({
    where: { id: trainId },
    include: {
      seats: true,
      route: {
        include: {
          routeStations: {
            include: { station: true },
          },
        },
      },
    },
  });
  if (!existingTrain) {
    throw new ConflictError("Train not found");
  }
  if (existingTrain.seats.length === 0) {
    throw new BadRequestError("Train not found");
  }
  if (!existingTrain.route) {
    throw new BadRequestError(
      "Train has no route defined. Create a route first ",
    );
  }
  const parsedDate = new Date(departureDate);
  if (isNaN(parsedDate.getTime())) {
    throw new BadRequestError("Invalid departure date format ");
  }
  const existingSchedule = await prisma.schedule.findUnique({
    where: { trainId_departureDate: { trainId, departureDate: parsedDate } },
  });
  if (existingSchedule) {
    throw new ConflictError(
      "Schedule already exists for this train on this date ",
    );
  }
  const schedule = await prisma.schedule.create({
    data: { trainId, departureDate: parsedDate },
  });
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
      seatId: s.id,
      seatNumber: s.seatNumber,
      seatType: s.seatType,
      price: s.price,
    })),
    route: existingTrain.route.routeStations.map((rs) => ({
      stationId: rs.station.id,
      stationName: rs.station.name,
      stationCode: rs.station.code,
      city: rs.station.city,
      sequenceNumber: rs.sequenceNumber,
      arrivalTime: rs.arrivalTime,
      departureTime: rs.departureTime,
      distanceFromOrigin: rs.distanceFromOrigin,
    })),
  };

  // This event goes to both inventory-service and search-service via Kafka
  await adminProducer.publishScheduleCreated(eventPayload);
  return "";
};

export const scheduleService = { createSchedule };
```

Note `existingTrain.seats.length === 0` still throws `BadRequestError("Train not found")` — a copy-pasted message that doesn't match the actual condition ("train has no seats", not "train doesn't exist" — that case is the `ConflictError` right above it). `createSchedule` still returns an empty string `""`, not the created schedule or the event payload. Neither of these was in scope for this fix pass — see [Known Issues](#known-issues--inconsistencies).

`eventPayload`'s shape is defined by the `ScheduleCreatedPayload` interface in `admin.producer.ts` (see next section) — it's deliberately denormalized (train + seats + route all inlined) so inventory-service and search-service don't need to call back into admin-service to react to a new schedule.

---

### 8. `kafka/producer/admin.producer.ts` — Event Publishing

A thin class wrapping the shared Kafka producer with domain-specific publish methods. The `RouteCreatedPayload` interface is new this pass — it's what makes `publishRouteCreated`'s call site in `train.service.ts` type-check now that it's actually being called with a denormalized shape instead of a bare `Route`:

```typescript
/**
 * Denormalized route snapshot consumed by search-service's indexTrainRoute —
 * it reads `train` (for trainId/trainNumber/trainName/seats) and
 * `routeStations` (each including its related `station`) directly off this
 * event, so both need to be inlined rather than just publishing the raw
 * Prisma `Route` row.
 */
export interface RouteCreatedPayload extends Route {
  train: Train & { seats: Seat[] };
  routeStations: (RouteStation & { station: Station })[];
}

/**
 * Denormalized schedule snapshot consumed by inventory-service and
 * search-service — carries the seat map and route so those services
 * don't need to call back into admin-service.
 */
export interface ScheduleCreatedPayload {
  scheduleId: string;
  trainId: string;
  trainNumber: string;
  trainName: string;
  coachName: string;
  totalSeats: number;
  departureDate: Date;
  status: ScheduleStatus;
  seats: {
    seatId: string;
    seatNumber: number;
    seatType: SeatType;
    price: number;
  }[];
  route: {
    stationId: string;
    stationName: string;
    stationCode: string;
    city: string;
    sequenceNumber: number;
    arrivalTime: string | null;
    departureTime: string | null;
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

  /**
   * Publishes a route-created event. Takes the denormalized
   * RouteCreatedPayload (not a raw Prisma `Route`) so search-service's
   * indexTrainRoute — which reads `train` and `routeStations` off the event
   * directly — has everything it needs without calling back into this service.
   */
  async publishRouteCreated(routeData: RouteCreatedPayload) { /* keyed by route-<id> */ }
  async publishScheduleCreated(scheduleData: ScheduleCreatedPayload) { /* keyed by schedule-<scheduleId> */ }
  async publishScheduleCancelled(schedule: Schedule) { /* keyed by schedule-<id> */ }
}

export default new AdminProducer();
```

The producer connects lazily on first use (`initialize()`), not at import time. Every `key` is derived from the entity's own id, so all events about the same entity land on the same Kafka partition and stay in order relative to each other.

Of the five publish methods, four are now actually reached:
- `publishStationCreated` and `publishTrainCreated` — unchanged, already worked before this pass.
- `publishRouteCreated` — its call site in `train.service.ts`'s `createRoute` used to be commented out (and, as commented, wouldn't even have type-checked against the old signature, which expected a plain Prisma `Route`). It now fires correctly with a `RouteCreatedPayload`.
- `publishScheduleCreated` — the call itself was always correct; it's now actually reachable because `schedule.route.ts` is mounted in `server.ts`.
- `publishScheduleCancelled` still has **no caller anywhere** in this codebase — there's no cancel-schedule route, controller, or service. Out of scope for this fix pass; see Known Issues.

---

### 9. `middlewares/` & `utils/` — Cross-Cutting Helpers

**`middlewares/cors.middleware.ts`** — whitelist check against `config.ALLOWED_ORIGINS` (comma-separated env var), credentials enabled, methods restricted to `GET/POST/PUT/DELETE/OPTIONS`. The origin list is built with `config.ALLOWED_ORIGINS ? config.ALLOWED_ORIGINS.split(",") : []`, so an unset `ALLOWED_ORIGINS` now safely resolves to an empty allow-list instead of throwing on `undefined.split(",")`.

**`middlewares/error.middleware.ts`** — `AppError` instances are returned with their own status/code; anything else is logged to the console and returned as a generic `500 INTERNAL_SERVER_ERROR`. Unchanged.

**`middlewares/req.middleware.ts`** — logs every request at `debug` on arrival, then logs method/path/status/duration at `info` once the response's `"finish"` event fires. Unchanged.

**`middlewares/user-context.middleware.ts`** (code unchanged from before — what changed is that it's now actually mounted):

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

Two things are fixed here: it's now imported and applied to every route in `station.route.ts`, `train.routes.ts`, and `schedule.route.ts` (before this pass, it was defined but never mounted anywhere, so it protected nothing); and `req.user = {...}` now type-checks, because of the new `types/express.d.ts` augmentation described above (before this pass, `npx tsc --noEmit` reported this as a real error). See [Lifecycle Case C](#case-c-any-route-with-no-x-user-id-header-auth-failure--new-behavior) for the resulting 401 flow.

**`utils/api-response.ts`** — `SuccessResponse`/`ErrorResponse` helpers that wrap `res.json()` in a consistent `{ success, message, data? }` shape. Unchanged.

**`utils/asyncHandler.ts`** — wraps an async controller so a rejected promise is forwarded to `next()` instead of crashing the process. Unchanged.

**`utils/error.ts`** — `AppError` base class plus `BadRequestError` (400), `UnauthorizedError` (401), `ForbiddenError` (403), `NotFoundError` (404), `ConflictError` (409), `TooManyRequestsError` (429), `InternalServerError` (500) — see the [Error Codes Reference](#error-codes-reference) for which of these are actually thrown anywhere. Unchanged.

**`utils/zod.formatter.ts`** — takes a `ZodError` and returns just the first issue's message as a plain string (not the full list of validation errors). Unchanged.

---

## Environment Variables

Variables actually read via `config.*` somewhere in `src/`, now that `config/index.ts` exists:

```bash
PORT=4003              # config/index.ts: Number(process.env.PORT) || 4003
NODE_ENV=development    # config/index.ts reads it into config.NODE_ENV, but nothing under
                        # src/ reads config.NODE_ENV — config/prisma.ts checks the raw
                        # process.env.NODE_ENV directly instead (see Known Issues)
LOG_LEVEL=info          # read by config/logger.ts
DATABASE_URL=           # read by config/prisma.ts, passed to the pg adapter
ALLOWED_ORIGINS=        # read by middlewares/cors.middleware.ts (comma-separated, now
                        # undefined-safe)
KAFKA_BROKER=           # read by config/kafka.ts (falls back to "localhost:9093" if unset)
KAFKA_CLIENT_ID=        # read by config/kafka.ts
INTERNAL_SERVICE_KEY=   # read into config.INTERNAL_SERVICE_KEY, but nothing under src/
                        # reads that field yet — currently unused (see Known Issues)
# SERVICE_NAME isn't an env var — config/index.ts reads it from package.json's own "name" field
```

`.env.example` is new this pass and defines exactly these variables with concrete local-dev values (`PORT=4003`, a local Postgres connection string, `localhost:9093` for Kafka, etc.) — and nothing else. There is no `.env` committed in this project; anyone running the service locally needs to copy `.env.example` to `.env` themselves. This is a change from the state this doc previously described, where a stale `.env` was said to define a long list of unrelated JWT/OTP/Mongo/Redis/SendGrid variables — that file isn't present in the project as it stands now.

---

## Error Codes Reference

| Class | Status | Thrown by, in this codebase |
|---|---|---|
| `BadRequestError` | 400 | `train.controller.ts` (empty seats/stations — unreachable in practice), `train.service.ts` (duplicate seat numbers; invalid station ids; non-contiguous sequence numbers), and `schedule.service.ts` (no seats, no route, bad date) |
| `UnauthorizedError` | 401 | `user-context.middleware.ts` — now mounted on every route in this service (station, train, schedule), so this fires for real whenever `x-user-id` is missing. Before this pass it was defined but never mounted, so it never fired |
| `ForbiddenError` | 403 | — (defined, not thrown anywhere) |
| `NotFoundError` | 404 | `train.service.ts` — `getTrainById` (train doesn't exist) and `createRoute` (train doesn't exist). The previous *incorrect* use of `NotFoundError` for "route already exists" is gone — that path now throws `ConflictError` instead (see Known Issues history) |
| `ConflictError` | 409 | `station.service.ts` (duplicate code), `train.service.ts` (duplicate train number, and now also route already exists for this train), and `schedule.service.ts` (train not found — a copy-pasted use of `ConflictError` for a not-found case; duplicate schedule for the same date) |
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
| `admin.route-created` | `trainService.createRoute` | ✅ yes — fixed this pass. `adminProducer.publishRouteCreated` was already implemented, but its only call site (in `trainService.createRoute`) used to be commented out; it now fires with a denormalized `RouteCreatedPayload` |
| `admin.schedule-created` | `scheduleService.createSchedule` | ✅ yes — the publish call itself was always correct, but `schedule.route.ts` wasn't mounted in `server.ts` before this pass, so nothing could reach `createSchedule` via HTTP. It's mounted now |
| `admin.schedule-cancelled` | — | ❌ `adminProducer.publishScheduleCancelled` exists but nothing calls it — there's no cancel-schedule route/controller/service anywhere in this codebase. Out of scope for this fix pass |
| `admin.train-updated`, `admin.station-updated`, `admin.route-updated` | — | ❌ defined in the shared constants file, no producer method for any of them exists here |

This service is a pure Kafka **producer** — it has no consumer anywhere. It never subscribes to any topic; it only ever publishes.

---

## Quick Start

```bash
cd admin-service
npm install
cp .env.example .env     # no .env is committed — copy the example and adjust as needed
npx prisma generate       # regenerates src/generated/prisma from prisma/schema.prisma

npm run dev                # nodemon + ts-node, hot reload
```

`npx tsc --noEmit` from `admin-service/` reports zero errors as of this pass. That's a static guarantee only — this service has **not** been run against a live Postgres instance or Kafka broker in the environment this fix pass ran in, so the requests below are shown as what the code is written to do, not as independently confirmed end-to-end behavior.

Every route below now requires an `x-user-id` header (see [Known Issue #12](#known-issues--inconsistencies) for what that header does and doesn't verify):

```bash
curl -X POST http://localhost:4003/stations/station \
  -H "Content-Type: application/json" \
  -H "x-user-id: 11111111-1111-1111-1111-111111111111" \
  -d '{"name":"New Delhi","code":"ndls","city":"Delhi","state":"Delhi"}'

curl -X POST http://localhost:4003/trains/train \
  -H "Content-Type: application/json" \
  -H "x-user-id: 11111111-1111-1111-1111-111111111111" \
  -d '{"trainNumber":"12301","trainName":"Rajdhani Express","coachName":"AC","seats":[{"seatNumber":1,"seatType":"LOWER","price":1500}]}'

# Now succeeds for a brand-new train (the existence-check inversion is fixed):
curl -X POST http://localhost:4003/trains/route \
  -H "Content-Type: application/json" \
  -H "x-user-id: 11111111-1111-1111-1111-111111111111" \
  -d '{"trainId":"<train-uuid>","stations":[{"stationId":"<station-uuid-1>","sequenceNumber":1,"departureTime":"06:10"},{"stationId":"<station-uuid-2>","sequenceNumber":2,"arrivalTime":"10:00"}]}'

# Now works — the routing bug is fixed and the param name matches:
curl http://localhost:4003/trains/train/<train-uuid> \
  -H "x-user-id: 11111111-1111-1111-1111-111111111111"

# Now reachable — schedule.route.ts is mounted at /schedules in server.ts:
curl -X POST http://localhost:4003/schedules/schedule \
  -H "Content-Type: application/json" \
  -H "x-user-id: 11111111-1111-1111-1111-111111111111" \
  -d '{"trainId":"<train-uuid>","departureDate":"2026-08-01"}'

# NEW — internal-only, for another backend service (booking-service), not the gateway:
curl http://localhost:4003/stations/station/internal/<station-uuid> \
  -H "x-internal-service-key: <same value as INTERNAL_SERVICE_KEY>"
```

---

## Debugging Tips

- **Any request returns 401 `"User context missing - must come through gateway"`** → `getUserContext` is now mounted on every route in this service; set an `x-user-id` header, or route the request through api-gateway, whose `requireAuth` sets that header after real JWT verification. Calling admin-service directly (bypassing the gateway) means you're responsible for setting this header yourself — admin-service does no verification of its own. See Lifecycle Case C and Known Issue #12.
- **A duplicate-station request now correctly returns 409** → `station.controller.ts`'s `createStation` awaits the service call (fixed this pass); if you ever see a 200 for a duplicate station code again, something has regressed. See Lifecycle Case D.
- **A station's Kafka event never arrives even though the row exists in Postgres** → `stationService.createStation`'s publish isn't wrapped in a `.catch` (unlike the train/route services); check for a Kafka connection error in the logs around the time of that request. Since the controller now awaits this call, a broker outage here will also surface to the client as a 500, not just silently vanish.
- **`POST /trains/route` now succeeds for a brand-new train** → the inverted existence check is fixed; if it 409s, that means a route genuinely already exists for that train (`Route.trainId` is `@unique`). See Lifecycle Case B.
- **`GET /trains/train/:trainId` now works** → mounted correctly with a matching param name; a 404 here means `trainService.getTrainById` genuinely didn't find that id, not a routing bug.
- **`POST /schedules/schedule` 404s** → double check the full path — `schedule.route.ts` is mounted at `/schedules` in `server.ts` and itself defines `POST /schedule`, so the combined path is `/schedules/schedule`, easy to typo as just `/schedule`.
- **A schedule's Kafka event never arrives even though the row exists in Postgres** → unlike train/route creation, `scheduleService.createSchedule`'s publish isn't wrapped in a `.catch` — a broker failure here throws and would surface as a 500.
- **Nothing about "cancel a schedule" works, and there's no route for it** → there's no cancel-schedule feature built. `adminProducer.publishScheduleCancelled` exists but has no caller anywhere. See Known Issues.
- **A request to `/trains/train` with duplicate seat numbers in the payload returns a 400** → that's `trainService.createTrain`'s own dedup check (`new Set(seatNumbers).size !== seatNumbers.length`), separate from anything Zod validates.
- **`tsc` fails locally even though this doc says it's clean** → make sure `npx prisma generate` has been run so `src/generated/prisma` exists; the generated client isn't checked into the repo.
- **Requests routed through api-gateway to `/admins/stations/station` or `/admins/trains/train` still 404** → that's a pre-existing, still-unfixed mismatch: the gateway registers those as `GET`, admin-service only defines `POST`. Unrelated to anything fixed in this pass. See Known Issue #4.

---

## Known Issues & Inconsistencies

Observed while reviewing the current code — documented here rather than fixed, since these are informational (same approach as the API Gateway's and Notification Service's docs). Several issues that were present the last time this doc was written have been fixed in this pass and are called out inline in the sections above rather than repeated here; what follows is what's still actually true today.

1. **This fix pass has only been verified statically.** `npx tsc --noEmit` reports zero errors, but no Postgres instance or Kafka broker was reachable in the environment these changes were made in — none of the flows in this document have been run end-to-end against a live database or broker. Treat "works when called" as "the logic reads correctly and the types check," not as confirmed runtime behavior.
2. **`admin.schedule-cancelled` still has no caller anywhere.** No cancel-schedule route, controller, or service exists — `adminProducer.publishScheduleCancelled` is fully implemented but dead code. Building this was out of scope for this fix pass.
3. **Cross-service routing mismatch, unchanged by this pass**: `api-gateway/src/routes/index.ts` registers `/admins/stations/station` and `/admins/trains/train` as `GET`, but admin-service only defines `POST` for both — requests routed through the gateway to either endpoint still 404 today. Worth noting, though: api-gateway's `requireAuth` middleware does correctly set `x-user-id` before proxying, so admin-service's newly-enforced `getUserContext` requirement would already be satisfied for gateway-routed traffic once/if the verb mismatch itself gets fixed.
4. **Unrelated dependencies remain in `package.json`**: `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`, `mongoose`, `otp-generator`, `resend`, `bcrypt`, `jsonwebtoken`, `ioredis`, `http-status` are all still listed, and a repo-wide check in this pass confirms nothing under `src/` imports any of them — not even now that `types/index.ts` (which used to import `mongoose`) has been deleted. The package.json itself wasn't trimmed.
5. **`npm run seed` still points at `src/services/seed.ts`**, which still doesn't exist — running that script still fails. The same issue is flagged in the API Gateway's and Notification Service's docs, likely from a shared `package.json` origin.
6. **`config.NODE_ENV` is defined but nothing reads it.** `config/prisma.ts` checks the raw `process.env.NODE_ENV` directly (`if (process.env.NODE_ENV !== "production")`) instead of going through `config.NODE_ENV` — both currently resolve to the same value, so this is an inconsistency rather than a bug, but it means the `config` object isn't the single source of truth it looks like it should be.
7. ~~`config.INTERNAL_SERVICE_KEY` is read into the config object, but nothing under `src/` reads it anywhere~~ **No longer true.** `middlewares/internal-auth.middleware.ts` (new, added to support booking-service's `stationClient`) reads `config.INTERNAL_SERVICE_KEY` and compares it against the `x-internal-service-key` header on `GET /stations/station/internal/:stationId` — this field is genuinely consumed now.
8. **`scheduleService.createSchedule` still returns `""`** (an empty string), not the created schedule or the event payload it just built and published — a caller still has no way to get the new schedule back from this function's return value alone.
9. **`scheduleService.createSchedule`'s "train has no seats" check still throws a copy-pasted message**: `if (existingTrain.seats.length === 0) throw new BadRequestError("Train not found")` — the message says "not found" for a train that was clearly just found; the real condition is "has zero seats." Not touched in this pass.
10. **No user-facing read/list/update/delete endpoints exist for stations** — the new `GET /stations/station/internal/:stationId` is internal-only (behind `internalAuth`, for other services), not something an end user or the admin UI can call through the gateway. There's still no update/delete for trains, routes, or schedules either. `getTrainById` remains the only user-facing read endpoint in the entire service today.
11. **`getUserContext` trusts the `x-user-id` header at face value.** It doesn't verify a JWT itself — it just checks the header is present and 401s if not. This is the same trust-the-gateway model used elsewhere in this repo (api-gateway does the real JWT verification and sets the header before proxying), but it's worth knowing explicitly: anything that can set that header directly — a misconfigured proxy, or a caller hitting admin-service's port without going through the gateway — can claim to be any user id.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
