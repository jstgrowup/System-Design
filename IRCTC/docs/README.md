# IRCTC Backend — Compiled Documentation

This file is a **straight concatenation** of every document currently in this
`docs/` folder (except `learning-mode.md` and `learning-progress.md`, which
are personal walkthrough-progress trackers rather than project
documentation — see `docs/learning-mode.md` if you want those). Nothing has
been summarized, paraphrased, or edited — each section below is the exact,
word-for-word content of its source file, in the same order the files sit on
disk. Where a claim in one document has since been superseded by a later
document (this repo's docs were written incrementally, and a few explicitly
correct earlier staleness in later passes), that's preserved as-is here too —
this file doesn't attempt to reconcile or de-duplicate anything.

If you only need one service's docs, it's faster to open that service's own
file directly (linked below) than to scroll this combined file. This file
exists for the cases where having everything in one place — one search, one
scroll — is more useful than the split.

**Source files compiled here, in order:**

1. [`admin-docs.md`](#file-admin-docs-md) — Admin Service — Complete Guide
2. [`api-contract.md`](#file-api-contract-md) — IRCTC Backend — API & Kafka Contract (As-Is)
3. [`api-gateway-docs.md`](#file-api-gateway-docs-md) — API Gateway — Complete Guide
4. [`booking-service-docs.md`](#file-booking-service-docs-md) — Booking Service — Complete Guide
5. [`implementation-plan.md`](#file-implementation-plan-md) — IRCTC Backend — Implementation State
6. [`inventory-service-docs.md`](#file-inventory-service-docs-md) — Inventory Service — Complete Guide
7. [`notification-service-docs.md`](#file-notification-service-docs-md) — Notification Service — Complete Guide
8. [`payment-service-docs.md`](#file-payment-service-docs-md) — Payment Service — Complete Guide
9. [`playlist-guide.md`](#file-playlist-guide-md) — Playlist Guide — "IRCTC Backend" by designKarle
10. [`search-service-docs.md`](#file-search-service-docs-md) — Search Service — Complete Guide
11. [`user-service-docs.md`](#file-user-service-docs-md) — User Service — Complete Guide

Not included in this compilation (by explicit choice, see the question this
file was generated from): `docs/learning-mode.md`, `docs/learning-progress.md`.
Also not included, since they live outside `docs/`: the root `readme.md`,
`missing.md`, and per-directory `CLAUDE.md` files.

---


---

<a id="file-admin-docs-md"></a>
## FILE: `docs/admin-docs.md`

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

// Must stay above the imports below — config/index.ts reads process.env at
// module-load time, and CommonJS require() (this project's module target)
// runs each import in file order, so dotenv.config() executes first.
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

1. `dotenv.config()` now runs on line 2, before `import app from "./server"` and every other import that transitively pulls in `config/index.ts`. Since `config/index.ts` builds its `Config` object by reading `process.env.*` at module-load time, this ordering matters: any variable set only in `.env` (not already in the shell environment) is now guaranteed to be populated by the time `config` is built. The file itself now carries a comment explaining why this ordering matters: this project's `tsconfig.json` sets `"module": "CommonJS"`, so each `import` compiles down to a `require()` call executed in file order (unlike ES modules, whose imports are hoisted) — meaning `dotenv.config()` genuinely must appear textually before the imports that depend on it, not just conceptually. Before this fix, `dotenv.config()` ran *after* the `import { config } from "./config"` line — too late, since by then `config/index.ts` had already been required and had already read `process.env.*`.
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
| `ForbiddenError` | 403 | `internal-auth.middleware.ts` — thrown when `x-internal-service-key` is missing or doesn't match `config.INTERNAL_SERVICE_KEY`, guarding `GET /stations/station/internal/:stationId`. This is new this pass — before `internalAuth` existed, nothing threw `ForbiddenError` anywhere |
| `NotFoundError` | 404 | `train.service.ts` — `getTrainById` (train doesn't exist) and `createRoute` (train doesn't exist); and `station.service.ts` — `getStationById` (station doesn't exist), new this pass for the internal lookup route. The previous *incorrect* use of `NotFoundError` for "route already exists" is gone — that path now throws `ConflictError` instead (see Known Issues history) |
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


---

<a id="file-api-contract-md"></a>
## FILE: `docs/api-contract.md`

# IRCTC Backend — API & Kafka Contract (As-Is)

This is a status-annotated reference for every HTTP route and Kafka topic that
exists in this repo's code, across all eight services. It documents **current
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

### `POST /api/users/auth/login` — ✅ WORKING (fixed)
- Middleware: `endpointRateLimit(10, 900_000)` (10 req / 15 min per IP+endpoint) → proxy
- No auth required (this route issues the token)
- Rewrite: `/users/auth/login` → strips `users` → forwards to `userService/auth/login`
  → `http://localhost:4001/auth/login`
- **Was broken, now fixed**: user-service used to mount login at `/api/v1/auth/login`,
  which the gateway's one-segment-strip rewrite could never reproduce (it can only
  ever forward to `/auth/login`, never `/api/v1/auth/login`). Fixed by dropping the
  version prefix on user-service's side — `server.ts` now mounts auth routes at
  plain `/auth`, matching the no-prefix convention every other service in this repo
  already uses, and matching the reference implementation. **Not verified live** —
  no reachable user-service/Redis/Postgres in the environment this was fixed in, so
  this is confirmed by re-reading the rewrite logic against the new mount path, not
  an observed request.

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

### `POST /api/bookings/bookings` — ✅ WIRED (login's fix unblocked this)
- Middleware: `requireAuth` → `endpointRateLimit(5, 60_000)` (5/min) → proxy
- Rewrite: `/bookings/bookings` → strips `bookings` → forwards to
  `http://localhost:4005/bookings` — **matches** booking-service's own
  `POST /bookings` mount exactly (unlike the old login-route bug, this
  path-rewrite was always correct).
- **Why this is now reachable in principle**: `requireAuth` needs a valid JWT,
  and `POST /api/users/auth/login` (see §1 above) now actually issues one, so
  this route is no longer blocked on the auth side. Booking-service itself
  still can't complete a real booking regardless — payment-service exists, but
  there's no real Razorpay merchant account to test its gateway calls against —
  see booking-service's and payment-service's own docs.

### `GET /api/bookings/bookings` — ✅ WIRED, same as above
- Middleware: `requireAuth` → `combinedRateLimit()` → proxy → `/bookings` on booking-service.

### `GET /api/bookings/bookings/:bookingId` — ✅ WIRED, same as above
- Rewrite → `/bookings/:bookingId` on booking-service — matches.

### `POST /api/bookings/bookings/:bookingId/verify-payment` — ✅ WIRED, same as above
- Rewrite → `/bookings/:bookingId/verify-payment` on booking-service — matches.

### `POST /api/bookings/bookings/:bookingId/cancel` — ✅ WIRED, same as above
- Rewrite → `/bookings/:bookingId/cancel` on booking-service — matches.

### `POST /api/payments/webhooks/razorpay` — ✅ WIRED (public, no auth)
- No middleware except the raw-body branch already in `index.ts` (written ahead
  of this route, before payment-service existed) → proxy.
- Rewrite: `/payments/webhooks/razorpay` → strips `payments` → forwards to
  `http://localhost:4006/webhooks/razorpay` — **matches** payment-service's own
  mount exactly, including the raw `Buffer` body the gateway now forwards
  through to payment-service's signature check unmodified.
- This route never needed a JWT at all (Razorpay calls it directly, and
  payment-service verifies its own webhook signature), so it was never blocked
  by the login-route bug in the first place. The only reason it can't be
  exercised for real is that there's no live Razorpay account configured to
  actually send a webhook.

### `GET /api/gateway/health` — ✅ WORKING
- No middleware, no proxy — self-contained.
- Response: `200 { success: true, message: "Gateway is healthy", timestamp: new Date().toString() }`

### Configured but never wired to a route
2 of the gateway's 7 known downstream services still have a `config.SERVICES.*`
URL and a pre-built circuit breaker but **no `createProxy()` call anywhere
references them**: `searchService` (4002), `notificationService` (4004).
`inventoryService` (4007) also has no route yet — every one of inventory-service's
HTTP routes is still only reachable by calling it directly. `userService`,
`adminService`, `bookingService`, and now `paymentService` (webhook only — its
internal routes have no gateway route, by design, since booking-service is meant
to call them directly with the shared secret) are the four ever proxied to.

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

Mounted in `server.ts`: `app.use("/auth", authRoutes)` and
`app.use("/user", userRoutes)` (this second one was never mounted before — every
route in `routes/user.route.ts` was ⛔ UNREACHABLE). Global middleware:
`helmet → corsMiddleware → reqLogger → cookieParser → express.json → routes →
errorHandler`. `authRoutes` used to be mounted at `/api/v1/auth` — dropped the
version prefix (now plain `/auth`) so the gateway's generic one-segment-strip
rewrite can actually reach it; see `/api/users/auth/login`'s entry in §1.

### `POST /auth/send-otp` — ✅ WORKING
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

### `POST /auth/verify-otp` — ✅ WORKING
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

### `POST /auth/login` — ✅ WORKING
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

### `POST /auth/refresh` — ✅ WORKING (mostly)
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

## 7. Booking Service (port 4005)

**Builds and typechecks** (`tsc --noEmit` passes clean) — **not verified live**, no
reachable Postgres/Redis/Kafka in the environment this was built in, and no Prisma
migration has even been generated yet. Ported from `irctc-backend-main/booking-service`
(a plain-JS reference implementation) into TypeScript. Every route below is mounted
in `server.ts` at root, behind `getUserContext` (the gateway's `x-user-id` header) —
there is no internal-only or public route here, unlike inventory-service.

### `POST /bookings` — ✅ WORKING (logic-wise)
**Body** (`zCreateBooking`): `scheduleId: uuid`, `seatIds: string[]` (min 1),
`passengers: {name, age, gender: MALE|FEMALE|OTHER}[]` (same length as `seatIds`),
`idempotencyKey: string`, optional `fromStationId`/`toStationId: uuid` and
`fromSeq`/`toSeq: positive int` (segment booking — if both seq values are given,
`fromSeq` must be less than `toSeq`).
**Flow**: checks `idempotencyKey` against `IdempotencyRecord` (a retried request
returns the original response, not a duplicate booking) → calls inventory-service's
`GET /schedules/:id/availability` (rejects a non-`ACTIVE` or already-departed
schedule) → `GET /schedules/:id/seats` to price and validate each requested seat →
acquires an all-or-nothing Redis lock on the sorted seat set → creates the `Booking`
row (+ `BookingSeat`/`Passenger` children, status `PENDING`) → saga step 1: `POST
/seats/lock` on inventory-service (status → `SEATS_HELD`) → saga step 2: `POST
/orders` on payment-service (status → `PAYMENT_PENDING`, stores `paymentOrderId`).
Any failure at any step compensates everything already completed and releases the
Redis lock. **Depends on payment-service, which doesn't exist in this repo** — the
saga will always fail at step 2 today (`ECONNREFUSED` after 3 retries), and the
booking ends up `FAILED` with its seat hold released.
**Success**: `201 { success: true, data: { bookingId, status, totalAmount,
lockExpiresAt, seats, passengers, paymentOrder: {paymentOrderId, gatewayOrderId,
amount, currency, keyId} } }`.

### `GET /bookings` — ✅ WORKING (logic-wise)
**Query params** (`zGetUserBookings`): `status?` (any `BookingStatus` value),
`page?` (default 1), `limit?` (default 10, max 100).
**Success**: `200 { success: true, data: { bookings: [...], pagination: {page,
limit, total, totalPages} } }` — only the caller's own bookings, newest first.

### `GET /bookings/:bookingId` — ✅ WORKING (logic-wise)
`404 NotFoundError` if the booking doesn't exist *or* belongs to a different
user — the two cases are indistinguishable in the response, deliberately, so a
client can't enumerate other users' booking IDs.
**Success**: `200 { success: true, data: <BookingDetail> }`.

### `POST /bookings/:bookingId/verify-payment` — ✅ WORKING (logic-wise)
**Body** (`zVerifyPayment`): `razorpayPaymentId`, `razorpaySignature` (both
required strings). Client-side path for a browser that completes checkout
directly and wants an immediate synchronous confirmation, separate from the
`payment.success` Kafka path. Calls payment-service's `POST
/orders/:paymentOrderId/verify` — **depends on payment-service**, same caveat as
`POST /bookings`.
**Success**: `200 { success: true, data: { bookingId, paymentStatus } }`.

### `POST /bookings/:bookingId/cancel` — ✅ WORKING (logic-wise)
Works from any non-terminal status. `409 ConflictError` if already
`CANCELLED`/`CANCELLING`/`FAILED`/`EXPIRED`/`CONFIRMING`. If `CONFIRMED`, releases
seats via inventory-service and attempts a refund via payment-service (refund
failure is logged, not thrown — cancellation still succeeds; see booking-service's
own docs for why). If `PAYMENT_PENDING`/`SEATS_HELD`, just releases the held seats.
**Success**: `200 { success: true, message: "Booking cancelled successfully",
data: { bookingId, status: "CANCELLED", refundInitiated } }`.

### Kafka — consumer (`kafka/consumer/booking.consumer.ts`, group
`booking-service-group`)
| Topic | Handler | Status |
|---|---|---|
| `payment.success` | `handlePaymentSuccess` — confirms seats, marks the booking `CONFIRMED` | ⏳ never fires end-to-end — payment-service now publishes this correctly, but neither service has been run live, and payment-service has no real Razorpay credentials to ever actually capture a payment |
| `payment.failed` | `handlePaymentFailure` — releases held seats, marks the booking `FAILED` | ⏳ never fires end-to-end — same reason |
| `admin.schedule-cancelled` | `handleScheduleCancelled` — cancels every active booking on that schedule, refunds confirmed ones | ⏳ never fires — no caller anywhere in admin-service (no cancel-schedule feature exists) |

Both idempotent via CAS on `Booking.version`, not a separate `IdempotencyRecord`
row (unlike inventory-service's Kafka handlers) — re-delivery of the same event is
a safe no-op because the handler's status precondition (e.g. `booking.status ===
"PAYMENT_PENDING"`) will already be false the second time through.

### Kafka — producer (`kafka/producer/booking.producer.ts`)
| Method | Topic | Payload | Called from |
|---|---|---|---|
| `publishBookingConfirmed` | `booking.confirmed` | booking + train + seats + passengers + station names, **including `email`/`firstName`** | `handlePaymentSuccess` |
| `publishBookingCancelled` | `booking.cancelled` | bookingId + reason + refundAmount, **including `email`/`firstName`** | `cancelBooking`, `handleScheduleCancelled` |
| `publishBookingFailed` | `booking.failed` | bookingId + reason, **including `email`/`firstName`** | `handlePaymentFailure`, `handleScheduleCancelled`'s expiry-job counterpart |

All three enrich the event with the user's `email`/`firstName` via `userClient`
(an internal call to user-service's `GET /user/internal/:userId`) before
publishing — this is what makes notification-service's `booking.confirmed`/
`failed`/`cancelled` handlers (which read `.email` off the event, previously
always `undefined` since nothing ever populated it) actually able to send an
email, once these topics start firing.

### Data model
`Booking(id, userId, scheduleId, trainId, trainNumber, trainName, departureDate,
status enum, totalAmount, seatCount, fromStationId?, toStationId?, fromSeq?,
toSeq?, idempotencyKey unique, paymentOrderId unique?, lockExpiresAt?,
failureReason?, version) · BookingSeat(id, bookingId, seatId, seatNumber, seatType,
price; unique[bookingId,seatId]) · Passenger(id, bookingId, name, age, gender,
seatId?) · SagaLog(id, bookingId, step enum, status enum, request json?, response
json?, error?) · IdempotencyRecord(id, eventKey unique, response json?,
processedAt)`.

---

## 8. Payment Service (port 4006)

**Builds and typechecks** (`tsc --noEmit` passes clean) — **not verified live**,
no reachable Postgres/Kafka in the environment this was built in, and no real
Razorpay merchant account exists to test any gateway call against even if there
were. Ported from `irctc-backend-main/payment-service`. Every route is mounted at
root and behind `internalAuth` (a shared secret) except the public webhook — this
service has no user-facing routes at all.

### `POST /orders` — ✅ WORKING (logic-wise, blocked by missing credentials)
**Body** (`zCreatePaymentOrder`): `bookingId: string`, `amount: positive number`,
`userId: string`, `idempotencyKey: string`.
**Flow**: checks `idempotencyKey` against `IdempotencyRecord` → calls the active
gateway's `createOrder(amount, "INR", bookingId, {bookingId, userId})` → creates a
`PaymentOrder` row (status `CREATED`) with the gateway's own order id → writes a
`PaymentAuditLog` row (`ORDER_CREATED`). **Fails today**: `RazorpayGateway`
requires real `RAZORPAY_KEY_ID`/`RAZORPAY_KEY_SECRET`, which don't exist in this
environment — every call reaches Razorpay's real API and gets an auth error.
**Success**: `201 { success: true, data: { paymentOrderId, gatewayOrderId,
amount, currency, status, gatewayProvider, keyId } }`.

### `GET /orders/:paymentOrderId` — ✅ WORKING (logic-wise)
Returns the full `PaymentOrder` row plus its `auditLogs` and `refunds` relations,
newest-first — no field-level DTO shaping, unlike the other four routes (ported
as-is from the reference, which had none either).
**Success**: `200 { success: true, data: <PaymentOrder + auditLogs + refunds> }`.
`404 NotFoundError` if the id doesn't exist.

### `POST /orders/:paymentOrderId/verify` — ✅ WORKING (logic-wise, same credential caveat)
**Body** (`zVerifyAndCapture`): `gatewayPaymentId: string`, `gatewaySignature: string`.
**Flow**: idempotent if already `CAPTURED` (returns immediately with a
`message`) → `409 ConflictError` if not `CREATED` → verifies the signature via
HMAC-SHA256 (`crypto.timingSafeEqual`) → on success, captures (status →
`CAPTURED`) and publishes `payment.success`; on failure, marks `FAILED`,
publishes `payment.failed`, and throws `400 BadRequestError("Payment signature
verification failed", "INVALID_SIGNATURE")`.
**Success**: `200 { success: true, data: { paymentOrderId, status, gatewayPaymentId } }`.

### `POST /refunds` — ✅ WORKING (logic-wise, same credential caveat)
**Body** (`zInitiateRefund`): `paymentOrderId: string`, `amount: positive number`,
`reason?: string`, `idempotencyKey: string`.
**Flow**: idempotency check → `409` unless status is `CAPTURED`/`PARTIALLY_REFUNDED`
→ `409` if no `gatewayPaymentId` exists yet → validates
`totalRefunded + amount <= paymentOrder.amount` (a `400` before any gateway call
if it would exceed) → calls the gateway's `initiateRefund` → creates a `Refund`
row (status `INITIATED`) → updates `PaymentOrder.status` to `REFUND_INITIATED`.
**Success**: `201 { success: true, data: { refundId, paymentOrderId, status,
amount, gatewayRefundId } }`.

### `POST /webhooks/razorpay` — ✅ WORKING (logic-wise) — the only public route
No auth — instead, `gateway.verifyWebhookSignature` checks the
`x-razorpay-signature` header via HMAC against the raw request body (mounted
with `express.raw()`, registered before `express.json()` in `server.ts` so the
bytes reach this handler unparsed). `400` on missing/invalid signature.
**Flow**: parses the raw body → looks up the `PaymentOrder` by the webhook's
`gatewayOrderId` (`{status:"ignored", reason:"order_not_found"}` if none matches)
→ writes a `PaymentAuditLog` row for the raw payload → dispatches on `event`:
`payment.captured`/`payment.authorized` → capture (same as verify's success
path); `payment.failed` → mark `FAILED`, publish `payment.failed`;
`refund.processed`/`refund.created` → marks the matching `Refund` `COMPLETED`
and recomputes the parent order's status (`REFUNDED` vs `PARTIALLY_REFUNDED`
based on the running total); anything else → `{status:"ignored", event}`.
**Always responds `200`** for any recognized event, specifically so Razorpay's
webhook delivery system stops retrying — this is true even for `"ignored"`
outcomes.
**Success**: `200 <WebhookHandlingResult>` (shape varies by event/outcome).

### Kafka — producer only (`kafka/producer/payment.producer.ts`)
| Method | Topic | Called from |
|---|---|---|
| `publishPaymentSuccess` | `payment.success` | `handlePaymentCaptured` (webhook path), `verifyAndCapturePayment` (client-verify success path) |
| `publishPaymentFailed` | `payment.failed` | `handlePaymentFailed` (webhook path), `verifyAndCapturePayment` (signature-failure path) |

This service has no Kafka **consumer** — it never subscribes to anything.

### Data model
`PaymentOrder(id, bookingId, userId, amount, currency default "INR", status
enum, idempotencyKey unique, gatewayProvider default "razorpay", gatewayOrderId
unique?, gatewayPaymentId unique?, gatewaySignature?, failureReason?, metadata
json?, version) · Refund(id, paymentOrderId, amount, reason?, status enum,
idempotencyKey unique, gatewayRefundId unique?, failureReason?, metadata json?)
· PaymentAuditLog(id, paymentOrderId, action, gatewayResponse json?, metadata
json?) · IdempotencyRecord(id, eventKey unique, response json?, processedAt)`.

---

## 9. Full Kafka topic matrix

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
| `admin.schedule-cancelled` | admin-service — zero call sites, no cancel-schedule feature built | search-service (`cancelSchedule`), inventory-service (`cancelScheduleInventory`), booking-service (`handleScheduleCancelled`) | ⏳ never fires |
| `inventory.seat-availability-updated` | inventory-service (every seat-mutating operation) | search-service (`updateSeatAvailability`) | ⏳ never fires end-to-end — inventory-service can now publish it, but only once it receives a `SCHEDULE_CREATED` event to seed a schedule first, and neither service has been run live |
| `booking.confirmed` / `booking.failed` / `booking.cancelled` | booking-service (`handlePaymentSuccess`/`handlePaymentFailure`/`cancelBooking`/`handleScheduleCancelled`) — **now includes an `email` field on every publish** | notification-service (would now actually send an email — the previously-missing `email` field is populated) | ⏳ never fires end-to-end yet — booking-service only reaches most of these publish sites via `payment.success`/`payment.failed`, which payment-service can't actually fire without real Razorpay credentials |
| `payment.success` / `payment.failed` | payment-service (`handlePaymentCaptured`/`handlePaymentFailed`/`verifyAndCapturePayment`) — both fully implemented and publish correctly | booking-service (`handlePaymentSuccess`/`handlePaymentFailure`, both fully implemented and subscribed) | ⏳ never fires end-to-end — payment-service can't reach a real `CAPTURED`/`FAILED` state without a live Razorpay account to call, and neither service has been run live regardless |
| `dlq.booking-service` | n/a (would-be DLQ target) | notification-service (falls to `default`) | inert |
| `dlq.inventory-service` | inventory-service, on 3 failed retries | none | effectively inert (nothing consumes this service's own DLQ) |
| `dlq.search-service` | search-service, on 3 failed retries — internal catch blocks that used to make this rarely reached are now fixed, so it triggers on a genuine Elasticsearch outage | none | fires when it should now, still nothing consumes it |
| `dlq.notification-service` | notification-service, on 3 failed retries | notification-service itself (subscribed to `Object.values(KAFKA_TOPICS)`, falls to `default`) | fires occasionally, consumed as "unknown topic" by the same service that produced it |

`DLQ_MAX_RETRIES = 3` (`shared/constants/kafka-topics.ts`), shared by every
service's `withDLQ` wrapper (`shared/utils/dlqHanlder.ts`).

---

## 10. Shared error-response shapes

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

## 11. Services referenced but not implemented in this repo

None — every service named anywhere in this repo's config, Kafka topic
constants, and gateway `SERVICES.*` map now has a real directory, code, and
`docs/README.md` (`inventory-service`, `booking-service`, and `payment-service`
were the three that used to be listed here; see §6, §7, and §8). What's still
missing is not a *service*, but real Razorpay credentials to exercise
payment-service's gateway calls against, and admin-service's cancel-schedule
feature — both are runtime/feature gaps within existing services, tracked in
`missing.md`, not unbuilt services.


---

<a id="file-api-gateway-docs-md"></a>
## FILE: `docs/api-gateway-docs.md`

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


---

<a id="file-booking-service-docs-md"></a>
## FILE: `docs/booking-service-docs.md`

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
      "EX",
      LEADER_TTL_SECONDS,
      "NX",
    );
    return result === "OK";
  } catch (err) {
    // If Redis is down, skip this cycle rather than having all instances run
    logger.error("Failed to acquire expiry job leadership", {
      error: (err as Error).message,
    });
    return false;
  }
}
```

`SET key value EX seconds NX` is a single atomic Redis command — either this instance is the first to call it this cycle (and becomes leader) or it isn't (another replica already holds the key), with no race window between "check" and "set." `LEADER_TTL_SECONDS` (25s) is deliberately shorter than the sweep interval (30s default) so a crashed leader's lock expires before the next cycle would otherwise be blocked waiting for it. This is the same pattern inventory-service uses for its own lock-expiry job, just backed by Redis instead of a Postgres advisory lock (this service already holds a Redis connection for seat locks, so it was the natural choice here).

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

All five routes are proxied through the API Gateway (`/api/bookings/bookings*`, see `api-gateway/src/routes/index.ts`), and — since a later pass fixed the Gateway's login-routing bug (user-service dropped its `/api/v1` prefix) — a real JWT can now be obtained through `POST /api/users/auth/login`, so these routes are reachable in principle for the first time. Booking-service still can't complete a real booking end-to-end regardless, since payment-service has no real Razorpay credentials to call — see its own docs.

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

1. ~~payment-service does not exist in this repo~~ **payment-service now exists** (ported in a later pass), matching the contract `paymentClient.ts` was written against (`POST /orders`, `GET /orders/:id`, `POST /orders/:id/verify`, `POST /refunds`). It has no real Razorpay merchant account behind it, though, so every saga step that calls `paymentClient` still fails today — now with an auth error from the real Razorpay API instead of a connection error. See `payment-service/docs/README.md`.
2. **`admin.schedule-cancelled` never fires.** admin-service has no cancel-schedule feature built at all (route, controller, or service) — `handleScheduleCancelled` is fully implemented and subscribed to, but structurally unreachable until that feature exists. Same situation as inventory-service's identical dependency on this event.
3. **No Prisma migration exists yet.** `prisma/schema.prisma` was authored for this port but `npx prisma migrate dev` has not been run in the environment this was built in (no reachable Postgres) — there is no `prisma/migrations/` directory yet, unlike every other service in this repo which has at least one `..._init` migration checked in.
4. **Not verified against live infrastructure.** No Postgres, Redis, or Kafka broker was reachable while this was built — `npx tsc --noEmit` passing clean is the only verification performed. Treat every "works when called" description above as "the logic reads correctly and the types check," not observed runtime behavior.
5. **Gender is validated as an enum (`MALE`/`FEMALE`/`OTHER`)**, tighter than the reference implementation, which stored it as a free-form string with no validation at all. This is a deliberate value-add consistent with this repo's stricter Zod-everywhere convention, not a behavior port — a client sending any other string now gets a 400 instead of having it silently stored.
6. ~~The gateway's own pre-existing login-route bug means no token can currently be minted through the gateway~~ **Fixed in a later pass** — user-service dropped its `/api/v1` prefix, so `POST /api/users/auth/login` now issues a real JWT, and `requireAuth` can set `x-user-id` before proxying to booking-service. Not verified live (no reachable Postgres/Redis/Kafka), so this is confirmed by re-reading the fix, not an observed request through the gateway.
7. **`verifyPayment`'s return shape differs slightly from the reference.** The reference returned `{ bookingId, status: 'CONFIRMED', message: 'Already confirmed' }` for the already-confirmed case and `{ bookingId, paymentStatus }` otherwise — two different shapes from one function. This port normalizes both to `{ bookingId, paymentStatus }` (using `"CONFIRMED"` as the `paymentStatus` value in the already-confirmed case) for a single consistent response shape, matching this repo's preference for one predictable DTO per endpoint.


---

<a id="file-implementation-plan-md"></a>
## FILE: `docs/implementation-plan.md`

# IRCTC Backend — Implementation State

This document exists so a fresh Claude session (or a new engineer) can understand the
whole system without re-reading every file first. It describes **what is actually
built and how it actually behaves today** — not a roadmap, not a wishlist. Every claim
below was verified directly against source during a full-repo audit; where something
is broken, that's stated as a fact about current behavior, not flagged as a to-do.

For the exact HTTP/Kafka contract (methods, paths, request/response shapes, per-route
status), see [`api-contract.md`](./api-contract.md) in this same folder. This document
is the narrative map; that one is the reference table.

Per-service deep dives with pasted-in code also exist at `<service>/docs/README.md`
for admin-service, api-gateway, notification-service, search-service,
inventory-service, booking-service, and payment-service (not user-service — see
below). Those are more detailed than this file for their one service; this file is
the only place that covers all seven plus how they connect.

---

## 1. What this system is

IRCTC-backend is a train-ticket-booking backend split into independent services that
talk to each other over Kafka, sitting behind a single API gateway. The intended
shape is: users sign up and log in (user-service), admins create stations/trains/
routes/schedules (admin-service), those changes get indexed into Elasticsearch for
fast searching (search-service) and seeded into inventory-service's seat tracking,
a booking flow reserves seats and orchestrates payment (booking-service, backed by
inventory-service and payment-service), with notification-service emailing the
user at each step.

**All six conceptual services now exist in this repo**: admin-service,
user-service, search-service, inventory-service, booking-service, and
payment-service, plus the always-present api-gateway and notification-service.
Every service typechecks; none has been verified against live infrastructure, and
payment-service specifically has no real Razorpay merchant account to test its
gateway calls against even if Postgres/Kafka were reachable — see its own
per-service note in §5 and `docs/README.md`.

---

## 2. Services at a glance

| Service | Port | Purpose | Builds? | Runtime status |
|---|---|---|---|---|
| **api-gateway** | 4000 | Single entry point; JWT auth, rate limiting, circuit breakers, reverse-proxies to downstream services | ✅ Yes | Starts fine; login now works end-to-end through it, but most other proxied routes are still broken or unverified (see §6) |
| **user-service** | 4001 | Signup (email+OTP), login, refresh-token rotation, user profile | ✅ Yes, `tsc --noEmit` passes clean | Auth routes still work as before; profile routes are now mounted (`updateProfile`/`deleteProfile` implemented, previously empty stubs that hung every request), `verifyOtp` no longer leaks the password hash, the welcome email is now sent, and a new internal-only user-lookup route exists for other services — see §5 and §6 |
| **search-service** | 4002 | Elasticsearch-backed train/station search, kept in sync via Kafka | ✅ Yes, `tsc --noEmit` passes clean | Code is complete and typechecks; **not verified live** (no reachable Elasticsearch/Kafka in this sandbox). The wrong-import build failure documented here previously was already stale by the time this was checked — the actual blockers were a bad controller import and three dead files referencing config fields that don't exist — see §5 and §6 |
| **admin-service** | 4003 | Staff-facing station/train/route/schedule management, publishes domain events | ✅ Yes, `tsc --noEmit` passes clean | Code is complete and typechecks; **not verified live** (no reachable Postgres/Kafka in this sandbox). All 4 routes now mount, `createRoute`'s inverted check and `createStation`'s missing `await` are fixed, `ROUTE_CREATED` now publishes, and every route is behind `getUserContext` — see §5 and §6 |
| **notification-service** | 4004 | Pure Kafka consumer — renders and sends transactional emails via Resend | ✅ Yes | Starts and runs correctly; only 2 of 5 email types it can send are ever actually triggered |
| **inventory-service** | 4007 | Per-schedule seat inventory: tracks available/locked/booked counts and individual seat state, supports segment (partial-journey) locking, kept in sync via Kafka from admin-service's schedule events | ✅ Yes, `tsc --noEmit` passes clean | Code is complete and typechecks; **not verified live** — this sandbox has no reachable Postgres/Kafka, so `npm run dev` and an actual HTTP/Kafka round-trip haven't been exercised. `admin.schedule-created` does fire now (admin-service's schedule route is mounted — see Tier 2 below); this service just isn't proxied through the gateway, so triggering it still means calling admin-service directly (see api-gateway row) |
| **booking-service** | 4005 | Orchestrates the booking saga (hold seats → create payment → confirm seats) across inventory-service and payment-service, with Redis distributed locking, CAS-based state transitions, and a background expiry job | ✅ Yes, `tsc --noEmit` passes clean | Ported from the `irctc-backend-main` reference (JS) into TypeScript. **Not verified live** (no reachable Postgres/Redis/Kafka in this sandbox); payment-service now exists (see next row), but with no real Razorpay account, so the saga still can't complete a real booking end-to-end — see §5 |
| **payment-service** | 4006 | Razorpay gateway adapter (`BaseGateway`/`RazorpayGateway`/factory pattern): creates orders, captures via webhook or client-side verify, refunds, publishes `payment.success`/`payment.failed` | ✅ Yes, `tsc --noEmit` passes clean | Ported from the `irctc-backend-main` reference (JS) into TypeScript. **Not verified live** — no reachable Postgres/Kafka in this sandbox, and no real Razorpay credentials exist to exercise any gateway call against even if there were — see §5 |

`admin-service` previously had no `.env.example` of its own and, per §7, once had
a real (gitignored, uncommitted) `.env` that was byte-for-byte user-service's —
both now have proper, distinct `.env.example` files with the ports the Gateway
already assumed (`4003` for admin, `4001` for user).

---

## 3. Architecture

```mermaid
flowchart TB
    Client(["Client / Frontend"])

    subgraph Edge["Edge"]
        GW["API Gateway :4000<br/>auth · rate limit · circuit breaker · proxy"]
    end

    subgraph Services["Services"]
        US["User Service :4001<br/>✅ builds + typechecks<br/>signup/login/refresh/profile, Postgres+Redis"]
        AS["Admin Service :4003<br/>✅ builds + typechecks, not verified live<br/>stations/trains/routes/schedules, Postgres"]
        SS["Search Service :4002<br/>✅ builds + typechecks, not verified live<br/>Elasticsearch-backed search"]
        NS["Notification Service :4004<br/>pure Kafka consumer, sends email via Resend"]
        IS["Inventory Service :4007<br/>✅ builds + typechecks, not verified live<br/>seat inventory + segment locks, Postgres"]
        BS["Booking Service :4005<br/>✅ builds + typechecks, not verified live<br/>saga orchestration, Postgres+Redis"]
        PS["Payment Service :4006<br/>✅ builds + typechecks, not verified live<br/>Razorpay gateway adapter, Postgres"]
    end

    subgraph Infra["Infrastructure (docker-compose)"]
        PG[("Postgres :5432")]
        RD[("Redis :6379")]
        KF{{"Kafka :9092/:9093<br/>+ Zookeeper"}}
        ES[("Elasticsearch :9200<br/>+ Kibana :5601")]
    end

    Client --> GW
    GW -- "proxies (login fixed;\nprofile still 404s)" --> US
    GW -- "proxies (broken: method mismatch)" --> AS
    GW -.->|"configured, no route wired up"| SS
    GW -.->|"configured, no route wired up"| NS
    GW -.->|"configured, no route wired up"| IS
    GW -- "proxies (wired up; reachable now\nthat login can mint a real JWT)" --> BS
    GW -- "webhook route proxied,<br/>no real Razorpay account to test" --> PS
    IS --> PG
    KF -.->|"would populate schedule/seat rows,<br/>if anyone ever calls POST /schedules/schedule"| IS

    US --> PG
    US --> RD
    US -- "notification.otp-email<br/>notification.welcome-email" --> KF
    AS --> PG
    AS -- "admin.station-created ✅ fires<br/>admin.train-created ✅ fires<br/>admin.route-created ✅ fires<br/>admin.schedule-created ✅ fires (route now mounted)<br/>admin.schedule-cancelled ❌ no caller, feature not built" --> KF
    KF --> SS
    SS --> ES
    KF --> NS
    BS --> PG
    BS --> RD
    BS -- "holds/confirms seats via HTTP" --> IS
    BS -- "HTTP — orders/verify/refunds,<br/>fails without real Razorpay creds" --> PS
    BS -- "booking.confirmed/.cancelled/.failed<br/>✅ fire (carry an email field now)" --> KF
    KF --> BS
    PS --> PG
    PS -- "payment.success/.failed<br/>✅ fire (webhook + client-verify paths)" --> KF
```

---

## 4. Infrastructure (`docker-compose.yml`)

| Container | Image | Ports | Used by |
|---|---|---|---|
| `postgres` | `postgres:15` | 5432 | admin-service, user-service (via Prisma) |
| `pgadmin` | `dpage/pgadmin4` | 8081 | Postgres admin UI |
| `redis` | `redis/redis-stack:6.2.6-v19` | 6379, 8001 | api-gateway (rate limiting), user-service (OTP/session/cache) |
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | 2181 | Kafka |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | 9092 (internal), 9093 (host) | every service that produces/consumes events |
| `kafka-ui` | `provectuslabs/kafka-ui` | 8080 | Kafka inspection UI |
| `elasticsearch` | `elasticsearch:8.12.0` | 9200 | search-service |
| `kibana` | `kibana:8.12.0` | 5601 | Elasticsearch inspection UI |

None of the five application services themselves are containerized in
`docker-compose.yml` — only their infrastructure dependencies are. Each service is
run locally (`npm run dev`) against these containers.

Note the Kafka broker address split: some services' `.env` point at `localhost:9092`
(admin-service, user-service) and others at `localhost:9093` (search-service,
notification-service) — both are valid per the compose file's dual listener setup
(`9092` internal/container-network, `9093` host-mapped), so this isn't a bug, just
worth knowing if you're comparing `.env` files side by side and the port looks
inconsistent.

---

## 5. Per-service notes

### api-gateway
The only service every client request is meant to go through. Middleware order:
`cors → helmet → reqLogger → (conditional raw-body for a Razorpay webhook path that
has no route) → json/urlencoded → cookieParser → morgan (dev only) → /health →
/api/* → notFound → errorMiddleware`.

Auth is JWT-based: `requireAuth` reads `Authorization: Bearer <token>` or an
`accessToken` cookie, verifies it, and attaches `req.user = { id }` plus forwards
identity downstream via an `x-user-id` header (downstream services trust this header
rather than re-verifying the JWT themselves — see user-service's `getUserContext`).

Proxying goes through a hand-rolled circuit breaker (`CLOSED → OPEN → HALF_OPEN`,
threshold 5 failures, 60s reset) per downstream service. Seven breakers are
constructed (one per conceptual service) but only **two are ever exercised** —
`userService` and `adminService` — because those are the only two `createProxy(...)`
calls anywhere in the route table. The path-rewrite rule is simple and applies
uniformly: strip the first path segment after `/api`, forward the rest verbatim.
This simplicity is exactly why routes break — see §6.

### user-service
Owns identity: `User` table in Postgres (id, firstName, lastName, email, optional
password, emailVerified, timestamps — no roles/sessions table, no OAuth fields even
though `GOOGLE_CLIENT_ID`/`SECRET` are read into config and never used — deliberately
out of scope for this pass, see below). Sessions live entirely in Redis, not Postgres:

- `otp:session:<id>` — pending-signup metadata + HMAC'd OTP, TTL-bound
- `otp:rate:<email>` / `otp:attempt:<email>` — hourly request cap / verify-attempt cap
- `refresh:<userId>:<deviceId>` — current refresh-token JTI, one entry per device (a
  new login on a second device doesn't invalidate the first — they're keyed
  independently)
- `user:<userId>` — cached profile, read by `getUserProfile`

Access tokens are short-lived (15m), refresh tokens long-lived (7d), both delivered
as httpOnly/secure/sameSite=strict cookies, never in the JSON body. Refresh rotation
detects token reuse (a mismatched JTI triggers session revocation) — a real, working
security feature, not just a checkbox.

**Fixed this pass**: `user.route.ts` (profile routes) is now mounted at `/user` in
`server.ts` — it was fully written but never wired up before. `updateProfile` and
`deleteProfile` were empty `// TODO` stubs that hung every request (`asyncHandler`
never sends a response if the wrapped function doesn't); both are implemented now
(`updateProfile` only allows changing `firstName`/`lastName` — email and password
are deliberately excluded, those need their own verification-gated flows).
`verifyOtp` no longer returns the bcrypt password hash to the client — the service
layer now strips it, matching every other read path. `sendWelcomeEmail` is now
actually called from `verifyOtp` (fire-and-forget, log-only on failure) — the
producer method and the notification-service consumer were both already correct,
nothing had ever called the producer. `getUserProfile`'s cache-miss bug (returning
the unscrubbed row instead of the scrubbed copy it just cached) is fixed. A new
`GET /user/internal/:userId` route exists behind a shared-secret `internalAuth`
middleware (mirroring the pattern already built for inventory-service) — needed by
booking-service, built later, to resolve a user's profile without a JWT. A missing
`Express.Request.user` type augmentation (why `tsc --noEmit` used to fail) is added.
The unused Mongoose/MongoDB wiring (`config/db.ts`, imported in `index.ts` but never
called — the real datastore is Postgres) is deleted outright, along with a
`types/index.ts` carrying the same unrelated RAG/`KnowledgeDoc` dead code found in
admin-service. `INTERNAL_SERVICE_KEY` is no longer a dead config field — the new
internal route actually reads it now.

**Still dead, out of scope for this pass**: `SENDGRID_API_KEY`, `GOOGLE_CLIENT_ID/SECRET`,
`RESEND_API_KEY`, `MAIL_SEND`, `NODE_ENV` — read into config, never consumed anywhere
in this service.

**Not verified live** — fixed and typechecked (`tsc --noEmit` passes clean) in a
sandbox with no reachable Postgres/Redis/Kafka.

### admin-service
Owns the staff-facing catalog: Station, Train (+ Seats), Route (+ RouteStations,
one route per train, enforced by a unique constraint), Schedule (one per
`(trainId, departureDate)`). All four resources are modeled cleanly in Prisma.

`config/index.ts` was present but **empty**, and `index.ts`/every config
submodule imported a `config/db.ts` that never existed anywhere in the project —
this is what actually blocked the build (the root README's "two missing files"
framing was slightly imprecise: one file was missing, the other existed but
empty). Fixed by populating `config/index.ts` (same shape as inventory-service's:
`SERVICE_NAME`, `PORT` default 4003, `DATABASE_URL`, `ALLOWED_ORIGINS`,
`KAFKA_BROKER`, `KAFKA_CLIENT_ID`, `INTERNAL_SERVICE_KEY`) and dropping the dead
`config/db.ts` import from `index.ts` — Prisma's own `config/prisma.ts` already
owns the DB connection, no separate connect call was ever needed. Also fixed:
`createStation` now `await`s the service call and returns a correct
`"Station created successfully"` message (was `"OTP sent successfully"`);
`createRoute`'s existence check was inverted (`if (!existingRoute) throw
"already exists"` — fixed to `if (existingRoute) throw`, also fixing the
"existis" typo); `getTrainById` is now mounted as `GET /trains/train/:trainId`
with the controller's own `:trainId` param name (was `POST /trains/route/:id`);
`schedule.route.ts` is now mounted at `/schedules` in `server.ts`; the
`ROUTE_CREATED` publish (previously commented out) now fires after a route is
created; every route across all three routers is now behind `getUserContext`
(previously zero auth was wired up anywhere in this service); the dead
`types/index.ts` (RAG/`KnowledgeDoc` types unrelated to this service's domain,
imported `mongoose` for no reason) was deleted. `admin.schedule-cancelled` still
has no caller — no cancel-schedule feature (route/controller/service) exists,
and building one wasn't in scope for this pass.

This service is a pure Kafka **producer** — it has no consumer of its own anywhere
in `src/kafka/`. **Not verified live** — fixed and typechecked
(`tsc --noEmit` passes clean) in a sandbox with no reachable Postgres/Kafka, so
no actual HTTP request or Kafka publish has been exercised yet.

### search-service
The only service backed by Elasticsearch instead of Postgres. Two indices:
`stations` (edge-ngram autocomplete + completion suggester) and `trains` (a train
document with a nested `route` array and a `schedules` array — routes and schedules
are not separate ES indices even though `ROUTE_INDEX`/`SCHEDULE_INDEX` constants
exist for them). It's a pure Kafka **consumer** with a small HTTP surface bolted on
top.

The compile failure previously documented here (a singular/plural import mismatch,
a default/named export mismatch on `errorMiddleware`) was already stale — both had
apparently been fixed independently before this pass started. What actually blocked
the build: `search.controller.ts` imported `searchService` from a nonexistent
`../services/inventory.service` (should be `../services/search.service` — another
instance of the copy-paste pattern, since that file's whole top doc-comment was
also admin-service's schedule-controller comment, now removed), and three files
(`config/redis.ts`, `middlewares/auth.middleware.ts`,
`middlewares/rate-limiting.middleware.ts`) — the unused api-gateway-style
scaffolding previously described as merely "inert" — actually reference config
fields (`REDIS_URL`, `JWT_ACCESS_SECRET`, `RATE_LIMIT_MAX_REQUESTS`,
`RATE_LIMIT_WINDOW_MS`) that don't exist on this service's trimmed-down `Config`
type. Since nothing outside that three-file cluster imported any of them, and
`tsc` type-checks every file matched by `include` regardless of whether it's
actually imported, this dead code blocked the whole service from compiling even
though it was functionally inert. All three files were deleted (confirmed unused
first). Also fixed: `searchTrains` now returns its real search results instead of
a hardcoded message; `debugStations`/`debugTrains` now call `getAllStations`/
`getAllTrains` instead of both calling `autocompleteStation`; `indexStation` now
sets the `name` field on the document it writes; `indexStation`/`indexSchedule`/
`cancelSchedule`/`updateSeatAvailability` no longer swallow their own
Elasticsearch errors internally, so `withDLQ`'s retry-then-DLQ path can actually
trigger; a `notFound` 404 handler (previously written but never mounted) is now
registered before the error middleware.

**Not verified live** — fixed and typechecked (`tsc --noEmit` passes clean) in a
sandbox with no reachable Elasticsearch or Kafka broker.

### notification-service
The simplest service in the repo: no HTTP routes at all (not even its own health
check — `server.ts` registers zero routes; the Express app only exists so the
process has something listening). It subscribes to **every** topic in
`KAFKA_TOPICS` (`Object.values(KAFKA_TOPICS)`, not an explicit list), handles 5 of
them, and logs-and-drops everything else including its own DLQ topic. Email
delivery goes through Resend (not SendGrid, despite `SENDGRID_API_KEY` being read
into config) with a 3-attempt retry inside `email-service.ts`, separate from the
Kafka-level 3-retry-then-DLQ mechanism in `shared/utils/dlqHanlder.ts`.

### inventory-service
Owns per-schedule seat state: `ScheduleInventory` (one row per admin-service
schedule, aggregate available/locked/booked counters recomputed from actual seat
rows rather than trusted as a running total), `SeatInventory` (one row per physical
seat per schedule), `SeatSegmentLock` (partial-journey locks — two segments overlap
when `a.fromSeq < b.toSeq AND b.fromSeq < a.toSeq`), `RouteStop` (ordered station
list per schedule, for resolving a station to its sequence number), and
`IdempotencyRecord` (guards the Kafka consumer against reprocessing).

It's a pure Kafka **consumer** (`admin.schedule-created` → `initializeInventory`,
`admin.schedule-cancelled` → `cancelScheduleInventory`) with a small HTTP surface:
`GET /schedules/:id/availability` (public), `GET /schedules/:id/seats` (end user via
gateway, or booking-service via an internal shared-secret header), and
`POST /seats/{lock,unlock,confirm,cancel-booking}` (internal-only, for
booking-service's saga). A background job (`utils/lockExpiry.ts`) uses a Postgres
advisory lock for leader election so only one running instance releases expired
seat/segment locks on an interval, then republishes `inventory.seat-availability-updated`.

Seat mutations use row-level `FOR UPDATE NOWAIT` locks plus a `retryTransaction`
wrapper that retries on Postgres serialization/deadlock errors — deliberate
pessimistic concurrency control, no Redis involved (unlike booking-service's
distributed locks).

Two things worth flagging about how this service reached its current state: the
Prisma schema, the Kafka consumer's event types, and the service layer were found
mid-session having reverted to an earlier stub/scaffold state (matching
admin-service's schema and a copy-pasted admin-style controller) despite a more
complete implementation having existed moments earlier in the same working
session — the version described here is the restored/completed one, verified via
`tsc --noEmit` and `prisma generate` only. **This has not been run against a live
Postgres/Kafka** — no broker or database was reachable in the environment this was
built in, so the HTTP routes and Kafka consumer are untested beyond static
type-checking.

### booking-service
The one service that talks to every other service for a single request. Owns
`Booking` (+ `BookingSeat`, `Passenger` children), `SagaLog` (an audit trail of
every saga step attempted, read back to know what to compensate on failure), and
`IdempotencyRecord` (guards `POST /bookings` against duplicate submissions).

It's a **saga orchestrator**: `POST /bookings` acquires a Redis distributed lock
on the requested seats (all-or-nothing, via a Lua script), holds them in
inventory-service, opens a payment order in payment-service, and returns payment
details to the client. A Kafka consumer reacts to `payment.success`/`payment.failed`
to confirm or release the held seats, and to `admin.schedule-cancelled` to cancel
every active booking on a cancelled schedule. Every status transition goes through
an optimistic-lock (CAS) helper keyed on the booking's own `version` column, so the
payment webhook, a user's cancel request, and the background expiry job can never
double-process the same booking even if they race — whichever one loses the CAS
throws `StaleStateError` and bails out silently rather than corrupting state.

Ported from `irctc-backend-main/booking-service` (a plain-JS reference
implementation) into TypeScript, following this repo's stricter conventions: every
request body validated with Zod (the reference validated manually), no `any`, and
every downstream service's response shape typed locally in `types/index.ts` rather
than imported across service boundaries. The saga logic itself — the three forward
steps, the three compensations, the CAS pattern, the segment-lock key scheme — is
an unmodified port of the reference's business logic.

Two prerequisites were added elsewhere in this repo to support this service:
admin-service gained a new `GET /stations/station/internal/:stationId` route
(behind `internalAuth`, mirroring inventory-service's existing internal-route
pattern) so booking-service's `stationClient` can resolve a station's name for
segment-booking confirmation emails; and the API Gateway gained proxy routes for
all five of booking-service's HTTP endpoints (see `api-gateway/src/routes/index.ts`).

**Cannot complete a real booking end-to-end today** — payment-service now exists
(see next section) and its saga path (`paymentClient.ts` → `POST /orders`) is
structurally correct, but there's no real Razorpay merchant account behind
payment-service, so `executeCreatePayment` fails with an auth error against the
real Razorpay API rather than a connection error. Also still blocked:
`admin.schedule-cancelled` never fires (admin-service has no cancel-schedule
feature — see its own section above), so `handleScheduleCancelled` is
unreachable in practice despite being fully implemented. **Not verified against
a live Postgres, Redis, or Kafka** — no migration has even been generated yet for
this service's Prisma schema, since no database was reachable while it was built;
`npx tsc --noEmit` passing clean is the only verification performed.

### payment-service
The only service that talks to a real external system (Razorpay) rather than
just other services in this repo. Owns `PaymentOrder` (one row per booking's
payment attempt), `Refund` (one row per refund, full or partial), `PaymentAuditLog`
(an append-only trail of every gateway interaction — order created, webhook
received, signature verified, refund initiated — for reconstructing what actually
happened independent of the current row's status), and `IdempotencyRecord`.

Structured around an **adapter pattern**: `BaseGateway` is an abstract class
defining six methods (`createOrder`, `verifyPaymentSignature`,
`verifyWebhookSignature`, `fetchPayment`, `initiateRefund`, `fetchRefund`);
`RazorpayGateway` is the only concrete implementation so far, chosen by
`gateway.factory.ts`'s singleton based on `config.PAYMENT_GATEWAY`.
`payment.service.ts` never imports the Razorpay SDK directly — only through this
interface — so a second gateway (Stripe, etc.) could be added later without
touching the business logic at all.

A payment can be captured **two different ways that converge on the same state**:
a public webhook (`POST /webhooks/razorpay`, called by Razorpay's own servers,
signature-verified via HMAC) and a client-side verify call
(`POST /orders/:id/verify`, called by booking-service right after checkout
completes in the browser). Both check the `PaymentOrder`'s current status before
acting, so whichever arrives first performs the capture and the second is a safe,
idempotent no-op.

Ported from `irctc-backend-main/payment-service` (a plain-JS reference
implementation) into TypeScript, following this repo's stricter conventions: Zod
validation on the three JSON routes (the reference validated manually), no `any`,
and the gateway interface modeled as a TypeScript abstract class rather than a
duck-typed object passed around at runtime. The adapter pattern itself, the
idempotency scheme, the webhook event-type dispatch, and the refund
running-total validation are an unmodified port of the reference's business logic.

Every route except the public webhook is behind `internalAuth` — this service
has no user-facing routes at all; booking-service is the only intended internal
caller, and the client's browser talks to Razorpay's own checkout widget
directly, never to this service.

**Not verified against a live Postgres or Kafka, and has no real Razorpay
credentials to test against even if it did** — there is no Razorpay merchant
account behind this port, so every gateway call (`createOrder`,
`verifyPaymentSignature` against a real signature, `initiateRefund`) fails with
an auth error against the real Razorpay API. No Prisma migration has been
generated yet either. `npx tsc --noEmit` passing clean is the only verification
performed.

---

## 6. Why nothing currently works end-to-end

This is the priority-ranked list of what's actually blocking the system, synthesized
from a full audit of every service. See `api-contract.md` for the file:line-level
detail behind each line.

**Tier 1 — a service can't even start:**
- ~~admin-service never builds~~ **Fixed.** `config/index.ts` was present but
  empty and `index.ts` imported a `config/db.ts` that never existed anywhere in
  the project — populated the former, dropped the dead import to the latter
  (Prisma's own `config/prisma.ts` already owns the DB connection). See §5.
- ~~search-service never compiles~~ **Fixed.** The originally-documented cause
  (a singular/plural import mismatch, a default/named export mismatch) was
  already stale by the time this was checked — the real blockers were a
  controller importing from a nonexistent path and three dead scaffold files
  referencing config fields that don't exist. See §5.

**Tier 2 — a service builds and starts, but its main entry points are broken:**
- ~~`POST /api/users/auth/login` forwards to `/auth/login`, but user-service
  mounted login at `/api/v1/auth/login`~~ **Fixed** — the gateway's "strip one
  segment" rewrite rule is deliberately generic and uniform across every
  proxied service, so rather than special-casing it for this one route,
  user-service's `server.ts` now mounts auth routes at plain `/auth` (dropping
  the version prefix), matching every other service in this repo. Login is
  the first gateway-proxied route confirmed structurally correct end-to-end.
  Two gateway-side issues remain: (b) `GET /api/users/user/profile` forwards to
  `/user/profile`, which now exists on user-service (`user.route.ts` is mounted at
  `/user` — see §5), but that file only defines `POST`/`PUT`/`DELETE
  /profile`, no `GET`, so the method mismatch remains; (c) the two admin routes
  (`GET /api/admins/stations/station`, `GET /api/admins/trains/train`) are
  registered as `GET` at the gateway but admin-service only defines `POST` for
  those paths — a method mismatch, still unfixed on the gateway side (admin-service
  itself now builds and runs, see §5, but the gateway can't reach it correctly yet).
  Both remaining ones are gateway-side fixes, not yet done.
- ~~user-service didn't typecheck~~ **Fixed** — `middlewares/user-context.middleware.ts`
  accesses `req.user`, but no `Express.Request` augmentation existed anywhere in the
  service to give `Request` a `user` field; added `types/express.d.ts`, matching the
  same fix already applied to admin-service and inventory-service.

**Tier 3 — real logic bugs that would misbehave once the above is fixed:**
- ~~`admin-service`'s `createRoute` had an inverted existence check~~ **Fixed** —
  was `if (!existingRoute) throw "already exists"` (blocking every train's *first*
  route), now `if (existingRoute) throw ConflictError(...)`.
- ~~`station.controller.createStation` didn't `await` the service call~~ **Fixed**
  — now awaited, and returns the correct `"Station created successfully"` message.
- ~~`admin-service`'s `getTrainById` was unreachable~~ **Fixed** — now mounted as
  `GET /trains/train/:trainId`, matching the controller's own param name.
- ~~`user-service`'s `verifyOtp` returned the newly-created user including the
  bcrypt password hash~~ **Fixed** — the service layer now strips `password`
  before returning, matching every other read path.
- ~~`user-service`'s `getUserProfile` returned the unscrubbed row on a cache
  miss~~ **Fixed** — now returns the same scrubbed copy it just cached instead
  of the raw row it just fetched.
- ~~`user-service`'s `updateProfile`/`deleteProfile` were empty `// TODO`
  stubs~~ **Fixed** — both implemented (`updateProfile` only allows
  `firstName`/`lastName`; `deleteProfile` deletes the row and clears the Redis
  cache entry), and `user.route.ts` is now mounted so they're reachable.
- ~~`search-service`'s `searchController.searchTrains` discarded its real search
  results~~ **Fixed** — now returns the actual result set instead of a
  hardcoded, copy-pasted `"Train created successfully"` message.
- ~~`search-service`'s `debugStations` and `debugTrains` both called
  `autocompleteStation`~~ **Fixed** — now call `getAllStations`/`getAllTrains`
  respectively, which exist for exactly this purpose.

**Tier 4 — Kafka plumbing gaps (nothing crashes, data just silently doesn't flow):**
- ~~`admin.route-created` never fired~~ **Fixed** — the publish call in
  `createRoute` was commented out (search-service's `trains` index could never be
  populated); now fires after a route is created (failure caught+logged, matching
  `createTrain`'s pattern, not re-thrown).
- ~~`admin.schedule-created` never fired in practice~~ **Fixed** — the publish call
  was always correct, but its only trigger (`POST /schedules/schedule`) was never
  mounted in admin-service's `server.ts`; it's mounted now, so calling that route
  should let inventory-service's consumer receive a real event and initialize a
  schedule's seat rows — unverified live, but no longer blocked at the admin-service
  end.
- `admin.schedule-cancelled` has no caller anywhere — the producer method exists,
  nothing invokes it, and there's no cancel-schedule route/controller/service at all.
- ~~`notification.welcome-email` was fully wired on both ends but never
  fired~~ **Fixed** — `verifyOtp` now calls `sendWelcomeEmail` right after
  account creation (fire-and-forget, log-only on failure).
- `booking.confirmed`/`booking.failed`/`booking.cancelled` can never fire — no
  `booking-service` exists in this repo to publish them. Even if one existed, the
  typed payload shapes notification-service expects have no `email` field, so the
  consumer would silently warn-and-skip rather than send anything.
- ~~search-service's own DLQ safety net didn't work~~ **Fixed** —
  `indexStation`/`indexSchedule`/`cancelSchedule`/`updateSeatAvailability` each
  caught their own Elasticsearch errors and logged them, so nothing ever
  propagated out to `withDLQ`; all four now let errors propagate, so the
  existing retry-then-DLQ mechanism can actually trigger on an Elasticsearch
  outage instead of silently dropping the write. (`indexTrainRoute` already
  didn't swallow its own errors, so it needed no change.)

---

## 7. Cross-cutting patterns worth knowing before touching this codebase

- **A copy-paste-comment pattern showed up in at least four places**, always the
  same shape: a new controller/route file is built by copying an unrelated existing
  one and adapting the logic, but not the comments above it. Confirmed instances
  (all cleaned up as part of fixing each service — see §5):
  `search-service/src/controllers/search.controller.ts` and
  `routes/search.routes.ts` had comments describing admin-service's
  schedule-creation feature, `POST /schedule`, `station.route.ts`/`train.routes.ts`
  mounting; `user-service/src/services/user.service.ts`'s `getUserProfile` had a
  docstring describing OTP-based registration, copied from `auth.service.ts`'s
  `sendOtp`. If you see a comment that doesn't match the code
  under it, check whether it was copied from somewhere else in the repo before
  assuming it's just stale.
- **`admin-service/.env` used to be user-service's `.env`, byte-for-byte** (same
  `PORT=4001`, same `KAFKA_CLIENT_ID=irctc-service`, same OTP/token/mail settings
  that only make sense for user-service) — this would have collided with
  user-service on port 4001 the moment admin-service's build was fixed. Neither
  service actually has a real `.env` checked in (both are gitignored), so this was
  never externally visible; a fresh `admin-service/.env.example` with its own
  `PORT=4003` and admin-appropriate values now exists to prevent reintroducing it.
- **The `docs/auth.md` file** that used to document user-service's auth flow in
  depth was intentionally removed as part of setting up this `docs/` folder — this
  file and `api-contract.md` are now the source of truth for that flow instead.
- **Unrelated dependencies are pinned in nearly every service's `package.json`**:
  `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`,
  `mongoose`, `resend` (used in notification-service, unused elsewhere), and
  `otp-generator` show up across admin-service, api-gateway, notification-service,
  search-service, and user-service, almost none of them imported by the service
  they sit in. All five services also share the same dangling
  `"seed": "tsx src/services/seed.ts"` npm script, pointing at a file that doesn't
  exist anywhere in the repo.
- **Two different Zod import styles coexist**, sometimes within the same service:
  `import { z } from "zod"` (most schema files) vs. `import { ZodError } from
  "zod/v4"` (every service's `zod.formatter.ts`). Not currently broken, just an
  inconsistency worth normalizing if anyone touches Zod version pinning.
- **`shared/utils/dlqHanlder.ts` is a real, misspelled filename** ("Hanlder", not
  "Handler") that every consuming service imports under its real, misspelled name.
  The file's own header comment describes the correctly-spelled path, which doesn't
  match its own filename.
- **Downstream services trust an `x-user-id` header instead of re-verifying JWTs.**
  This only works if every request truly comes through the gateway's `requireAuth`
  first. Since the gateway's own proxied routes are currently all broken (§6),
  this trust boundary has never been exercised end-to-end in practice — worth
  re-checking once the routing bugs are fixed, to confirm nothing downstream can be
  reached by forging that header directly against a service's own port.
- **`dotenv.config()` runs too late in every service's `index.ts` that follows this
  pattern** (confirmed in user-service and inventory-service; worth checking the
  rest): the file's own top-level imports (`./server`, which transitively imports
  `./config`) execute before `dotenv.config()` is ever called on the line below
  them, so `config/index.ts`'s `process.env.*` reads see an environment that
  hasn't had `.env` loaded into it yet. Every value silently falls back to its
  hardcoded default (`PORT`, `DATABASE_URL`, `KAFKA_BROKER`, etc.) unless those
  variables happen to already be set in the real OS environment. Fixed in
  inventory-service by moving `dotenv.config()` to the first line of `index.ts`,
  before any other import.
- **A service's own `shared/`-style dependency can be uninstalled even when the
  service itself is** — `shared/` is its own npm package (own `package.json` +
  lockfile) separate from every service's `node_modules`, and its `kafkajs`
  dependency needs `npm ci` run inside `shared/` itself, not just inside whichever
  service imports `shared/utils/dlqHanlder.ts`. Without that, every consuming
  service's `tsc --noEmit` fails with `Cannot find module 'kafkajs'` on that one
  shared file, which reads as a code bug but is actually a missed install step.

---

## 8. Where to look next

- Exact request/response contract for every route and Kafka topic across all
  services: [`api-contract.md`](./api-contract.md).
- Deep, code-pasted walkthroughs for admin-service, api-gateway,
  notification-service, search-service, inventory-service, booking-service, and
  payment-service: `<service>/docs/README.md`.
- Root-level system map and cross-service flow diagrams: `/readme.md`.
- Standing punch list: `/missing.md`.


---

<a id="file-inventory-service-docs-md"></a>
## FILE: `docs/inventory-service-docs.md`

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


---

<a id="file-notification-service-docs-md"></a>
## FILE: `docs/notification-service-docs.md`

# Notification Service — Complete Guide

Single source of truth for the IRCTC Notification Service: what it does, how a message flows through it, and how each piece works — written in plain English, with the actual current code inline.

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [Message Lifecycle](#message-lifecycle)
5. [Component Breakdown](#component-breakdown)
   - [index.ts — Entry Point](#1-indexts--entry-point)
   - [server.ts — The (Mostly Empty) Express App](#2-serverts--the-mostly-empty-express-app)
   - [config/ — Configuration, Startup Checks, Kafka, Logger](#3-config--configuration-startup-checks-kafka-logger)
   - [kafka/email-consumer.ts — Reading Messages & Routing](#4-kafkaemail-consumerts--reading-messages--routing)
   - [services/email-service.ts — Actually Sending Email](#5-servicesemail-servicets--actually-sending-email)
   - [templates/index.ts — The Email HTML](#6-templatesindexts--the-email-html)
   - [types/index.ts](#7-typesindexts)
6. [Environment Variables](#environment-variables)
7. [Kafka Topics Reference](#kafka-topics-reference)
8. [Quick Start](#quick-start)
9. [Debugging Tips](#debugging-tips)
10. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **Notification Service** is a background worker, not a normal web API. Its only job is:

- **Listen** to Kafka for events published by other services (a user signed up, an OTP was requested, a booking succeeded or failed, etc.)
- **Send an email** for each event, using [Resend](https://resend.com) as the email-delivery provider
- **Retry** failed sends (both in-process, and at the Kafka level), and give up to a **dead-letter queue (DLQ)** if a message keeps failing

It does start an Express server and listen on a port (`4004` by default), but — unlike the API Gateway — that Express app has **no routes at all**. All the real work happens through the Kafka consumer, not through HTTP requests. Think of the port as "something is alive," not "you can call this over HTTP."

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                     OTHER MICROSERVICES                       │
│   user-service, booking-service, etc. — they PUBLISH events   │
│   to Kafka, they don't call this service directly.             │
└───────────────────────────┬───────────────────────────────────┘
                            │ publishes JSON messages
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                      KAFKA (localhost:9093)                    │
│  Topics that exist for this service (see shared/constants):   │
│   - notification.otp-email                                    │
│   - notification.welcome-email                                │
│   - notification.booking-email   (defined, nothing handles it)│
│   - notification.payment-email   (defined, nothing handles it)│
│   - booking.confirmed / booking.failed / booking.cancelled    │
│   - dlq.notification-service     (this service's own DLQ)     │
│                                                                 │
│  This service actually subscribes to EVERY topic that exists  │
│  in the shared registry, not just the ones above — see         │
│  Known Issue #6.                                                │
└───────────────────────────┬───────────────────────────────────┘
                            │ consumer group: "notification-service-group"
                            ▼
┌───────────────────────────────────────────────────────────────┐
│         NOTIFICATION SERVICE  (Express listens on :4004,       │
│                    but has no routes)                          │
│                                                                 │
│  1. emailConsumer.start() connects + subscribes to all topics  │
│  2. Every message is wrapped by withDLQ():                     │
│       - parse JSON                                             │
│       - track how many times THIS message has failed           │
│       - after 3 failures → forward it to the DLQ topic, move on│
│  3. handleMessage() looks at the topic name and calls the      │
│     matching handler (handleOtpEmail, handleWelcomeEmail, ...) │
│  4. The handler calls EmailService.sendXxxEmail(...)           │
│  5. EmailService builds an HTML string (templates/index.ts)    │
│     and calls Resend's API, retrying up to 3× with backoff     │
│     if Resend itself fails                                      │
└───────────────────────────┬───────────────────────────────────┘
                            │ HTTPS
                            ▼
                    ┌─────────────────────┐
                    │  Resend             │
                    │  (email delivery)   │
                    └─────────────────────┘
```

`docker-compose.yml` (at the IRCTC root) only starts the **infrastructure** — Postgres, Redis, Zookeeper, Kafka, Kafka UI. There's no container for the notification service itself; you run it locally with `npm run dev`, and it connects to Kafka at `localhost:9093` (the `PLAINTEXT_HOST` listener docker-compose exposes for that exact purpose).

---

## File Structure

```
notification-service/
├── src/
│   ├── index.ts                     # Loads env vars, starts the service, then app.listen()
│   ├── server.ts                    # Express app — just express.json(), no routes registered
│   ├── config/
│   │   ├── config.ts                # Env vars → one plain config object
│   │   ├── db.ts                    # Despite the name, does NOT touch a database — see below
│   │   ├── kafka.ts                 # Kafka client, consumer, DLQ producer, graceful shutdown
│   │   └── logger.ts                # Winston logger
│   ├── kafka/
│   │   └── email-consumer.ts        # Subscribes to topics, routes each message to a handler
│   ├── services/
│   │   └── email-service.ts         # Builds each email and sends it via Resend, with retries
│   ├── templates/
│   │   └── index.ts                 # HTML email templates + the TypeScript types for event data
│   └── types/
│       └── index.ts                 # Empty — no shared types defined here
├── docs/                            # This documentation
├── package.json
├── tsconfig.json
└── .env
```

It also reaches outside its own folder into the repo-wide `shared/` package:

```
shared/
├── constants/kafka-topics.ts   # KAFKA_TOPICS — every topic name used across all services
├── utils/dlqHanlder.ts         # withDLQ() — the retry + dead-letter-queue wrapper (note: filename
│                               # really is missing a "d", see Known Issue #12)
└── types/index.ts              # AuthenticatedRequest — not used by this service
```

`tsconfig.json` sets `rootDir: ".."`, which points one level above `notification-service/` (i.e. at the whole `IRCTC/` folder) — that's what lets it compile `.ts` files it reaches via `../../../shared/...` imports.

---

## Message Lifecycle

### Case A: An OTP email request comes in (happy path)

```
1.  user-service publishes { email, otp, ttlMinutes } to topic "notification.otp-email"
2.  Our consumer.run() picks it up, wrapped by withDLQ()
3.  withDLQ: JSON.parse the message value → succeeds
4.  handleMessage(topic, parsedValue) matches KAFKA_TOPICS.OTP_EMAIL
5.  handleOtpEmail(data): checks email + otp are present, calls
      emailService.sendOtpEmail(email, otp, ttlMinutes || 5)
6.  EmailService builds HTML via getOtpTemplate(otp, ttlMinutes)
7.  resend.emails.send({ from, to, subject, html }) → succeeds
8.  Logged: "Email sent successfully to <email>"
9.  withDLQ clears the retry counter for this message — done
```

### Case B: A booking-confirmed event comes in, but has no `email` field

```
1.  booking-service publishes a BookingConfirmedData payload to "booking.confirmed"
2.  handleMessage routes it to handleBookingConfirmed(data)
3.  handleBookingConfirmed looks for data.email — but BookingConfirmedData
    (see templates/index.ts) has no "email" field defined at all
4.  email is undefined → logs a warning "Skipping booking-confirmed email —
    no email on event" and returns WITHOUT sending anything or throwing
5.  withDLQ sees no error was thrown → treats this as a success, clears the
    retry counter. No email is sent, and nothing looks broken in the logs
    unless you specifically look for that warning.
```

This is the current, real behavior for all three booking topics (confirmed / failed / cancelled) — see [Known Issue #11](#known-issues--inconsistencies).

### Case C: Resend keeps failing → retries → eventually goes to the DLQ

```
1.  A welcome-email message arrives, handleWelcomeEmail calls
    emailService.sendWelcomeEmail(email, firstName)
2.  sendWithRetry() calls resend.emails.send() → it throws
3.  EmailService retries in-process: waits 1s, tries again (attempt 2/3)
4.  Still fails: waits 2s, tries again (attempt 3/3)
5.  Still fails: sendWithRetry gives up and re-throws the error
6.  This error propagates out of handleWelcomeEmail, out of handleMessage,
    and is caught by withDLQ's own try/catch
7.  withDLQ has its OWN retry counter, keyed by "topic:partition:offset"
      - attempt 1 of DLQ_MAX_RETRIES (3): logs the error, re-throws
      - kafkajs's consumer-level retry logic redelivers the SAME message
        (the offset was never committed), withDLQ sees the same key again
      - this repeats up to 3 times total
8.  On the 3rd failure, withDLQ forwards the raw message to
    "dlq.notification-service" with headers describing the original
    topic/offset/error, then lets the consumer move on to the next message
```

So there are **two separate retry layers** stacked on top of each other — see [Known Issue #10](#known-issues--inconsistencies).

---

## Component Breakdown

### 1. `index.ts` — Entry Point

```typescript
import app from "./server";
import { startNotificationService } from "./config/db";
import dotenv from "dotenv";
import { config } from "./config/config";

dotenv.config();
startNotificationService().then(() => {
  return app.listen(config.PORT, () => {
    console.log(`Server running on port ${config.PORT}`);
  });
});
```

In plain English: load the `.env` file, start the Kafka consumer (via `startNotificationService`), and once that's done, start listening on the HTTP port too. If `startNotificationService()` fails, it calls `process.exit(1)` itself (see below) — `app.listen()` is never reached.

---

### 2. `server.ts` — The (Mostly Empty) Express App

```typescript
import express from "express";

const app = express();
app.use(express.json());

export default app;
```

That's the entire file. No routes, no health-check endpoint, nothing. It exists so the process can `app.listen()` on a port, but as of today there is nothing you can actually call over HTTP on this service.

---

### 3. `config/` — Configuration, Startup Checks, Kafka, Logger

**`config/config.ts`** — every environment variable this service reads, in one place:

```typescript
export const config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT) || 4004,
  NODE_ENV: process.env.NODE_ENV || "development",
  LOG_LEVEL: process.env.LOG_LEVEL || "info",
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS,
  SENDGRID_API_KEY: process.env.SENDGRID_API_KEY,
  KAFKA_BROKER: process.env.KAFKA_BROKER,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  MAIL_SEND: process.env.MAIL_SEND,
  FRONTEND_URL: process.env.FRONTEND_URL,
  RESEND_API_KEY: process.env.RESEND_API_KEY,
};
```

Unlike the API Gateway's config, there's no "throw if missing" check in this file — that check lives in `db.ts` instead (below). Two of these values (`SENDGRID_API_KEY`, `FRONTEND_URL`) are read into the config object but never used anywhere else in the code — see Known Issues.

**`config/db.ts`** — despite its name, this file does **not** connect to any database. What it actually does is: validate required env vars are set, then start the Kafka consumer, then wire up process-level crash handlers.

```typescript
export async function startNotificationService(): Promise<void> {
  try {
    logger.info("Starting Notification Service...");

    const requiredEnvVars = ["RESEND_API_KEY", "MAIL_SEND", "KAFKA_BROKER"];
    const missing = requiredEnvVars.filter((varName) => !process.env[varName]);
    if (missing.length > 0) {
      throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
    }

    await emailConsumer.start();
    logger.info("✅ Notification Service started successfully");
    logger.info("Service is ready to process notifications");
  } catch (error) {
    console.log(error);

    const err = error as Error;
    logger.error("Failed to start Notification Service", { error: err.message, stack: err.stack });
    process.exit(1);   // the whole process exits if startup fails
  }
}

process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection", { reason, promise });
});

process.on("uncaughtException", (error: Error) => {
  logger.error("Uncaught Exception", { error: error.message, stack: error.stack });
  process.exit(1);
});
```

In plain English: if `RESEND_API_KEY`, `MAIL_SEND`, or `KAFKA_BROKER` aren't set, the service logs an error and exits immediately rather than starting half-broken. Note the catch block also does a plain `console.log(error)` before the structured `logger.error` call — so a startup failure prints twice, once as a raw console dump and once as a formatted log line. `mongoose` is imported at the top of this file but never actually used — likely a leftover from an earlier version that did talk to a database.

**`config/kafka.ts`** — sets up the Kafka client, the consumer that reads messages, and a separate producer used only to publish to the dead-letter queue:

```typescript
const kafka = new Kafka({
  clientId: config.KAFKA_CLIENT_ID,
  brokers: [config.KAFKA_BROKER || "localhost:9093"],
  logLevel: logLevel.ERROR,
  retry: {
    initialRetryTime: 300,
    retries: 10,
    maxRetryTime: 30000,
    multiplier: 2,
  },
});

const consumer: Consumer = kafka.consumer({
  groupId: "notification-service-group",
  sessionTimeout: 30000,     // broker waits this long before considering us dead
  heartbeatInterval: 3000,   // we ping the broker this often to prove we're alive
});

const producer: Producer = kafka.producer({ allowAutoTopicCreation: true, retry: { retries: 3 } });
```

The producer is only connected lazily, the first time something needs to go to the DLQ (`connectProducer()`), not at startup — since most messages never need it. `SIGTERM`/`SIGINT` both trigger a graceful shutdown that disconnects the consumer (and producer, if it was ever connected) before exiting. Note `logLevel: logLevel.ERROR` on the Kafka client — kafkajs's own internal logs (connection attempts, retries, broker metadata) are suppressed down to errors only, which is part of why a bad `KAFKA_BROKER` can look like total silence for a while rather than a stream of visible retry attempts.

**`config/logger.ts`** — a single shared Winston logger, reads `LOG_LEVEL` from config (unlike the API Gateway, where the equivalent value is hardcoded and broken):

```typescript
const logger = winston.createLogger({
  level: config.LOG_LEVEL,
  defaultMeta: { service: config.SERVICE_NAME },
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp, service }) =>
      `[${timestamp}] [${level.toUpperCase()}] [${service}]: ${message}`),
  ),
  transports: [new winston.transports.Console()],
});
```

---

### 4. `kafka/email-consumer.ts` — Reading Messages & Routing

**Startup — connect and subscribe to every topic:**

```typescript
async start(): Promise<void> {
    try {
      await consumer.connect();
      await connectProducer(); // needed for DLQ publishing

      logger.info("Email consumer connected to Kafka");

      // KAFKA_TOPICS is shared across every service, so this subscribes to
      // admin/inventory/payment/DLQ topics too, not just the ones handled
      // below — those all fall through to the "Unknown topic" warning.
      await consumer.subscribe({
        topics: Object.values(KAFKA_TOPICS),
        fromBeginning: false,
      });

      await consumer.run({
        eachMessage: withDLQ(
          producer,
          KAFKA_TOPICS.DLQ_NOTIFICATION,
          logger,
          async ({
            topic,
            parsedValue,
          }: {
            topic: KafkaTopic;
            parsedValue: unknown;
          }) => {
            logger.info(`Processing message from topic: ${topic}`);
            await this.handleMessage(topic, parsedValue);
          },
        ),
      });

      logger.info("Email consumer is running and listening for messages...");
    } catch (error) {
      const err = error as Error;
      logger.error("Failed to start email consumer", { error: err.message });
      throw error;
    }
  }
```

If `consumer.connect()`, `subscribe()`, or `run()` throw during startup, this method logs the error and re-throws — which is what makes `startNotificationService()` in `db.ts` catch it and call `process.exit(1)`.

**Routing — one topic, one handler:**

```typescript
private async handleMessage(topic: KafkaTopic, data: unknown): Promise<void> {
    switch (topic) {
      case KAFKA_TOPICS.OTP_EMAIL:
        await this.handleOtpEmail(data as OtpEmailData);
        break;

      case KAFKA_TOPICS.WELCOME_EMAIL:
        await this.handleWelcomeEmail(data as WelcomeEmailData);
        break;

      case KAFKA_TOPICS.BOOKING_CONFIRMED:
        await this.handleBookingConfirmed(data as BookingConfirmedData);
        break;

      case KAFKA_TOPICS.BOOKING_FAILED:
        await this.handleBookingFailed(data as BookingFailedData);
        break;

      case KAFKA_TOPICS.BOOKING_CANCELLED:
        await this.handleBookingCancelled(data as BookingCancelledData);
        break;

      default:
        logger.warn(`Unknown topic: ${topic}`);
    }
  }
```

Anything not in that list — including `notification.booking-email`, `notification.payment-email`, every `admin.*`/`inventory.*`/`payment.*` topic, and every service's DLQ topic (this service is subscribed to all of them) — falls into `default` and is just logged as "Unknown topic."

**The three booking handlers all follow the same shape** (shown once, `handleBookingConfirmed`):

```typescript
private async handleBookingConfirmed(data: BookingConfirmedData): Promise<void> {
  const email = (data as unknown as { email?: string }).email;
  const { bookingId } = data;

  if (!email) {
    logger.warn(`Skipping booking-confirmed email — no email on event`, { bookingId });
    return;
  }

  await emailService.sendBookingConfirmedEmail(email, data);
  logger.info(`Booking confirmed email sent to ${email}`, { bookingId });
}
```

The comment right above this in the source is worth repeating verbatim, because it explains a real gap: *"BookingConfirmedData has no `email` field — it comes from a separate source on the event; adjust this once you confirm where email actually lives on the real Kafka payload (see note below)."* In other words: today, this only sends an email if the incoming JSON happens to carry an extra `email` property that isn't part of the documented type.

---

### 5. `services/email-service.ts` — Actually Sending Email

**The retry wrapper every send goes through:**

```typescript
private async sendWithRetry(msg: EmailMessage, retries = 0): Promise<SendResult> {
  try {
    const { data, error } = await resend.emails.send({
      from: msg.from, to: msg.to, subject: msg.subject, html: msg.html,
    });
    if (error) throw new Error(error.message);

    logger.info(`Email sent successfully to ${msg.to}`, { subject: msg.subject, attempt: retries + 1, id: data?.id });
    return { success: true };
  } catch (error: any) {
    logger.error(`Email sending failed (attempt ${retries + 1}/${this.maxRetries})`, { to: msg.to, error: error.message });

    if (retries < this.maxRetries - 1) {
      const delay = Math.pow(2, retries) * 1000;   // 1s, then 2s, then 4s
      await new Promise((resolve) => setTimeout(resolve, delay));
      return this.sendWithRetry(msg, retries + 1);
    }
    throw error;   // out of retries — let the caller (and eventually withDLQ) handle it
  }
}
```

**The five public methods** all follow the same pattern: build a subject + HTML body from a template, then call `sendWithRetry`.

```typescript
async sendOtpEmail(
  email: string,
  otp: string,
  ttlMinutes: number,
): Promise<SendResult> {
  const msg: EmailMessage = {
    to: email,
    from: this.from,
    subject: "Your DesignKarle verification code",
    html: getOtpTemplate(otp, ttlMinutes),
  };
  return this.sendWithRetry(msg);
}
```

`this.from` comes from `config.MAIL_SEND` (the "from" address) — note the name doesn't read as an email address at first glance. The other four (`sendWelcomeEmail`, `sendBookingConfirmedEmail`, `sendBookingFailedEmail`, `sendBookingCancelledEmail`) are identical in shape, just pointing at a different template and subject line.

---

### 6. `templates/index.ts` — The Email HTML

This file has two jobs: define the **shape of the data** each email needs (as TypeScript interfaces), and build the **HTML string** for each email type.

```typescript
export interface BookingConfirmedData {
  bookingId: string;
  firstName?: string;
  trainName: string;
  trainNumber: string | number;
  fromStationName?: string;
  toStationName?: string;
  departureDate: string | Date;
  passengers?: Passenger[];
  seats?: Seat[];
  totalAmount: number;
}
```

Note there's no `email` field here — that's exactly the gap described above in the consumer section.

Six template functions exist: `getOtpTemplate`, `getWelcomeTemplate`, `getTicketConfirmationTemplate`, `getBookingConfirmedTemplate`, `getBookingFailedTemplate`, `getBookingCancelledTemplate`. Each returns a self-contained inline-styled HTML `<div>`. A small helper, `formatDate()`, turns a `Date`/date-string into a friendly `en-IN` format (e.g. "15 Jul 2026") for the booking templates, falling back to the raw value if it can't be parsed.

Two lookup tables translate machine-readable reasons into friendly sentences:

```typescript
const FAILURE_REASON_MESSAGES: Record<FailureReason, string> = {
  payment_failed: "Your payment could not be processed.",
  confirm_seats_failed: "We could not confirm your seats with the inventory system.",
  booking_timeout: "Your booking expired before payment was completed.",
};
```

`getTicketConfirmationTemplate` (and its `TicketData` interface) is fully written but nothing in `email-service.ts` or `email-consumer.ts` currently calls it — see Known Issues.

---

### 7. `types/index.ts`

Empty — one blank line, no types defined. All the types this service actually uses (`BookingConfirmedData`, etc.) live in `templates/index.ts` instead.

---

## Environment Variables

```bash
PORT=4004
NODE_ENV=development
LOG_LEVEL=info

# Required — startNotificationService() exits the process if any of these are missing
RESEND_API_KEY=<your Resend API key>
MAIL_SEND=<the "from" email address to send as>
KAFKA_BROKER=localhost:9093

KAFKA_CLIENT_ID=notification-service
ALLOWED_ORIGINS=http://localhost:3000

# Present in .env / config.ts but not read anywhere in the current code:
SENDGRID_API_KEY=
FRONTEND_URL=
```

---

## Kafka Topics Reference

All topic names are defined once in `shared/constants/kafka-topics.ts` and imported from there — nothing in this service hardcodes a topic string.

| Topic | Who publishes it | Handled by this service? |
|---|---|---|
| `notification.otp-email` | user-service | ✅ `handleOtpEmail` — sends the OTP email |
| `notification.welcome-email` | user-service | ✅ `handleWelcomeEmail` — sends the welcome email |
| `notification.booking-email` | — (labelled a notification topic, but nothing publishes or handles it today) | ❌ falls into "Unknown topic" |
| `notification.payment-email` | — (same as above) | ❌ falls into "Unknown topic" |
| `booking.confirmed` | booking-service | ⚠️ handled, but only sends an email if the event happens to include an `email` field (it isn't part of the typed shape) |
| `booking.failed` | booking-service | ⚠️ same caveat as above |
| `booking.cancelled` | booking-service | ⚠️ same caveat as above |
| every `admin.*`, `inventory.*`, `payment.*` topic, and every service's `dlq.*` topic (including this service's own `dlq.notification-service`) | other services | ❌ not meant for this service, but it's subscribed anyway — see Known Issue #6 |

---

## Quick Start

```bash
cd notification-service
npm install

# .env needs at minimum RESEND_API_KEY, MAIL_SEND, and KAFKA_BROKER (the app exits without them)
npm run dev        # nodemon + ts-node, hot reload
# or
npm run build && npm start
```

Kafka must be reachable at `KAFKA_BROKER` (default `localhost:9093`) for the consumer to start. From the IRCTC root, `docker-compose up -d kafka zookeeper` will bring up the broker this service expects to talk to.

There's no HTTP endpoint to curl — the only way to see this service doing something is to publish a message to one of the topics above (e.g. via `kafka-ui` at `localhost:8080`, or from the service that normally publishes it) and watch the console logs.

```bash
# Example: publish a test OTP message via kafka-ui (localhost:8080) to topic
# "notification.otp-email" with body:
{ "email": "you@example.com", "otp": "123456", "ttlMinutes": 5 }
```

---

## Debugging Tips

- **Nothing happens when a message is published** → check the consumer actually connected (`logger.info("Email consumer connected to Kafka")` should appear in the logs on startup). If `KAFKA_BROKER` is wrong, `kafka.ts`'s retry settings mean it'll keep quietly retrying for a while before giving any error.
- **"Skipping ... — no email on event" in the logs** → this is expected today for `booking.confirmed/failed/cancelled` unless the publisher includes an extra `email` field the type doesn't declare — see Known Issue #11.
- **"Unknown topic" warnings flooding the logs** → expected, since this consumer subscribes to every topic in the shared registry, not just the ones it handles — see Known Issue #6. Harmless, but noisy.
- **Emails aren't arriving but no error is logged** → check `RESEND_API_KEY` and `MAIL_SEND` are correct; also check Resend's own dashboard/logs, since a "success" here just means Resend's API accepted the request.
- **A message keeps reappearing and eventually goes to `dlq.notification-service`** → that means `sendWithRetry` exhausted its 3 attempts, the error bubbled up, and `withDLQ` also exhausted its 3 attempts. Check the DLQ message's headers (`dlq-error`, `dlq-original-topic`, `dlq-original-offset`) for why it failed.
- **Process exits immediately with "Missing required environment variables"** → one of `RESEND_API_KEY`, `MAIL_SEND`, `KAFKA_BROKER` isn't set in `.env`.

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, since these are informational (same approach as the API Gateway's docs):

1. **The Express app (`server.ts`) has zero routes**, not even a `/health` endpoint. The service is really a background Kafka worker; the HTTP port it listens on doesn't do anything today.
2. **`config/db.ts` doesn't connect to a database** despite the name — it validates env vars, starts the Kafka consumer, and sets up crash handlers. It imports `mongoose` but never uses it, likely left over from an earlier version.
3. **`mongoose` and the `@langchain/*` packages** are dependencies in `package.json`, but nothing under `src/` imports or uses them.
4. **`SENDGRID_API_KEY`** is read into `config.ts` and present in `.env`, but nothing in the codebase ever uses it — only `RESEND_API_KEY` (via the `resend` package) actually sends mail.
5. **`FRONTEND_URL`** is also read into config but never used. `getWelcomeTemplate`'s login link uses `config.ALLOWED_ORIGINS` instead (a CORS allow-list value, not necessarily a single URL) — this looks like the wrong variable was wired into the template.
6. **The consumer subscribes to every topic in `KAFKA_TOPICS`** (`Object.values(KAFKA_TOPICS)`), including topics meant for completely different services (`admin.*`, `inventory.*`, `payment.*`) and every service's DLQ topic — including its own, `dlq.notification-service`. All of these are harmless but land in the same "Unknown topic" warning.
7. **`notification.booking-email` and `notification.payment-email`** are documented in `shared/constants/kafka-topics.ts` as notification topics, but this consumer's `switch` never handles either one.
8. **`getTicketConfirmationTemplate`** (and its `TicketData` interface) is fully implemented in `templates/index.ts` but nothing currently calls it — dead code today.
9. **Branding is inconsistent across templates**: the OTP and welcome emails sign off as "Team DesignKarle" (with a DesignKarle heading), while the booking emails sign off as "Team IRCTC" — looks like the templates were adapted from a different product without a full find-and-replace.
10. **Two retry layers are stacked**: `EmailService.sendWithRetry` retries a failing send 3× in-process (1s/2s/4s backoff) before giving up; if it still throws, `withDLQ` retries the same Kafka message up to `DLQ_MAX_RETRIES` (3) more times before sending it to the DLQ. Worth knowing both exist when debugging a slow-to-fail message.
11. **The three booking handlers** (`handleBookingConfirmed/Failed/Cancelled`) read `.email` off the incoming data via a cast to `{ email?: string }`, but the actual typed interfaces (`BookingConfirmedData`, etc.) have no `email` field. As written, these three handlers only send an email if the real Kafka payload happens to carry an extra `email` property outside the documented type — otherwise they silently log a warning and skip sending.
12. **File name typo**: `shared/utils/dlqHanlder.ts` is missing the "d" in "Handler." The file's own header comment references the correctly-spelled `dlqHandler` in its usage example, which doesn't match the real file name.
13. **`npm run seed` points at `src/services/seed.ts`**, which doesn't exist in this project — running that script will fail (same issue flagged in the API Gateway's docs, likely from a shared `package.json` origin).
14. **`startNotificationService`'s catch block calls both `console.log(error)` and `logger.error(...)`** — a startup failure gets printed twice: once as a raw, unformatted console dump of the error object, and once as a structured Winston log line. Likely leftover debug code that was never cleaned up.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.


---

<a id="file-payment-service-docs-md"></a>
## FILE: `docs/payment-service-docs.md`

# Payment Service — Complete Guide

Single source of truth for the IRCTC Payment Service: what it does, how a request/webhook flows through it, and how each piece works — written in plain English, with the actual current code inline.

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [Lifecycle Walkthroughs](#lifecycle-walkthroughs)
5. [Component Breakdown](#component-breakdown)
   - [index.ts / server.ts — Entry Point & Express App](#1-indexts--serverts--entry-point--express-app)
   - [services/gateways/ — The Adapter Pattern](#2-servicesgateways--the-adapter-pattern)
   - [services/payment.service.ts — Core Logic](#3-servicespaymentservicets--core-logic)
   - [controllers/ and routes/ — The HTTP Surface](#4-controllers-and-routes--the-http-surface)
   - [kafka/producer/payment.producer.ts](#5-kafkaproducerpaymentproducerts)
6. [Environment Variables](#environment-variables)
7. [Kafka Topics & HTTP Routes Reference](#kafka-topics--http-routes-reference)
8. [Quick Start](#quick-start)
9. [Debugging Tips](#debugging-tips)
10. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **Payment Service** is the only service in this system that talks to a real external payment gateway (Razorpay). Its job is narrow and deliberately isolated from booking logic:

- **Creates a gateway order** (`POST /orders`) — asks Razorpay for an order id, records a `PaymentOrder` row, and hands back what the client needs to open Razorpay's checkout widget.
- **Captures a payment two different ways**: a client-side path (`POST /orders/:id/verify`, called by booking-service right after checkout completes in the browser) and a **webhook path** (`POST /webhooks/razorpay`, called by Razorpay's own servers whenever a payment's state changes) — both converge on the same `PaymentOrder` row, and both are idempotent, so whichever one arrives first "wins" and the second is a safe no-op.
- **Initiates refunds** (`POST /refunds`) — full or partial, with running-total validation so refunds can never exceed what was actually captured.
- **Notifies booking-service** over Kafka (`payment.success` / `payment.failed`) once a payment is captured or fails, rather than booking-service polling for status.
- **Isolates the gateway behind an adapter interface** (`BaseGateway`) so a second provider (Stripe, etc.) could be added later without touching `payment.service.ts` at all — only `RazorpayGateway` and `gateway.factory.ts`'s switch statement would need a sibling.

Every route except the public webhook is behind `internalAuth` (a shared-secret header) — this service has no user-facing routes at all; booking-service is the only intended caller of everything except Razorpay itself.

**Ported from a reference JavaScript implementation** (`irctc-backend-main/payment-service`) into TypeScript, following this repo's conventions: Zod validation on the three JSON routes (the reference validated manually), no `any`, and the gateway adapter modeled as an abstract TypeScript class rather than a duck-typed object. The business logic — the adapter pattern itself, the idempotency scheme, the webhook event-type dispatch, the refund running-total check — is an unmodified port. **Not verified live** — no reachable Postgres/Kafka in the environment this was built in, and there are no real Razorpay API credentials to test against even if there were; every "works when called" claim below means "the logic reads correctly and the types check."

---

## Architecture

```
┌───────────────────────────┐                          ┌──────────────────────┐
│      Booking Service       │                          │       Razorpay        │
│  (the only internal caller)│                          │  (external gateway)   │
└──────────────┬────────────┘                          └───────────┬───────────┘
               │ x-internal-service-key                             │ webhook POST
               ▼                                                     │ (signed, public)
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PAYMENT SERVICE (Port 4006)                          │
│                                                                               │
│  Internal HTTP surface (routes/payment.routes.ts, behind internalAuth):      │
│    POST /orders                    — createPaymentOrder                     │
│    GET  /orders/:paymentOrderId    — getPaymentOrder                        │
│    POST /orders/:paymentOrderId/verify — verifyAndCapturePayment            │
│    POST /refunds                   — initiateRefund                         │
│                                                                               │
│  Public HTTP surface (routes/webhook.routes.ts, express.raw() body):         │
│    POST /webhooks/razorpay         — razorpayWebhook (signature-verified,   │
│                                       not internalAuth-guarded)              │
│                                                                               │
│  Gateway adapter (services/gateways/):                                      │
│    BaseGateway (abstract) <- RazorpayGateway, chosen by gateway.factory.ts  │
│    based on PAYMENT_GATEWAY — payment.service.ts never imports Razorpay's   │
│    SDK directly, only ever calls through this interface                     │
│                                                                               │
│  Kafka producer (kafka/producer/payment.producer.ts):                       │
│    payment.success / payment.failed — fired after a capture/failure is      │
│    durably recorded in Postgres, never before                               │
└──────────────────────────┬───────────────────────────┬──────────────────────┘
                           │ Postgres                    │ Razorpay SDK (HTTPS)
                           ▼                              ▼
                  payment_orders, refunds,        orders.create / payments.fetch /
                  payment_audit_logs,             payments.refund / payments.fetchRefund
                  idempotency_records

                           │ Kafka
                           ▼
                  booking-service (payment.success -> confirms seats,
                                    payment.failed  -> releases seats)
```

---

## File Structure

```
payment-service/
├── src/
│   ├── index.ts                           # Starts the HTTP server (no consumer — this service only produces)
│   ├── server.ts                          # Express app — webhook routes mounted BEFORE express.json()
│   ├── config/
│   │   ├── index.ts                       # Env vars -> typed Config object, including Razorpay credentials
│   │   ├── prisma.ts                      # PrismaClient singleton (via @prisma/adapter-pg)
│   │   ├── kafka.ts                       # Kafka client + producer only — no consumer in this service
│   │   └── logger.ts                      # Winston logger
│   ├── controllers/
│   │   ├── payment.controller.ts          # The 4 internal routes — validates with Zod, calls the service
│   │   └── webhook.controller.ts          # The 1 public webhook route — reads req.body as a raw Buffer
│   ├── routes/
│   │   ├── payment.routes.ts              # POST /orders, GET /orders/:id, POST /orders/:id/verify, POST /refunds
│   │   └── webhook.routes.ts              # POST /webhooks/razorpay, mounted with express.raw()
│   ├── services/
│   │   ├── payment.service.ts             # All business logic: order creation, webhook dispatch, refunds
│   │   └── gateways/
│   │       ├── base.gateway.ts            # Abstract class every gateway adapter must implement
│   │       ├── razorpay.gateway.ts        # The only concrete adapter so far
│   │       └── gateway.factory.ts         # Singleton chooser, keyed by config.PAYMENT_GATEWAY
│   ├── kafka/
│   │   └── producer/payment.producer.ts   # Publishes payment.success/failed
│   ├── middlewares/
│   │   ├── cors.middleware.ts             # Origin whitelist
│   │   ├── error.middleware.ts            # Global error formatter
│   │   ├── req.middleware.ts              # Request/response logging
│   │   └── internal-auth.middleware.ts    # Shared-secret check — guards every route except the webhook
│   ├── utils/
│   │   ├── error.ts                       # AppError + subclasses
│   │   ├── api-response.ts                # SuccessResponse / ErrorResponse helpers
│   │   ├── asyncHandler.ts                # Wraps async route handlers, forwards errors to next()
│   │   └── zod.formatter.ts               # Turns the first ZodError issue into a plain message
│   ├── types/
│   │   ├── index.ts                       # Gateway adapter contract types, Razorpay webhook payload shapes, DTOs
│   │   └── zod.ts                         # Zod schemas for the 3 JSON request bodies
│   └── generated/prisma/                  # Prisma Client output (gitignored, regenerated by `prisma generate`)
├── prisma/
│   └── schema.prisma                      # PaymentOrder, Refund, PaymentAuditLog, IdempotencyRecord
├── docs/                                  # This documentation
├── package.json
├── tsconfig.json
├── prisma.config.ts
├── nodemon.json
└── .env.example
```

No `types/express.d.ts` here, unlike every other service — `internalAuth` only checks a header and calls `next()`, it never assigns anything onto `req.user`, so there's no `Express.Request` augmentation needed for this service to typecheck.

`tsconfig.json` sets `rootDir: ".."`, the same pattern every other service in this repo uses, so `kafka/producer/payment.producer.ts` can compile its `../../../../shared/constants/kafka-topics` import.

---

## Lifecycle Walkthroughs

### Case A: A booking is paid for via the webhook path (the path Razorpay actually uses)

```
1.  booking-service calls POST /orders with {bookingId, amount, userId,
    idempotencyKey} — createPaymentOrder checks idempotencyKey first (a
    retried request returns the original order, not a second one), then
    calls RazorpayGateway.createOrder(amount, "INR", bookingId, {...})
2.  A PaymentOrder row is created, status CREATED, with the gateway's own
    order id stored as gatewayOrderId. A PaymentAuditLog row ("ORDER_CREATED")
    captures Razorpay's raw response for later debugging.
3.  Response: {paymentOrderId, gatewayOrderId, amount, currency, status,
    gatewayProvider, keyId} — booking-service returns keyId/gatewayOrderId to
    the client so its browser can open Razorpay's checkout widget directly
    (this service never sees the client's browser at all)
4.  The user completes checkout in Razorpay's widget. Razorpay's own servers
    POST to this service's public POST /webhooks/razorpay with a
    payment.captured event, signed with RAZORPAY_WEBHOOK_SECRET
5.  razorpayWebhook reads req.body as a raw Buffer (express.raw() ran instead
    of express.json() for this one path) and the x-razorpay-signature header
6.  paymentService.handleWebhook verifies the signature via
    RazorpayGateway.verifyWebhookSignature — an HMAC-SHA256 comparison using
    crypto.timingSafeEqual, so a forged webhook 400s before touching the DB
7.  Looks up the PaymentOrder by gatewayOrderId (from the webhook's payment
    entity) → writes a PaymentAuditLog row for the raw webhook payload →
    dispatches to handlePaymentCaptured since event === "payment.captured"
8.  handlePaymentCaptured: idempotent check (already CAPTURED? already
    non-CREATED? both short-circuit safely) → updates status to CAPTURED,
    stores gatewayPaymentId → publishes payment.success to Kafka
    (paymentOrderId, bookingId, gatewayPaymentId, amount) — a publish
    failure here is caught and logged, not thrown; the capture itself
    already committed to Postgres and is the source of truth
9.  Response: 200 {status: "captured", paymentOrderId} — Razorpay's webhook
    delivery system stops retrying once it sees 200, regardless of the
    business-level status in the body (this service always returns 200 for
    any recognized event, even "ignored" ones, specifically to prevent
    Razorpay from retrying events it has no reason to retry)
10. booking-service's Kafka consumer picks up payment.success and confirms
    the held seats (see booking-service's own docs, Lifecycle Case A)
```

### Case B: The same payment is also verified client-side (idempotency in action)

```
1.  Independently of the webhook above, the client's browser (having
    completed Razorpay's checkout) tells booking-service the payment
    succeeded, and booking-service calls
    POST /orders/:paymentOrderId/verify with {gatewayPaymentId,
    gatewaySignature}
2.  verifyAndCapturePayment looks up the PaymentOrder — if its status is
    already CAPTURED (the webhook won the race), it returns
    {paymentOrderId, status: "CAPTURED", gatewayPaymentId, message:
    "Payment already captured"} immediately, without re-verifying anything
3.  If the webhook hasn't arrived yet (status is still CREATED),
    verifyPaymentSignature checks the client-supplied signature against
    orderId+paymentId — if valid, this path itself performs the capture
    (updates status, publishes payment.success) instead of waiting for the
    webhook
4.  Either order of arrival (webhook first, or verify-call first) converges
    on the same CAPTURED state and exactly one payment.success publish —
    whichever path's status update happens second finds the order already
    CAPTURED and returns early
```

### Case C: A refund exceeds the refundable amount (validation failure)

```
1.  booking-service calls POST /refunds with {paymentOrderId, amount: 500,
    reason: "user_cancelled", idempotencyKey}
2.  initiateRefund checks idempotency first, then loads the PaymentOrder
    with its existing refunds — say ₹1000 was captured and ₹600 already
    refunded once
3.  totalRefunded (600) + amount (500) = 1100 > paymentOrder.amount (1000)
    → throws 400 BadRequestError("Refund amount (500) exceeds refundable
    amount (400)") before ever calling Razorpay — no gateway call, no
    Refund row created, nothing to compensate
4.  A caller retrying with a smaller amount (e.g. 400) would pass this
    check, call RazorpayGateway.initiateRefund, and succeed
```

---

## Component Breakdown

### 1. `index.ts` / `server.ts` — Entry Point & Express App

```typescript
// Webhook routes MUST be registered before express.json() — they need the
// raw request body for Razorpay's signature verification, and express.json()
// would otherwise consume and parse the stream first.
app.use(webhookRoutes);

// JSON parsing for every other route
app.use(express.json());
```

This ordering is the one thing in this file that isn't optional — if `express.json()` ran first, it would consume and parse the webhook's body stream before `express.raw()` (mounted per-route inside `webhook.routes.ts`) ever got a chance to see the original bytes, and Razorpay's HMAC signature check would fail for every webhook. `index.ts` itself is the simplest entry point of any service that talks to Kafka in this repo — no consumer to start, so it's just `app.listen()` plus graceful shutdown (`disconnectProducer()` on `SIGTERM`/`SIGINT`).

---

### 2. `services/gateways/` — The Adapter Pattern

```typescript
export abstract class BaseGateway {
  public readonly providerName: string;
  protected constructor(providerName: string) { this.providerName = providerName; }

  abstract createOrder(amount: number, currency: string, receipt: string, notes?: Record<string, string>): Promise<GatewayOrderResult>;
  abstract verifyPaymentSignature(orderId: string, paymentId: string, signature: string): boolean;
  abstract verifyWebhookSignature(rawBody: string | Buffer, signature: string): boolean;
  abstract fetchPayment(paymentId: string): Promise<GatewayPaymentResult>;
  abstract initiateRefund(paymentId: string, amount: number, notes?: Record<string, string>): Promise<GatewayRefundResult>;
  abstract fetchRefund(paymentId: string, refundId: string): Promise<GatewayRefundFetchResult>;
}
```

Six methods every gateway must implement — `payment.service.ts` calls only these, never Razorpay's SDK directly. `gateway.factory.ts` is a singleton chooser (`getGateway()` returns the same instance every call) keyed off `config.PAYMENT_GATEWAY` (`"razorpay"` today); adding Stripe later means writing `stripe.gateway.ts` and one more `case` in the factory's `switch`, with zero changes to `payment.service.ts`.

`razorpay.gateway.ts` is the concrete implementation — worth calling out two things ported exactly from the reference:
- **Amounts are converted to paise (`amount * 100`) going out, and back to rupees (`/ 100`) coming in** — Razorpay's API is paise-denominated, but this service's own `PaymentOrder.amount` column and every other service's `totalAmount`/`price` fields are rupee-denominated. This conversion happens only inside `razorpay.gateway.ts`; nothing outside this one file ever sees paise.
- **The Razorpay SDK throws plain objects on API errors, not `Error` instances** — `createOrder`'s catch block reads `err.error?.description` defensively rather than `err.message`, and re-throws as a proper `BadRequestError` so the rest of the service only ever deals with this repo's own `AppError` hierarchy.

---

### 3. `services/payment.service.ts` — Core Logic

The idempotency helper, generic over the wrapped function's return type:

```typescript
const withIdempotency = async <T>(key: string, fn: () => Promise<T>): Promise<T> => {
  const existing = await prisma.idempotencyRecord.findUnique({ where: { eventKey: key } });
  if (existing) {
    logger.info(`Idempotent request detected: ${key}`);
    return existing.response as unknown as T;
  }
  const result = await fn();
  await prisma.idempotencyRecord.create({ data: { eventKey: key, response: toJson(result) } });
  return result;
};
```

Both `createPaymentOrder` and `initiateRefund` wrap their entire body in this — the key is prefixed (`payment-order:`/`refund:`) so the two operations' idempotency keys can never collide even if a caller reused the same string for both. `handleWebhook` uses a *different* idempotency mechanism (checking `PaymentOrder.status` directly, e.g. "already CAPTURED, return early") rather than this helper, because webhook redelivery isn't keyed by a caller-supplied idempotency key at all — Razorpay just retries the same event.

`toJson` is a small local cast helper (`value as Prisma.InputJsonValue`) needed because `GatewayOrderResult`/webhook payloads/etc. are plain TypeScript interfaces without an index signature, which don't structurally satisfy Prisma's `InputJsonValue` on their own — the same pattern booking-service's `saga.service.ts` uses for its own `SagaLog.response` writes.

---

### 4. `controllers/` and `routes/` — The HTTP Surface

Every route in `payment.routes.ts` is behind `internalAuth` — there is no user-facing route anywhere in this service; a client's browser talks to Razorpay directly (via the `keyId`/`gatewayOrderId` returned from `createPaymentOrder`), never to this service. This is a deliberate departure from the reference's manual validation (`if (!bookingId || !amount || ...) throw new BadRequestError(...)`) in favor of Zod schemas (`zCreatePaymentOrder`, `zVerifyAndCapture`, `zInitiateRefund`), matching this repo's convention — the required-field checks are identical, plus proper number/string typing instead of truthy checks (e.g. `amount: 0` now correctly fails validation instead of passing a truthy-but-wrong check).

`webhook.controller.ts` is the one handler in this service where `req.body` is a `Buffer`, not a parsed object — a type comment calls this out explicitly since it's easy to assume every controller in this codebase sees JSON.

---

### 5. `kafka/producer/payment.producer.ts`

Structurally identical to every other producer in this repo (lazy-connect, `idempotent: true`, keyed by `payment-<paymentOrderId>` so all events about one payment land on the same partition in order) — the two publish methods, `publishPaymentSuccess`/`publishPaymentFailed`, are called from three different call sites in `payment.service.ts` (`handlePaymentCaptured`, `handlePaymentFailed`, and `verifyAndCapturePayment`'s two branches), all wrapped in `.catch()` at the call site rather than inside the producer — a Kafka outage here is logged, not thrown, since the payment's own state is already durably committed by the time any of these calls happen.

---

## Environment Variables

```bash
PORT=4006
NODE_ENV=development
LOG_LEVEL=info

DATABASE_URL=postgresql://admin:irctcpass@localhost:5432/payment_service_db?schema=public
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:4000

KAFKA_BROKER=localhost:9093
KAFKA_CLIENT_ID=payment-service

INTERNAL_SERVICE_KEY=change-me-to-a-shared-secret

PAYMENT_GATEWAY=razorpay
RAZORPAY_KEY_ID=your-razorpay-key-id
RAZORPAY_KEY_SECRET=your-razorpay-key-secret
RAZORPAY_WEBHOOK_SECRET=your-razorpay-webhook-secret
```

Every variable is actually read by `config/index.ts`. `RAZORPAY_KEY_ID`/`KEY_SECRET`/`WEBHOOK_SECRET` have no real values in this environment — there is no live Razorpay account to test against, so every gateway call would fail with an auth error against the real API even once Postgres/Kafka are reachable. This is expected; see [Known Issues](#known-issues--inconsistencies).

---

## Kafka Topics & HTTP Routes Reference

### Kafka topics

| Topic | Direction | Published from |
|---|---|---|
| `payment.success` | published | `handlePaymentCaptured` (webhook path), `verifyAndCapturePayment` (client-verify path) |
| `payment.failed` | published | `handlePaymentFailed` (webhook path), `verifyAndCapturePayment`'s signature-failure branch |

This service has no Kafka **consumer** at all — it never subscribes to anything, only publishes.

### HTTP routes

| Method & Path | Auth | Status |
|---|---|---|
| `POST /orders` | `x-internal-service-key` | Creates a gateway order + `PaymentOrder` row. Depends on real Razorpay credentials to actually call out; fails today without them. |
| `GET /orders/:paymentOrderId` | `x-internal-service-key` | Returns the order plus its full audit-log and refund history. |
| `POST /orders/:paymentOrderId/verify` | `x-internal-service-key` | Client-side capture path; idempotent against the webhook path (see Lifecycle Case B). |
| `POST /refunds` | `x-internal-service-key` | Validates the running refund total before calling the gateway. |
| `POST /webhooks/razorpay` | none (public) — signature-verified instead | The only route Razorpay itself calls; always returns `200` for any recognized event so Razorpay stops retrying (see Lifecycle Case A, step 9). |
| `GET /health` | none | Checks Postgres (`SELECT 1`); `503` if unreachable. |
| `GET /` | none | Static "Hello from payment-service" string. |

Only the webhook route is actually registered in the API Gateway — `POST /payments/webhooks/razorpay` (see `api-gateway/src/routes/index.ts`), exposed publicly as `POST /api/payments/webhooks/razorpay`; the raw-body middleware branch for this exact path already existed in the gateway's `index.ts` before this service did, written ahead of time for exactly this route. The four internal routes are **not** proxied through the API Gateway at all — booking-service calls payment-service directly at `config.PAYMENT_SERVICE_URL` (see `booking-service/src/services/paymentClient.ts`), bypassing the gateway entirely, the same way it reaches every other internal service.

---

## Quick Start

```bash
cd payment-service
npm install

# Generate the Prisma client (writes into src/generated/prisma, gitignored)
npx prisma generate

# .env needs at minimum DATABASE_URL, KAFKA_BROKER, INTERNAL_SERVICE_KEY
# (must match booking-service's own INTERNAL_SERVICE_KEY exactly), and real
# Razorpay credentials for anything gateway-related to actually work
npm run dev        # nodemon, hot reload
```

Postgres and Kafka must both be reachable — from the IRCTC root,
`docker-compose up -d postgres kafka zookeeper` brings up the infrastructure
this service expects. Apply the schema with `npx prisma migrate dev` before
starting the service (no migration exists yet in this port — see
[Known Issues](#known-issues--inconsistencies)).

```bash
curl http://localhost:4006/health
# { "success": true, "message": "Payment Service is healthy", "database": true, "timestamp": "..." }

# Every non-webhook route needs the internal-service header:
curl -X POST http://localhost:4006/orders \
  -H "Content-Type: application/json" \
  -H "x-internal-service-key: change-me-to-a-shared-secret" \
  -d '{"bookingId":"<uuid>","amount":1500,"userId":"<uuid>","idempotencyKey":"test-1"}'
# Fails today without real RAZORPAY_KEY_ID/KEY_SECRET — see Known Issues.
```

---

## Debugging Tips

- **Every gateway call fails with an auth error from Razorpay** → there are no real Razorpay credentials configured in this environment (see [Known Issues](#known-issues--inconsistencies)) — this is expected, not a bug in this service's code.
- **Webhook signature verification always fails** → check that `RAZORPAY_WEBHOOK_SECRET` matches what's configured in the Razorpay dashboard for this specific webhook endpoint, and confirm the request actually went through `express.raw()` — if `express.json()` ran first for this path (e.g. after an unrelated middleware-ordering change), the body bytes handed to `verifyWebhookSignature` won't match what Razorpay signed.
- **A refund silently doesn't happen even though booking-service called `initiateRefund`** → check this service's own logs for a thrown `ConflictError`/`BadRequestError` — refund validation (status must be `CAPTURED`/`PARTIALLY_REFUNDED`, amount can't exceed what's refundable) happens before any gateway call, so a rejected refund never reaches Razorpay at all.
- **`payment.success`/`payment.failed` never reach booking-service** → check this service's logs for "Failed to publish ... " — publish failures are logged, not thrown (the payment's own state change already committed), so a Kafka outage here silently leaves booking-service unconfirmed.
- **A payment gets captured twice (double `payment.success`)** → shouldn't happen — both `handlePaymentCaptured` and `verifyAndCapturePayment` check `paymentOrder.status` before proceeding and return early if it's not `CREATED`. If you see this, check for a race between the two paths that isn't covered by the current status check (worth flagging as a real bug if reproduced against live Postgres, since this hasn't been verified live).

---

## Known Issues & Inconsistencies

Observed while porting this service — documented here rather than fixed, since these are informational (same approach as every other service's docs in this repo):

1. **No real Razorpay credentials exist anywhere.** `RAZORPAY_KEY_ID`/`RAZORPAY_KEY_SECRET`/`RAZORPAY_WEBHOOK_SECRET` in `.env.example` are placeholders — there is no Razorpay merchant account behind this port. Every gateway call (`createOrder`, `verifyPaymentSignature` against a real signature, `initiateRefund`) will fail against the real Razorpay API even once Postgres/Kafka are reachable. This mirrors every other external dependency in this repo (not verified live), just with an extra layer — there's no way to verify this one live without a real merchant account, unlike Postgres/Kafka/Redis which just need `docker-compose up`.
2. **No Prisma migration exists yet.** `prisma/schema.prisma` was authored for this port but `npx prisma migrate dev` has not been run (no reachable Postgres in this environment) — there is no `prisma/migrations/` directory yet, unlike most other services in this repo.
3. **Not verified against live infrastructure.** No Postgres or Kafka broker was reachable while this was built — `npx tsc --noEmit` passing clean is the only verification performed.
4. **The webhook response's outer `status: "ok"` from the reference is effectively dead code.** Every branch of `handleWebhook` already sets its own `status` field (`"captured"`, `"failed"`, `"ignored"`, etc.), so spreading the result over a literal `{status: "ok", ...result}` always gets overwritten by `result.status` — this port just returns `result` directly (`res.status(200).json(result)`), which produces the byte-identical response the reference always actually sent, just without the dead literal. TypeScript's `noEmit` check on this repo's `strict` settings flags the duplicate-key literal as an error, which is what surfaced this.
5. **`getPaymentOrder`'s return type is inferred from Prisma directly** (not a hand-written DTO like `CreatePaymentOrderResult`/`RefundResult`) — it returns the full `PaymentOrder` row plus its `auditLogs`/`refunds` relations verbatim, including internal fields like `version` and `idempotencyKey` that a client arguably shouldn't need. This matches the reference's behavior exactly (no field-level DTO shaping existed there either); flagged here as a design choice worth revisiting, not a bug.
6. **Refund amount validation (`totalRefunded + amount > paymentOrder.amount`) doesn't account for floating-point accumulation error** — `amount` is a Prisma `Float`, and summing several partial refunds could in principle drift by fractions of a paisa. Ported as-is from the reference; not something this pass introduced or fixed.


---

<a id="file-playlist-guide-md"></a>
## FILE: `docs/playlist-guide.md`

# Playlist Guide — "IRCTC Backend" by designKarle

This is a learning-plan companion to the YouTube playlist this repo was built
from: [**IRCTC Backend with Node.js, PostgreSQL, Kafka, Docker, Redis, Elastic
Search & Razorpay**](https://www.youtube.com/playlist?list=PLhNPruYZ0mVOF-poB1cDcU8ICktc_ehxs)
by **designKarle — System Design By Shivam Tiwari**.

- **22 videos, ~20h 15m total.**
- Because this codebase *is* (a reworked, bug-fixed version of) what the
  playlist builds, every entry below points at the actual service/files that
  video produced — so instead of just watching passively, you can open the
  real, already-debugged implementation side by side and compare.
- Where this repo's own docs (`docs/implementation-plan.md`,
  `docs/api-contract.md`, `missing.md`, each `<service>/docs/README.md`)
  already describe a bug or gap in that area, it's called out — the video
  may show the original/simpler version; this repo's code has since diverged
  (fixed some things, still has other open gaps).
- Check the `[ ]` box in your own copy as you finish each one, or just track
  progress mentally — this file doesn't need to be kept perfectly in sync.

---

## Progress checklist

| # | Video | Duration | Maps to |
|---|---|---|---|
| [ ] 1 | [IRCTC Backend PART - 1](https://youtu.be/K_cTtCXCPeY) | 14:40 | Project overview, no specific service |
| [ ] 2 | [Project Setup — User-Microservice](https://youtu.be/wDe7oeNua2U) | 17:29 | `user-service/` skeleton |
| [ ] 3 | [Redis Singleton + Prisma ORM](https://youtu.be/C4h0EFqP4FY) | 9:06 | `user-service/src/config/redis.ts`, `config/prisma.ts` |
| [ ] 4 | [What is Docker? docker-compose.yml](https://youtu.be/MQ-Vugljmd4) | 26:46 | root `docker-compose.yml` |
| [ ] 5 | [Signup using OTP](https://youtu.be/Xa074pxNSnU) | 1:24:27 | `user-service` auth: send-otp/verify-otp |
| [ ] 6 | [Login + Refresh Token Rotation](https://youtu.be/Jxjfz2QGtwU) | 50:59 | `user-service` auth: login/refresh |
| [ ] 7 | [Google Authentication](https://youtu.be/D_3DPelMSzA) | 52:28 | ⚠️ Not actually built in this repo (see below) |
| [ ] 8 | [Kafka Integration in user-service](https://youtu.be/i4Gdo-y0ni0) | 44:19 | `user-service` → `notification-service` |
| [ ] 9 | [Redis: 500ms → 20ms latency](https://youtu.be/EKb0CRwr-8E) | 19:12 | `user-service` profile caching |
| [ ] 10 | [Building an API Gateway](https://youtu.be/2CyP5sBohNA) | 1:09:48 | `api-gateway/` |
| [ ] 11 | [Intro to Elasticsearch](https://youtu.be/PNc32dfdQrI) | 17:33 | Conceptual — prep for `search-service` |
| [ ] 12 | [Publishing Kafka events from Admin Service](https://youtu.be/yXK3XmY8I9g) | 55:44 | `admin-service` producer (stations/trains) |
| [ ] 13 | [Admin Service Kafka events, Part 2](https://youtu.be/MyQ-X0SbylE) | 42:15 | `admin-service` producer (routes/schedules) |
| [ ] 14 | [Storing data into Elasticsearch](https://youtu.be/tPoFlhYqMxA) | 58:25 | `search-service` Kafka consumer / indexing |
| [ ] 15 | [Search using Elasticsearch](https://youtu.be/v_Q-ZBcxF84) | 48:04 | `search-service` search/autocomplete |
| [ ] 16 | [Initialising Inventory](https://youtu.be/M1kLBpLZvs0) | 44:38 | `inventory-service` Kafka consumer |
| [ ] 17 | [Booking Service: SAGA, Idempotency, Concurrency](https://youtu.be/XM5pS8Jq2yU) | 1:47:15 | `booking-service` saga orchestration |
| [ ] 18 | [Inventory: lockSeats/unlockSeats/confirmSeats](https://youtu.be/AGyRlkJ5Qv4) | 2:46:41 | `inventory-service` HTTP routes |
| [ ] 19 | [Payment Service: Razorpay Adapter Pattern](https://youtu.be/an5N19xMoag) | 1:52:16 | `payment-service/` |
| [ ] 20 | [Kafka Events + Optimistic Concurrency](https://youtu.be/ey6i5YOBeBk) | 1:33:07 | `booking-service` payment.success/failed consumer |
| [ ] 21 | [cancelBooking + Optimistic Concurrency](https://youtu.be/8lV4_9DmdCg) | 40:15 | `booking-service` cancel flow |
| [ ] 22 | [Final Testing of Booking Feature](https://youtu.be/AOO3cLhaMqA) | 39:52 | End-to-end test of the whole saga |

---

## 1. IRCTC Backend PART - 1
[Watch (14:40)](https://youtu.be/K_cTtCXCPeY)

**Covers:** Project scope and architecture overview — what a real IRCTC-style
backend needs (signup/login, admin catalog, search, booking, payment,
notifications) and why it's split into microservices instead of one app.

**Maps to this repo:** The whole system — read the root
[`readme.md`](../readme.md) §1–2 alongside this video; it's the same picture,
already fully wired together.

**Docs to reference (this repo):**
- [`readme.md` §1 The Big Picture](../readme.md#1-the-big-picture) — the architecture diagram this video is building toward
- [`readme.md` §2 Meet the Services](../readme.md#2-meet-the-services) — what each of the 7 services below is for and its current build status
- [`readme.md` §3 Jargon Buster](../readme.md#3-jargon-buster) — JWT/OTP/Kafka/DLQ/circuit breaker/Redis/Prisma/Elasticsearch defined in plain English before you meet them in code
- [`docs/implementation-plan.md` §1 What this system is](./implementation-plan.md#1-what-this-system-is)

---

## 2. Project Setup — User-Microservice
[Watch (17:29)](https://youtu.be/wDe7oeNua2U)

**Covers:** Scaffolding one Express + TypeScript service from scratch —
`package.json`, `tsconfig.json`, folder layout (`routes/controllers/services`).

**Maps to this repo:** `user-service/` top-level structure — compare against
its `package.json`, `tsconfig.json`, and `src/server.ts`. Every other service
in this repo (`admin-service`, `search-service`, etc.) repeats this exact
skeleton, so this video is really "how every service here is shaped."

**Docs to reference (this repo):**
- [`user-service/docs/README.md` §File Structure](../user-service/docs/README.md#file-structure) — the exact folder layout to compare your scaffold against
- [`user-service/docs/README.md` §2 server.ts — The Express App](../user-service/docs/README.md#2-serverts--the-express-app)
- [`user-service/docs/README.md` §Quick Start](../user-service/docs/README.md#quick-start) — the `npm install && npm run dev` commands this repo actually uses

---

## 3. Redis Singleton + Prisma ORM
[Watch (9:06)](https://youtu.be/C4h0EFqP4FY)

**Covers:** Wrapping `ioredis` in a singleton class so the app doesn't open a
new connection per import, and setting up Prisma as the Postgres ORM.

**Maps to this repo:** `user-service/src/config/redis.ts` (the
`RedisClient.getInstance()` pattern — reused near-identically in
`api-gateway`, `booking-service`) and `user-service/src/config/prisma.ts`
(the `pg` adapter + `global`-cached client, reused by every service that
touches Postgres). One real bug this repo's docs found and fixed in this
exact file: a stray `console.log` was printing the full Redis connection
string — including any embedded password — on every service start.

**Docs to reference (this repo):**
- [`user-service/docs/README.md` §3 config/ — Env, Prisma, Redis, Kafka, Logger](../user-service/docs/README.md#3-config--env-prisma-redis-kafka-logger) — the actual `RedisClient`/Prisma singleton code, pasted in full
- [`admin-service/docs/README.md` §3 config/](../admin-service/docs/README.md#3-config--env-kafka-logger-prisma) — the same Prisma-singleton pattern in a second service, for comparison
- [`user-service/docs/README.md` §Known Issues](../user-service/docs/README.md#known-issues--inconsistencies) — where the leaked-connection-string `console.log` bug is written up

---

## 4. What is Docker? Docker Setup: docker-compose.yml
[Watch (26:46)](https://youtu.be/MQ-Vugljmd4)

**Covers:** Docker/Docker Compose fundamentals, then writing the
`docker-compose.yml` that brings up every piece of shared infrastructure.

**Maps to this repo:** the root [`docker-compose.yml`](../docker-compose.yml)
— Postgres, pgAdmin, Redis + Redis Insight, Kafka + Zookeeper + Kafka UI,
Elasticsearch + Kibana. Note none of the *application* services
(user-service, booking-service, etc.) are containerized here — only their
infra dependencies are; you still run each service with `npm run dev`.

**Docs to reference (this repo):**
- [`readme.md` §10 Running It Locally](../readme.md#10-running-it-locally) — the container table (ports + admin UIs) and the exact `docker-compose up -d` step
- [`docs/implementation-plan.md` §4 Infrastructure](./implementation-plan.md#4-infrastructure-docker-composeyml) — explains the `9092`/`9093` dual-listener split (container-network vs. host-mapped) some services' `.env` files rely on

---

## 5. Implementing Signup using OTP
[Watch (1:24:27)](https://youtu.be/Xa074pxNSnU)

**Covers:** Email+password signup gated behind a 6-digit OTP: hashing the
password before it ever touches Redis, HMAC-ing the OTP so it's never stored
in plaintext, rate-limiting OTP requests, and publishing a Kafka event so
another service sends the actual email.

**Maps to this repo:** `user-service/src/services/auth.service.ts`
(`sendOtp`/`verifyOtp`), `user-service/src/utils/otp.ts`
(`generateAndStoreOtp`/`verifyOtpViaUnHashing`), Redis keys
`otp:session:<uuid>` / `otp:rate:<email>` / `otp:attempt:<email>`. This is
called out in this repo's own docs as **the one flow that's fully built,
wired up, and confirmed working end-to-end** — a good one to get comfortable
with first since everything else in the system builds on the same patterns.

**Docs to reference (this repo):**
- [`readme.md` §4 How Signup & Login Actually Work Today](../readme.md#4-how-signup--login-actually-work-today) — the full sequence diagram, in plain English, with the security decisions explained
- [`user-service/docs/README.md` §6 Auth — controller, service, and its utils](../user-service/docs/README.md#6-auth--controller-service-and-its-utils) — the actual `sendOtp`/`verifyOtp` code
- [`user-service/docs/README.md` §Lifecycle Walkthroughs, Case A](../user-service/docs/README.md#lifecycle-walkthroughs) — a byte-for-byte trace of every Redis key touched
- [`docs/api-contract.md` §2 User Service](./api-contract.md) — exact request/response shapes and every error code this flow can return

---

## 6. Implementing Login and Refresh Token Rotation
[Watch (50:59)](https://youtu.be/Jxjfz2QGtwU)

**Covers:** JWT access + refresh tokens, httpOnly cookies, and **refresh
token rotation with reuse detection** — spotting a stolen/replayed refresh
token and killing the session.

**Maps to this repo:** `user-service/src/services/auth.service.ts`
(`login`/`rotateRefreshToken`), `utils/auth.ts` (JWT sign/verify),
`utils/device-fingerprint.ts` (device scoping via
`sha256(user-agent|ip|accept)`), Redis key `refresh:<userId>:<deviceId>`. See
`user-service/docs/README.md`'s Lifecycle Case B for a full walkthrough of the
reuse-detection edge case.

**Docs to reference (this repo):**
- [`user-service/docs/README.md` §Lifecycle Walkthroughs, Case B](../user-service/docs/README.md#lifecycle-walkthroughs) — the stolen-refresh-token replay scenario, traced step by step against the real Redis keys
- [`user-service/docs/README.md` §6 Auth](../user-service/docs/README.md#6-auth--controller-service-and-its-utils) — `login`/`rotateRefreshToken`'s actual source
- [`readme.md` §3 Jargon Buster](../readme.md#3-jargon-buster) — the plain-English JWT definition if you want the concept before the code

---

## 7. Google Authentication Implementation
[Watch (52:28)](https://youtu.be/D_3DPelMSzA)

**Covers:** Adding "Sign in with Google" as a second signup/login path via
OAuth 2.0.

**⚠️ Maps to this repo: nothing built.** `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET`
are read into `user-service`'s config object but there is no OAuth
route/controller/service anywhere in this codebase — this repo's own docs
flag it explicitly as "a deliberate, explicit out-of-scope decision, not an
oversight." Treat this video as optional/reference-only unless you want to
build this feature yourself on top of the current `user-service`.

**Docs to reference (this repo):**
- [`user-service/docs/README.md` §Environment Variables](../user-service/docs/README.md#environment-variables) — confirms `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET` are read but genuinely unused
- [`user-service/docs/README.md` §Known Issues](../user-service/docs/README.md#known-issues--inconsistencies) — item 3, the explicit "out of scope, not an oversight" note

---

## 8. Kafka Integration in user-service for OTP and Email Notifications
[Watch (44:19)](https://youtu.be/i4Gdo-y0ni0)

**Covers:** Publishing a Kafka event from `user-service` when an OTP is
generated, and building the consumer side (`notification-service`) that
listens for it and sends the email.

**Maps to this repo:** `user-service/src/kafka/producer/notification-producer.ts`
(`sendOtpEmail`/`sendWelcomeEmail`, topics `notification.otp-email` /
`notification.welcome-email`) and the whole of `notification-service/`
(`kafka/email-consumer.ts`, `services/email-service.ts`, `templates/index.ts`).
Note: `notification.otp-email` is the one Kafka flow this repo's docs confirm
has actually been watched working live end-to-end.

**Docs to reference (this repo):**
- [`user-service/docs/README.md` §8 kafka/producer/notification-producer.ts](../user-service/docs/README.md#8-kafkaproducernotification-producerts) — the producer side, `sendOtpEmail`
- [`notification-service/docs/README.md`](../notification-service/docs/README.md) — the whole consumer side: architecture, message lifecycle, and every template
- [`readme.md` §8 The Kafka Announcement Board](../readme.md#8-the-kafka-announcement-board--who-talks-to-whom) — the full producer→topic→consumer map, with `notification.otp-email` marked as the one flow actually confirmed working live

---

## 9. Redis reduced the latency from 500ms to 20ms
[Watch (19:12)](https://youtu.be/EKb0CRwr-8E)

**Covers:** Cache-aside pattern for a user's profile — read Redis first, fall
back to Postgres on a miss, write through on update.

**Maps to this repo:** `user-service/src/services/user.service.ts`'s
`getUserProfile` (Redis key `user:<userId>`, TTL `REDIS_USER_TTL`). Worth
knowing: this repo's docs found and fixed a real bug here — a cold-cache read
used to leak the unscrubbed row (password hash included) while a warm-cache
read didn't, because only one of the two return paths stripped the password
field.

**Docs to reference (this repo):**
- [`user-service/docs/README.md` §7 User Profile — controller + service](../user-service/docs/README.md#7-user-profile--controller--service) — the actual `getUserProfile` cache-first code, with the cold/warm-cache bug called out inline
- [`docs/api-contract.md` §2 User Service](./api-contract.md) — the `GET /user/profile` request/response shape

---

## 10. I built my own API Gateway in just 60 minutes
[Watch (1:09:48)](https://youtu.be/2CyP5sBohNA)

**Covers:** Writing a reverse-proxy gateway from scratch: JWT auth
middleware, Redis-backed sliding-window rate limiting, a hand-rolled circuit
breaker, and path-rewrite proxying to downstream services.

**Maps to this repo:** the whole `api-gateway/` service — `middlewares/auth.middleware.ts`
(`requireAuth`), `middlewares/rate-limiting.middleware.ts` (the sorted-set
sliding window), `services/proxy.ts` (circuit breaker + `createProxy`),
`routes/index.ts` (the route table). See `api-gateway/docs/README.md` for the
full component breakdown with every function's source pasted in — a good
one to read alongside the video rather than after.

**Docs to reference (this repo):**
- [`api-gateway/docs/README.md` §4 auth.middleware.ts](../api-gateway/docs/README.md#4-authmiddlewarets--authentication) — the real `requireAuth` source
- [`api-gateway/docs/README.md` §5 rate-limiting.middleware.ts](../api-gateway/docs/README.md#5-rate-limitingmiddlewarets--rate-limiting) — the sliding-window Redis algorithm, with the exact Lua-free pipeline shown
- [`api-gateway/docs/README.md` §6 services/proxy.ts — Proxy & Circuit Breaker](../api-gateway/docs/README.md#6-servicesproxyts--proxy--circuit-breaker) — the `CLOSED → OPEN → HALF_OPEN` state machine
- [`readme.md` §7 What Happens When Something Fails](../readme.md#7-what-happens-when-something-fails) — the plain-English version of the same circuit breaker

---

## 11. Introduction to Elastic Search
[Watch (17:33)](https://youtu.be/PNc32dfdQrI)

**Covers:** Elasticsearch fundamentals — indices, documents, mappings,
analyzers — as prep before building `search-service`. Mostly conceptual, not
tied to a specific commit in this repo.

**Maps to this repo:** background for `search-service/src/config/elasticsearch.ts`,
which defines the `stations` (edge-ngram + completion suggester) and `trains`
(nested `route`/`schedules`/`seatSummary`) indices you'll meet in videos 14–15.

**Docs to reference (this repo):**
- [`search-service/docs/README.md` §2 config/ — Config, Logger, Kafka, Elasticsearch](../search-service/docs/README.md#2-config--config-logger-kafka-elasticsearch) — the actual `initIndices`/`recreateIndices` code
- [`search-service/docs/README.md` §Elasticsearch Indices Reference](../search-service/docs/README.md#elasticsearch-indices-reference) — which indices exist, which are declared-but-unused, and why

---

## 12. Publishing events to Kafka from Admin Service
[Watch (55:44)](https://youtu.be/yXK3XmY8I9g)

**Covers:** Building `admin-service`'s station/train creation endpoints and
publishing `admin.station-created` / `admin.train-created` events so other
services can react.

**Maps to this repo:** `admin-service/src/controllers/station.controller.ts`,
`train.controller.ts`, and `kafka/producer/admin.producer.ts`
(`publishStationCreated`/`publishTrainCreated`). Worth knowing while watching:
this repo's docs found two real bugs introduced right around here that are
now fixed — `createStation` not `await`-ing its own service call (so a
duplicate-station conflict silently produced a false 200), and the whole
service failing to boot at all because `config/index.ts` was empty.

**Docs to reference (this repo):**
- [`admin-service/docs/README.md` §5 Station Creation — controller + service](../admin-service/docs/README.md#5-station-creation--controller--service) — includes the fixed `await` bug and the corrected response message
- [`admin-service/docs/README.md` §8 kafka/producer/admin.producer.ts](../admin-service/docs/README.md#8-kafkaproduceradminproducerts--event-publishing) — every publish method, and which ones actually fire today
- [`admin-service/docs/README.md` §Known Issues](../admin-service/docs/README.md#known-issues--inconsistencies) — the `config/index.ts`-was-empty boot failure, in full

---

## 13. Admin Service Kafka events, Part 2
[Watch (42:15)](https://youtu.be/MyQ-X0SbylE)

**Covers:** Continuing admin-service — defining a train's route and creating
schedules, publishing `admin.route-created` / `admin.schedule-created`.

**Maps to this repo:** `admin-service/src/services/train.service.ts`
(`createRoute`) and `services/schedule.service.ts` (`createSchedule`). Two
real bugs this repo's docs found and fixed live right in this code path: the
route's "does this already exist" check was **inverted** (blocking every
train's *first* route rather than a genuine duplicate), and the
`admin.route-created` publish call was commented out entirely. Compare the
video's version against `train.service.ts`'s current `createRoute` to see
exactly what changed.

**Docs to reference (this repo):**
- [`admin-service/docs/README.md` §6 Train & Route — controller + service](../admin-service/docs/README.md#6-train--route--controller--service) — the inverted-check bug shown before/after
- [`admin-service/docs/README.md` §7 Schedule — controller + service](../admin-service/docs/README.md#7-schedule--controller--service) — why the schedule event inlines train+seats+route instead of the consumer calling back
- [`docs/api-contract.md` §3 Admin Service](./api-contract.md) — the exact `RouteCreatedPayload`/`ScheduleCreatedPayload` shapes consumers expect

---

## 14. Storing data into Elastic Search
[Watch (58:25)](https://youtu.be/tPoFlhYqMxA)

**Covers:** `search-service`'s Kafka consumer — reacting to admin-service's
events by writing/updating Elasticsearch documents.

**Maps to this repo:** `search-service/src/kafka/search.service.ts` (the
consumer) and `services/search.service.ts`'s `indexStation`/`indexTrainRoute`/
`indexSchedule`. One correctness bug this repo's docs found here:
`indexStation`'s written document was missing the `name` field even though it
was right there on the event.

**Docs to reference (this repo):**
- [`search-service/docs/README.md` §3 kafka/search.service.ts — Kafka Consumer](../search-service/docs/README.md#3-kafkasearchservicets--kafka-consumer) — the consumer's topic-routing `switch`
- [`search-service/docs/README.md` §4 services/search.service.ts — Indexing & Search](../search-service/docs/README.md#4-servicessearchservicets--indexing--search) — `indexStation`/`indexTrainRoute`, with the missing-`name`-field fix shown
- [`readme.md` §7 What Happens When Something Fails](../readme.md#7-what-happens-when-something-fails) — this repo's own DLQ pattern, which this bug used to defeat by swallowing errors internally

---

## 15. Search using Elastic Search
[Watch (48:04)](https://youtu.be/v_Q-ZBcxF84)

**Covers:** The read side — resolving a station name/code fuzzily and
running a nested query to find trains that run between two stations in the
right order.

**Maps to this repo:** `search-service/src/services/search.service.ts`'s
`searchTrains`/`resolveStation`/`autocompleteStation`. A good one to trace
through carefully — `resolveStation`'s three-tier fallback (exact code match
→ completion suggester → fuzzy `multi_match`) is worth understanding before
you hit `GET /trains?from=...&to=...` yourself.

**Docs to reference (this repo):**
- [`search-service/docs/README.md` §4 services/search.service.ts](../search-service/docs/README.md#4-servicessearchservicets--indexing--search) — `searchTrains`/`resolveStation` source, plus the nested-query/`inner_hits` shape explained
- [`search-service/docs/README.md` §Request/Event Lifecycle, Case C](../search-service/docs/README.md#requestevent-lifecycle) — a full trace of `GET /trains?from=...&to=...`
- [`docs/api-contract.md` §4 Search Service](./api-contract.md) — the exact query params and response shape

---

## 16. Initialising Inventory
[Watch (44:38)](https://youtu.be/M1kLBpLZvs0)

**Covers:** `inventory-service`'s Kafka consumer — turning an
`admin.schedule-created` event into per-seat `SeatInventory` rows plus a
`ScheduleInventory` aggregate row.

**Maps to this repo:** `inventory-service/src/kafka/consumer/inventory.consumer.ts`
and `services/inventory.service.ts`'s `initializeInventory`. Worth knowing:
this repo's own docs flag that, as of writing, admin-service's schedule route
not being mounted meant this consumer would sit with an empty database in
practice — check `admin-service/src/server.ts` to confirm that's now fixed.

**Docs to reference (this repo):**
- [`inventory-service/docs/README.md` §5 kafka/consumer/inventory.consumer.ts](../inventory-service/docs/README.md#5-kafkaconsumerinventoryconsumerts--reading-events) — the consumer, and why it only subscribes to the two topics it actually handles
- [`inventory-service/docs/README.md` §Lifecycle Walkthroughs, Case A](../inventory-service/docs/README.md#lifecycle-walkthroughs) — a step-by-step trace of `initializeInventory`
- [`inventory-service/docs/README.md` §4 prisma/schema.prisma](../inventory-service/docs/README.md#4-prismaschemaprisma--data-model) — the `IdempotencyRecord` table and why aggregates are always recomputed, never trusted as a running total

---

## 17. Booking Service Implementation: SAGA, Idempotency, Concurrency
[Watch (1:47:15)](https://youtu.be/XM5pS8Jq2yU)

**Covers:** The big one conceptually — why a booking needs a **saga**
(hold seats → create payment → confirm seats, with explicit compensation on
failure) instead of a single database transaction, since it spans two other
services' databases.

**Maps to this repo:** `booking-service/src/services/saga.service.ts` (the
three forward steps + three compensations) and `services/booking.service.ts`'s
`createBooking`. Read `booking-service/docs/README.md`'s Lifecycle Walkthrough
Case A alongside this video — it traces the exact same flow step by step
against the current code.

**Docs to reference (this repo):**
- [`booking-service/docs/README.md` §5 services/saga.service.ts — Saga Steps](../booking-service/docs/README.md#5-servicessagaservicets--saga-steps) — `compensateAll` and the three forward/compensation pairs, in full
- [`booking-service/docs/README.md` §Lifecycle Walkthroughs, Case A](../booking-service/docs/README.md#lifecycle-walkthroughs) — the complete happy-path trace this video builds toward
- [`booking-service/docs/README.md` §3 prisma/schema.prisma](../booking-service/docs/README.md#3-prismaschemaprisma--data-model) — `SagaLog`/`IdempotencyRecord`/`version` explained

---

## 18. Inventory: lockSeats, unlockSeats, confirmSeats
[Watch (2:46:41)](https://youtu.be/AGyRlkJ5Qv4)

**Covers:** The other side of the saga — inventory-service's HTTP routes
that actually hold, release, and confirm seats, including **partial-journey
segment locking** so two passengers can share a seat across non-overlapping
legs of a route.

**Maps to this repo:** `inventory-service/src/services/inventory.service.ts`
— `lockSeats`/`unlockSeats`/`confirmSeats`/`cancelBooking`, plus the
`recomputeSegmentSeatStatuses`/`recountScheduleAggregates` helpers and the
`SeatSegmentLock` table. This is the longest video for a reason — the
overlap-check logic (`a.fromSeq < b.toSeq AND b.fromSeq < a.toSeq`) is the
trickiest piece of business logic in the whole repo; `inventory-service/docs/README.md`
Lifecycle Case B walks through a concrete two-passenger example.

**Docs to reference (this repo):**
- [`inventory-service/docs/README.md` §7 services/inventory.service.ts — The Core Logic](../inventory-service/docs/README.md#7-servicesinventoryservicets--the-core-logic) — `recomputeSegmentSeatStatuses`/`recountScheduleAggregates`, pasted in full
- [`inventory-service/docs/README.md` §Lifecycle Walkthroughs, Case B](../inventory-service/docs/README.md#lifecycle-walkthroughs) — the concrete two-passenger segment-overlap example
- [`inventory-service/docs/README.md` §10 utils/lockExpiry.ts](../inventory-service/docs/README.md#10-utilslockexpiryts--the-background-sweep) — the Postgres advisory-lock leader election for the expiry sweep

---

## 19. Payment Service: Razorpay Integration using Adapter Design Pattern
[Watch (1:52:16)](https://youtu.be/an5N19xMoag)

**Covers:** Isolating the Razorpay SDK behind an abstract `BaseGateway`
interface so the rest of the service never imports Razorpay directly — order
creation, webhook signature verification, refunds.

**Maps to this repo:** `payment-service/src/services/gateways/base.gateway.ts`,
`razorpay.gateway.ts`, `gateway.factory.ts`, and `services/payment.service.ts`.
Important gap to know going in: there are **no real Razorpay credentials**
configured anywhere in this repo (`.env.example` only has placeholders) — so
every gateway call will fail with an auth error against the real Razorpay
API, by design of this environment, not a bug in the code.

**Docs to reference (this repo):**
- [`payment-service/docs/README.md` §2 services/gateways/ — The Adapter Pattern](../payment-service/docs/README.md#2-servicesgateways--the-adapter-pattern) — the `BaseGateway` abstract class and `RazorpayGateway`'s implementation
- [`payment-service/docs/README.md` §Lifecycle Walkthroughs, Case A](../payment-service/docs/README.md#lifecycle-walkthroughs) — the webhook-path trace, signature verification included
- [`payment-service/docs/README.md` §Known Issues](../payment-service/docs/README.md#known-issues--inconsistencies) — item 1, confirming no real Razorpay account exists in this environment

---

## 20. Handling Kafka Events with Optimistic Concurrency Control
[Watch (1:33:07)](https://youtu.be/ey6i5YOBeBk)

**Covers:** How `booking-service` reacts to `payment.success`/`payment.failed`
without double-processing a booking, even if the payment webhook, a user's
cancel request, and a background expiry job all race on the same row.

**Maps to this repo:** `booking-service/src/services/booking.service.ts`'s
`casUpdateBooking` helper (a compare-and-swap on the `version` column) and
`handlePaymentSuccess`/`handlePaymentFailure`. `StaleStateError` is what gets
thrown when the CAS loses a race — trace one call site to see how the loser
just bails out silently rather than corrupting state.

**Docs to reference (this repo):**
- [`booking-service/docs/README.md` §6 services/booking.service.ts — Core Orchestration](../booking-service/docs/README.md#6-servicesbookingservicets--core-orchestration) — the `casUpdateBooking` helper, pasted in full
- [`booking-service/docs/README.md` §Lifecycle Walkthroughs, Case A steps 5–6](../booking-service/docs/README.md#lifecycle-walkthroughs) — `handlePaymentSuccess` traced against the real CAS check
- [`docs/api-contract.md` §7 Booking Service](./api-contract.md) — the `payment.success`/`payment.failed` consumer status

---

## 21. cancelBooking with Optimistic Concurrency Control
[Watch (40:15)](https://youtu.be/8lV4_9DmdCg)

**Covers:** Letting a user cancel a booking at any non-terminal state,
releasing seats and (if a payment was captured) triggering a refund — using
the same CAS pattern from video 20 so a cancel can't race a confirming
payment.

**Maps to this repo:** `booking-service/src/services/booking.service.ts`'s
`cancelBooking`. Note the rollback behavior if the downstream
`inventoryClient.cancelBooking` call fails: the booking is rolled back from
`CANCELLING` back to `CONFIRMED` so the user can retry, rather than getting
stuck mid-transition.

**Docs to reference (this repo):**
- [`booking-service/docs/README.md` §Lifecycle Walkthroughs, Case C](../booking-service/docs/README.md#lifecycle-walkthroughs) — the confirmed-booking cancel/refund path, including the CANCELLING-rollback-on-failure behavior
- [`payment-service/docs/README.md` §Lifecycle Walkthroughs, Case C](../payment-service/docs/README.md#lifecycle-walkthroughs) — the refund running-total validation on the payment-service side

---

## 22. Final Testing of Booking Feature
[Watch (39:52)](https://youtu.be/AOO3cLhaMqA)

**Covers:** Exercising the whole booking saga end-to-end against real,
running infrastructure — signup → login → search → book → pay → confirm.

**Maps to this repo:** this is exactly the gap this repo's own docs are most
honest about — **nothing in this system has been verified live** in the
environment these docs were written in (no reachable Postgres/Redis/Kafka/
Elasticsearch, and no real Razorpay account). If you follow this video with
`docker-compose up -d` and your own service `.env` files filled in, **you'll
likely be the first to actually exercise this end-to-end** — see root
[`readme.md`](../readme.md) §10 for the exact commands, and
`docs/api-contract.md` for what each route/topic currently claims vs. what's
actually confirmed working.

**Docs to reference (this repo):**
- [`readme.md` §9 Current Status at a Glance](../readme.md#9-current-status-at-a-glance) — the per-service "starts up? / reachable end-to-end?" table to check yourself off against
- [`readme.md` §10 Running It Locally](../readme.md#10-running-it-locally) — the admin-UI ports (Kafka UI, Kibana, pgAdmin, Redis Insight) this repo's own `docker-compose.yml` brings up, which are the fastest way to *see* each saga step actually happening
- [`docs/api-contract.md`](./api-contract.md) — every route/topic's exact status tag (WORKING/BROKEN/UNREACHABLE/etc.) to verify against as you test
- [`missing.md`](../missing.md) — the standing punch list of what's still open; a good checklist to work through once basic testing succeeds

---

## Suggested order if you want to pace yourself

The playlist order already matches dependency order (each service needs the
previous one's events), so watching top-to-bottom works. If you want natural
stopping points to run/test what you've built so far:

1. **Videos 1–9** → user-service is fully functional (signup, login, refresh,
   profile). Stop here and actually hit it with `curl` before moving on.
2. **Videos 10–15** → gateway + admin + search are wired together. Stop and
   watch a station flow into Elasticsearch via Kafka UI.
3. **Videos 16–22** → inventory + booking + payment complete the saga. This
   is the long stretch (video 18 alone is 2h46m) — it's fine to split it
   across a few sessions.


---

<a id="file-search-service-docs-md"></a>
## FILE: `docs/search-service-docs.md`

# Search Service — Complete Guide

Single source of truth for the IRCTC Search Service: what it does, how a request/event flows through it, and how each piece works, with the actual current code inline.

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [Request/Event Lifecycle](#requestevent-lifecycle)
5. [Component Breakdown](#component-breakdown)
   - [index.ts — Entry Point](#1-indexts--entry-point)
   - [config/ — Config, Logger, Kafka, Elasticsearch](#2-config--config-logger-kafka-elasticsearch)
   - [kafka/search.service.ts — Kafka Consumer](#3-kafkasearchservicets--kafka-consumer)
   - [services/search.service.ts — Indexing & Search](#4-servicessearchservicets--indexing--search)
   - [controllers/, routes/, types/, utils/ — HTTP surface](#5-controllers-routes-types-utils--http-surface)
6. [Environment Variables](#environment-variables)
7. [Kafka Topics Reference](#kafka-topics-reference)
8. [Elasticsearch Indices Reference](#elasticsearch-indices-reference)
9. [Quick Start](#quick-start)
10. [Debugging Tips](#debugging-tips)
11. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **Search Service** is IRCTC's read-optimized search layer, backed by Elasticsearch instead of the relational data admin-service owns. As it exists today:

- **Consumes Kafka events** from admin-service (station/route/schedule lifecycle) and inventory-service (seat availability) to keep two Elasticsearch indices (`stations`, `trains`) up to date
- **Indexes stations** for autocomplete (edge-ngram + completion suggester) and **trains** with a nested `route` and a `schedules` array for date/availability filtering
- **Serves search over HTTP** — `GET /trains`, `GET /autocomplete`, and two debug endpoints (`GET /debug/stations`, `GET /debug/trains`) are mounted at root in `index.ts` and reach `services/search.service.ts`'s `searchTrains`/`autocompleteStation`/`getAllStations`/`getAllTrains`
- Used to carry a chunk of **leftover api-gateway scaffold** (JWT auth, Redis-backed rate limiting) that predated the Elasticsearch/Kafka work and no longer compiled against this service's trimmed-down config — that dead code has since been deleted (see below)

**This service now compiles cleanly.** `npx tsc --noEmit` from `search-service/` exits 0, confirmed in this session. That was not always true this session: this doc previously described the service as failing to build because `index.ts` imported a singular `./routes/search.route` module and default-imported `errorHandler` — both claims were already stale by the time this session's investigation started (`index.ts` already correctly imports `./routes/search.routes`, plural, and the named export `{ errorMiddleware }`). The *real* build blockers, found and fixed in this session, were different from what this doc used to describe — see [Known Issues](#known-issues--inconsistencies) items 1–2 for exactly what was wrong and how it was fixed.

**Not verified against live infrastructure.** Nothing in this session was run against a real Elasticsearch cluster or Kafka broker — none was reachable in the environment this work was done in. "Works when called" below means "type-checks and the logic reads correctly as intended," not "was observed to work end-to-end." Treat this as the current confidence level until someone runs it against live infra.

---

## Architecture

```
┌─────────────────────────────┐        ┌─────────────────────────────┐
│         admin-service        │        │      inventory-service       │
│  (station/route/schedule      │        │  (seat booked/released)      │
│   lifecycle)                  │        │                              │
└──────────────┬───────────────┘        └──────────────┬───────────────┘
               │ Kafka                                  │ Kafka
               │ admin.station-created  ✅ fires today   │ inventory.seat-
               │ admin.route-created    ✅ fires today   │ availability-updated
               │   (train.service.ts's createRoute calls  │ ✅ fires today
               │    adminProducer.publishRouteCreated —   │ (inventory.service.ts
               │    reachable via POST /trains/route)      │ calls it in 7 places)
               │ admin.schedule-created ✅ fires today    │
               │   (schedule.service.ts's createSchedule  │
               │    calls publishScheduleCreated —         │
               │    reachable via POST /schedules/schedule)│
               │ admin.schedule-cancelled ❌ never fires   │
               │   (publishScheduleCancelled has no caller │
               │    anywhere in admin-service)             │
               ▼                                          ▼
┌───────────────────────────────────────────────────────────────────────┐
│                          SEARCH SERVICE                                │
│                                                                         │
│  kafka/search.service.ts (SearchConsumer):                            │
│   consumer.subscribe([STATION_CREATED, ROUTE_CREATED,                 │
│                        SCHEDULE_CREATED, SCHEDULE_CANCELLED,           │
│                        SEAT_AVAILABILITY_UPDATED])                     │
│   consumer.run({ eachMessage: withDLQ(...) })                          │
│     → switch(topic) → services/search.service.ts's index* functions   │
│     → 3 failed attempts on one message → published to DLQ_SEARCH      │
│                                                                         │
│  services/search.service.ts:                                          │
│   INDEX OPERATIONS (called by the consumer above) ── write path       │
│   SEARCH OPERATIONS (searchTrains, autocompleteStation, ...) ── read   │
│   path — reachable over HTTP via controllers/routes below              │
│                                                                         │
│  routes/search.routes.ts → controllers/search.controller.ts:           │
│   GET /trains, GET /autocomplete, GET /debug/stations,                 │
│   GET /debug/trains — all call into services/search.service.ts         │
│                                                                         │
│  index.ts (Express app): corsMiddleware → helmet → reqLogger →        │
│   express.json → cookieParser → express.static("../public")           │
│   → searchRoutes → GET /health → notFound → errorMiddleware            │
└───────────────────────┬─────────────────────────────┬─────────────────┘
                         │ @elastic/elasticsearch       │
                         ▼                              │
              ┌─────────────────────────┐               │
              │   Elasticsearch          │               │
              │   stations  ✅ written    │               │
              │   trains    ✅ written    │               │
              │   routes    — declared,  │               │
              │   schedules — never used │               │
              └─────────────────────────┘               │
                                                          ▼
                                          A client calling GET /trains,
                                          /autocomplete, /debug/* reaches
                                          searchTrains/autocompleteStation/
                                          getAllStations/getAllTrains for
                                          real — not verified against a
                                          live Elasticsearch this session
```

---

## File Structure

```
search-service/
├── src/
│   ├── index.ts                          # Express app bootstrap — compiles and starts cleanly
│   ├── config/
│   │   ├── index.ts                      # Env vars → typed Config (SERVICE_NAME, PORT, NODE_ENV,
│   │   │                                 #   LOG_LEVEL, ELASTICSEARCH_URL, KAFKA_BROKER,
│   │   │                                 #   KAFKA_CLIENT_ID, ALLOWED_ORIGINS — nothing else)
│   │   ├── logger.ts                     # Winston logger
│   │   ├── kafka.ts                      # kafkajs client + consumer + DLQ producer
│   │   └── elasticsearch.ts              # ES client + index definitions + initIndices/recreateIndices
│   ├── kafka/
│   │   └── search.service.ts             # SearchConsumer — despite the filename, this is the Kafka
│   │                                     #   consumer, not services/search.service.ts (see note below)
│   ├── services/
│   │   └── search.service.ts             # Indexing (write) + search (read) logic, Elasticsearch-backed
│   ├── controllers/
│   │   └── search.controller.ts          # HTTP handlers: searchTrains, autoComplete, debugStations,
│   │                                     #   debugTrains — thin wrappers around services/search.service.ts
│   ├── middlewares/
│   │   ├── cors.middleware.ts            # Used by index.ts
│   │   ├── error.middleware.ts           # Used by index.ts (named export `errorMiddleware`)
│   │   ├── req.middleware.ts             # Used by index.ts
│   │   └── not-found.middleware.ts       # Used by index.ts — mounted right before errorMiddleware
│   ├── routes/
│   │   └── search.routes.ts              # GET /trains, /autocomplete, /debug/stations, /debug/trains —
│   │                                     #   mounted at root in index.ts
│   ├── types/
│   │   ├── index.ts                      # Empty
│   │   └── zod.ts                        # zSearchTrains — validates ?from=&to=&date= on GET /trains
│   └── utils/
│       ├── error.ts                      # AppError + subclasses
│       ├── asyncHandler.ts               # Wraps an async handler, forwards rejections to next() —
│       │                                 #   used by every handler in search.controller.ts
│       ├── api-response.ts               # SuccessResponse/ErrorResponse helpers — only ErrorResponse
│       │                                 #   is currently used (search.controller.ts's 400 path)
│       └── zod.formatter.ts              # formatZodError — first Zod issue message, or a fallback string
├── docs/                                  # This documentation
├── package.json
├── tsconfig.json
└── .env.example                           # New this session — see Environment Variables
```

**Naming note:** the Kafka consumer lives at `src/kafka/search.service.ts`, and the indexing/search logic lives at `src/services/search.service.ts` — two different files with the identical basename in different folders. `services/search.service.ts` imports the consumer's event-type interfaces with `import type { ... } from "../kafka/search.service"`; `kafka/search.service.ts` imports the indexing logic with `import searchService from "../services/search.service"`. Both imports resolve correctly (the paths differ), but the shared basename is worth knowing about before grepping for "search.service" and assuming there's only one file.

`tsconfig.json` sets `rootDir: ".."`, mirroring every other service in this repo — it lets the project compile files reached via `../../shared/...`-style imports. `kafka/search.service.ts` is the one file here that actually uses this (`../../../shared/constants/kafka-topics` and `../../../shared/utils/dlqHanlder` — note the shared file's own name is misspelled "dlqHanlder", not "dlqHandler"; this service imports it under its real, misspelled name rather than renaming the shared file).

**Three files that no longer exist:** `config/redis.ts`, `middlewares/auth.middleware.ts`, and `middlewares/rate-limiting.middleware.ts` were deleted this session. They were leftover api-gateway scaffold — nothing imported any of them (confirmed via grep before deleting), but each referenced `Config` fields (`REDIS_URL`, `JWT_ACCESS_SECRET`, `RATE_LIMIT_MAX_REQUESTS`, `RATE_LIMIT_WINDOW_MS`) that don't exist on this service's trimmed-down `Config` type. Because `tsconfig.json`'s `include: ["src/**/*.ts"]` matches every `.ts` file under `src/` regardless of whether anything imports it, `tsc` was type-checking these three dead files anyway — and their errors were blocking the *entire service* from compiling, not just themselves. Deleting them was the fix (see [Known Issues](#known-issues--inconsistencies) item 2).

---

## Request/Event Lifecycle

### Case A: `STATION_CREATED` event arrives (the one Kafka path that can actually fire today)

```
1.  admin-service's stationService.createStation publishes to admin.station-created
    after inserting a station row (see admin-service's own docs) — this call is real
    and reachable via POST /stations/station on that service.
2.  SearchConsumer.start()'s consumer.run() receives the message on topic
    KAFKA_TOPICS.STATION_CREATED
3.  withDLQ(...) parses the message value as JSON → parsedValue: unknown
4.  switch(topic) matches STATION_CREATED → parsedValue is cast to
    StationCreatedEvent and passed to searchService.indexStation(event)
5.  indexStation: event.data is the station payload ({id, name, code, city, state})
6.  esClient.index({ index: "stations", id: station.id, document: {...}, refresh: true })
      — document now includes stationId/name/code/city/suggest (name was missing
      before this session's fix — see Known Issues)
7.  logger.info(`Indexed station ${station.name} (${station.code})`)
8.  indexStation no longer has its own try/catch (removed this session) — if
    esClient.index throws, the error now propagates out of the switch branch,
    out of the eachMessage callback, and into withDLQ, which retries up to
    DLQ_MAX_RETRIES times before publishing the raw message to dlq.search-service.
```

### Case B: `ROUTE_CREATED` event arrives (also reachable today — this doc previously said it never fires)

```
1.  admin-service's trainService.createRoute publishes to admin.route-created after
    inserting the Route + RouteStation rows — reachable via POST /trains/route.
    The publish call passes the denormalized { ...route, train: existingTrain }
    (train includes its seats) so this consumer doesn't need to call back into
    admin-service for anything.
2.  SearchConsumer.start()'s consumer.run() receives the message on topic
    KAFKA_TOPICS.ROUTE_CREATED
3.  withDLQ(...) parses the message value as JSON → parsedValue: unknown
4.  switch(topic) matches ROUTE_CREATED → parsedValue is cast to RouteCreatedEvent
    and passed to searchService.indexTrainRoute({ train, routeStations })
5.  indexTrainRoute builds a seatSummary by counting train.seats by seatType, builds
    a TrainDocument (trainId/trainNumber/trainName/route/schedules: []/seatSummary),
    and esClient.index()'s it into the "trains" index
6.  It then also re-indexes every station in routeStations into the "stations"
    index (same shape as indexStation) so autocomplete stays current for stations
    that were created before this route existed
7.  logger.info(`Indexed train ${train.trainNumber} with ${routeStations.length} stations`)
8.  indexTrainRoute has no internal try/catch — if either esClient.index() call
    throws, the error propagates out to withDLQ, which retries up to
    DLQ_MAX_RETRIES times before publishing the raw message to dlq.search-service.
```

### Case C: A search request over HTTP (now reachable — was documented as unreachable before this session)

```
1.  Client calls GET /trains?from=NDLS&to=BCT&date=2026-08-01
2.  routes/search.routes.ts routes this to
    searchController.searchTrains (controllers/search.controller.ts)
3.  zSearchTrains.safeParse(req.query) validates from/to/date; on failure, responds
    400 with the first Zod issue's message via formatZodError
4.  On success, searchService.searchTrains({ from, to, date }) resolves "NDLS"/"BCT"
    via resolveStation (exact code match → completion-suggester fuzzy match →
    multi_match fuzzy match, in that order)
5.  A nested query against the "trains" index finds trains whose route contains
    both stations, with inner_hits so the matching stop's own fields (times,
    sequence number) come back per-hit
6.  Results are filtered to keep only hits where the "from" stop's sequenceNumber
    is before the "to" stop's — i.e. the train actually runs in that direction
7.  If `date` was given, the matching ACTIVE schedule for that date (if any) is
    attached to each result
8.  The controller responds 200 with { success: true, data: { from, to, date,
    count, trains } } — this used to be a hardcoded { success: true, message:
    "Train created successfully" } that discarded the real result; fixed this
    session (see Known Issues)
9.  This whole path type-checks cleanly and reads correctly, but hasn't been
    exercised against a live Elasticsearch — see the Overview's note on
    unverified live behavior.
```

---

## Component Breakdown

### 1. `index.ts` — Entry Point

```typescript
import "dotenv/config";
import path from "path";
import express, { Request, Response } from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import { config } from "./config";
import logger from "./config/logger";
import { initIndices, recreateIndices } from "./config/elasticsearch";
import { corsMiddleware } from "./middlewares/cors.middleware";
import { reqLogger } from "./middlewares/req.middleware";
import searchRoutes from "./routes/search.routes";
import searchConsumer from "./kafka/search.service";
import { disconnectAll } from "./config/kafka";
import { notFound } from "./middlewares/not-found.middleware";
import { errorMiddleware } from "./middlewares/error.middleware";

const app = express();

app.use(corsMiddleware);
app.use(
  helmet({
    crossOriginOpenerPolicy: false,
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", "'unsafe-inline'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", "data:"],
        connectSrc: ["'self'"],
      },
    },
  }),
);
app.use(reqLogger);
app.use(express.json());
app.use(cookieParser());

// Serve frontend static files
app.use(express.static(path.join(__dirname, "..", "public")));

// Mount search routes at root (gateway strips first path segment)
app.use(searchRoutes);

app.get("/health", (req: Request, res: Response) =>
  res.json({ status: "ok", service: config.SERVICE_NAME }),
);
app.use(notFound);
app.use(errorMiddleware);

const startServer = async (): Promise<void> => {
  if (process.env.ES_RECREATE_INDICES === "true") {
    await recreateIndices();
  } else {
    await initIndices();
  }
  await searchConsumer.start();

  const server = app.listen(config.PORT, () => {
    logger.info(
      `${config.SERVICE_NAME} running on http://localhost:${config.PORT}`,
    );
  });

  const shutdown = async (): Promise<void> => {
    logger.info("Shutting down...");
    server.close(async () => {
      await disconnectAll();
      process.exit(0);
    });
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
};

startServer();
```

This imports `./routes/search.routes` (plural) and the named export `{ errorMiddleware }` — both correct today, and both were already correct before this session's fixes started (a previous version of this doc claimed `index.ts` imported a nonexistent singular `./routes/search.route` and a nonexistent default-exported `errorHandler`; that description was stale and has been corrected). The one real change to this file this session: it now imports `{ notFound }` from `middlewares/not-found.middleware.ts` and mounts it (`app.use(notFound)`) right before `errorMiddleware` — previously that middleware existed but was never wired in.

Startup order: build/recreate Elasticsearch indices first (`ES_RECREATE_INDICES=true` wipes and rebuilds `stations`/`trains` from scratch — see `config/elasticsearch.ts`), then start the Kafka consumer, then start listening for HTTP. Shutdown is the reverse: stop accepting connections, then `disconnectAll()` (disconnects both the Kafka consumer and, if connected, the DLQ producer) before exiting. `express.static(path.join(__dirname, "..", "public"))` points at a `public/` directory that doesn't exist in this project — harmless (Express just won't find any static files to serve), but worth knowing if you expected a frontend to be served from here.

---

### 2. `config/` — Config, Logger, Kafka, Elasticsearch

**`config/index.ts`** — the only env vars this service actually reads:

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
  ELASTICSEARCH_URL: string | undefined;
  KAFKA_BROKER: string | undefined;
  KAFKA_CLIENT_ID: string | undefined;
  ALLOWED_ORIGINS: string | undefined;
}

export const config: Config = {
  SERVICE_NAME: packageJson.name,
  PORT: Number(process.env.PORT) || 4002,
  NODE_ENV: process.env.NODE_ENV || "development",
  LOG_LEVEL: process.env.LOG_LEVEL || "info",
  ELASTICSEARCH_URL: process.env.ELASTICSEARCH_URL,
  KAFKA_BROKER: process.env.KAFKA_BROKER,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS,
};
```

This is a deliberately trimmed-down `Config` — it has no `REDIS_URL`, `JWT_ACCESS_SECRET`, `RATE_LIMIT_*`, or `SERVICES`. That's exactly why the leftover api-gateway files that referenced those fields (`config/redis.ts`, `auth.middleware.ts`, `rate-limiting.middleware.ts`) no longer compiled — and exactly why they were deleted this session rather than patched (see [Known Issues](#known-issues--inconsistencies) item 2).

**`config/kafka.ts`** — kafkajs client, a `Consumer`, and a `Producer` used only for DLQ publishing:

```typescript
const kafka = new Kafka({
  clientId: config.KAFKA_CLIENT_ID,
  brokers: [config.KAFKA_BROKER || "localhost:9093"],
  logLevel: logLevel.ERROR,
  retry: { initialRetryTime: 300, retries: 8, maxRetryTime: 30000 },
});

const consumer: Consumer = kafka.consumer({
  groupId: "search-service-group-v2",
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

const producer: Producer = kafka.producer({
  allowAutoTopicCreation: true,
  retry: { retries: 3 },
});

let isProducerConnected = false;

const connectProducer = async (): Promise<void> => {
  if (!isProducerConnected) {
    await producer.connect();
    isProducerConnected = true;
    logger.info("Kafka producer connected (DLQ)");
  }
};

const disconnectAll = async (): Promise<void> => {
  await consumer.disconnect();
  if (isProducerConnected) {
    await producer.disconnect();
    isProducerConnected = false;
  }
  logger.info("Kafka consumer disconnected");
};

export { kafka, consumer, producer, connectProducer, disconnectAll };
```

Unlike admin-service's `admin.producer.ts` (which exists to *publish* domain events), this file's producer exists **only** to publish to the dead-letter topic when message processing fails — this service is a pure consumer of domain events. `groupId: "search-service-group-v2"` means every process running this service shares partition assignment; scaling out adds throughput rather than duplicate processing.

**`config/elasticsearch.ts`** — the ES client plus index bootstrapping:

```typescript
const esClient = new Client({ node: config.ELASTICSEARCH_URL });

const STATION_INDEX = "stations";
const TRAIN_INDEX = "trains";
const ROUTE_INDEX = "routes";
const SCHEDULE_INDEX = "schedules";

const initIndices = async (): Promise<void> => {
  // creates "stations" (edge_ngram autocomplete analyzer + completion suggester)
  // if it doesn't exist yet, and "trains" (nested route + schedules +
  // seatSummary mapping) if that doesn't exist yet
};

const recreateIndices = async (): Promise<void> => {
  // deletes "stations" and "trains" if present, then calls initIndices() again
};

export { esClient, STATION_INDEX, TRAIN_INDEX, ROUTE_INDEX, SCHEDULE_INDEX, initIndices, recreateIndices };
```

`ROUTE_INDEX` and `SCHEDULE_INDEX` are exported constants naming indices that **never get created** — `initIndices`/`recreateIndices` only ever touch `STATION_INDEX`/`TRAIN_INDEX`. Routes and schedules are instead folded *into* the `trains` index (as a nested `route` array and a `schedules` array respectively) rather than getting their own indices — so these two constants are currently unused outside their own declaration. This predates this session's fixes and is still accurate.

`config/logger.ts` is a plain Winston logger (same shape as admin-service's), except its inline comment is stale: it says `config.LOG_LEVEL is hardcoded to "4"`, which was true of an earlier version of `config/index.ts` but isn't true of the version shown above (`LOG_LEVEL` now reads `process.env.LOG_LEVEL || "info"`) — a documentation artifact left behind by an earlier config rewrite, not something touched this session, and not a functional bug.

---

### 3. `kafka/search.service.ts` — Kafka Consumer

```typescript
export interface StationCreatedEvent {
  eventType: "STATION_CREATED";
  data: { id: string; name: string; code: string; city: string; state?: string | null };
  timestamp: string;
}

export interface RouteCreatedEvent {
  train: { id: string; trainNumber: string; trainName: string; seats?: { seatType: string }[] };
  routeStations: {
    station: { id: string; name: string; code: string; city: string };
    sequenceNumber: number;
    arrivalTime: string | null;
    departureTime: string | null;
    distanceFromOrigin: number;
  }[];
}

export interface ScheduleCreatedEvent {
  scheduleId: string;
  trainId: string;
  departureDate: string;
  status: string;
  seats?: { seatId: string; seatNumber: number; seatType: string; price: number }[];
}

export interface ScheduleCancelledEvent {
  eventType: "SCHEDULE_CANCELLED";
  data: { id: string; trainId: string; status: string };
  timestamp: string;
}

export interface SeatAvailabilityUpdatedEvent {
  scheduleId: string;
  trainId: string;
  available: number;
  locked: number;
  booked: number;
}

class SearchConsumer {
  async start(): Promise<void> {
    await consumer.connect();
    await connectProducer(); // needed for DLQ publishing
    logger.info("Search consumer connected");

    await consumer.subscribe({
      topics: [
        KAFKA_TOPICS.STATION_CREATED,
        KAFKA_TOPICS.ROUTE_CREATED,
        KAFKA_TOPICS.SCHEDULE_CREATED,
        KAFKA_TOPICS.SCHEDULE_CANCELLED,
        KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED,
      ],
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: withDLQ<unknown>(
        producer,
        KAFKA_TOPICS.DLQ_SEARCH,
        logger,
        async ({ topic, partition, message, parsedValue }) => {
          logger.info(`Processing ${topic}`, { partition, offset: message.offset });

          switch (topic) {
            case KAFKA_TOPICS.STATION_CREATED:
              await searchService.indexStation(parsedValue as StationCreatedEvent);
              break;
            case KAFKA_TOPICS.ROUTE_CREATED:
              await searchService.indexTrainRoute(parsedValue as RouteCreatedEvent);
              break;
            case KAFKA_TOPICS.SCHEDULE_CREATED:
              await searchService.indexSchedule(parsedValue as ScheduleCreatedEvent);
              break;
            case KAFKA_TOPICS.SCHEDULE_CANCELLED:
              await searchService.cancelSchedule(parsedValue as ScheduleCancelledEvent);
              break;
            case KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED:
              await searchService.updateSeatAvailability(parsedValue as SeatAvailabilityUpdatedEvent);
              break;
            default:
              logger.warn(`Unknown topic: ${topic}`);
          }
        },
      ),
    });

    logger.info("Search consumer running...");
  }
}

export default new SearchConsumer();
```

This file was not changed this session — it's shown here unmodified. The event interfaces are defined here (not in a shared `types/` file) because this consumer is the natural owner of "what shape does a message on topic X have" — mirroring how admin-service's `admin.producer.ts` owns its own event interfaces on the publish side. `parsedValue` arrives from `withDLQ` as `unknown` (it's the direct result of `JSON.parse`-ing the message off the wire); the `as StationCreatedEvent` / etc. casts in each `switch` branch are the one place this codebase deliberately asserts an untyped external payload into a known shape, rather than typing it `any` — `services/search.service.ts` never sees an `any` from this boundary. `withDLQ` (from `shared/utils/dlqHanlder.ts`) retries a failing handler up to `DLQ_MAX_RETRIES` times before forwarding the raw message to `KAFKA_TOPICS.DLQ_SEARCH` and moving on, so one poison message can't block the whole partition forever. That retry/DLQ path now actually gets exercised for the four index functions this session touched — see the next section.

---

### 4. `services/search.service.ts` — Indexing & Search

Split into two halves. The **index operations** (called only by the Kafka consumer above):

```typescript
const indexStation = async (event: StationCreatedEvent): Promise<void> => {
  const station = event.data;
  if (!station) return;

  await esClient.index({
    index: STATION_INDEX,
    id: station.id,
    document: {
      stationId: station.id,
      name: station.name,
      code: station.code,
      city: station.city,
      suggest: {
        input: [station.name, station.code, station.city].filter(Boolean),
        weight: 10,
      },
    },
    refresh: true,
  });
  logger.info(`Indexed station ${station.name} (${station.code})`);
};
```

Two things changed here this session:

1. **`name` is now written.** The document used to have `stationId`/`code`/`city`/`suggest` but not `name`, even though `StationDocument`'s type marks `name` as optional specifically to model a station that hasn't gotten its name yet. Fixed to include `name: station.name`.
2. **The try/catch is gone.** `indexStation` used to catch its own Elasticsearch errors, log them, and swallow them — which meant `withDLQ` (wrapping the whole `eachMessage` handler in `kafka/search.service.ts`) never saw the error, so an Elasticsearch outage would silently drop the write instead of ending up on `dlq.search-service` after retries. The same fix (remove the internal try/catch, let the error propagate) was applied to `indexSchedule`, `cancelSchedule`, and `updateSeatAvailability` — all four now let Elasticsearch errors bubble up to `withDLQ`. `indexTrainRoute` already had no try/catch of its own around its Elasticsearch calls, so it needed no change; confirm this by reading the function below.

```typescript
const indexTrainRoute = async (
  routeEvent: RouteCreatedEvent,
): Promise<void> => {
  const { train, routeStations } = routeEvent;
  if (!train || !routeStations) return;

  const seatSummary: SeatSummary = {
    total: 0,
    LOWER: 0,
    MIDDLE: 0,
    UPPER: 0,
    SIDE_LOWER: 0,
    SIDE_UPPER: 0,
  };
  (train.seats || []).forEach((s) => {
    seatSummary.total++;
    if (seatSummary[s.seatType] !== undefined) seatSummary[s.seatType]++;
  });

  const doc: TrainDocument = {
    trainId: train.id,
    trainNumber: train.trainNumber,
    trainName: train.trainName,
    route: routeStations.map((rs) => ({
      stationId: rs.station.id,
      stationName: rs.station.name,
      stationCode: rs.station.code,
      sequenceNumber: rs.sequenceNumber,
      arrivalTime: rs.arrivalTime,
      departureTime: rs.departureTime,
      distanceFromOrigin: rs.distanceFromOrigin,
    })),
    schedules: [],
    seatSummary,
  };

  await esClient.index({
    index: TRAIN_INDEX,
    id: train.id,
    document: doc,
    refresh: true,
  });

  // Also index/update stations for autocomplete
  for (const rs of routeStations) {
    await esClient.index({
      index: STATION_INDEX,
      id: rs.station.id,
      document: {
        stationId: rs.station.id,
        name: rs.station.name,
        code: rs.station.code,
        city: rs.station.city,
        suggest: {
          input: [rs.station.name, rs.station.code, rs.station.city].filter(
            Boolean,
          ),
          weight: 10,
        },
      },
      refresh: true,
    });
  }

  logger.info(
    `Indexed train ${train.trainNumber} with ${routeStations.length} stations`,
  );
};
```

No try/catch here before or after this session — errors already propagated to `withDLQ` correctly.

The **search operations** (fully implemented, and now reachable from HTTP via `controllers/search.controller.ts` — see section 5):

```typescript
const searchTrains = async ({
  from,
  to,
  date,
}: {
  from: string;
  to: string;
  date?: string;
}): Promise<SearchTrainsResult> => {
  const fromStation = await resolveStation(from);
  const toStation = await resolveStation(to);

  if (!fromStation)
    return { trains: [], message: `Station "${from}" not found` };
  if (!toStation) return { trains: [], message: `Station "${to}" not found` };

  // nested query against "trains", one clause per station, each with its own
  // inner_hits so we get back the matching stop's own sequenceNumber/times
  const result = (await esClient.search({ index: TRAIN_INDEX, query, size: 50 }))
    as unknown as TrainSearchResult;

  const trains = result.hits.hits
    .map((hit): SearchTrainMatch | null => {
      // keep only hits where fromHit.sequenceNumber < toHit.sequenceNumber
      // (i.e. the train actually travels in the requested direction)
    })
    .filter((t): t is SearchTrainMatch => t !== null);

  return { from: {...}, to: {...}, date: date || "any", count: trains.length, trains };
};

const resolveStation = async (input: string): Promise<StationDocument | null> => {
  // 1. exact code match  2. completion suggester (typo-tolerant)  3. fuzzy multi_match
};
```

Every `esClient.search`/`esClient.index`/`esClient.update` call in this file is typed against a small set of locally-declared interfaces (`StationDocument`, `TrainDocument`, `RouteStop`, `ScheduleSummary`, `SeatSummary`, plus `EsHit`/`EsSimpleSearchResult`/`EsSuggestResult`/`TrainSearchResult` for the response shapes) rather than the Elasticsearch client's own deep response generics or a blanket `any` — the wire response is asserted into these shapes once (`as unknown as X`) right after the call, and everything downstream is fully typed. Every `catch` block narrows its error with a shared `errorMessage(err: unknown): string` helper (`err instanceof Error ? err.message : String(err)`) instead of asserting `err as Error`.

`autocompleteStation`, `getAllStations`, and `getAllTrains` are also defined here and exported the same way; they're what `controllers/search.controller.ts` calls for `GET /autocomplete`, `GET /debug/stations`, and `GET /debug/trains` respectively (see section 5).

---

### 5. `controllers/`, `routes/`, `types/`, `utils/` — HTTP surface

This section replaces what an earlier version of this doc called "leftover api-gateway scaffold" — that description was accurate for `config/redis.ts`, `middlewares/auth.middleware.ts`, and `middlewares/rate-limiting.middleware.ts`, all three of which have been **deleted this session** (see [Known Issues](#known-issues--inconsistencies) item 2). What's described below is the real, live, currently-mounted HTTP surface, unrelated to that dead scaffold.

**`controllers/search.controller.ts`** — one thin `asyncHandler`-wrapped function per route:

```typescript
import { Request, Response, NextFunction } from "express";
import { zSearchTrains } from "../types/zod";
import asyncHandler from "../utils/asyncHandler";
import searchService from "../services/search.service";
import { ErrorResponse } from "../utils/api-response";
import { formatZodError } from "../utils/zod.formatter";

/**
 * GET /trains?from=Delhi&to=Mumbai&date=2025-07-15
 *
 * Searches for trains running between two stations, optionally filtered to
 * a specific departure date. Station names/codes are fuzzy-resolved (see
 * searchService.resolveStation).
 */
const searchTrains = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const result = zSearchTrains.safeParse(req.query);
    if (!result.success) {
      return ErrorResponse(res, 400, {
        message: formatZodError(result.error),
      });
    }

    const { from, to, date } = result.data;
    const response = await searchService.searchTrains({ from, to, date });

    return res.status(200).json({ success: true, data: response });
  },
);

/**
 * GET /autocomplete?q=del
 */
const autoComplete = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const { q } = req.query;

    const response = await searchService.autocompleteStation(q as string);

    return res.status(200).json({ success: true, data: response });
  },
);

/**
 * GET /debug/stations — lists every indexed station document, for
 * inspecting the Elasticsearch index directly rather than through the
 * autocomplete/fuzzy-search paths.
 */
const debugStations = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const response = await searchService.getAllStations();

    return res.status(200).json({ success: true, data: response });
  },
);

/**
 * GET /debug/trains — lists every indexed train document.
 */
const debugTrains = asyncHandler(
  async (req: Request, res: Response, next: NextFunction) => {
    const response = await searchService.getAllTrains();

    return res.status(200).json({ success: true, data: response });
  },
);

export const searchController = {
  searchTrains,
  autoComplete,
  debugStations,
  debugTrains,
};
```

Three separate fixes landed on this file this session:

1. **The import was wrong.** `searchService` used to be imported from `../services/inventory.service`, a module that doesn't exist in this project — fixed to `../services/search.service`. This alone was one of the two real compile blockers this session found (see [Known Issues](#known-issues--inconsistencies) item 1).
2. **The whole file's top doc-comment was copy-pasted from admin-service's `schedule.controller.ts`** — it described a `POST /schedule` endpoint, request body validation against `zSchedule`, and a note about a "Train created successfully" message being a leftover from `train.controller.ts`. None of that describes this file. It's been removed and replaced with accurate per-handler comments (shown above).
3. **`searchTrains` used to discard its own result.** It called `searchService.searchTrains(...)`, computed `response`, and then responded with a hardcoded `{ success: true, message: "Train created successfully" }` — the real Elasticsearch result was thrown away. Fixed to `res.status(200).json({ success: true, data: response })`.
4. **`debugStations` and `debugTrains` were copy-paste bugs.** Both used to call `searchService.autocompleteStation(q as string)` — identical to what `autoComplete` does, and using a `q` query param that these debug routes don't even take. Fixed: `debugStations` now calls `searchService.getAllStations()`, `debugTrains` now calls `searchService.getAllTrains()` — both functions already existed in `services/search.service.ts` for exactly this purpose.

**`routes/search.routes.ts`** — maps the four HTTP routes to the controller above:

```typescript
import { Router } from "express";
import { searchController } from "../controllers/search.controller";

const router = Router();

// Mounted at root in index.ts (the gateway strips the first path segment
// before forwarding, so no "/search" prefix is needed here).
// GET /search/trains?from=Delhi&to=Mumbai&date=2025-07-15
router.get("/trains", searchController.searchTrains);

// GET /search/autocomplete?q=del
router.get("/autocomplete", searchController.autoComplete);

// Debug endpoints
router.get("/debug/stations", searchController.debugStations);
router.get("/debug/trains", searchController.debugTrains);

export default router;
```

The file-level comment used to be a copy-paste of admin-service's stale note about an unmounted `POST /schedule` route ("there is currently no HTTP path that reaches scheduleController.createSchedule..."), which described admin-service's dead code, not this file. It's been replaced with the one-line, accurate comment shown above.

**`middlewares/not-found.middleware.ts`** — a 404 handler that now actually runs:

```typescript
// ============================================
// 404 Not Found Middleware
// ============================================
// Registered in index.ts right after searchRoutes. Express only reaches this
// handler if no earlier route matched the request, so its job is simply to
// turn "no match" into a proper NotFoundError.

import { Request, Response, NextFunction } from "express";
import { NotFoundError } from "../utils/error";

/**
 * Catch-all handler for unmatched routes — should be registered after
 * all other routes but before the global error middleware.
 */
export function notFound(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  // Pass a NotFoundError (404) to the next error-handling middleware
  // (errorMiddleware) instead of responding directly — keeps error
  // formatting centralized in one place.
  next(new NotFoundError(`Route ${req.method} ${req.path} not found`));
}
```

This function was always fully written and correct, but `index.ts` never called `app.use(notFound)` — so it never ran, and any unmatched route just fell through to Express's default 404 HTML page instead of this service's JSON error shape. Fixed this session by mounting it in `index.ts` right before `errorMiddleware`. Its own comment used to say it was "Registered in index.ts right after `app.use(\"/api\", gatewayRouter)`" — a copy-paste from api-gateway's code, not something that ever existed in `index.ts` here — the comment is corrected now too.

**`types/zod.ts`** — validates the query params for `GET /trains`:

```typescript
import { z } from "zod";

export const zSearchTrains = z.object({
  from: z
    .string({ error: "Origin station is required" })
    .trim()
    .min(1, "Origin station is required")
    .max(50, "Origin station cannot exceed 50 characters"),
  to: z
    .string({ error: "Destination station is required" })
    .trim()
    .min(1, "Destination station is required")
    .max(50, "Destination station cannot exceed 50 characters"),
  date: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/, "Date must be in YYYY-MM-DD format")
    .optional(),
});
export type SearchTrainsQuery = z.infer<typeof zSearchTrains>;
```

Not touched this session — shown for completeness since it wasn't in a previous version of this doc either. `types/index.ts` (a separate file) is empty.

**`utils/api-response.ts`** and **`utils/zod.formatter.ts`** round out the HTTP surface: `ErrorResponse`/`SuccessResponse` are small `res.json(...)` wrappers (only `ErrorResponse` is actually called today, from `searchTrains`'s 400 path), and `formatZodError` pulls the first Zod issue's message off a `ZodError` for that same 400 response. `utils/asyncHandler.ts` wraps every controller function shown above, forwarding any rejected promise to `next()` so it reaches `errorMiddleware`. None of these four files were changed this session.

---

## Environment Variables

Everything actually read via `config.*` or `process.env` directly:

```bash
PORT=4002
NODE_ENV=development
LOG_LEVEL=info

ELASTICSEARCH_URL=http://localhost:9200
KAFKA_BROKER=localhost:9093
KAFKA_CLIENT_ID=search-service

ALLOWED_ORIGINS=http://localhost:3000,http://localhost:4000

# Read directly via process.env in index.ts, not through config/index.ts:
ES_RECREATE_INDICES=false   # wipes + rebuilds stations/trains on startup if "true" — destructive, leave false/unset for normal use
```

A `.env.example` matching the block above was added this session (didn't exist before). Note there's still no `REDIS_URL`, `JWT_ACCESS_SECRET`, `JWT_REFRESH_SECRET`, or any `RATE_LIMIT_*` variable — consistent with those being concerns of the deleted leftover-scaffold files, not omissions.

---

## Kafka Topics Reference

| Topic | Subscribed here? | Actually fires in this system today? |
|---|---|---|
| `admin.station-created` | ✅ → `indexStation` | ✅ yes — admin-service's `stationService.createStation` publishes it on every `POST /stations/station` |
| `admin.route-created` | ✅ → `indexTrainRoute` | ✅ yes — admin-service's `trainService.createRoute` calls `adminProducer.publishRouteCreated(...)` (not commented out), reachable via `POST /trains/route`. This doc previously said this call was commented out; that was stale by the time of this staleness audit — the call is live in current source |
| `admin.schedule-created` | ✅ → `indexSchedule` | ✅ yes — `scheduleService.createSchedule` calls `publishScheduleCreated`, and `schedule.route.ts` is mounted at `/schedules` in admin-service's `server.ts` (`POST /schedules/schedule`). This doc previously said the route was never mounted; that was stale — it is mounted in current source |
| `admin.schedule-cancelled` | ✅ → `cancelSchedule` | ❌ no — `publishScheduleCancelled` exists on `admin.producer.ts` but nothing in admin-service calls it anywhere (still true as of this audit) |
| `inventory.seat-availability-updated` | ✅ → `updateSeatAvailability` | ✅ yes — inventory-service's `inventory.service.ts` calls `inventoryProducer.publishSeatAvailabilityUpdated(...)` in 7 places (not commented out). Confirmed by reading the source during this audit; still not verified against a live broker |
| `dlq.search-service` | Published to (not subscribed) | Only when a handler above throws 3 times in a row for the same message (`DLQ_MAX_RETRIES`) — reachable for all five index-side handlers, since their internal try/catch blocks were removed in an earlier session |

In short: as the rest of this system is currently wired, `STATION_CREATED`, `ROUTE_CREATED`, `SCHEDULE_CREATED`, and `SEAT_AVAILABILITY_UPDATED` are all known to actually reach this consumer today — only `SCHEDULE_CANCELLED` remains structurally unreachable (no caller anywhere in admin-service). This is a reversal of what an earlier version of this doc claimed (that only station-creation events fired); see [Known Issues](#known-issues--inconsistencies) items 9–10 for what changed.

---

## Elasticsearch Indices Reference

| Index (constant) | Created by `initIndices`? | Written by | Read by |
|---|---|---|---|
| `stations` (`STATION_INDEX`) | ✅ | `indexStation`, `indexTrainRoute`'s per-station loop | `resolveStation`, `autocompleteStation`, `getAllStations` |
| `trains` (`TRAIN_INDEX`) | ✅ | `indexTrainRoute`, `indexSchedule`, `cancelSchedule`, `updateSeatAvailability` — all four are now confirmed reachable in practice (see Kafka Topics Reference above), except `cancelSchedule` which needs `admin.schedule-cancelled` to fire and that topic still never does | `searchTrains`, `getAllTrains` |
| `routes` (`ROUTE_INDEX`) | ❌ never created | — | — |
| `schedules` (`SCHEDULE_INDEX`) | ❌ never created | — | — |

Routes and schedules live *inside* the `trains` index (a nested `route` array and a plain `schedules` array respectively) rather than as their own indices — `ROUTE_INDEX`/`SCHEDULE_INDEX` are exported names for indices that don't exist. This predates this session's fixes and is unchanged.

---

## Quick Start

```bash
cd search-service
npm install
```

**This now type-checks and starts cleanly** — `npx tsc --noEmit` exits 0, confirmed in this session. Note it has **not** been run against a live Elasticsearch or Kafka broker in this session (neither was reachable in this environment), so "starts cleanly" here means the process boots and the code is logically sound, not that a full end-to-end run was observed.

```bash
# .env needs at least ELASTICSEARCH_URL, KAFKA_BROKER, KAFKA_CLIENT_ID, ALLOWED_ORIGINS
# Elasticsearch and Kafka both need to be reachable for this to do anything useful
npm run dev              # nodemon + ts-node, hot reload

curl http://localhost:4002/health
# { "status": "ok", "service": "Search Service" }

curl "http://localhost:4002/trains?from=NDLS&to=BCT"
# { "success": true, "data": { "trains": [], "message": "Station \"NDLS\" not found" } }
# (or a real result set, once stations/trains are actually indexed via Kafka)
```

---

## Debugging Tips

- **A station never shows up in Elasticsearch even though it was created in admin-service** → check this consumer's own logs; an Elasticsearch failure inside `indexStation` now propagates to `withDLQ` and, after `DLQ_MAX_RETRIES` attempts, lands on `dlq.search-service` — check that topic, not just the console, for dropped writes. Also confirm `KAFKA_BROKER`/`ELASTICSEARCH_URL` point at reachable services.
- **Trains never appear in the `trains` index** → check whether `admin.route-created` actually fired (an earlier version of this doc said it structurally couldn't — that's no longer true; admin-service's `POST /trains/route` does publish it today). If the topic did fire, check this consumer's logs / `dlq.search-service` for a failed `indexTrainRoute` instead.
- **Schedules never get added to a train's `schedules` array** → same shape of check: `admin.schedule-created` is reachable today via `POST /schedules/schedule` in admin-service (an earlier version of this doc said the route wasn't mounted — it now is), so look at `indexSchedule`'s logs / DLQ rather than assuming the event never fires.
- **A schedule cancellation never reflects in Elasticsearch** → this one genuinely can't happen yet: nothing in admin-service calls `publishScheduleCancelled`, so `cancelSchedule` is never invoked regardless of what this service does.
- **`GET /trains` responds with `{ trains: [], message: "Station ... not found" }`** → `resolveStation` couldn't find that station via exact code match, completion suggester, or fuzzy `multi_match` — check the `stations` index actually has a document for it (`GET /debug/stations`).
- **`GET /debug/stations` or `/debug/trains` looks empty** → these call `getAllStations()`/`getAllTrains()` (a plain `match_all` query, `size: 100`) — an empty result means the corresponding index genuinely has no documents yet, most likely because the Kafka events that would populate it (see the topics table) haven't fired.
- **`ES_RECREATE_INDICES=true` and startup takes a while / logs "Deleted index"** → that's `recreateIndices()` wiping and rebuilding `stations`/`trains` from scratch; unset it (or set to anything other than `"true"`) for a normal create-if-missing startup.
- **`npx tsc --noEmit` suddenly fails again after adding a new file under `src/`** → remember `tsconfig.json`'s `include: ["src/**/*.ts"]` type-checks every `.ts` file under `src/`, whether or not anything imports it. This is exactly what happened with the three now-deleted leftover scaffold files — a single unused, half-finished file can block the whole service from compiling.

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, since these are informational (same approach as the API Gateway's, Notification Service's, and Admin Service's docs). Items 1–7 describe an earlier session's fixes to this service's own code (kept here for the historical record of what was wrong and why); items 8, 11–16 are pre-existing and still open; items 9–10 describe upstream admin-service behavior that a later staleness audit found had changed (or had simply been misdocumented) since this doc was last updated — see each item for detail.

1. **This doc previously described the wrong build blocker.** An earlier version claimed `index.ts` imported a nonexistent `./routes/search.route` (singular) and default-imported a nonexistent `errorHandler`. Both claims were already stale by the start of this session — `index.ts` already correctly imported `./routes/search.routes` (plural) and the named export `{ errorMiddleware }`. That description has been corrected throughout this doc.
2. **The real build blockers, found and fixed this session:**
   - `controllers/search.controller.ts` imported `searchService` from `../services/inventory.service`, which doesn't exist in this project — fixed to `../services/search.service`.
   - Three unused, api-gateway-style scaffold files — `config/redis.ts`, `middlewares/auth.middleware.ts`, `middlewares/rate-limiting.middleware.ts` — referenced `Config` fields (`REDIS_URL`, `JWT_ACCESS_SECRET`, `RATE_LIMIT_MAX_REQUESTS`, `RATE_LIMIT_WINDOW_MS`) that don't exist on this service's trimmed-down `Config`. Nothing imported any of the three (confirmed via grep before deleting), but `tsc` still type-checked them anyway because `tsconfig.json`'s `include: ["src/**/*.ts"]` matches every `.ts` file under `src/` regardless of import status — so this dead code alone was blocking the *entire service* from compiling. All three were deleted. `npx tsc --noEmit` now exits 0.
3. **`searchController.searchTrains` used to discard its own result.** It computed a real Elasticsearch result via `searchService.searchTrains(...)` and then responded with a hardcoded `{ success: true, message: "Train created successfully" }` instead of the computed data — a copy-paste leftover from a train-creation endpoint elsewhere. Fixed to return `{ success: true, data: response }`.
4. **`debugStations` and `debugTrains` were copy-paste bugs.** Both called `searchService.autocompleteStation(q as string)` — identical to `/autocomplete`'s own handler, and referencing a `q` param these debug routes don't take. Fixed: `debugStations` now calls `getAllStations()`, `debugTrains` now calls `getAllTrains()` — both already existed in `services/search.service.ts` for exactly this purpose.
5. **`indexStation`'s document was missing a top-level `name` field**, even though `StationDocument`'s type marks it optional specifically to model this. Fixed by adding `name: station.name` to the indexed document.
6. **Four index-operation functions used to swallow their own Elasticsearch errors.** `indexStation`, `indexSchedule`, `cancelSchedule`, and `updateSeatAvailability` each had a try/catch that logged an ES error and returned normally — since `withDLQ` (wrapping the whole `eachMessage` handler) only retries/forwards errors that propagate *out* of the wrapped handler, an Elasticsearch outage inside any of these four used to be silently dropped instead of landing on `dlq.search-service`. Fixed by removing all four try/catch blocks so errors now propagate to `withDLQ`. `indexTrainRoute` already had no internal try/catch and needed no change.
7. **`notFound` (in `middlewares/not-found.middleware.ts`) was fully written but never mounted.** `index.ts` never called `app.use(notFound)`, so unmatched routes fell through to Express's default 404 page instead of this service's JSON error shape. Fixed by mounting it right before `errorMiddleware`. Its own comment also used to say it was "Registered in index.ts right after `app.use(\"/api\", gatewayRouter)`" — a copy-paste from api-gateway, not accurate here — corrected to describe its actual mount point (right after `searchRoutes`).
8. **`ROUTE_INDEX` and `SCHEDULE_INDEX`** (in `config/elasticsearch.ts`) name Elasticsearch indices that `initIndices`/`recreateIndices` never create — route and schedule data live inside the `trains` index's nested `route`/`schedules` fields instead. Predates this session, unchanged.
9. **`admin.route-created` now actually fires — this doc previously said otherwise.** A prior version of this doc claimed `trainService.createRoute`'s call to `adminProducer.publishRouteCreated(...)` was commented out. As of this staleness audit, that's not true: the call is live (`train.service.ts` line ~145, wrapped in a `.catch` that logs rather than rethrows), reachable via `POST /trains/route`, so `indexTrainRoute` is invoked for real whenever a route is attached to a train. Whether admin-service's own commit history actually re-enabled this call, or the earlier doc was simply wrong, wasn't investigated — only current source was checked.
10. **`admin.schedule-created` now actually fires too — also previously misdocumented.** A prior version of this doc said the HTTP route that triggers schedule creation was never mounted in admin-service. As of this audit, `admin-service/src/server.ts` does `app.use("/schedules", scheduleRoutes)`, and `schedule.route.ts` maps `POST /schedule` (→ `/schedules/schedule`) to `scheduleController.createSchedule`, which calls `scheduleService.createSchedule`, which calls `adminProducer.publishScheduleCreated(...)` unconditionally (not wrapped in `.catch` — a Kafka failure there throws and turns the request into a 500, per that function's own comment). So `indexSchedule` is invoked for real on every successful schedule creation.
11. **Not verified against live infrastructure.** No Elasticsearch cluster or Kafka broker was reachable in the environment this session's fixes were made in. Everything above about the service "working" means it type-checks and the logic reads correctly — nobody has observed a station actually land in a live `stations` index, or a live `GET /trains` call return real results, since these fixes were made. Treat this as the single biggest open risk before calling this service done.
12. **`config/logger.ts`'s inline comment about `LOG_LEVEL` being hardcoded to `"4"` is stale** — that was true of an earlier version of `config/index.ts`; the current one reads `process.env.LOG_LEVEL || "info"`. Purely a documentation artifact in the code, predates this session, not a functional issue.
13. **Two files share the basename `search.service.ts`** — the Kafka consumer at `src/kafka/search.service.ts` and the indexing/search logic at `src/services/search.service.ts`. Both compile and import each other correctly (via distinct relative paths), but it's an easy source of confusion when searching the codebase by filename alone.
14. **`express.static(path.join(__dirname, "..", "public"))`** in `index.ts` points at a `public/` directory that doesn't exist anywhere in this project — harmless at runtime (Express simply serves nothing from it), but dead configuration.
15. **`npm run seed` points at `src/services/seed.ts`**, which doesn't exist in this project — running that script fails. The same issue is already flagged in several other services' docs in this repo, likely from a shared `package.json` origin.
16. **Unrelated dependencies in `package.json`**: `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`, `mongoose`, `resend`, `morgan`, `jsonwebtoken`, `ioredis` are all listed, but nothing in `src/` imports any of them now that the auth/rate-limit/redis scaffold files that used to reference some of them (`jsonwebtoken`, `ioredis`) are deleted. Worth pruning from `package.json` at some point, though that's a `package.json` change, not something this documentation pass makes.

None of the above (aside from what's described as fixed in items 1–7, and the corrected upstream-firing status in items 9–10) are being changed as part of this documentation pass — flagging the rest here so they're visible next time someone works on this service. This pass only ever edits this file, never `search-service/src/` or any other service's code.


---

<a id="file-user-service-docs-md"></a>
## FILE: `docs/user-service-docs.md`

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

