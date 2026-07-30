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
               │ admin.route-created    ❌ never fires   │ availability-updated
               │   (publish call is commented out in     │
               │    admin-service's train.service.ts)     │
               │ admin.schedule-created ❌ never fires    │
               │   (the HTTP route that triggers it is    │
               │    never mounted in admin-service)       │
               │ admin.schedule-cancelled                 │
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

### Case B: `ROUTE_CREATED` event — subscribed to, but structurally can never arrive

```
1.  SearchConsumer subscribes to KAFKA_TOPICS.ROUTE_CREATED and, if a message ever
    arrived, would cast it to RouteCreatedEvent and call
    searchService.indexTrainRoute({ train, routeStations }) — fully implemented,
    would index a "trains" document with a nested route and a seatSummary.
2.  In practice this never happens: admin-service's trainService.createRoute has its
    adminProducer.publishRouteCreated(...) call commented out (verified directly in
    admin-service's source this session — see that service's own docs, Known Issue
    about the commented-out publish). No producer in this system ever sends a
    message on admin.route-created today.
3.  Net effect: the "trains" Elasticsearch index can never actually be populated
    through this consumer as the system is currently wired, independent of anything
    in this service — this service's own compile errors are fixed, but the upstream
    gap in admin-service is not.
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
| `admin.route-created` | ✅ → `indexTrainRoute` | ❌ no — admin-service's only call to `publishRouteCreated` is commented out (verified in that service's own source/docs this session) |
| `admin.schedule-created` | ✅ → `indexSchedule` | ❌ no — the publish call itself is correct in admin-service, but the HTTP route that would trigger it (`schedule.route.ts`) is never mounted there |
| `admin.schedule-cancelled` | ✅ → `cancelSchedule` | ❌ no — nothing in admin-service calls `publishScheduleCancelled` at all |
| `inventory.seat-availability-updated` | ✅ → `updateSeatAvailability` | Not verified this session — inventory-service's source wasn't reviewed as part of this pass |
| `dlq.search-service` | Published to (not subscribed) | Only when a handler above throws 3 times in a row for the same message (`DLQ_MAX_RETRIES`) — now actually reachable for all four index-side handlers, since their internal try/catch blocks were removed this session |

In short: as the rest of this system is currently wired, only station-creation events are known to actually reach this consumer — everything else this service subscribes to depends on gaps documented in admin-service's own docs, not on anything in this service.

---

## Elasticsearch Indices Reference

| Index (constant) | Created by `initIndices`? | Written by | Read by |
|---|---|---|---|
| `stations` (`STATION_INDEX`) | ✅ | `indexStation`, `indexTrainRoute`'s per-station loop | `resolveStation`, `autocompleteStation`, `getAllStations` |
| `trains` (`TRAIN_INDEX`) | ✅ | `indexTrainRoute`, `indexSchedule`, `cancelSchedule`, `updateSeatAvailability` | `searchTrains`, `getAllTrains` |
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
- **Trains never appear in the `trains` index no matter what** → this isn't a bug in this service to chase — admin-service never actually publishes `admin.route-created` today (see the Kafka Topics table above), so `indexTrainRoute` is never invoked in practice.
- **Schedules never update a train's `schedules` array** → same root cause, different reason: the admin-service HTTP route that would trigger `admin.schedule-created` isn't mounted there.
- **`GET /trains` responds with `{ trains: [], message: "Station ... not found" }`** → `resolveStation` couldn't find that station via exact code match, completion suggester, or fuzzy `multi_match` — check the `stations` index actually has a document for it (`GET /debug/stations`).
- **`GET /debug/stations` or `/debug/trains` looks empty** → these call `getAllStations()`/`getAllTrains()` (a plain `match_all` query, `size: 100`) — an empty result means the corresponding index genuinely has no documents yet, most likely because the Kafka events that would populate it (see the topics table) haven't fired.
- **`ES_RECREATE_INDICES=true` and startup takes a while / logs "Deleted index"** → that's `recreateIndices()` wiping and rebuilding `stations`/`trains` from scratch; unset it (or set to anything other than `"true"`) for a normal create-if-missing startup.
- **`npx tsc --noEmit` suddenly fails again after adding a new file under `src/`** → remember `tsconfig.json`'s `include: ["src/**/*.ts"]` type-checks every `.ts` file under `src/`, whether or not anything imports it. This is exactly what happened with the three now-deleted leftover scaffold files — a single unused, half-finished file can block the whole service from compiling.

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, since these are informational (same approach as the API Gateway's, Notification Service's, and Admin Service's docs). Items 1–7 describe this session's fixes (kept here for the historical record of what was wrong and why); items 8+ are pre-existing and still open.

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
9. **`admin.route-created` never actually fires.** Verified directly in admin-service's source: `trainService.createRoute`'s call to `adminProducer.publishRouteCreated(...)` is commented out. This means `indexTrainRoute` — the only code path that ever creates a `trains` index document — is never invoked in the system as currently wired, independent of anything in this service. Predates this session.
10. **`admin.schedule-created` never actually fires either**, for a different reason: admin-service's `schedule.service.ts` does correctly call `publishScheduleCreated`, but the HTTP route that would trigger schedule creation (`schedule.route.ts`) is never mounted in admin-service's `server.ts`. Predates this session.
11. **Not verified against live infrastructure.** No Elasticsearch cluster or Kafka broker was reachable in the environment this session's fixes were made in. Everything above about the service "working" means it type-checks and the logic reads correctly — nobody has observed a station actually land in a live `stations` index, or a live `GET /trains` call return real results, since these fixes were made. Treat this as the single biggest open risk before calling this service done.
12. **`config/logger.ts`'s inline comment about `LOG_LEVEL` being hardcoded to `"4"` is stale** — that was true of an earlier version of `config/index.ts`; the current one reads `process.env.LOG_LEVEL || "info"`. Purely a documentation artifact in the code, predates this session, not a functional issue.
13. **Two files share the basename `search.service.ts`** — the Kafka consumer at `src/kafka/search.service.ts` and the indexing/search logic at `src/services/search.service.ts`. Both compile and import each other correctly (via distinct relative paths), but it's an easy source of confusion when searching the codebase by filename alone.
14. **`express.static(path.join(__dirname, "..", "public"))`** in `index.ts` points at a `public/` directory that doesn't exist anywhere in this project — harmless at runtime (Express simply serves nothing from it), but dead configuration.
15. **`npm run seed` points at `src/services/seed.ts`**, which doesn't exist in this project — running that script fails. The same issue is already flagged in several other services' docs in this repo, likely from a shared `package.json` origin.
16. **Unrelated dependencies in `package.json`**: `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`, `mongoose`, `resend`, `morgan`, `jsonwebtoken`, `ioredis` are all listed, but nothing in `src/` imports any of them now that the auth/rate-limit/redis scaffold files that used to reference some of them (`jsonwebtoken`, `ioredis`) are deleted. Worth pruning from `package.json` at some point, though that's a `package.json` change, not something this documentation pass makes.

None of the above (aside from what's described as fixed in items 1–7, which were fixed in the session this doc was updated to reflect) are being changed as part of this documentation pass — flagging the rest here so they're visible next time someone works on this service.
