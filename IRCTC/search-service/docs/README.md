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
   - [Leftover api-gateway scaffold (routes/, some middlewares/, config/redis.ts)](#5-leftover-api-gateway-scaffold)
6. [Environment Variables](#environment-variables)
7. [Kafka Topics Reference](#kafka-topics-reference)
8. [Elasticsearch Indices Reference](#elasticsearch-indices-reference)
9. [Quick Start](#quick-start)
10. [Debugging Tips](#debugging-tips)
11. [Known Issues & Inconsistencies](#known-issues--inconsistencies)

---

## Overview

The **Search Service** is meant to be IRCTC's read-optimized search layer, backed by Elasticsearch instead of the relational data admin-service owns. As it exists today:

- **Consumes Kafka events** from admin-service (station/route/schedule lifecycle) and inventory-service (seat availability) to keep two Elasticsearch indices (`stations`, `trains`) up to date
- **Indexes stations** for autocomplete (edge-ngram + completion suggester) and **trains** with a nested `route` and a `schedules` array for date/availability filtering
- **Implements search logic** (`searchTrains`, `autocompleteStation`, `resolveStation`, `getAllStations`/`getAllTrains`) in `search.service.ts` — but **nothing calls these from HTTP today**: `index.ts` imports a `routes/search.route` module that doesn't exist in this project, so there is currently no way to reach any of this over the network
- Still carries a large chunk of **leftover api-gateway scaffold** (JWT auth, Redis-backed rate limiting, a proxy/circuit-breaker layer, a gateway-style route table) that predates the Elasticsearch/Kafka work and no longer compiles against this service's trimmed-down config

**Important: this service does not currently compile.** Two independent problems in `src/index.ts` alone: it imports `./routes/search.route`, which doesn't exist anywhere in `src/`, and it default-imports `errorHandler` from `middlewares/error.middleware.ts`, which only has a *named* export (`errorMiddleware`) — there is no default export to import. Running `npx tsc --noEmit` from `search-service/` reports both, plus several more from the leftover scaffold (see [Known Issues](#known-issues--inconsistencies)). Everything below describes what the code is written to do — the two files this session's work focused on (`kafka/search.service.ts` and `services/search.service.ts`) compile cleanly on their own; the rest of the service doesn't yet.

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
│   path — fully implemented, but nothing currently calls them:          │
│   index.ts imports routes/search.route, which doesn't exist.          │
│                                                                         │
│  index.ts (Express app): corsMiddleware → helmet → reqLogger →        │
│   express.json → cookieParser → express.static("../public")           │
│   → (would mount searchRoutes here — module missing) → GET /health    │
│   → errorHandler (import itself is broken — see Overview)             │
│                                                                         │
│  Leftover api-gateway scaffold, unused by index.ts, doesn't compile:   │
│   routes/index.ts, middlewares/auth+rate-limiting.middleware.ts,       │
│   config/redis.ts — see Known Issues                                   │
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
                                          nothing consumes search-service's
                                          own output today — no HTTP route
                                          exists to serve a search request
```

---

## File Structure

```
search-service/
├── src/
│   ├── index.ts                          # Express app bootstrap — currently fails to compile (see Overview)
│   ├── config/
│   │   ├── index.ts                      # Env vars → typed Config (SERVICE_NAME, PORT, NODE_ENV,
│   │   │                                 #   LOG_LEVEL, ELASTICSEARCH_URL, KAFKA_BROKER,
│   │   │                                 #   KAFKA_CLIENT_ID, ALLOWED_ORIGINS — nothing else)
│   │   ├── logger.ts                     # Winston logger
│   │   ├── kafka.ts                      # kafkajs client + consumer + DLQ producer
│   │   ├── elasticsearch.ts              # ES client + index definitions + initIndices/recreateIndices
│   │   └── redis.ts                      # Leftover — references config.REDIS_URL, which no longer
│   │                                     #   exists on Config; doesn't compile (see Known Issues)
│   ├── kafka/
│   │   └── search.service.ts             # SearchConsumer — despite the filename, this is the Kafka
│   │                                     #   consumer, not services/search.service.ts (see note below)
│   ├── services/
│   │   └── search.service.ts             # Indexing (write) + search (read) logic, Elasticsearch-backed
│   ├── middlewares/
│   │   ├── cors.middleware.ts            # Used by index.ts
│   │   ├── error.middleware.ts           # Used by index.ts, but the import in index.ts is broken
│   │   ├── req.middleware.ts             # Used by index.ts
│   │   ├── auth.middleware.ts            # Leftover — references config.JWT_ACCESS_SECRET, doesn't compile
│   │   ├── rate-limiting.middleware.ts   # Leftover — references config.RATE_LIMIT_*, doesn't compile
│   │   └── not-found.middleware.ts       # Leftover — compiles fine, but unused (not wired into index.ts)
│   ├── routes/
│   │   └── index.ts                      # Leftover api-gateway route table — unused, doesn't compile
│   │                                     #   (references services/proxy.ts, which no longer exists)
│   ├── types/
│   │   └── index.ts                      # Empty
│   └── utils/
│       ├── error.ts                      # AppError + subclasses
│       └── asyncHandler.ts               # Wraps an async handler, forwards rejections to next() —
│                                         #   defined, not currently imported anywhere
├── docs/                                  # This documentation
├── package.json
├── tsconfig.json
└── .env
```

**Naming note:** the Kafka consumer lives at `src/kafka/search.service.ts`, and the indexing/search logic lives at `src/services/search.service.ts` — two different files with the identical basename in different folders. `services/search.service.ts` imports the consumer's event-type interfaces with `import type { ... } from "../kafka/search.service"`; `kafka/search.service.ts` imports the indexing logic with `import searchService from "../services/search.service"`. Both imports resolve correctly (the paths differ), but the shared basename is worth knowing about before grepping for "search.service" and assuming there's only one file.

`tsconfig.json` sets `rootDir: ".."`, mirroring every other service in this repo — it lets the project compile files reached via `../../shared/...`-style imports. `kafka/search.service.ts` is the one file here that actually uses this (`../../../shared/constants/kafka-topics` and `../../../shared/utils/dlqHanlder` — note the shared file's own name is misspelled "dlqHanlder", not "dlqHandler"; this service imports it under its real, misspelled name rather than renaming the shared file).

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
6.  esClient.index({ index: "stations", id: station.id, document: {...} , refresh: true })
      — document has stationId/code/city/suggest, but NOT `name` (see Known Issues —
      this field was dropped from the write at some point after this file was first written)
7.  logger.info(`Indexed station ${station.name} (${station.code})`)
8.  On any ES error, the catch block logs it and swallows it — withDLQ's retry/DLQ
    logic in step 3 only kicks in for errors thrown by the handler itself, so an
    error caught and logged inside indexStation never reaches the DLQ.
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
    through this consumer as the system is currently wired, even once this
    service's own compile errors are fixed.
```

### Case C: A search request, if `routes/search.route.ts` existed (documented as the intended flow — currently unreachable)

```
1.  Client would call something like GET /search/trains?from=NDLS&to=BCT&date=2026-08-01
2.  A route handler (not present in this project) would call
    searchService.searchTrains(from, to, date)
3.  searchTrains resolves "NDLS"/"BCT" via resolveStation (exact code match →
    completion-suggester fuzzy match → multi_match fuzzy match, in that order)
4.  A nested query against the "trains" index finds trains whose route contains
    both stations, with inner_hits so the matching stop's own fields (times,
    sequence number) come back per-hit
5.  Results are filtered to keep only hits where the "from" stop's sequenceNumber
    is before the "to" stop's — i.e. the train actually runs in that direction
6.  If `date` was given, the matching ACTIVE schedule for that date (if any) is
    attached to each result
7.  Returns { from, to, date, count, trains } — or { trains: [], message } if
    either station couldn't be resolved at all
8.  This function is fully implemented and type-checked cleanly this session, but
    step 1 has no route to trigger it — see Known Issues.
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
import errorHandler from "./middlewares/error.middleware";
import { reqLogger } from "./middlewares/req.middleware";

import searchRoutes from "./routes/search.route";
import searchConsumer from "./kafka/search.service";
import { disconnectAll } from "./config/kafka";

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
app.use(errorHandler);

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

Two independent reasons `npx tsc --noEmit` fails on this file today:
- `import searchRoutes from "./routes/search.route"` — this module doesn't exist anywhere in `src/`.
- `import errorHandler from "./middlewares/error.middleware"` — that file exports `errorMiddleware` as a *named* export only; there's no default export, so `errorHandler` resolves to nothing.

Startup order otherwise: build/recreate Elasticsearch indices first (`ES_RECREATE_INDICES=true` wipes and rebuilds `stations`/`trains` from scratch — see `config/elasticsearch.ts`), then start the Kafka consumer, then start listening for HTTP. Shutdown is the reverse: stop accepting connections, then `disconnectAll()` (leaves both the Kafka consumer and, if connected, the DLQ producer disconnected) before exiting. `express.static(path.join(__dirname, "..", "public"))` points at a `public/` directory that doesn't exist in this project either — harmless (Express just won't find any static files to serve), but worth knowing if you expected a frontend to be served from here.

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

This is a deliberately trimmed-down `Config` — it no longer has `REDIS_URL`, `JWT_ACCESS_SECRET`, `RATE_LIMIT_*`, or `SERVICES`, which is exactly why the leftover api-gateway files (`config/redis.ts`, `auth.middleware.ts`, `rate-limiting.middleware.ts`, `routes/index.ts`) no longer compile — they still reference those removed fields.

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

`ROUTE_INDEX` and `SCHEDULE_INDEX` are exported constants naming indices that **never get created** — `initIndices`/`recreateIndices` only ever touch `STATION_INDEX`/`TRAIN_INDEX`. Routes and schedules are instead folded *into* the `trains` index (as a nested `route` array and a `schedules` array respectively) rather than getting their own indices — so these two constants are currently unused outside their own declaration.

`config/logger.ts` is a plain Winston logger (same shape as admin-service's), except its inline comment is stale: it says `config.LOG_LEVEL is hardcoded to "4"`, which was true of an earlier version of `config/index.ts` but isn't true of the version shown above (`LOG_LEVEL` now reads `process.env.LOG_LEVEL || "info"`) — a documentation artifact left behind by the config rewrite, not a functional bug.

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

The event interfaces are defined here (not in a shared `types/` file) because this consumer is the natural owner of "what shape does a message on topic X have" — mirroring how admin-service's `admin.producer.ts` owns its own event interfaces on the publish side. `parsedValue` arrives from `withDLQ` as `unknown` (it's the direct result of `JSON.parse`-ing the message off the wire); the `as StationCreatedEvent` / etc. casts in each `switch` branch are the one place this codebase deliberately asserts an untyped external payload into a known shape, rather than typing it `any` — `services/search.service.ts` never sees an `any` from this boundary. `withDLQ` (from `shared/utils/dlqHanlder.ts`) retries a failing handler up to `DLQ_MAX_RETRIES` times before forwarding the raw message to `KAFKA_TOPICS.DLQ_SEARCH` and moving on, so one poison message can't block the whole partition forever.

---

### 4. `services/search.service.ts` — Indexing & Search

Split into two halves. The **index operations** (called only by the Kafka consumer above):

```typescript
const indexStation = async (event: StationCreatedEvent): Promise<void> => {
  const station = event.data;
  if (!station) return;

  try {
    await esClient.index({
      index: STATION_INDEX,
      id: station.id,
      document: {
        stationId: station.id,
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
  } catch (err) {
    logger.error(`Failed to index station: ${errorMessage(err)}`);
  }
};
```

Note the `document` here has no `name` field, even though `StationDocument`'s type marks it optional for exactly this reason — a station indexed only through `STATION_CREATED` (before any route touches it) has no `name` in Elasticsearch, even though the event it was built from clearly had one. `indexTrainRoute`'s per-station reindex (below) *does* write `name`, so a station is only missing it until the first route referencing it is indexed.

`indexTrainRoute`, `indexSchedule`, `cancelSchedule`, and `updateSeatAvailability` follow the same shape: parse the typed event, build/patch an Elasticsearch document (`index` for a fresh document, a Painless `update` script for in-place array mutation on the `trains` index's `schedules` field), log, and on failure log-and-swallow rather than rethrow — so a bad Elasticsearch write never crashes the consumer loop directly (though see the DLQ note above: because these catch blocks swallow the error, `withDLQ` never sees a failure here to retry or forward).

The **search operations** (fully implemented, not currently reachable from HTTP — see Known Issues):

```typescript
const searchTrains = async (from: string, to: string, date?: string): Promise<SearchTrainsResult> => {
  const fromStation = await resolveStation(from);
  const toStation = await resolveStation(to);
  if (!fromStation) return { trains: [], message: `Station "${from}" not found` };
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

---

### 5. Leftover api-gateway scaffold

These files were carried over from an api-gateway-style scaffold and predate the Elasticsearch/Kafka rewrite. None of them are imported by `index.ts`; all of them fail `npx tsc --noEmit` on their own because they reference `Config` fields this service's trimmed-down `config/index.ts` no longer has:

| File | References | Compiles? |
|---|---|---|
| `config/redis.ts` | `config.REDIS_URL` | ❌ — `REDIS_URL` isn't on `Config` |
| `middlewares/auth.middleware.ts` | `config.JWT_ACCESS_SECRET` | ❌ — not on `Config` |
| `middlewares/rate-limiting.middleware.ts` | `config.RATE_LIMIT_MAX_REQUESTS`, `config.RATE_LIMIT_WINDOW_MS` | ❌ — neither on `Config` |
| `routes/index.ts` | `config.SERVICES.*`, and imports `../services/proxy` | ❌ — `SERVICES` isn't on `Config`, and `services/proxy.ts` doesn't exist in this project at all |
| `middlewares/not-found.middleware.ts` | only `NotFoundError` | ✅ compiles, but unused |
| `middlewares/cors.middleware.ts`, `middlewares/error.middleware.ts`, `middlewares/req.middleware.ts` | `config.ALLOWED_ORIGINS` / `config.NODE_ENV` / logger only | ✅ compile, and **are** used by `index.ts` |

`utils/error.ts` (the `AppError` hierarchy) and `utils/asyncHandler.ts` are generic enough that they don't reference any removed config — both compile fine, `asyncHandler` just isn't imported anywhere yet (there's no route file to use it in).

---

## Environment Variables

Everything actually read via `config.*` in the live (non-leftover) parts of this service:

```bash
PORT=4002
NODE_ENV=development
LOG_LEVEL=info

ELASTICSEARCH_URL=http://localhost:9200
KAFKA_BROKER=localhost:9093
KAFKA_CLIENT_ID=search-service

ALLOWED_ORIGINS=http://localhost:3000,http://localhost:4000,http://localhost:4001

# Read directly via process.env in index.ts, not through config/index.ts:
ES_RECREATE_INDICES=true   # wipes + rebuilds stations/trains on startup instead of a no-op create-if-missing
```

This matches the service's actual `.env` file exactly. Note `.env` does **not** define `REDIS_URL`, `JWT_ACCESS_SECRET`, `JWT_REFRESH_SECRET`, or any `RATE_LIMIT_*` variable — consistent with those being leftover-scaffold concerns this service no longer uses, not omissions.

---

## Kafka Topics Reference

| Topic | Subscribed here? | Actually fires in this system today? |
|---|---|---|
| `admin.station-created` | ✅ → `indexStation` | ✅ yes — admin-service's `stationService.createStation` publishes it on every `POST /stations/station` |
| `admin.route-created` | ✅ → `indexTrainRoute` | ❌ no — admin-service's only call to `publishRouteCreated` is commented out (verified in that service's own source/docs this session) |
| `admin.schedule-created` | ✅ → `indexSchedule` | ❌ no — the publish call itself is correct in admin-service, but the HTTP route that would trigger it (`schedule.route.ts`) is never mounted there |
| `admin.schedule-cancelled` | ✅ → `cancelSchedule` | ❌ no — nothing in admin-service calls `publishScheduleCancelled` at all |
| `inventory.seat-availability-updated` | ✅ → `updateSeatAvailability` | Not verified this session — inventory-service's source wasn't reviewed as part of this pass |
| `dlq.search-service` | Published to (not subscribed) | Only when a handler above throws 3 times in a row for the same message (`DLQ_MAX_RETRIES`) |

In short: as the rest of this system is currently wired, only station-creation events are known to actually reach this consumer — everything else this service subscribes to depends on gaps documented in admin-service's own docs.

---

## Elasticsearch Indices Reference

| Index (constant) | Created by `initIndices`? | Written by | Read by |
|---|---|---|---|
| `stations` (`STATION_INDEX`) | ✅ | `indexStation`, `indexTrainRoute`'s per-station loop | `resolveStation`, `autocompleteStation`, `getAllStations` |
| `trains` (`TRAIN_INDEX`) | ✅ | `indexTrainRoute`, `indexSchedule`, `cancelSchedule`, `updateSeatAvailability` | `searchTrains`, `getAllTrains` |
| `routes` (`ROUTE_INDEX`) | ❌ never created | — | — |
| `schedules` (`SCHEDULE_INDEX`) | ❌ never created | — | — |

Routes and schedules live *inside* the `trains` index (a nested `route` array and a plain `schedules` array respectively) rather than as their own indices — `ROUTE_INDEX`/`SCHEDULE_INDEX` are exported names for indices that don't exist.

---

## Quick Start

```bash
cd search-service
npm install
```

**This fails to start today** — see the Overview's compile-failure note. `npx tsc --noEmit` will show the `routes/search.route` and `errorHandler` errors (plus the leftover-scaffold ones) without needing Elasticsearch or Kafka running at all.

Once `routes/search.route.ts` exists and the `errorHandler` import is fixed, the intended startup is:

```bash
# .env needs at least ELASTICSEARCH_URL, KAFKA_BROKER, KAFKA_CLIENT_ID, ALLOWED_ORIGINS
# Elasticsearch and Kafka both need to be reachable before this will do anything useful
npm run dev              # nodemon + ts-node, hot reload

curl http://localhost:4002/health
# { "status": "ok", "service": "Search Service" }
```

There is currently no working search endpoint to curl — see [Known Issues](#known-issues--inconsistencies).

---

## Debugging Tips

- **`Cannot find module './routes/search.route'`** → that file doesn't exist in this project; `index.ts` imports it unconditionally.
- **`Module '.../error.middleware' has no default export`** → `error.middleware.ts` only exports `errorMiddleware` (named); `index.ts`'s `import errorHandler from ...` needs to become a named import.
- **A station never shows up in Elasticsearch even though it was created in admin-service** → check this consumer's own logs for `Failed to index station: ...`; also confirm `KAFKA_BROKER`/`ELASTICSEARCH_URL` point at reachable services.
- **Trains never appear in the `trains` index no matter what** → this isn't a bug in this service to chase — admin-service never actually publishes `admin.route-created` today (see the Kafka Topics table above), so `indexTrainRoute` is never invoked in practice.
- **Schedules never update a train's `schedules` array** → same root cause, different reason: the admin-service HTTP route that would trigger `admin.schedule-created` isn't mounted there.
- **`property 'REDIS_URL'/'JWT_ACCESS_SECRET'/'RATE_LIMIT_*'/'SERVICES' does not exist on type 'Config'`** → you're looking at one of the leftover api-gateway files (`config/redis.ts`, `auth.middleware.ts`, `rate-limiting.middleware.ts`, `routes/index.ts`); none of them are wired into `index.ts` and none of them compile against this service's actual config.
- **`ES_RECREATE_INDICES=true` and startup takes a while / logs "Deleted index"** → that's `recreateIndices()` wiping and rebuilding `stations`/`trains` from scratch; unset it (or set to anything other than `"true"`) for a normal create-if-missing startup.

---

## Known Issues & Inconsistencies

Observed while reviewing the code — documented here rather than fixed, since these are informational (same approach as the API Gateway's, Notification Service's, and Admin Service's docs):

1. **The service doesn't compile.** `index.ts` imports `./routes/search.route`, which doesn't exist anywhere in `src/`, and default-imports `errorHandler` from `middlewares/error.middleware.ts`, which only exports `errorMiddleware` as a named export. Confirmed with `npx tsc --noEmit`.
2. **No HTTP route exists to reach any search functionality.** `services/search.service.ts`'s `searchTrains`, `autocompleteStation`, `resolveStation`, `getAllStations`, and `getAllTrains` are all fully implemented and type-check cleanly, but nothing in this project calls any of them — there is no route file, so they're only reachable by importing the module directly (e.g. from a test).
3. **`admin.route-created` never actually fires.** Verified directly in admin-service's source this session: `trainService.createRoute`'s call to `adminProducer.publishRouteCreated(...)` is commented out. This means `indexTrainRoute` — the only code path that ever creates a `trains` index document — is never invoked in the system as currently wired, independent of anything in this service.
4. **`admin.schedule-created` never actually fires either**, for a different reason: admin-service's `schedule.service.ts` does correctly call `publishScheduleCreated`, but the HTTP route that would trigger schedule creation (`schedule.route.ts`) is never mounted in admin-service's `server.ts`.
5. **`ROUTE_INDEX` and `SCHEDULE_INDEX`** (in `config/elasticsearch.ts`) name Elasticsearch indices that `initIndices`/`recreateIndices` never create — route and schedule data live inside the `trains` index's nested `route`/`schedules` fields instead.
6. **`indexStation`'s document is missing a top-level `name` field**, even though `StationDocument`'s type marks it optional specifically to model this. A station indexed only via `STATION_CREATED` (i.e. before any route references it) has no `name` in Elasticsearch until `indexTrainRoute`'s per-station reindex loop touches it.
7. **Errors caught inside the index-operation functions are logged and swallowed, not rethrown.** Since `withDLQ` only retries/forwards-to-DLQ on errors that propagate out of the handler it wraps, an Elasticsearch failure inside `indexStation`/`indexSchedule`/`cancelSchedule`/`updateSeatAvailability` is silently dropped rather than ending up on `dlq.search-service` — the DLQ safety net doesn't actually cover the most likely failure mode (Elasticsearch being unreachable).
8. **A large chunk of leftover api-gateway scaffold doesn't compile** against this service's trimmed-down `Config`: `config/redis.ts` (`config.REDIS_URL`), `middlewares/auth.middleware.ts` (`config.JWT_ACCESS_SECRET`), `middlewares/rate-limiting.middleware.ts` (`config.RATE_LIMIT_MAX_REQUESTS`/`RATE_LIMIT_WINDOW_MS`), and `routes/index.ts` (`config.SERVICES.*`, plus an import of `services/proxy.ts`, which doesn't exist in this project at all). None of these are imported by `index.ts`.
9. **`middlewares/not-found.middleware.ts` and `utils/asyncHandler.ts` compile fine but are unused** — the former isn't wired into `index.ts`'s middleware chain, the latter has no route file to wrap a handler in.
10. **`config/logger.ts`'s inline comment about `LOG_LEVEL` being hardcoded to `"4"` is stale** — that was true of an earlier version of `config/index.ts`; the current one reads `process.env.LOG_LEVEL || "info"`. Purely a documentation artifact in the code, not a functional issue.
11. **Two files share the basename `search.service.ts`** — the Kafka consumer at `src/kafka/search.service.ts` and the indexing/search logic at `src/services/search.service.ts`. Both compile and import each other correctly (via distinct relative paths), but it's an easy source of confusion when searching the codebase by filename alone.
12. **`express.static(path.join(__dirname, "..", "public"))`** in `index.ts` points at a `public/` directory that doesn't exist anywhere in this project — harmless at runtime (Express simply serves nothing from it), but dead configuration.
13. **`npm run seed` points at `src/services/seed.ts`**, which doesn't exist in this project — running that script fails. The same issue is already flagged in several other services' docs in this repo, likely from a shared `package.json` origin.
14. **Unrelated dependencies in `package.json`**: `@langchain/cohere`, `@langchain/core`, `@langchain/groq`, `@langchain/openai`, `mongoose`, `resend`, `morgan`, `jsonwebtoken`, `ioredis` are all listed, but only the leftover-scaffold files (dead code per #8) import the auth/rate-limit/redis-related ones, and nothing imports the LangChain/mongoose/resend ones at all.

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
