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
