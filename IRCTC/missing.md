# Missing / Broken Items — IRCTC Backend

A punch list of everything currently broken, missing, or dead across the
repo, ranked worst-first. Compiled from each service's own `docs/README.md`
"Known Issues" sections, plus a few things verified directly against source
while writing the root [`README.md`](README.md).

Nothing here has been fixed yet — this is a snapshot of current behavior,
not a changelog.

---

## Tier 1 — Blocking (nothing works because of these)

- [ ] **Admin Service fails to start entirely.** `src/index.ts` imports
      `./config` and `./config/db`; every file under `src/config/` (plus
      `middlewares/cors.middleware.ts`) imports a `config` object from
      `./`/`../config`/`./index` — none of these resolve to a real file.
      `npx tsc --noEmit` from `admin-service/` confirms it.
      _File: `admin-service/src/index.ts`, `admin-service/src/config/*`_

- [ ] **Search Service fails to start entirely.** `src/index.ts` imports
      `./routes/search.route`, which doesn't exist anywhere in the project, and
      default-imports `errorHandler` from `middlewares/error.middleware.ts`,
      which only has a named export (`errorMiddleware`).
      _File: `search-service/src/index.ts`_

- [ ] **The Gateway's login route forwards to the wrong path.** It rewrites
      `POST /api/users/auth/login` → `userService:4001/auth/login`, but
      user-service actually mounts that route at `/api/v1/auth/login`. Every
      login attempt through the Gateway 404s.
      _Files: `api-gateway/src/routes/index.ts`, `api-gateway/src/services/proxy.ts`,
      `user-service/src/server.ts`_

- [ ] **User-service's profile routes are never mounted.**
      `user.route.ts` (`getProfile` / `updateProfile` / `deleteProfile`) is fully
      written but `server.ts` never imports or `app.use()`s it. The Gateway's
      profile proxy 404s independent of the path issue above.
      _File: `user-service/src/server.ts`_

> **Net effect:** nothing reachable through the API Gateway currently works.
> Two of the five services can't even boot.

---

## Tier 2 — Real logic bugs (would misbehave even once wiring is fixed)

- [ ] **`createRoute`'s existing-route check is inverted.**
      `if (!existingRoute) throw new NotFoundError("Route already exists for this train")`
      throws "already exists" exactly when no route exists yet — no train can
      ever get its _first_ route created through this endpoint.
      _File: `admin-service/src/services/train.service.ts`_

- [ ] **`station.controller.createStation` never awaits the service call.**
      The `200 OK` response fires before the DB write / Kafka publish settle. A
      duplicate-station request (which should be a `409`) instead looks like a
      success to the caller; the real error becomes an unhandled promise
      rejection in the server logs.
      _File: `admin-service/src/controllers/station.controller.ts`_

- [ ] **The schedule-creation feature is fully built but unreachable.**
      `schedule.route.ts` defines `POST /schedule` but is never mounted in
      `server.ts` — dead from the outside.
      _File: `admin-service/src/server.ts`_

- [ ] **`getTrainById` always fails.** It's routed as
      `POST /trains/route/:id` (wrong HTTP verb for a read, param named `:id`),
      but the controller reads `req.params.trainId` — always `undefined`, so it
      always 400s with "Train Id is missing" before the service function ever
      runs.
      _Files: `admin-service/src/routes/train.routes.ts`,
      `admin-service/src/controllers/train.controller.ts`_

- [ ] **Booking-email handlers silently do nothing.** `handleBookingConfirmed`
      / `Failed` / `Cancelled` read `data.email`, but the typed event shapes
      (`BookingConfirmedData`, etc.) have no `email` field at all. No error is
      thrown — it just logs a warning and skips sending.
      _File: `notification-service/src/kafka/email-consumer.ts`_

- [ ] **The welcome email is never sent.** `notification-service` fully
      implements `handleWelcomeEmail`, but nothing in `user-service` ever calls
      `notificationProducer.sendWelcomeEmail(...)` — no call site exists anywhere
      in the codebase.
      _File: `user-service/src/services/auth.service.ts` (or lack thereof)_

---

## Tier 3 — Kafka plumbing gaps (data doesn't flow, nothing crashes)

- [ ] **`admin.route-created` never fires.** The publish call in
      `trainService.createRoute` is commented out, so Search's `trains` index
      can never be populated as the system is currently wired.
      _File: `admin-service/src/services/train.service.ts`_

- [ ] **`admin.schedule-created` never fires.** The publish call itself is
      correct, but it's unreachable because the schedule route is never mounted
      (see Tier 2).
      _File: `admin-service/src/services/schedule.service.ts`_

- [ ] **`admin.schedule-cancelled` has no caller anywhere** — the producer
      method exists (`publishScheduleCancelled`) but nothing invokes it.
      _File: `admin-service/src/kafka/producer/admin.producer.ts`_

- [ ] **Search Service's own dead-letter safety net doesn't actually work.**
      Errors inside `indexStation` / `indexSchedule` / `cancelSchedule` /
      `updateSeatAvailability` are caught and logged _inside_ the function, so
      they never propagate out to `withDLQ` — an Elasticsearch outage would
      silently drop writes instead of landing in `dlq.search-service`.
      _File: `search-service/src/services/search.service.ts`_

- [ ] **Notification Service subscribes to every Kafka topic that exists**,
      including ones meant for other services entirely (`admin.*`,
      `inventory.*`, `payment.*`, every service's own DLQ topic) — all land in a
      generic "Unknown topic" warning. Harmless, but noisy.
      _File: `notification-service/src/kafka/email-consumer.ts`_

---

## Tier 4 — Dead code, leftovers, and cosmetic issues

- [ ] **Search Service carries a whole unused leftover API-Gateway
      scaffold** — `middlewares/auth.middleware.ts`, `rate-limiting.middleware.ts`,
      `routes/index.ts`, `config/redis.ts` — none of it is imported by `index.ts`,
      and none of it compiles against the service's current trimmed-down config.

- [ ] **Copy-pasted response messages.** Station creation returns
      `"OTP sent successfully"`; schedule creation returns
      `"Train created successfully"`.
      _Files: `admin-service/src/controllers/station.controller.ts`,
      `admin-service/src/controllers/schedule.controller.ts`_

- [ ] **Unrelated dependencies sitting in multiple `package.json`s**
      (`@langchain/*`, `mongoose`, `resend`, `otp-generator`) that nothing under
      `src/` imports, in `admin-service`, `api-gateway`, `notification-service`,
      and `search-service`.

- [ ] **`api-gateway/package.json`'s `name` field says `"Notification
Service"`** — leftover from copying the file when the gateway was
      scaffolded.

- [ ] **`LOG_LEVEL` is hardcoded to `"4"`** in the Gateway's config instead
      of reading from the environment. Winston doesn't recognize `"4"` as a
      valid level, so setting `LOG_LEVEL` in `.env` has zero effect.
      _File: `api-gateway/src/config/index.ts`_

- [ ] **`npm run seed` points at `src/services/seed.ts`** in every service —
      that file doesn't exist anywhere in the repo, so the script always fails.

- [ ] **Small typos left as-is**: `shared/utils/dlqHanlder.ts` (missing a
      "d" in "Handler"), `"Route already existis for this train"`,
      `"Sequence Numbers must be continous starting free"`.

- [ ] **Admin Service has zero authentication wired up anywhere.**
      `getUserContext` middleware is fully implemented but never mounted in
      `server.ts` or any route file — anyone who can reach the service's port can
      create stations, trains, and routes.
      _File: `admin-service/src/middlewares/user-context.middleware.ts`_

- [ ] **`admin-service/src/types/index.ts` defines `KnowledgeDoc` /
      `RAGResponse`** — a document-embedding/RAG shape unrelated to stations,
      trains, routes, or schedules. Nothing imports either interface.

---

## Not a bug, just not built yet

- **Booking Service, Payment Service, Inventory Service** don't exist in
  this repository at all — only their names, ports, and Kafka topics are
  reserved for the future (in `shared/constants/kafka-topics.ts` and
  `api-gateway`'s service-URL config).
- **User Service has no `docs/README.md` yet** — unlike the other four
  services. `docs/auth.md` and the root `README.md` are the best references
  for it today.
