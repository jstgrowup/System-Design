# Missing / Broken Items — IRCTC Backend

A punch list of everything currently broken, missing, or dead across the
repo, ranked worst-first. Compiled from each service's own `docs/README.md`
"Known Issues" sections, plus a few things verified directly against source
while writing the root [`README.md`](README.md).

Nothing here has been fixed yet — this is a snapshot of current behavior,
not a changelog.

---

## Tier 1 — Blocking (nothing works because of these)

- [x] **Admin Service fails to start entirely.** ~~`src/index.ts` imports
      `./config` and `./config/db`~~ — `config/index.ts` was present but
      empty, and `config/db.ts` never existed anywhere (a dead Mongoose-style
      leftover). Fixed: populated `config/index.ts`, dropped the dead import.
      `npx tsc --noEmit` now passes clean — **not verified live**, no
      reachable Postgres/Kafka in the environment this was fixed in.
      _File: `admin-service/src/index.ts`, `admin-service/src/config/*`_

- [x] **Search Service fails to start entirely.** The documented cause (an
      import mismatch on `./routes/search.route` and a default/named export
      mismatch on `errorHandler`) was already stale — both had apparently been
      fixed independently. The real blockers: `search.controller.ts` imported
      from a nonexistent `../services/inventory.service` (fixed to
      `../services/search.service`), and three dead scaffold files
      (`config/redis.ts`, `middlewares/auth.middleware.ts`,
      `middlewares/rate-limiting.middleware.ts`) referenced config fields that
      don't exist on this service's `Config` type — `tsc` type-checks every
      file matched by `include` whether or not anything imports it, so this
      dead code blocked the whole build. Deleted all three (confirmed unused
      first). `npx tsc --noEmit` now passes clean — **not verified live**.
      _File: `search-service/src/index.ts`_

- [x] **The Gateway's login route forwards to the wrong path.** Fixed —
      user-service used to mount login at `/api/v1/auth/login`, which the
      Gateway's rewrite (`POST /api/users/auth/login` → strips `users` →
      forwards to `userService:4001/auth/login`) could never reach. Fixed on
      the user-service side: `server.ts` now mounts auth routes at plain
      `/auth` (no version prefix), matching every other service in this repo
      and the reference implementation, rather than special-casing the
      Gateway's otherwise-uniform one-segment-strip rewrite rule.
      **Not verified live** — no reachable user-service/Postgres/Redis in the
      environment this was fixed in.
      _Files: `user-service/src/server.ts`_

- [x] **User-service's profile routes were never mounted.** Fixed —
      `user.route.ts` is now mounted at `/user` in `server.ts`. The Gateway's
      profile proxy still 404s independent of this fix (see api-gateway's
      GET/POST method mismatch, a separate not-yet-done task).
      _File: `user-service/src/server.ts`_

> **Net effect:** login is the first route reachable through the API Gateway
> that actually works end-to-end at the code level. Every other proxied route
> is still either broken (profile's method mismatch, admin's method
> mismatches) or untested live. User Service, Admin Service, Search Service,
> Inventory Service, Booking Service, and Payment Service now all
> build/typecheck, but none of them has been run against a live database,
> Elasticsearch, Redis, or broker.

---

## Tier 2 — Real logic bugs (would misbehave even once wiring is fixed)

- [x] **`createRoute`'s existing-route check is inverted.** Fixed — now
      `if (existingRoute) throw new ConflictError("Route already exists for this train")`.
      Also fixed the "existis" typo and wired up the `ROUTE_CREATED` publish
      (was commented out) — with the correct denormalized `{...route, train}`
      payload search-service's `indexTrainRoute` actually expects, not the bare
      `Route` row the original commented-out code would have sent.
      _File: `admin-service/src/services/train.service.ts`_

- [x] **`station.controller.createStation` never awaits the service call.**
      Fixed — now awaited, and returns a correct `"Station created successfully"`
      message (was `"OTP sent successfully"`).
      _File: `admin-service/src/controllers/station.controller.ts`_

- [x] **The schedule-creation feature is fully built but unreachable.**
      Fixed — `schedule.route.ts` is now mounted at `/schedules` in `server.ts`.
      _File: `admin-service/src/server.ts`_

- [x] **`getTrainById` always fails.** Fixed — now routed as
      `GET /trains/train/:trainId`, matching the controller's own param name.
      _Files: `admin-service/src/routes/train.routes.ts`,
      `admin-service/src/controllers/train.controller.ts`_

- [ ] **Booking-email handlers silently do nothing.** `handleBookingConfirmed`
      / `Failed` / `Cancelled` read `data.email`, but the typed event shapes
      (`BookingConfirmedData`, etc.) have no `email` field at all. No error is
      thrown — it just logs a warning and skips sending.
      _File: `notification-service/src/kafka/email-consumer.ts`_

- [x] **The welcome email was never sent.** Fixed — `auth.service.ts`'s
      `verifyOtp` now calls `notificationProducer.sendWelcomeEmail(...)` right
      after account creation (fire-and-forget, log-only on failure).
      `notification-service`'s `handleWelcomeEmail` was already correct.
      _File: `user-service/src/services/auth.service.ts`_

---

## Tier 3 — Kafka plumbing gaps (data doesn't flow, nothing crashes)

- [x] **`admin.route-created` never fired.** Fixed — the publish call in
      `trainService.createRoute` was commented out; now fires with the
      denormalized `{...route, train}` payload search-service's
      `indexTrainRoute` actually needs. Both services now build; this hasn't
      been observed working live (no reachable Kafka/Elasticsearch to verify).
      _File: `admin-service/src/services/train.service.ts`_

- [x] **`admin.schedule-created` never fired.** Fixed — the publish call
      itself was always correct; the schedule route is mounted now.
      _File: `admin-service/src/services/schedule.service.ts`_

- [ ] **`admin.schedule-cancelled` has no caller anywhere** — the producer
      method exists (`publishScheduleCancelled`) but nothing invokes it.
      _File: `admin-service/src/kafka/producer/admin.producer.ts`_

- [x] **Search Service's own dead-letter safety net didn't actually work.**
      Fixed — errors inside `indexStation` / `indexSchedule` / `cancelSchedule` /
      `updateSeatAvailability` were caught and logged _inside_ the function, so
      they never propagated out to `withDLQ`; all four now let errors
      propagate, so an Elasticsearch outage lands on `dlq.search-service`
      instead of silently dropping the write.
      _File: `search-service/src/services/search.service.ts`_

- [ ] **Notification Service subscribes to every Kafka topic that exists**,
      including ones meant for other services entirely (`admin.*`,
      `inventory.*`, `payment.*`, every service's own DLQ topic) — all land in a
      generic "Unknown topic" warning. Harmless, but noisy.
      _File: `notification-service/src/kafka/email-consumer.ts`_

---

## Tier 4 — Dead code, leftovers, and cosmetic issues

- [x] **Search Service carried a whole unused leftover API-Gateway
      scaffold** — `middlewares/auth.middleware.ts`, `rate-limiting.middleware.ts`,
      `config/redis.ts` (`routes/index.ts` never actually existed, unlike this
      bullet originally claimed). None of it was imported by `index.ts`, and it
      didn't compile against the service's current trimmed-down `Config` type
      — which meant it silently blocked the *entire service* from compiling,
      since `tsc` checks every file matched by `include` regardless of import
      status. Confirmed unused, then deleted.

- [x] **Copy-pasted response messages.** Fixed — station creation now returns
      `"Station created successfully"` (was `"OTP sent successfully"`);
      schedule creation now returns `"Schedule created successfully"` (was
      `"Train created successfully"`).
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

- [ ] **Small typo left as-is**: `shared/utils/dlqHanlder.ts` (missing a "d" in
      "Handler") — left alone since every service imports it under this exact
      misspelled path; renaming it is a cross-service change, not a one-line
      fix. (The two admin-service message typos this bullet used to list —
      `"existis"` and `"continous starting free"` — are fixed now, see Tier 2.)

- [x] **Admin Service has zero authentication wired up anywhere.** Fixed —
      `getUserContext` is now applied on every route across all three routers
      (station, train, schedule).
      _File: `admin-service/src/middlewares/user-context.middleware.ts`_

- [x] **`admin-service/src/types/index.ts` defined `KnowledgeDoc` /
      `RAGResponse`** — a document-embedding/RAG shape unrelated to stations,
      trains, routes, or schedules, importing `mongoose` for no reason.
      Confirmed nothing imported either interface; the file was deleted.

---

## Not a bug, just not built yet

- **Payment Service now exists and typechecks** (ported from
  `irctc-backend-main`'s reference JS implementation, adapter-pattern gateway
  interface with Razorpay as the only concrete adapter) — isn't verified
  against a real Postgres/Kafka, and there's no real Razorpay merchant account
  to test any gateway call against even if there were. This is what still
  blocks booking-service from ever completing a real booking end-to-end
  today — the dependency is a real service now, not a missing one, but its
  gateway calls fail with an auth error instead of a connection error. See
  `payment-service/docs/README.md` and `docs/api-contract.md` §8.
- **Booking Service now exists and typechecks** (ported from
  `irctc-backend-main`'s reference JS implementation) — isn't verified against
  a real Postgres/Redis/Kafka, and no Prisma migration has been generated yet.
  Its saga can hold seats via inventory-service, but always fails at the
  create-payment step since Payment Service has no real Razorpay credentials.
  See `booking-service/docs/README.md` and `docs/api-contract.md` §7.
- **Inventory Service now exists and typechecks**, but isn't verified against a
  real Postgres/Kafka, isn't proxied through the Gateway yet, and can't receive
  a real event even once running — admin-service's `POST /schedule` (the only
  trigger for `admin.schedule-created`) is still never mounted. See
  `inventory-service/docs/README.md` and `docs/api-contract.md` §6.
- **User Service has no `docs/README.md` yet** — unlike the other six
  services. `docs/auth.md` and the root `README.md` are the best references
  for it today.
