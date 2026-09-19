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
