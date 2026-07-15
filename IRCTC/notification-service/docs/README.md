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
  } catch (error) {
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

In plain English: if `RESEND_API_KEY`, `MAIL_SEND`, or `KAFKA_BROKER` aren't set, the service logs an error and exits immediately rather than starting half-broken. `mongoose` is imported at the top of this file but never actually used — likely a leftover from an earlier version that did talk to a database.

**`config/kafka.ts`** — sets up the Kafka client, the consumer that reads messages, and a separate producer used only to publish to the dead-letter queue:

```typescript
const kafka = new Kafka({
  clientId: config.KAFKA_CLIENT_ID,
  brokers: [config.KAFKA_BROKER || "localhost:9093"],
  retry: { initialRetryTime: 300, retries: 10, maxRetryTime: 30000, multiplier: 2 },
});

const consumer: Consumer = kafka.consumer({
  groupId: "notification-service-group",
  sessionTimeout: 30000,     // broker waits this long before considering us dead
  heartbeatInterval: 3000,   // we ping the broker this often to prove we're alive
});

const producer: Producer = kafka.producer({ allowAutoTopicCreation: true, retry: { retries: 3 } });
```

The producer is only connected lazily, the first time something needs to go to the DLQ (`connectProducer()`), not at startup — since most messages never need it. `SIGTERM`/`SIGINT` both trigger a graceful shutdown that disconnects the consumer (and producer, if it was ever connected) before exiting.

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
  await consumer.connect();
  await connectProducer();               // ready in case we need the DLQ

  await consumer.subscribe({
    topics: Object.values(KAFKA_TOPICS),  // literally every topic that exists — see Known Issue #6
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: withDLQ(
      producer,
      KAFKA_TOPICS.DLQ_NOTIFICATION,
      logger,
      async ({ topic, parsedValue }) => {
        await this.handleMessage(topic, parsedValue);
      },
    ),
  });
}
```

**Routing — one topic, one handler:**

```typescript
private async handleMessage(topic: KafkaTopic, data: unknown): Promise<void> {
  switch (topic) {
    case KAFKA_TOPICS.OTP_EMAIL:          await this.handleOtpEmail(data as OtpEmailData); break;
    case KAFKA_TOPICS.WELCOME_EMAIL:      await this.handleWelcomeEmail(data as WelcomeEmailData); break;
    case KAFKA_TOPICS.BOOKING_CONFIRMED:  await this.handleBookingConfirmed(data as BookingConfirmedData); break;
    case KAFKA_TOPICS.BOOKING_FAILED:     await this.handleBookingFailed(data as BookingFailedData); break;
    case KAFKA_TOPICS.BOOKING_CANCELLED:  await this.handleBookingCancelled(data as BookingCancelledData); break;
    default: logger.warn(`Unknown topic: ${topic}`);
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

The comment right above this in the source is worth repeating verbatim, because it explains a real gap: *"BookingConfirmedData has no `email` field — it comes from a separate source on the event; adjust this once you confirm where email actually lives on the real Kafka payload."* In other words: today, this only sends an email if the incoming JSON happens to carry an extra `email` property that isn't part of the documented type.

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

    logger.info(`Email sent successfully to ${msg.to}`, { attempt: retries + 1, id: data?.id });
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
async sendOtpEmail(email: string, otp: string, ttlMinutes: number): Promise<SendResult> {
  return this.sendWithRetry({
    to: email, from: this.from,
    subject: "Your DesignKarle verification code",
    html: getOtpTemplate(otp, ttlMinutes),
  });
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

None of the above are being changed as part of this documentation pass — flagging them here so they're visible next time someone works on this service.
