# IRCTC Backend — How This Whole Thing Works

This is a from-scratch clone of an IRCTC-style train ticketing backend, built as
**microservices** instead of one big application.

> **Microservice, in one sentence:** instead of one giant program that does
> everything (sign people up, manage trains, search, send emails...), the work
> is split into several small, independent programs that each do _one_ job and
> talk to each other over the network. If one of them crashes, the others can
> often keep running.

This document is the map. It won't teach you how every line of code works —
each service has its own deep-dive doc for that (linked at the bottom) — but
after reading this, you should understand **what exists, how the pieces are
supposed to fit together, and what actually works today versus what's still
broken or unbuilt.**

---

## 1. The Big Picture

```mermaid
flowchart TB
    Client["🧑 Client<br/>(browser / mobile app / Postman)"]

    subgraph GatewayBox["API Gateway — port 4000"]
        GW["Checks your login token<br/>Limits how many requests you can make<br/>Forwards the request to the right service"]
    end

    subgraph ServicesBox["The Services (each is its own program)"]
        US["User Service — 4001<br/>Signup, login, sessions"]
        AS["Admin Service — 4003<br/>Stations, trains, routes, schedules"]
        SS["Search Service — 4002<br/>Search for trains/stations"]
        NS["Notification Service — 4004<br/>Sends emails"]
        Ghost["Booking / Payment / Inventory<br/>Services — 4005 / 4006 / 4007<br/>⚠️ talked about everywhere,<br/>but do not exist in this repo yet"]
    end

    subgraph InfraBox["Shared Infrastructure (not code — just servers)"]
        PG["🗄️ PostgreSQL<br/>permanent storage: users, trains,<br/>stations, routes, schedules"]
        RD["⚡ Redis<br/>short-lived storage: OTP codes,<br/>login sessions, rate-limit counters"]
        KF["📨 Kafka<br/>the 'announcement board' services<br/>use to tell each other things happened"]
        ES["🔍 Elasticsearch<br/>a fast, search-optimized copy<br/>of station/train data"]
    end

    Client --> GW
    GW --> US
    GW -.->|"configured, but no working\nroute reaches these today"| AS
    GW -.-> SS
    GW -.-> NS
    GW -.-> Ghost

    US --> PG
    US --> RD
    AS --> PG
    SS --> ES

    US -- "announces: 'a new OTP\nneeds emailing'" --> KF
    AS -- "announces: 'a new station\n/ train was created'" --> KF
    KF -- delivered to --> NS
    KF -- delivered to --> SS
```

**Read this diagram as:** a client only ever talks to the **API Gateway**.
The Gateway is supposed to be the single front door that forwards requests to
whichever service actually knows how to handle them. Services almost never
call each other directly over HTTP — when one service needs to tell another
"something happened," it posts an announcement to **Kafka**, and whichever
services care about that announcement pick it up on their own time. This is
called **event-driven communication**, and it's why a service being slow or
down doesn't necessarily stop the others from working.

The dotted arrows above are deliberate — they mean "this connection is wired
up in config, but nothing actually flows through it successfully yet." More
on exactly where and why in section 6.

---

## 2. Meet the Services

| Service                                    | Port               | What it's _for_, in plain words                                                                                                                                                                        | Right now                                                                                           |
| ------------------------------------------ | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| **API Gateway**                            | 4000               | The receptionist. Every request from the outside world is supposed to knock here first — it checks your login token, makes sure you're not spamming the server, and forwards you to the right service. | ✅ Runs, but only 2 routes are wired up, and both are currently broken (see §6)                     |
| **User Service**                           | 4001               | Handles "who are you." Signup (with an email OTP), login, issuing/renewing the tokens that prove you're logged in, and profile editing.                                                                | ✅ The auth part works well and is thoroughly tested/documented. Profile editing is now built too (was unbuilt/unmounted) — not verified live. |
| **Admin Service**                          | 4003               | The back office. Where railway staff would add new stations, trains, the route a train follows, and which dates it runs.                                                                               | ✅ Code complete, typechecks. ⚠️ Not verified live (no reachable DB/Kafka in this environment)      |
| **Search Service**                         | 4002               | The "find a train" feature, backed by Elasticsearch instead of the regular database, for fast searching.                                                                                               | ✅ Code complete, typechecks. ⚠️ Not verified live (no reachable Elasticsearch/Kafka in this environment) |
| **Notification Service**                   | 4004               | A background worker with no real webpage of its own. It just listens for "someone needs an email" announcements on Kafka and sends them.                                                               | ✅ Runs correctly as designed                                                                       |
| **Inventory Service**                      | 4007               | Tracks how many seats are left on a train's schedule (available/locked/booked), including partial-journey seat locking so two passengers can share a seat across non-overlapping legs.                | ✅ Code complete, typechecks. ⚠️ Not verified live (no reachable DB/Kafka in this environment) — can now receive real events from Admin Service in principle, see §5 |
| **Booking / Payment Services**             | 4005 / 4006        | Would handle seat booking and payments.                                                                                                                                                                 | ❌ Don't exist in this repository — only their _names_ and Kafka topics are reserved for the future |

Everything is written in **TypeScript** with **Express** (a web framework),
and each service is its own standalone program with its own `package.json` —
there's no single command that starts "the app"; you start each service you
want to run separately (see §8).

---

## 3. Jargon Buster

A short glossary for anything above (or below) that might be unfamiliar:

| Term                        | What it actually means here                                                                                                                                                                                                    |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **JWT** (JSON Web Token)    | A signed, tamper-proof string that proves "this is user #123" without the server needing to look anything up. Used for login sessions.                                                                                         |
| **OTP**                     | One-Time Password — the 6-digit code emailed to you during signup, valid for a few minutes.                                                                                                                                    |
| **Kafka / "topic"**         | Kafka is a message board. A "topic" is one named channel on that board (e.g. `admin.station-created`). Services **publish** messages to a topic, and other services **subscribe** to read them.                                |
| **DLQ** (Dead-Letter Queue) | If a service tries to process a Kafka message and keeps failing (e.g. 3 times), it gives up, sets that message aside in a special "problem pile" topic, and moves on — so one broken message can't jam the whole line forever. |
| **Circuit breaker**         | A safety switch in the Gateway. If a service fails 5 times in a row, the Gateway stops even trying to call it for 60 seconds (instead of waiting for a slow timeout every single time), then cautiously tests it again.        |
| **Redis**                   | An in-memory database used for things that should disappear after a while — OTP codes, login sessions, rate-limit counters — rather than permanent records.                                                                    |
| **Prisma**                  | A tool that lets the code talk to the PostgreSQL database using regular TypeScript objects instead of writing raw SQL.                                                                                                         |
| **Elasticsearch**           | A database built specifically for fast searching (autocomplete, fuzzy matching), used here to make train/station search quick.                                                                                                 |

---

## 4. How Signup & Login Actually Work Today

This is the **one flow in the entire repository that is fully built, wired
up, and works end-to-end** — but only when you call the User Service
**directly** (`http://localhost:4001/...`), not through the Gateway. (Why not
through the Gateway is explained in §6.)

```mermaid
sequenceDiagram
    participant C as Client
    participant U as User Service (:4001)
    participant R as Redis
    participant P as Postgres
    participant K as Kafka
    participant N as Notification Service

    C->>U: POST /api/v1/auth/send-otp<br/>{firstName, email, password}
    U->>P: Is this email already registered?
    P-->>U: No
    U->>U: Hash the password (never store it plain)
    U->>R: Save {otp (hashed), signup details}<br/>for 5 minutes
    U->>K: "Hey, someone needs an OTP email"
    K->>N: delivers the announcement
    N->>N: Build the email, send via Resend
    U-->>C: 200 OK + a cookie identifying this signup attempt

    C->>U: POST /api/v1/auth/verify-otp<br/>{otp}
    U->>R: Does this OTP match?
    R-->>U: Yes
    U->>P: Create the permanent user row
    U-->>C: 201 Created — account exists now

    C->>U: POST /api/v1/auth/login<br/>{email, password}
    U->>P: Check the password
    U->>R: Save a login session,<br/>tied to this specific device
    U-->>C: 200 OK + accessToken (15 min)<br/>+ refreshToken (7 days) cookies

    Note over C,U: 15 minutes later, the accessToken expires...
    C->>U: POST /api/v1/auth/refresh<br/>(cookies sent automatically)
    U->>R: Is this the newest refresh token<br/>we issued for this device?
    R-->>U: Yes
    U-->>C: New accessToken + refreshToken<br/>(old one is now invalid)
```

A couple of details worth knowing, in plain English:

- The OTP itself is **never stored anywhere in readable form** — only a
  scrambled (HMAC) version, so even someone who broke into Redis couldn't
  read out the actual code.
- Every time you refresh your token, the _old_ refresh token stops working.
  If someone ever steals a refresh token and tries to reuse an old one after
  the real user already refreshed, the server notices the mismatch and kills
  the whole session — forcing a fresh login. This defends against stolen
  tokens.
- A **welcome email** is defined and ready to send in the Notification
  Service. The User Service now actually asks for one after a successful
  signup — this used to never happen, but hasn't been watched working live yet.

The complete, byte-for-byte breakdown of this flow (every error code, every
Redis key, every security decision and why) lives in
[`docs/auth.md`](docs/auth.md).

---

## 5. How Admin Data Is _Supposed_ to Reach Search

This is the flow that keeps the Search Service's Elasticsearch copy of
stations/trains up to date whenever an admin adds something new.

```mermaid
sequenceDiagram
    participant Staff as Railway Staff (Admin UI)
    participant A as Admin Service (:4003)
    participant DB as Postgres
    participant K as Kafka
    participant S as Search Service (:4002)
    participant ES as Elasticsearch

    Staff->>A: POST /stations/station {name, code, city}
    A->>DB: Save the new station
    A->>K: "A new station was created"
    K->>S: delivers the announcement
    S->>ES: Index it, so it's searchable

    Staff->>A: POST /trains/train {trainNumber, seats...}
    A->>DB: Save the new train + its seats
    A->>K: "A new train was created"
    Note over S,ES: nothing listens for this one today —<br/>trains alone aren't enough to search a journey,<br/>you also need the route

    Staff->>A: POST /trains/route {trainId, stations...}
    A->>DB: Save the route
    A->>K: "A new route was created" (train + route inlined)
```

**This is the flow that's supposed to happen, and the Admin Service side of it
now works** — it used to fail to start entirely (`config/index.ts` existed but
was empty, and it imported a `config/db.ts` that never existed anywhere in the
project), `createRoute`'s existence check was inverted (blocking every train's
first route), and the `ROUTE_CREATED` publish was commented out. All three are
fixed now, and creating a _schedule_ (a specific date a train runs) is
connected to a real URL for the first time. **None of this has been run
against a live Postgres/Kafka**, though — it's verified only by `tsc --noEmit`
passing clean, in an environment with no reachable database or broker.

**The Search Service side is fixed too now.** Its `searchTrains` handler used to
compute a real result and then discard it for a hardcoded message; `debug/stations`
and `debug/trains` both called the wrong function (a copy-paste bug); the service
also carried three dead files left over from an earlier scaffold that referenced
config values that don't exist, which silently blocked the whole service from
compiling even though nothing actually used them. All of that is fixed —
**but still not verified against a live Elasticsearch or Kafka**, so treat
"Admin Service publishes it, Search Service can index it" as true on paper, not
as something actually observed working end-to-end.

---

## 6. What Happens When You Go Through the Gateway Today

The Gateway is supposed to be the only door into the system. Right now, it
only has two routes wired up at all — and **both of them are broken**, for
two different, unrelated reasons:

```mermaid
flowchart TD
    A["Client calls<br/>POST /api/users/auth/login<br/>through the Gateway"] --> B["Gateway rewrites the path<br/>and forwards to<br/>user-service:4001/auth/login"]
    B --> C{"Does that path exist<br/>on the User Service?"}
    C -->|"No — the real route lives at<br/>/api/v1/auth/login, not /auth/login"| D["❌ 404 Not Found"]

    E["Client calls<br/>GET /api/users/user/profile<br/>through the Gateway"] --> F["Gateway forwards to<br/>user-service:4001/user/profile"]
    F --> G{"Is the profile route file<br/>even mounted in the User<br/>Service's server.ts?"}
    G -->|"No — it's written, but never<br/>imported/registered anywhere"| H["❌ 404 Not Found"]
```

In other words: **as this repo stands, nothing reachable through the Gateway
currently works.** The only way to exercise the working login flow described
in §4 is to call the User Service directly on port 4001, bypassing the
Gateway entirely. This isn't a deliberate design choice — it's a gap between
how the Gateway assumes services are laid out and how they're actually laid
out today.

The Gateway's other safety features (rate limiting, the circuit breaker)
still work correctly in isolation — they just don't currently have any
working request to protect, since nothing gets past the routing mismatch
above.

---

## 7. What Happens When Something Fails

Two independent safety nets exist in this system, at two different layers.

**Layer 1 — the Gateway's circuit breaker** (protects against a downstream
service being _slow or down_, for HTTP calls):

```mermaid
stateDiagram-v2
    [*] --> CLOSED
    CLOSED --> OPEN: 5 failures in a row
    OPEN --> HALF_OPEN: 60 seconds pass
    HALF_OPEN --> CLOSED: the next test request succeeds
    HALF_OPEN --> OPEN: the next test request also fails
```

In plain terms: if a service fails 5 times back to back, the Gateway stops
even trying to reach it for a full minute — it fails instantly with "service
unavailable" instead of making every single caller wait through a slow
timeout. After a minute, it lets exactly one request through as a test; if
that succeeds, it goes back to normal.

**Layer 2 — the dead-letter queue** (protects against a Kafka message that a
consumer _keeps failing to process_):

```mermaid
flowchart LR
    M["A Kafka message arrives"] --> H{"Does the handler<br/>process it successfully?"}
    H -->|Yes| Done["✅ Done, move to the next message"]
    H -->|"No (attempt 1 of 3)"| R1["Wait, then retry"]
    R1 --> H
    H -->|"No (attempt 3 of 3 — out of retries)"| DLQ["📮 Forward the raw message to a<br/>'dead letter' topic<br/>(e.g. dlq.notification-service)<br/>along with WHY it failed"]
    DLQ --> Move["Move on to the next message —<br/>this one won't block the whole queue forever"]
```

This exists so that one badly-formed or unprocessable message can't jam an
entire Kafka topic — everything behind it in line keeps flowing, and the
problem message is set aside somewhere a human can go look at it later.

---

## 8. The Kafka Announcement Board — Who Talks to Whom

```mermaid
flowchart LR
    subgraph Publishers["Services that ANNOUNCE things"]
        US2["User Service"]
        AS2["Admin Service"]
        IS2["Inventory Service"]
    end

    subgraph Topics["Kafka Topics (channels)"]
        T1["notification.otp-email"]
        T2["notification.welcome-email"]
        T3["admin.station-created"]
        T4["admin.train-created"]
        T5["admin.route-created"]
        T6["admin.schedule-created"]
        T8["inventory.seat-availability-updated"]
        T7["booking.* / payment.* topics"]
    end

    subgraph Listeners["Services that LISTEN for things"]
        NS2["Notification Service"]
        SS2["Search Service"]
        IS2L["Inventory Service"]
    end

    US2 -->|"actually publishes"| T1 --> NS2
    US2 -.->|"defined, but never<br/>actually published"| T2
    T2 -.-> NS2

    AS2 -->|"publishes"| T3 --> SS2
    AS2 -->|"publishes"| T4
    Note1["(nothing listens for train-created yet)"]
    T4 --- Note1

    AS2 -->|"publishes"| T5 --> SS2

    AS2 -->|"publishes"| T6
    T6 --> SS2
    T6 --> IS2L

    IS2 -->|"publishes"| T8 --> SS2

    T7 -.->|"nobody publishes these —<br/>booking/payment\nservices don't exist yet"| T7b["(nobody's listening either)"]
```

**The short version:** as of this pass, admin-service, search-service, and
inventory-service all build and typecheck, so every solid arrow above is now
structurally wired end-to-end at the code level — `admin.station-created`,
`admin.route-created`, and `admin.schedule-created` all leave Admin Service
correctly, and both Search Service and Inventory Service have real handlers
waiting for them. **None of this has actually been observed working**,
though — this was verified with `tsc --noEmit` in a sandbox with no reachable
Postgres, Elasticsearch, or Kafka, so treat the solid arrows as "should work"
rather than "confirmed working." `notification.otp-email` (User → Notification)
remains the one flow anyone has actually watched succeed. Dotted lines are
still genuinely unbuilt: `notification.welcome-email` has no caller, and
`booking.*`/`payment.*` have no publisher because those services don't exist yet.

---

## 9. Current Status at a Glance

| Service                       | Starts up?                  | Fully reachable end-to-end?                            | Biggest reason why not                                         |
| ----------------------------- | --------------------------- | ------------------------------------------------------ | -------------------------------------------------------------- |
| API Gateway                   | ✅ Yes                      | ⚠️ Only 2 routes exist, and both 404                   | Path mismatch on login; a GET/POST method mismatch on the profile route (now mounted on the User Service side, but the Gateway still can't reach it correctly) |
| User Service                  | ✅ Yes                      | ✅ Yes — _if called directly, not through the Gateway_ | The Gateway forwards to the wrong path                         |
| Admin Service                 | ⚠️ Not verified live (code complete, typechecks) | ❌ | Not proxied through the Gateway correctly yet (method mismatch); no cancel-schedule feature exists either |
| Search Service                | ⚠️ Not verified live (code complete, typechecks) | ❌ | Not proxied through the Gateway yet; nothing has actually published an event to it live either |
| Notification Service          | ✅ Yes                      | ✅ Yes, as a background worker (no web routes to test) | —                                                              |
| Inventory Service             | ⚠️ Not verified live (code complete, typechecks) | ❌ | Not proxied through the Gateway yet, and its one real trigger (Admin Service's schedule creation) hasn't been exercised live either |
| Booking / Payment             | —                           | —                                                      | Don't exist in this repo yet                                   |

None of the above are being fixed as part of this document — this is a
snapshot of what the code actually does today, so anyone picking this repo
up doesn't waste time assuming something works when it doesn't. Each broken
detail is explained in depth in that service's own doc (linked below).

---

## 10. Running It Locally

**Step 1 — start the shared infrastructure** (Postgres, Redis, Kafka,
Elasticsearch, plus their admin UIs):

```bash
docker-compose up -d
```

| Thing                     | URL                   |
| ------------------------- | --------------------- |
| Postgres                  | `localhost:5432`      |
| pgAdmin (Postgres UI)     | http://localhost:8081 |
| Redis                     | `localhost:6379`      |
| Redis Insight UI          | http://localhost:8001 |
| Kafka (from your machine) | `localhost:9093`      |
| Kafka UI                  | http://localhost:8080 |
| Elasticsearch             | http://localhost:9200 |
| Kibana (Elasticsearch UI) | http://localhost:5601 |

**Step 2 — start whichever service(s) you actually need.** Each is
independent:

```bash
cd user-service && npm install && npm run dev   # port 4001
cd api-gateway  && npm install && npm run dev   # port 4000
# admin-service and search-service currently fail to start — see §9
```

Each service needs its own `.env` file — see that service's own doc (§11)
for exactly which variables it reads.

**Step 3 — try the one flow that's guaranteed to work**, hitting the User
Service directly:

```bash
curl -X POST http://localhost:4001/api/v1/auth/send-otp \
  -H "Content-Type: application/json" \
  -d '{"firstName":"Alice","email":"alice@example.com","password":"SecurePass1"}'

# check the email inbox tied to your RESEND_API_KEY / MAIL_SEND for the OTP,
# then:
curl -X POST http://localhost:4001/api/v1/auth/verify-otp \
  -H "Content-Type: application/json" \
  --cookie "otp_session=<value from the Set-Cookie header above>" \
  -d '{"otp":"123456"}'

curl -X POST http://localhost:4001/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"SecurePass1"}'
```

---

## 11. Where to Go Deeper

Every service has its own detailed doc — full architecture diagram, every file
explained with its actual current code pasted in, every environment variable,
and a full list of known bugs/dead code found while writing it:

| Doc                                                                          | Covers                                                                         |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| [`docs/auth.md`](docs/auth.md)                                               | The complete signup/login/refresh flow, security decisions, every Redis key    |
| [`api-gateway/docs/README.md`](api-gateway/docs/README.md)                   | Routing, auth middleware, rate limiting, circuit breaker                       |
| [`user-service/docs/README.md`](user-service/docs/README.md)                 | Auth, profile, the internal user-lookup route                                 |
| [`admin-service/docs/README.md`](admin-service/docs/README.md)               | Stations, trains, routes, schedules                                           |
| [`search-service/docs/README.md`](search-service/docs/README.md)             | Elasticsearch indexing + search logic                                         |
| [`notification-service/docs/README.md`](notification-service/docs/README.md) | The Kafka-driven email worker                                                  |
| [`inventory-service/docs/README.md`](inventory-service/docs/README.md)       | Seat inventory, segment locking, the lock-expiry job — including why it's untested live |
