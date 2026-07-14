## 🎬 URL Shortener System Design (Design Bitly / TinyURL) | HLD + LLD + DB + Code Explained | Ep. 18

## 📌 One-Line Summary

A comprehensive end-to-end guide to designing a highly scalable URL shortener like Bitly or TinyURL, focusing on Base62 encoding, database sharding, and high-availability architecture.

## 🗂️ Topics Covered (Index)

1. Introduction to Link Shorteners
2. Functional Requirements
3. Non-Functional Requirements
4. High-Level Design (HLD) & Tech Stack
5. Low-Level Design (LLD): URL Generation Logic
6. URL Shortening Algorithms (Random, MD5, Base62)
7. API Design (Endpoints & Contracts)
8. Database Design (Schema & Scaling)
9. Capacity Estimation (Storage & Traffic)
10. Caching Strategy (Redis & CDNs)
11. Scaling: Load Balancing, Replication, and Sharding
12. Security & Rate Limiting
13. Analytics & Failure Management
14. Microservices vs. Monolithic Approach
15. Pseudo-code Implementation

## 📝 Detailed Notes

### Introduction to Link Shorteners

- A URL shortener converts a long, complex URL (e.g., 500 characters) into a short, manageable link (10–20 characters).
- Examples include **Bitly** and **TinyURL**.
- It acts as a gateway that redirects users from a short alias back to the original long URL.

### Functional Requirements

- **URL Shortening:** Convert a long URL into a unique short URL.
- **Redirection:** When a user clicks the short link, they must be redirected to the original long URL.
- **Custom Alias (Tail):** Users should be able to provide a custom string (e.g., `bit.ly/my-link`).
- **Analytics:** Tracking clicks and redirection counts for a dashboard.
- **User Management:** Support for profiles, sign-ups, and link history (optional but recommended for custom tails).
- **URL Expiry:** Ability to set a lifespan for a link (e.g., delete after 2 days or 5 days).

### Non-Functional Requirements

- **High Availability:** The system should aim for 99.99% uptime; it must never be down.
- **Low Latency:** Redirection must be extremely fast (ideally <100ms); users shouldn't wait seconds for a page to load.
- **Scalability:** Must handle millions of link shortening requests daily.
- **Read/Write Performance:** High throughput for both creating links and frequent redirection lookups.

### High-Level Design (HLD) & Tech Stack

- **Frontend:** React.js or a simple website for link submission and dashboards.
- **Backend:** **Java Spring Boot** (preferred for professional scale) or MERN stack.
- **Caching:** **Redis** to store frequently accessed mappings and reduce DB load.
- **Database:** **SQL (MySQL/PostgreSQL)** for relational data and consistency.
- **Infrastructure:** **AWS** for hosting, using **ALB (Application Load Balancer)** and Auto-scaling groups.

### Low-Level Design (LLD): URL Shortening Algorithms

The instructor discusses three main options for generating the "tail" or alias:

1.  **Random String Generation:** Using a package to pick 6 random characters (A-Z, a-z, 0-9).
    - ⚠️ **Warning:** High risk of **collisions** (generating the same string twice).
2.  **Hashing (MD5/SHA):** Generating a long hash and taking the first 7 characters.
    - ⚠️ **Warning:** While popular, there is still a small chance of collision at a massive scale.
3.  **Counter + Base62 Encoding (Best Case):**
    - Use a central **Counter** (e.g., 1, 2, 3...) and convert that decimal number into **Base62** (using 0-9, a-z, A-Z).
    - **Math:** A 7-character Base62 string allows for **$62^7 \approx 3.5$ Trillion unique URLs**.
    - This ensures **zero collisions** because the counter is always unique.

### API Design

1.  **Shorten URL:** `POST /api/short`
    - **Request:** `{ "longUrl": "...", "tail": "optional" }`.
    - **Response:** `{ "shortUrl": "..." }`.
2.  **Redirect:** `GET /{tail}`
    - **Logic:** Fetch the long URL from Cache/DB and perform an **HTTP Redirection**.

### Database Design

- **Table Name:** `URL_Mapping`.
- **Columns:**
  - `id`: BigInt (Primary Key).
  - `short_tail`: Varchar (Unique/Indexed).
  - `long_url`: Text (to handle long strings up to 500+ chars).
  - `user_id`: Int (Optional link to user).
  - `click_count`: Int (Initial 0, increments on every hit).
  - `expiry_time`: Timestamp.
  - `created_at`: Timestamp.
- 🎯 **Interview Tip:** Always **index** the `short_tail` column for fast retrieval during redirection.

### Capacity Estimation

- **Traffic:** Assume **10 Million URLs per month** (~3–4 Lakh per day).
- **Storage:** ~600 bytes per record (500 bytes for URL + 100 bytes for metadata).
- **Total Storage:** 6GB per month $\rightarrow$ **~72GB per year**.
- **Throughput:** Calculate Requests Per Second (RPS) based on peak daily traffic to determine server size.

### Scaling Strategies

- **Database Replication:** Use a **Master-Slave (Primary-Replica)** architecture. Write to Master; Read from Slaves to scale read traffic.
- **Database Sharding:** Partition data across multiple DB instances based on the first letter of the tail (e.g., Shard 1: A-M, Shard 2: N-Z) to speed up searches.
- **Load Balancing:** Use **Nginx** or **AWS ALB** to distribute traffic across multiple API server pods.
- **CDNs:** Use **Cloudflare** for CDN caching to reduce latency for global users.

### Security & Rate Limiting

- **Rate Limiting:** Implement **Token Bucket** or **Leaky Bucket** algorithms to prevent bots from spamming the system (e.g., 10 URLs/hour).
- **Validation:** Sanitize input URLs and use **CAPTCHA** to ensure users are human.
- **Phishing Protection:** Use blacklists to prevent shortening malicious or malware-carrying URLs.

### Analytics & Failure Management

- **Bulk Processing:** Use **Apache Kafka** to ingest millions of click events without slowing down the primary DB.
- **Failure Handling:** Maintain **Backup/Fallback DBs** in different geographical locations to recover from data center failures.

## 📊 Diagrams & Visual Explanations

### 1. Request Flow (Redirection)

```text
[User Browser] --(1. GET /yt-link)--> [Load Balancer]
                                          |
    <--(4. HTTP 302 Redirect)---   [2. API Server]
                                          |
                                   [3. Check Redis Cache]
                                          | (If Miss)
                                   [4. Query DB Shard]
```

_Description: Illustrates the fast-path for redirection using Cache first._

### 2. Database Sharding Logic

```text
      [ Shard Router ]
       /      |       \
[DB A-M]   [DB N-Z]   [DB 0-9]
```

_Description: Shows how data is partitioned to ensure no single database instance becomes a bottleneck._

## 💻 Code Snippets & Commands

### ✅ Shown in video: Conceptual Controller (Java-style)

```java
// POST endpoint to shorten URL
@PostMapping("/shorten")
public String shorten(@RequestBody Request req) {
    String tail = req.getTail();
    if (tail == null) {
        // Use Base62 + Counter logic if no custom tail provided
        tail = urlService.generateBase62Hash(counter.incrementAndGet());
    }
    db.save(req.getLongUrl(), tail); // Logic to save to SQL
    return "www.tsv.com/" + tail;
}

// GET endpoint for redirection
@GetMapping("/{tail}")
public void redirect(@PathVariable String tail, HttpServletResponse response) {
    // 1. Fetch from Cache/DB
    String originalUrl = urlService.fetchLongUrl(tail);
    // 2. Redirect
    response.sendRedirect(originalUrl);
}
```

### 🧪 Generated learning example: Base62 Logic (Pseudo-code)

```javascript
const CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

function encodeBase62(counter) {
  let result = "";
  while (counter > 0) {
    result = CHARS[counter % 62] + result;
    counter = Math.floor(counter / 62);
  }
  return result.padStart(7, "0"); // Ensure 7-char tail
}
```

## 🔁 Comparisons & Trade-offs

| Strategy           | Random String | MD5 Hashing      | Counter + Base62            |
| :----------------- | :------------ | :--------------- | :-------------------------- |
| **Collision Risk** | High          | Low              | **Zero**                    |
| **Complexity**     | Low           | Medium           | High (Needs unique counter) |
| **Length**         | Fixed         | Can be truncated | Increases with counter      |

## 🎯 Interview Questions

1. **How do you handle URL collisions in your design?**
   By using a unique central counter and encoding it in Base62. Since the counter never repeats, the generated tail is guaranteed to be unique.
2. **Why use Redis for a URL shortener?**
   Because redirection is a read-heavy operation. Storing popular mappings in an in-memory cache like Redis avoids slow database queries.
3. **What is the difference between Range-based and Hash-based sharding?**
   Range-based (e.g., A-M) is easier to conceptualize but can lead to "hot partitions" if most links start with common letters. Hash-based distributes data more evenly.
4. **How would you handle a viral link from a celebrity like Carry Minati?**
   By using a CDN (Cloudflare) and multi-layer caching with a TTL. Popular links should be cached globally at the "Edge".
5. **How do you prevent a bot from creating 1 million links in a minute?**
   By implementing rate limiting algorithms like Token Bucket and using CAPTCHA for link creation.

## ⚡ Quick Revision Flashcards

1. **Q:** What is the primary purpose of a URL shortener? **A:** Redirection and space saving.
2. **Q:** What is Base62? **A:** Encoding using 0-9, a-z, and A-Z (62 characters).
3. **Q:** How many URLs can 7-char Base62 support? **A:** ~3.5 Trillion.
4. **Q:** Which HTTP code is used for redirection? **A:** 301 (Permanent) or 302 (Temporary).
5. **Q:** What is a "Hot Key" problem? **A:** When one link gets millions of clicks, overloading a specific DB shard.
6. **Q:** What tool handles high-volume click logging? **A:** Apache Kafka.
7. **Q:** Why use Text instead of Varchar for long URLs? **A:** Varchar has character limits; long URLs can exceed 500+ chars.
8. **Q:** What is a "Tail"? **A:** The unique alias at the end of the short URL.
9. **Q:** What is a "Dead Letter Queue" (contextual)? **A:** Storage for failed messages in the background analytics flow [context from Ep 14].
10. **Q:** What is the storage cost for 10M links for 1 year? **A:** ~72GB.

## 🔗 Connections to Other System Design Topics

- **SQL vs NoSQL (Ep 10):** Choosing SQL for strict consistency in link mapping.
- **Caching (Ep 09):** Essential for performance during viral link events.
- **Load Balancing (Ep 08):** Required for the API layer to handle 10M+ monthly users.

## 📌 Key Takeaways

- Use **Base62 + Counter** for a collision-free generation logic.
- **Redirection (Read)** is 100x more frequent than **Shortening (Write)**; optimize accordingly.
- **Sharding** and **Replication** are non-negotiable for horizontal database scaling.
- **Redis** caching can handle 95% of redirection traffic if implemented correctly.
- Always implement **security validations** to prevent the platform from being used for phishing.
