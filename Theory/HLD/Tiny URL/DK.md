## 🎬 TinyUrl System Design | Bitly System Design | URL Shortener System Design

## 📌 One-Line Summary

A deep-dive into designing a scalable URL shortening service (like TinyURL or Bitly) using Base62 encoding, distributed counters managed by Apache ZooKeeper, and NoSQL databases for high-volume storage.

## 🗂️ Topics Covered (Index)

1. Introduction to URL Shortening Services
2. Functional and Non-Functional Requirements
3. Clarifying Questions for Interviews (Assumptions & Estimations)
4. Capacity Estimation (Storage for 10 Years)
5. Database Selection (SQL vs. NoSQL)
6. URL Shortening Logic: MD5 Hash vs. Base 10 vs. Base 62
7. The Collision Problem in Distributed Systems
8. Scaling with Counters and the Single Point of Failure
9. ZooKeeper: The "Teacher/Coordinator" Solution
10. Final System Architecture and Request Flow
11. Performance Optimization with Redis Caching

## 📝 Detailed Notes

### Introduction to URL Shortening Services

- A URL shortener converts a long, cumbersome URL into a short, unique URL.
- When a user accesses the short URL, the system redirects them to the original long URL.
- 💡 **Example:** Converting a long LinkedIn profile URL into a short alias like `tinyurl.com/shivam-tiwari-27`.

### Functional and Non-Functional Requirements

- **Functional Requirements:**
  - User provides a long URL.
  - System generates a short, unique URL.
- **Non-Functional Requirements:**
  - **Low Latency:** Generating and redirecting URLs should not take minutes (aim for sub-second performance).
  - **High Availability:** The service must be online and accessible at all times.

### Interview Strategy: Assumptions & Estimations

Before designing, ask the interviewer these critical questions:

1. **Traffic Volume:** 60 million monthly active users (MAU).
2. **URL Length:** The unique code for the short URL will be 7 characters.
3. **Storage Duration:** The system must store URLs for 10 years.

### Capacity Estimation

- **Data per Entry:**
  - **Long URL:** Max ~2048 characters (2KB).
  - **Short URL:** Domain (`www.tinyurl.com` = 10 chars) + Unique Code (7 chars) = 17 bytes.
  - **Metadata:** `Created_at` (7 bytes) and `Expired_at` (7 bytes) stored as Epoch time.
  - **Total per entry:** Approximately 231 bytes.
- **Total Storage Needed:**
  - 60M users/month = ~121.86 GB/month.
  - 12 months = ~1.646 TB/year.
  - **10 Years = ~14.6 TB total.**

### Database Selection

- **Choice:** **NoSQL Database**.
- **Reasoning:** 14.6 TB is a huge volume that will grow. While RDBMS provides ACID properties, it is difficult to scale horizontally. NoSQL scales easily and handles high-volume traffic/storage more efficiently.

### URL Shortening Logic & Algorithms

The system needs to generate a 7-character "Unique Code".

- **MD5 Hash:** Generates a 32-character Base62 output. Truncating it to 7 characters leads to **collisions** (two different long URLs getting the same short code), causing data corruption.
- **Base 10:** Uses digits 0-9. For 7 characters, it only provides 10 million unique combinations, which is insufficient for 60M+ users.
- **Base 62:** Uses `a-z`, `A-Z`, and `0-9` (Total 62 chars).
  - 7 characters = $62^7 \approx$ **3.5 Trillion unique combinations**.
  - Even at 1,000 URLs/sec, this lasts for **100 years**.
  - **Conclusion:** Base 62 is the optimal choice.

### The Collision Problem in Distributed Systems

- If we use a random number generator for Base62, two different application servers might generate the same random number (e.g., '6') simultaneously.
- This would result in the same short URL for two different users.
- ⚠️ **Warning:** Checking the database before every insertion works for single-server systems but fails in parallel/scaled systems due to race conditions.

### Scaling with Counters

- To ensure uniqueness, use a **Counter** that increments for every new request.
- **Problem:** A single central counter is a **Single Point of Failure (SPOF)**—if it crashes, the whole system stops.
- **Regional Counters:** Assigning ranges (e.g., Server 1 gets 1M-2M, Server 2 gets 2M-3M) is complex to manage and reset manually.

### ZooKeeper: The "Teacher" Solution

ZooKeeper is a coordination service used to manage distributed counters.

- 💡 **Analogy:** ZooKeeper is like a **Class Teacher** managing students (App Servers).
  - It assigns unique names/IDs to students.
  - It tracks who is present/absent (Online/Offline status).
  - It helps students coordinate and prevents "fighting" (collisions).
  - It decides who the "Monitor" (Master Node) is.
- **How it works:** ZooKeeper maintains counter ranges (1-100k, 100k-200k, etc.) in its memory.
- When an App Server comes online, ZooKeeper assigns it an available range. Once a server reaches its limit, it requests a new range from ZooKeeper.

## 📊 Diagrams & Visual Explanations

### 1. Short URL Structure

```text
[ Domain Name ] + [ Unique Code ]
www.tinyurl.com +   abc123Z       = Shortened URL
(10 chars)          (7 chars)
```

_Total length: 17 chars/bytes._

### 2. High-Level Architecture

```text
[ User ]
   |
[ Load Balancer ]
   |
   ---------------------------------
   |               |               |
[App Server 1]  [App Server 2]  [App Server 3] <--- [ ZooKeeper ]
   | (Range 1-1M)  | (Range 1M-2M) | (Range 2M-3M)    (Manager)
   ---------------------------------
   |               |
[ NoSQL DB ]    [ Redis Cache ]
(Storage)        (Fast Reads)
```

_Description: ZooKeeper assigns unique numeric ranges to servers. Servers use these numbers to generate Base62 codes, store them in NoSQL, and cache them in Redis._

## 💻 Code Snippets & Commands

### ✅ Shown in video: Base62 Character Set

```text
a b c ... z (26)
A B C ... Z (26)
0 1 2 ... 9 (10)
Total = 62 characters
```

### 🧪 Generated learning example: Base62 Conversion Logic

```python
# Conceptual logic for converting the Counter ID to Base62
def encode_base62(counter_id):
    characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    base62_hash = ""
    while counter_id > 0:
        base62_hash = characters[counter_id % 62] + base62_hash
        counter_id //= 62
    return base62_hash.rjust(7, '0') # Pad to ensure 7-character length
```

## 🔁 Comparisons & Trade-offs

| Algorithm            | Base 10                | MD5 Hash                       | Base 62                         |
| :------------------- | :--------------------- | :----------------------------- | :------------------------------ |
| **Output Type**      | Digits only (0-9)      | Alpha-numeric (Binary/String)  | Alpha-numeric (a-z, A-Z, 0-9)   |
| **Max Combinations** | 10 Million (Low)       | High (32 chars)                | **3.5 Trillion** (Optimal)      |
| **Collision Risk**   | High (if capacity hit) | High (if truncated to 7 chars) | **Zero** (if used with Counter) |
| **Length**           | 7 characters           | 32 characters (Too long)       | 7 characters                    |

## 🎯 Interview Questions

1. **Why not use RDBMS for TinyURL?**
   While it provides ACID, RDBMS is hard to scale for the 14.6 TB volume and high traffic associated with 60M users; NoSQL is more horizontally scalable.
2. **What is the primary benefit of Base62 over MD5?**
   MD5 generates a 32-character string which is too long; truncating it causes collisions. Base62 with a counter provides trillions of unique, short 7-character strings with zero collisions.
3. **What role does ZooKeeper play in this architecture?**
   It acts as a distributed coordination service that manages and assigns unique counter ranges to application servers, preventing duplicate ID generation.
4. **How do you handle a "Hot Key" (popular link) problem?**
   By using **Redis** to cache the mapping of the long and short URL for faster retrieval and reduced database latency.
5. **How long can a 7-character Base62 system last?**
   At a rate of 1,000 unique URLs per second, the system can last for approximately 100 years.

## ⚡ Quick Revision Flashcards

1. **Q:** What is the MAU assumption? **A:** 60 Million.
2. **Q:** Total data for 10 years? **A:** ~14.6 TB.
3. **Q:** Size of one entry? **A:** ~231 bytes.
4. **Q:** Preferred algorithm? **A:** Base62.
5. **Q:** Why use ZooKeeper? **A:** Distributed counter coordination.
6. **Q:** How many combinations in $62^7$? **A:** 3.5 Trillion.
7. **Q:** What is the SPOF in counter logic? **A:** A single central counter.
8. **Q:** What happens if an App Server goes down in ZooKeeper? **A:** ZooKeeper tracks it and blocks the entry.
9. **Q:** Role of Load Balancer? **A:** Distributes requests based on server load.
10. **Q:** Is the domain part of the unique code? **A:** No, the domain is static; only the 7-char code changes.

## 🔗 Connections to Other System Design Topics

- **NoSQL Scaling:** Links to Sharding and Replication (as mentioned in Ep 11).
- **Caching:** Links to Episode 09 (Redis/Memcached for latency reduction).
- **Load Balancing:** Links to Episode 08.
- **Distributed Coordination:** The use of ZooKeeper is a prerequisite for advanced microservice management.

## 📌 Key Takeaways

- Always clarify **volume, length, and time** before designing.
- **Base62** is the industry standard for short URL aliases due to its high combination density.
- **Distributed counters** are necessary to prevent collisions, but require a coordinator like **ZooKeeper** to avoid SPOF and range conflicts.
- **NoSQL** is the preferred storage for the high-volume nature of URL mappings.
- **Redis** is essential for maintaining the "Low Latency" requirement during redirection.
