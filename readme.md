# IRCTC Backend System Clone

This project is a production-ready **IRCTC-like backend system** built from scratch using a microservices architecture. It demonstrates how to build scalable, distributed systems using industry-standard engineering practices.

## Tech Stack

- **Language/Framework:** NodeJS
- **Architecture:** Microservices, Event-Driven
- **Communication:** Apache Kafka
- **Search:** Elasticsearch
- **Caching:** Redis
- **Orchestration/Deployment:** Docker, AWS EC2

## Key Features

- **User Management:** Signup with OTP, Login, Google Authentication, and Refresh Token Rotation.
- **Search:** Fuzzy search and autocomplete using Elasticsearch.
- **Booking System:** Distributed transactions using the **SAGA pattern**, concurrency handling, idempotency, and Redis distributed locking.
- **Inventory Management:** Row-level exclusive locking for seat management.
- **Payments:** Adapter design pattern for multiple payment gateway integration with Razorpay webhooks.
- **API Gateway:** Custom implementation for authentication, rate limiting, and request routing.

## System Architecture

This project follows a clean microservices approach, moving logic like authentication from individual services to a centralized **API Gateway** to reduce code duplication and improve scalability. It ensures data consistency through asynchronous event processing with **Kafka** and robust database transaction management.
