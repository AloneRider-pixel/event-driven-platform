# 🚀 Event-Driven Distributed Order Platform

**Scalable microservices e-commerce backend** built with event-driven architecture using Kafka for asynchronous communication between services.

---

## 🏗️ Architecture

```
                    React / Postman
                         ↓
                   API Gateway (FastAPI)
                   Auth + Rate Limiting
                         ↓
                   Order Service
                   (Kafka Producer)
                         ↓
                    ┌────┴────┐
                    │  Kafka   │
                    └────┬────┘
              ┌──────────┼──────────┐
              ↓          ↓          ↓
         Payment    Inventory   Notification
         Service     Service      Service
              └──────────┼──────────┘
                         ↓
                   PostgreSQL + Redis
```

### Event Flow
1. **Customer** places order via API Gateway
2. **Order Service** validates and persists, publishes `OrderCreated` event to Kafka
3. **Payment Service** consumes event, processes payment, publishes `PaymentCompleted` or `PaymentFailed`
4. **Inventory Service** consumes `OrderCreated`, reserves stock, publishes `InventoryReserved` or `InventoryInsufficient`
5. **Notification Service** consumes all events, sends emails/SMS to customer
6. **Dead Letter Queue** catches failed events for manual review

---

## ✨ Features

### Microservices
- **API Gateway** — Authentication (JWT), rate limiting, request routing, API versioning
- **Order Service** — CRUD orders, saga orchestration, idempotency keys
- **Payment Service** — Payment processing, refund handling, retry logic
- **Inventory Service** — Stock reservation, release on cancellation, optimistic locking
- **Notification Service** — Email/SMS dispatch, template engine, delivery tracking

### Event-Driven Architecture
- **Kafka Topics** — Separate topics per domain (orders, payments, inventory, notifications)
- **Consumer Groups** — Independent scaling per service
- **Dead Letter Queues** — Failed events routed for manual intervention
- **Event Sourcing** — Full audit trail of all state changes
- **Saga Pattern** — Distributed transaction management with compensating actions

### Reliability
- **Idempotency** — Duplicate event handling without side effects
- **Retries** — Exponential backoff with jitter
- **Circuit Breakers** — Prevent cascade failures
- **Health Checks** — Per-service and aggregate health monitoring
- **Graceful Degradation** — Services function independently

### Security & Performance
- **JWT Authentication** — Token-based auth with role-based access
- **Rate Limiting** — Redis-backed sliding window per user
- **Caching** — Redis cache for hot data (products, inventory)
- **Database Indexing** — Optimized queries with composite indexes
- **Connection Pooling** — Efficient DB connections

### Observability
- **Structured Logging** — JSON logs with correlation IDs across services
- **Metrics** — Prometheus-compatible metrics per service
- **Distributed Tracing** — Request correlation across microservices
- **Health Endpoints** — Deep health checks with dependency status

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Services | Python 3.11, FastAPI, SQLAlchemy |
| Messaging | Apache Kafka (Confluent), aiokafka |
| Cache | Redis 7 |
| Database | PostgreSQL 16 |
| Auth | JWT (PyJWT), bcrypt |
| Infra | Docker, Docker Compose |
| CI/CD | GitHub Actions |
| Testing | Pytest, httpx, locust |

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose

### 1. Clone & Configure

```bash
git clone https://github.com/AloneRider-pixel/event-driven-platform.git
cd event-driven-platform
cp .env.example .env
```

### 2. Start All Services

```bash
docker-compose up -d
```

This starts:
- **Zookeeper** → localhost:2181
- **Kafka** → localhost:9092
- **PostgreSQL** → localhost:5432
- **Redis** → localhost:6379
- **API Gateway** → localhost:8000
- **Order Service** → localhost:8001
- **Payment Service** → localhost:8002
- **Inventory Service** → localhost:8003
- **Notification Service** → localhost:8004

### 3. Test the Platform

```bash
# Create an order
curl -X POST http://localhost:8000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-001", "quantity": 2, "customer_id": "CUST-001"}'

# Check order status
curl http://localhost:8000/api/v1/orders/ORD-001

# View Kafka topics
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

---

## 📁 Project Structure

```
event-driven-platform/
├── shared/                      # Shared libraries
│   ├── models.py                # Pydantic models shared across services
│   ├── events.py                # Event definitions & schemas
│   ├── kafka_utils.py           # Kafka producer/consumer helpers
│   └── middleware.py            # Common middleware (logging, correlation)
├── api-gateway/                 # API Gateway
│   ├── Dockerfile
│   ├── main.py                  # FastAPI app with routing
│   ├── auth.py                  # JWT authentication
│   ├── rate_limiter.py          # Redis-backed rate limiting
│   └── routes/                  # API endpoints (v1)
├── order-service/               # Order Microservice
│   ├── Dockerfile
│   ├── main.py                  # FastAPI + Kafka producer
│   ├── models/                  # SQLAlchemy models
│   ├── services/                # Business logic
│   ├── kafka/                   # Kafka event publishing
│   └── tests/                   # Service tests
├── payment-service/             # Payment Microservice
│   ├── Dockerfile
│   ├── main.py                  # FastAPI + Kafka consumer
│   ├── consumer.py              # Kafka event handler
│   └── services/                # Payment processing
├── inventory-service/           # Inventory Microservice
│   ├── Dockerfile
│   ├── main.py                  # FastAPI + Kafka consumer
│   ├── consumer.py              # Stock reservation logic
│   └── services/                # Inventory management
├── notification-service/        # Notification Microservice
│   ├── Dockerfile
│   ├── main.py                  # FastAPI + Kafka consumer
│   ├── consumer.py              # Event-driven notifications
│   └── templates/               # Email/SMS templates
├── scripts/                     # Utility scripts
│   └── load_test.py             # Locust load testing
├── .github/workflows/ci.yml    # CI/CD pipeline
├── docker-compose.yml
└── .env.example
```

---

## 🔌 API Endpoints

### Orders (v1)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/orders` | Create new order |
| GET | `/api/v1/orders/{id}` | Get order details |
| GET | `/api/v1/orders` | List orders (paginated) |
| PUT | `/api/v1/orders/{id}/cancel` | Cancel order |
| POST | `/api/v1/orders/{id}/retry` | Retry failed order |

### Inventory
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/inventory/{product_id}` | Check stock |
| PUT | `/api/v1/inventory/{product_id}` | Update stock |

### Health & Monitoring
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Service health check |
| GET | `/metrics` | Prometheus metrics |
| GET | `/ready` | Readiness probe |

---

## 📊 Kafka Topics

| Topic | Producer | Consumer(s) | Description |
|-------|----------|-------------|-------------|
| `order.created` | Order Service | Payment, Inventory, Notification | New order placed |
| `order.cancelled` | Order Service | Payment, Inventory, Notification | Order cancelled |
| `payment.completed` | Payment Service | Order, Notification | Payment successful |
| `payment.failed` | Payment Service | Order, Notification | Payment failed |
| `inventory.reserved` | Inventory Service | Order, Notification | Stock reserved |
| `inventory.insufficient` | Inventory Service | Order, Notification | Out of stock |
| `notification.sent` | Notification Service | — | Notification delivered |
| `*.dlq` | All services | — | Dead letter queues |

---

## 📝 License

MIT
