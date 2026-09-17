# Event-Driven Platform Architecture

```mermaid
flowchart LR
    CLIENT[Client / Postman] --> GW[FastAPI API Gateway]
    GW --> ORD[Order Service]
    ORD --> K[(Kafka)]
    K --> PAY[Payment Service]
    K --> INV[Inventory Service]
    K --> NOTIF[Notification Service]
    K --> DLQ[Dead Letter Queues]
    ORD --> PG[(PostgreSQL)]
    INV --> PG
    PAY --> PG
    GW --> REDIS[(Redis)]
```

## Core patterns

- Domain services communicate asynchronously through Kafka events.
- Idempotency protects consumers from duplicate delivery.
- Retries use backoff; failed messages can move to dead-letter queues.
- Saga-style coordination handles distributed order/payment/inventory state transitions.
- Correlation IDs and structured logs make cross-service requests traceable.
