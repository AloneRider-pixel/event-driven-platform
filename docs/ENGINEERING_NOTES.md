# Engineering Notes

## Engineering focus
A distributed order-processing backend demonstrating asynchronous service communication and reliability patterns.

## Key design decisions
- **Kafka events:** decouples order, payment, inventory, and notification workflows.
- **Consumer groups:** allows services to scale independently.
- **Idempotency:** duplicate events should not create duplicate side effects.
- **Saga-style coordination:** compensating actions address failures across service boundaries.
- **DLQs and retries:** failed event processing is isolated for investigation and recovery.
- **Correlation IDs and metrics:** make cross-service behavior observable.

## Verification checklist
- Start the stack with Docker Compose.
- Create an order through the API gateway.
- Inspect produced Kafka events and downstream state changes.
- Exercise retry and failure paths.
- Run service tests and load tests.

## Portfolio note
Architecture claims should remain aligned with implemented service behavior and test coverage.
