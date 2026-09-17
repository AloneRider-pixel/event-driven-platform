# Contributing to Event-Driven Platform

This repository is a distributed-systems portfolio project. Changes should preserve service boundaries and failure-handling behavior.

## Development workflow

1. Branch from `main` and make one focused change.
2. Add or update service-level and integration tests for behavioral changes.
3. Validate Kafka event schemas and compatibility when changing events.
4. Run the CI test suite locally before opening a pull request.
5. Never commit secrets, credentials, generated data, or production configuration.

## Quality expectations

- Keep services independently testable and deployable.
- Treat events as contracts: document producers, consumers, payload changes, and compatibility implications.
- Preserve idempotency, retries, dead-letter handling, and correlation IDs where applicable.
- Document meaningful architectural trade-offs.

## Pull requests

Include the affected services, event-flow impact, tests executed, and any operational or failure-mode considerations.
