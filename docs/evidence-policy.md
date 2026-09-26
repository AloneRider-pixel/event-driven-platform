# Evidence and reproducibility policy

This repository demonstrates distributed-systems behavior. Architecture claims describe implemented mechanisms; performance claims require measured evidence.

## Measured claims

A benchmark should record the workload, message volume, consumer/producer configuration, dependency versions, environment, sample count, duration, failure definition, and commit SHA. Keep raw or summarized results as CI artifacts when practical.

## Reliability claims

Statements about delivery guarantees, idempotency, retries, dead-letter handling, or compensation should be supported by tests or reproducible failure scenarios. A green CI run means the configured scenarios passed; it is not a guarantee of production behavior.

## Synthetic workloads

Load tests and local broker simulations are controlled experiments. They must not be presented as evidence of Internet-scale capacity without appropriate workload and environment documentation.
