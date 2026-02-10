# Tradeoffs

## What we deliberately did NOT do
- No full online training loop: high risk without strong labeling and approvals.
- No complex feature store or state backend (RocksDB/Redis): kept local for clarity.
- No Kafka dependency by default: included optional Redpanda to mirror reality.

## Consequences
- Isolation Forest is not truly online; drift is handled operationally (monitor + review).
- In-memory state is not HA by itself. In prod, you would persist state and run replicas.
