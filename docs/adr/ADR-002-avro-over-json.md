# ADR-002: Avro Over JSON for Financial Event Serialisation

**Status:** Accepted
**Date:** 2026-08-16

## Context
Financial events cross service, team and time boundaries. A producer change that silently drops a
field or widens a type corrupts downstream risk evaluation and the audit archive, and the corruption
is discovered late.

## Decision
Serialise all primary financial event payloads with Apache Avro against Confluent Schema Registry
with backward compatibility enforced in CI. JSON is permitted only for control-plane HTTP APIs,
structured logs and audit manifest metadata.

## Alternatives
- **JSON.** Human-readable and trivially debuggable. Rejected: no enforced contract, no compatibility
  gate, and roughly three to five times the wire size at 50,000 events/sec.
- **Protobuf.** Comparable compactness and a strong schema story. Rejected: Avro's registry
  integration and schema-resolution model are the more common fit in the Kafka ecosystem the project
  is demonstrating, and Parquet audit output maps cleanly from Avro records.

## Consequences
Debugging requires registry-aware tooling. Schema evolution is constrained to backward-compatible
changes on the main branch: new optional fields with defaults, no removals, no type changes. A
breaking change requires a new topic version.
