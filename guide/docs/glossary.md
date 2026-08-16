# Glossary

Terms as this repository uses them. Where a term has a general meaning and a narrower local one, the
local one is what the code means.

**ADR.** Architecture Decision Record. One file per decision in `docs/adr/`, ADR-001 through ADR-030.
Cited by number throughout this guide and in the source comments.

**At-least-once.** The delivery contract. Idempotent producers, explicit offset commits, and
deduplication by a deterministic key. Duplicates arrive, and every consumer must tolerate them. The
platform is never described as exactly-once (ADR-019).

**Compacted topic.** A topic where the broker keeps the latest record per key and discards the rest.
`reference-data.instruments` is the only one. On a compacted topic the key is row identity, and a null
value is a deletion.

**Deterministic plane.** Planes 1 to 3: ingestion, streaming, audit. Carries the throughput and
latency targets, and may not depend on the agent plane.

**DLQ, dead-letter queue.** `{original-topic}.dlq`, lowercase. Carries a `DeadLetterEvent` with the
delivered bytes, the cause chain, the attempt count and the broker coordinates of the quarantined
record.

**Durability profile.** The producer settings in `ProducerDurabilityConfiguration`, applied
programmatically to every producer factory so a service cannot ship weaker settings by omission.

**Evidence topic.** A topic the audit service archives. FR-05.1 lists seventeen; four have a producer
today.

**FULL compatibility.** Confluent's mutual-read compatibility level. A new schema must read data
written by the old one, and the old schema must read data written by the new one. Chosen over
BACKWARD because BACKWARD alone permits field removal.

**Idempotency key.** For the audit path, `topic-partition-offset`. Broker coordinates, so a duplicate
is recognisable without understanding the payload.

**Maker-checker.** The governance rule that a `RiskMaker` may not approve a change they proposed.
Enforced in policy, not in the UI, and not considered done without a passing negative test (ADR-016).

**MSK IAM.** How Kafka authorization works in the cloud profile: AWS IAM scoped per topic, per
consumer group, per identity (ADR-009). Kafka ACLs exist for the local strict-security profile.

**Offset profile.** The consumer settings in `ConsumerAcknowledgementConfiguration`:
`enable.auto.commit=false` and `MANUAL_IMMEDIATE`, forced platform-wide.
`isolation.level` is deliberately unset.

**Plane.** One of the five architectural layers. See [Five planes](planes.md).

**Plane isolation.** The rule that no ingestion, streaming or audit module may depend on an agent
module, on Neo4j or on an LLM provider. Enforced by `./gradlew checkPlaneIsolation`.

**Poison record.** A record that cannot be processed and will not improve on retry. Quarantined per
record to the dead-letter topic, with the offset advancing (ADR-027). Never handled by an
event-type-wide circuit breaker.

**Precedent graph.** A Neo4j projection of reviewed cases, derived and rebuildable. PostgreSQL stays
authoritative, and the agent must degrade when the graph is unavailable (ADR-022). Not built.

**Principal.** A workload identity, named in the service's `kafka-acls.yml` and matching the service
name. `User:trade-producer` on the broker.

**RESTRICTED.** A data classification. `traderId`, `accountId`, alert investigation content, risk-rule
parameters under review, audit evidence payloads and security event detail must never appear in
high-cardinality logs or unmasked to unauthorised roles.

**Schema baseline.** The accepted copy of every schema, in
`contracts/src/test/resources/schema-baseline`. The compatibility gate compares against it, and
`./gradlew updateSchemaBaseline` is how a deliberate break is accepted.

**Strict-security profile.** The local stack profile with SASL_SSL, the `StandardAuthorizer` and ACLs
rendered from the committed policies. The `dev` profile is plaintext and is never evidence of
production security.

**Subject.** A Schema Registry entry, named `{topic}-value` here. Nineteen registered, provisioned
from `subjects.tsv` rather than auto-registered by producers.

**Testcontainers.** How every integration test gets a real broker. `apache/kafka-native:4.1.0` for the
plain stack, `apache/kafka:4.1.0` for the authenticated one, `confluentinc/cp-schema-registry:7.9.1`
for the registry.

**Tombstone.** A null value on a compacted topic, which deletes the key. Legitimate to read, never
published by `reference-data-service`, and classified as `"Tombstone"` by the audit decoder.

**Workload identity.** A per-service identity with its own credential, policy and consumer group. No
shared wildcard roles, and no long-lived static credentials for application workloads (ADR-010,
ADR-013).
