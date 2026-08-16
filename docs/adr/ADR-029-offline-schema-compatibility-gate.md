# ADR-029: Offline FULL-Compatibility Schema Gate Against a Committed Baseline

**Status:** Accepted
**Date:** 2026-08-16

## Context

FR-02.2 requires that a schema change breaking compatibility fails the pipeline before merge. The
architecture describes this as "a CI job runs schema compatibility checks against the registry on
every pull request", and states the evolution policy as "backward compatible changes only in the
main branch. New optional fields with defaults. No field removal. No type changes."

Two questions were open: what compatibility level actually enforces that policy, and what the check
compares against.

## Decision

**Level: FULL, not BACKWARD.** Every schema must be mutually readable with the accepted baseline.

BACKWARD alone does not enforce the stated policy. Under BACKWARD, removing a field is compatible: a
new reader simply ignores the field the old writer emitted. The policy says "no field removal", and
only FULL rejects it. FULL is also the level FR-02.3 implies, since two schema versions are live
during rolling upgrades and an old consumer must still read a new producer's output.

This was found empirically. The first implementation used BACKWARD, and a test that deleted a field
from `PositionSnapshotEvent` passed. The gate was correct for the level chosen and the level was
wrong for the policy.

**Comparison target: a committed baseline, not a live registry.** The schemas in
`contracts/src/main/avro/` are validated against `contracts/src/test/resources/schema-baseline/`,
which is version controlled. The check is a plain JUnit test using Avro's `SchemaValidatorBuilder`,
requiring no broker, no registry and no network.

Accepting a deliberate break is explicit: `./gradlew updateSchemaBaseline` copies the current
schemas over the baseline, producing a reviewable diff in the pull request that introduces the break.

## Alternatives

- **Check against a running Confluent Schema Registry in CI.** Closest to the production mechanism.
  Rejected as the primary gate: the result depends on what happens to be registered in a shared
  registry at the time the job runs, so the same commit can pass and later fail without changing.
  It also needs Kafka and Schema Registry containers on every pull request for a check that is pure
  schema algebra. The registry remains the runtime enforcement point; this gate is the merge-time one.
- **Confluent's `kafka-schema-registry-maven-plugin`.** Maven only. This is a Gradle build.
- **A Gradle community registry plugin.** Adds a third-party dependency in the merge path and still
  requires a live registry.
- **Review discipline with no automated check.** Rejected outright: NFR-06 and the project's
  no-security-by-diagram principle both require the control to be executable.

## Consequences

The gate is deterministic, runs in about a second, and needs no infrastructure, so it can run on
every pull request including forks. It catches field removal, type changes, mandatory fields added
without a default, and enum symbol removal. All four cases were verified to fail, and adding an
optional field with a default was verified to pass.

The baseline is a second copy of every schema and must be kept honest. `updateSchemaBaseline` is the
only sanctioned way to move it, and moving it is visible in review.

This gate does not validate registry subject naming, subject-level compatibility configuration, or
serializer behaviour against a real broker. Those are integration concerns and land with the
producer services, which run against Testcontainers Kafka and Schema Registry.
