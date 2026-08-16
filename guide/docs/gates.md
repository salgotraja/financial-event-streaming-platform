# Build gates and CI

[![What ./gradlew build actually runs](diagrams/build-gates.svg)](diagrams/build-gates.svg){: .diagram title="Open the full-size diagram" }
Click the diagram to open it full size. Source: `guide/docs/diagrams/build-gates.drawio`.
{: .diagram-hint }

Four things fail `./gradlew build` rather than warn. Two of them are rules about the shape of the
codebase rather than about its behaviour, and those two run as their own CI steps so a violation is
named in the job list rather than buried in a test summary.

## Gate 1: schema compatibility

```bash
./gradlew :contracts:test
```

Every `.avsc` must stay mutually readable with the committed baseline. FULL rather than BACKWARD,
offline rather than against a registry. Covered in full in [Event contracts](contracts.md).

## Gate 2: plane isolation

```bash
./gradlew checkPlaneIsolation
```

No module under `services/ingestion`, `services/streaming` or `services/audit` may depend on a
`:services:agent` project or on an agent-plane library. Covered in
[Modules and the isolation rule](modules.md).

## Gate 3: the ACL policies render

```bash
./gradlew renderKafkaAcls
```

Reads every `services/*/*/src/main/resources/security/kafka-acls.yml` with the same parser the tests
use and writes `build/kafka-acls.args`.

This runs on every build, which is the point. A malformed or unrenderable policy fails here rather
than days later when someone brings up the strict-security stack, and the task fails outright if it
finds no policy file at all, because every service commits one.

```groovy
doFirst {
    if (policyFiles.isEmpty()) {
        throw new GradleException('No service kafka-acls.yml found. Every service commits one.')
    }
}
```

## Gate 4: the tests

Three categories, all under each module's `check` task.

**Unit tests.** Pure logic, no Spring context. JUnit 5, Mockito, AssertJ.

**Integration tests.** `KafkaAvroStack`: a real broker on `apache/kafka-native:4.1.0` plus a real
Confluent Schema Registry on `confluentinc/cp-schema-registry:7.9.1`, on a shared Docker network,
started once per JVM.

The registry is not optional scaffolding. The platform serialises financial payloads with Avro against
a registry, and a producer test that skips it proves the object graph works while leaving the part
that actually breaks in production, schema resolution and subject compatibility, untested.

**Authorization tests.** `SecureKafkaStack`: a real broker on `apache/kafka:4.1.0` with SASL/PLAIN and
the `StandardAuthorizer`. Two broker images in one run is the cost of authenticated local tests. See
[Gotchas](gotchas.md#the-native-image-cannot-act-as-a-sasl-server) for why the images differ.

A running Docker daemon is required.

## Required coverage by category

From `.claude/rules/testing.md`. These follow from the requirements and are not optional for a module
to be considered complete:

| Category | What it must show |
| --- | --- |
| Idempotency | The same event processed twice produces a single effect |
| Poison record | A malformed record is quarantined to `{topic}.dlq`, the offset advances, and the next record on the partition processes normally |
| Dependency failure | A Redis or PostgreSQL outage opens the circuit at the dependency boundary and does not quarantine otherwise valid records |
| Negative authorization | At least one ALLOW and two DENY per service, driven by the committed policy |
| Agent tool boundary | Unknown tool denied, undeclared argument rejected, injected instruction text ignored, failed required tool produces `ESCALATE` rather than `NO_FLAG` |
| Plane isolation | A build-level check |
| Rebuild | Read models and the precedent graph rebuild from event history and report a reconciliation result |

Four of those seven are exercised today. The other three belong to services that do not exist.

## The CI workflow

`.github/workflows/ci.yml`, on every pull request and on pushes to `main`:

```yaml
- name: Schema compatibility gate
  run: ./gradlew :contracts:test --tests '*SchemaCompatibilityTest'

- name: Plane isolation gate
  run: ./gradlew checkPlaneIsolation

- name: Build and test
  run: ./gradlew build
```

Java 25 from Corretto, `gradle/actions/setup-gradle` for caching, and test reports uploaded with
`if: always()` so a failing run still produces something to read.

Running the two gates ahead of the wider build duplicates a little work and buys a clear signal: a
compatibility break or a forbidden dependency shows up as a named failed step.

## What is deliberately absent

No formatter and no linter are configured. `check` maps to the tests plus `checkPlaneIsolation` plus
`renderKafkaAcls`, and nothing else.

## This guide's own build

`.github/workflows/guide.yml` builds the site with `mkdocs build --strict` on every pull request that
touches `guide/`, and publishes to GitHub Pages from `main`. Strict mode fails on a broken internal
link or a referenced-but-missing diagram, so a dead page fails CI rather than shipping.
