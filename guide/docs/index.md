# Learn the platform by following one event

A code-accurate companion to the Financial Event Streaming Platform: what is built, how it works, and
how to prove each claim from the repository.

!!! warning "What this guide covers, and what it does not"

    The design describes about twenty services. Six modules exist. This guide follows the **code**, not
    the design: every file path, class name, topic, setting and number below was read out of the source
    tree, and where the design describes a target the code has not reached, the guide says so in that
    section rather than in a footnote.

    One page, [Specified, not built](not-built.md), covers the rest. Nothing on any other page is a plan.

    Every path this guide cites is in the repository and can be opened. Design documents that are not
    committed are never cited as files, so there is nothing here to chase that you do not have.

## What the platform is

Trade executions, market-data ticks, corporate actions and instrument reference data enter as Avro
events on Kafka. The design calls for enrichment, governed risk evaluation, position read models, an
immutable evidence archive, and a separate, optional, human-gated LLM investigation layer.

What runs today is the spine of that design: four producers, one consumer, the shared durability and
offset profiles both of them inherit, an authenticated local broker with per-service authorization,
and two build gates that fail rather than warn.

All market data and financial identifiers are synthetic. The platform performs no money movement, no
trade execution and no regulatory reporting.

## Delivery state at a glance

| Area | State |
| --- | --- |
| Event contracts, 16 Avro schemas, 31 generated types | Built |
| Schema compatibility gate, FULL, offline | Built |
| Plane isolation gate | Built |
| Shared producer durability profile | Built |
| Shared consumer offset profile and DLQ publisher | Built |
| Four ingestion producers | Built |
| Audit archival consumer | Built, writing to a logging stand-in rather than to S3 |
| Per-service Kafka policy, authenticated broker, negative authorization tests | Built |
| Service identity binding, each service proven to authenticate as its own principal | Built, against a local broker with ACLs, not against MSK IAM |
| Service container images, one per module, from buildpacks | Built |
| Local stack, both profiles, with observability | Built |
| Deterministic streaming, CDC migration, control plane, agent plane | Not started |
| Throughput and latency evidence | Not measured |

The two rows at the bottom matter as much as the rest. The architecture states a 50,000 events/sec
deterministic target and a sub-200ms risk latency budget. No sustained-throughput run has been done,
so this guide never repeats those numbers as achievements.

## Start here

Pick the route that matches your question.

**You want to know what this is and why it exists.** Read
[What this platform is for](purpose.md), then [Target architecture](architecture.md). Those two pages
describe intent; every other page describes code that exists.

**You want the shape of the thing.** Read [Follow one trade](spine.md), slowly, once. It is the spine:
a single `TradeEvent` from a publisher call to an archived record, and to the dead-letter topic when
the payload cannot be decoded. If only one page makes the platform legible, it is that one.

**You want to know why the layout is what it is.** Read [Five planes](planes.md), then
[Modules and the isolation rule](modules.md). The plane model explains almost every structural
decision in the repository, including one the build enforces.

**You are about to change a schema.** Read [Event contracts](contracts.md) first. Two independent
checks stand between an edited `.avsc` and a running producer, and one of them will fail your build.

**You are adding a service.** Read [Workload authorization](authorization.md). A service is not
complete until it commits a least-privilege policy and proves one allowed path and two denied ones.

**You want to run it.** Read [The local stack](local-stack.md). One script, two profiles, and a
provisioning sequence that Compose alone cannot express.

**You hit something strange.** Read [Gotchas](gotchas.md). Every entry cost someone real time.

## Prerequisites

JDK 25 and a running Docker daemon. The integration tests start real Kafka and Schema Registry
containers, so the first run pulls images and takes a few minutes.

```bash
./gradlew build                    # compile, test, and run every gate
./gradlew test                     # unit tests only, no Docker needed
./gradlew integrationTest          # the Testcontainers-backed tests
./gradlew spotlessApply            # fix formatting the build would reject
./gradlew checkPlaneIsolation      # the plane dependency rule on its own
./gradlew :contracts:test          # the schema compatibility gate on its own
scripts/local-stack.sh up dev      # the local stack, plaintext profile
```

`test` and `integrationTest` split one `src/test` source set by class name, so `./gradlew test` alone
runs without Docker. See [Build gates and CI](gates.md#gate-6-the-tests).

Gradle, not Maven. There is no `mvnw` in this repository.

## How the guide is organised

Each page answers one question and cites the file that answers it. Where a design decision has an ADR,
the ADR number appears next to the claim. The same numbers appear in the source comments, so you can
see from the code itself which decision a given line rests on.

This guide grows with the platform. What it does not cover yet, and the discipline for keeping it
true as the code changes, is in [Maintaining this guide](maintaining.md).

The [Code to proof map](proof.md) is the shortest path from a behaviour you want to trust to the test
that demonstrates it. A useful habit with this codebase: read the implementation, then its focused
unit test, then the integration test that runs it against a real broker. The three layers give you
intent, local behaviour and the broker-shaped boundary in that order.

## The one invariant worth memorising

The agent plane is asynchronous and optional. A model provider outage, throttling, graph
unavailability or budget exhaustion must never block the deterministic path or add an availability
dependency to it. The two planes carry separate service objectives, and the throughput target belongs
to deterministic streaming alone.

The build enforces the structural half of this. No module under `services/ingestion`,
`services/streaming` or `services/audit` may depend on an agent module, on Neo4j, or on an LLM
provider, and `./gradlew checkPlaneIsolation` fails rather than warns.
