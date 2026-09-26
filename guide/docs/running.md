# Running a service

Every service runs from Gradle or an IDE against [the local stack](local-stack.md), under a Spring
profile named `dev`. The profile points Kafka at the stack's brokers, gives the service a port of its
own, and turns on the API docs, with the actuator endpoints included in them and Scalar's telemetry
switched off.

```bash
scripts/local-stack.sh up dev
SPRING_PROFILES_ACTIVE=dev ./gradlew :services:streaming:risk-alert-service:bootRun
```

The environment variable is the form to prefer, because it is also what an IDE run configuration
takes. On the Gradle command line the profile has to go through `--args`; a bare
`--spring.profiles.active=dev` is rejected by Gradle, not by Spring:

```bash
./gradlew :services:streaming:risk-alert-service:bootRun --args='--spring.profiles.active=dev'
```

The `dev` profile targets `scripts/local-stack.sh up dev` only. The strict-security stack keeps the same
broker ports but requires SASL_SSL, and the profile sets no client security, so a service started this
way against it fails its handshake.

## Every service

| Service | Command | Port |
| --- | --- | --- |
| trade-producer | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:ingestion:trade-producer:bootRun` | 8091 |
| market-data-simulator | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:ingestion:market-data-simulator:bootRun` | 8092 |
| corporate-action-producer | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:ingestion:corporate-action-producer:bootRun` | 8093 |
| reference-data-service | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:ingestion:reference-data-service:bootRun` | 8094 |
| market-data-cache-projector | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:streaming:market-data-cache-projector:bootRun` | 8095 |
| trade-enrichment-service | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:streaming:trade-enrichment-service:bootRun` | 8096 |
| risk-alert-service | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:streaming:risk-alert-service:bootRun` | 8097 |
| position-exposure-service | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:streaming:position-exposure-service:bootRun` | 8098 |
| audit-service | `SPRING_PROFILES_ACTIVE=dev ./gradlew :services:audit:audit-service:bootRun` | 8099 |

The ports start at 8091 because Kafka UI holds 8080 and Schema Registry 8081. `ApiDocsProfileTest` in
`platform-common` fails the build if two services share a port, or if a service is added without the
profile.

Run one per terminal. Two Gradle runs in the same checkout share one build directory and have failed
against each other here, so to run several at once, build the jars with `./gradlew bootJar` and start each with
`SPRING_PROFILES_ACTIVE=dev java -jar services/<plane>/<service>/build/libs/<service>-0.0.1-SNAPSHOT.jar`.

## What each one needs from the stack

Every service needs the brokers and Schema Registry. Three need a store as well, and each finds it on
the default port without configuration:

| Service | Store | Default | Override |
| --- | --- | --- | --- |
| market-data-cache-projector | Redis | `localhost:6379` | `SPRING_DATA_REDIS_PORT` |
| trade-enrichment-service | Redis | `localhost:6379` | `SPRING_DATA_REDIS_PORT` |
| risk-alert-service | PostgreSQL | `localhost:5432/risk_alert` | `RISK_ALERT_DB_URL` |
| position-exposure-service | PostgreSQL | `localhost:5432/position_exposure` | `POSITION_EXPOSURE_DB_URL` |

The overrides are for a machine where something else already holds 5432 or 6379. The
`position_exposure` database exists only on a PostgreSQL volume created after its init script landed;
[the local stack](local-stack.md#postgresql-the-first-store-of-record) explains why an older volume
lacks it, and the service then fails at startup with a password authentication error for its role.

The producers publish nothing by default. Each one's generation or seeding mode is off unless its
flag is set: `FES_TRADE_PRODUCER_GENERATION_ENABLED`, `MARKET_DATA_GENERATION_ENABLED`,
`FES_CORPORATE_ACTION_PRODUCER_SEED_ENABLED` and `REFERENCE_DATA_SEED_ENABLED`, each `true` to turn
it on.

## Telling a running service from a connected one

`/actuator/health/readiness` reporting `UP` means the application context started. No service adds a
Kafka health indicator of its own, so what that proves about the broker differs by service.

trade-enrichment-service and risk-alert-service load state from Kafka before their context finishes
starting, so for those two `UP` does mean the brokers answered. Pointed at the wrong address, they
never become ready and exit after 60 seconds with `Timeout expired while fetching topic metadata`.
That 60 seconds is the Kafka client's `default.api.timeout.ms` on the partition lookup, not the
service's own load timeout, which governs only the catch-up read that follows it.

For every other service `UP` says nothing about Kafka. The consumers keep retrying in the background,
and the producers do not contact a broker until they send their first record. Check the log for
`Connection to node -1 ... could not be established` at WARN, and check Kafka UI at
`http://localhost:8080` for the consumer group or the record you expected.

## API docs

Under the `dev` profile each service serves its OpenAPI document and a
[Scalar](https://scalar.com) UI over it:

```text
http://localhost:<port>/scalar          the UI
http://localhost:<port>/v3/api-docs     the OpenAPI JSON
```

No service has a REST controller yet, so today the document lists only the actuator endpoints
(`/actuator`, `/actuator/health`, `/actuator/info`, `/actuator/prometheus`). A service's real contract
is the Avro schemas it consumes and produces, in [Event contracts](contracts.md) and
[Topics and schemas](topics.md). The UI is in place so that the first controller shows up in it with
no further wiring.

Without the profile, both paths return 404. The base configuration sets `springdoc.api-docs.enabled`
and `scalar.enabled` to `false`, so a deployed service does not publish its API surface unless someone
decides it should. Scalar's telemetry is switched off in the profile too.

The dependency is `springdoc-openapi-starter-webmvc-scalar`, pinned in `gradle/libs.versions.toml`.
It also brings `scalar-webmvc`, which carries a `/scalar` controller of its own; springdoc excludes that
one, and `ApiDocsExposureTest` in `trade-producer` asserts that exactly one handler, springdoc's, owns
the path.
