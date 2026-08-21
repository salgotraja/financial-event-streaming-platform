# Gotchas

Every entry cost someone real time. Each one names where the behaviour lives so you can check it
rather than take it on trust.

## The native image cannot act as a SASL server

`KafkaAvroStack` uses `apache/kafka-native:4.1.0` because it starts faster. `SecureKafkaStack` pins
the JVM image `apache/kafka:4.1.0` instead, and so do both Compose profiles.

This is not an oversight to align. Under GraalVM, `SaslServerAuthenticator.createSaslServer` fails to
load the `Subject` methods it needs, and the client sees only a connection dropped mid-authentication.
There is no error that names the cause.

Two broker images in one test run is the cost of authenticated local tests.

## `allow.everyone.if.no.acl.found` defaults to true

Kafka's default is permissive. With it left alone, an identity holding no ACL is allowed everything,
and every denial assertion in the suite passes while proving nothing.

Both the fixture and the strict-security profile set it to `false`. `SecureKafkaStackTest` exists to
prove the resulting refusal is an authorization error and not a timeout.

## A denial that arrives as a timeout looks like a pass

An authorization failure that surfaces as a hang is indistinguishable, in a test, from a broker that
is slow or unreachable. Two things guard against it.

`SecureKafkaStack.producerConfig` sets `max.block.ms` to a bounded 20 seconds, so a denial that
arrives as metadata failure fails fast instead of hanging.

The read-denial test uses `assign` rather than `subscribe`. Subscribing needs a consumer group, and an
identity with no group grant is refused for the group before the broker considers the topic. Which of
the two errors surfaces first is a race, so the test removes the group from the picture entirely by
leaving `group.id` unset.

## `SecureKafkaStack` grants are additive, so one authorization class per JVM

`apply` only ever creates ACLs. The container is static and never reset.

Gradle forks a test JVM per module, so today each module's single authorization class gets a clean
broker. Put two such classes in one JVM and the second inherits the first's grants, which turns a
denial assertion into a pass without failing anything. If a module ever needs two, add a revoke step
rather than assuming isolation.

## Kafka's resource flags are hyphenated where the enum name is not

`kafka-acls.sh` takes `--transactional-id`, while the `ResourceType` enum constant is
`TRANSACTIONAL_ID`. Lowercasing alone emits a flag the CLI rejects.

```java
return resourceType.name().toLowerCase().replace('_', '-');
```

The current policies only use `TOPIC` and `GROUP`, so this would have broken the first time someone
added a policy for a resource type nobody had used yet, in someone else's session. Fixed in `8fcf6bf`.

## macOS ships bash 3.2, where an empty array under `set -u` is an error

```bash
extra=()
"${extra[@]}"          # unbound variable, bash 3.2, set -u
${extra[@]+"${extra[@]}"}   # correct
```

This made every `dev`-profile call in `local-stack.sh` fail while `strict-security` worked, because
only the secure profile populated the array.

## Switching stack profiles needs `destroy` first

The two profiles differ in listener security protocol, so a broker whose data directory was formatted
under one will not come up under the other. `local-stack.sh` records the active profile in
`.active-profile` and refuses to start the wrong one:

```console
error: the dev stack is provisioned; the profiles differ in listener security protocol.
Run: scripts/local-stack.sh destroy
```

## Topic auto-creation quietly produces single-partition topics

`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`. Without it, a producer writing to an unprovisioned topic
gets a one-partition topic that works perfectly and caps throughput at one consumer instance. Nothing
fails; the ceiling just arrives much later and with no obvious cause.

## "Already exists" and "failed" are not the same outcome

`kafka-topics.sh --create --if-not-exists` returns success in both cases, so a script that counts
non-error exits reports topics it never created. `local-stack.sh` inspects the output for `Created`
and dies on a non-zero status:

```bash
if [ $status -ne 0 ]; then
  die "could not create topic $name: $output"
fi
```

The comment in the script records the failure: it once reported 22 topics present against a broker
that had none.

## An empty stdin makes the console producer exit successfully

The least-privilege probe has to actually send a record. `echo probe |` into
`kafka-console-producer.sh` sends one; an empty stdin makes the producer exit zero without ever
contacting the topic, which looks like a pass and proves nothing.

The same probe checks the ALLOW half first, because a denial alone cannot distinguish an enforced
policy from a client that cannot connect at all.

## A missing registry subject fails at the first record, not at startup

Production runs `auto.register.schemas=false`. A topic whose subject was never provisioned looks
healthy until something publishes to it.

That is exactly how the audit service's dead-letter path stayed broken: the four `{topic}.dlq`
subjects were absent, so quarantine would have failed on the first poison record. They are now rows in
`deploy/compose/subjects.tsv`.

## A schema that embeds another must register as a reference

`EnrichedTradeEvent` embeds `TradeEvent`. Registering it with the type inlined gives the registry two
definitions of one record, which then drift independently.

```text
trades.enriched	EnrichedTradeEvent.avsc	dev.engnotes.fes.events.TradeEvent=trades.raw-value
```

Referenced subjects must be listed first in `subjects.tsv`, because registration resolves the
reference to a concrete version at the time the referring subject is registered.

## Avro `stringType` changes your call sites

`contracts/build.gradle` sets `stringType = 'String'`. The generated accessors still return
`CharSequence` in places, which is why `TradeEventPublisher` writes `trade.getTicker().toString()`.
Treat a generated accessor's return type as something to check rather than assume.

## Marking a record processed before the write completes drops it

```java
sink.write(archived);
recentlyArchived.add(archived.idempotencyKey());
```

Reversed, a failed write looks like a completed one to the retry that follows, and the record is
dropped rather than quarantined. Nothing fails anywhere. This is the general shape of the bug in any
deduplicate-then-process consumer.

## `.dlq`, not `.DLT`

Spring Kafka's default dead-letter suffix is `.DLT`. This platform uses lowercase `.dlq`, derived in
code from the source topic name. A service that configures its own name ships a topic no replay tool
looks at.

## The OTel Collector configuration in the spec no longer starts

The specification this stack was written against names the `jaeger` and `loki` exporters. Both were
removed upstream: Jaeger takes OTLP directly now, and Loki ingests OTLP over HTTP at `/otlp`.

`deploy/compose/observability/otel-collector.yaml` therefore departs from the specification on purpose,
and says so at the top of the file. Traces go to the debug exporter, because FR-09.1 names no trace
backend.

## A test that reads the wall clock is a test that fails at 3am

Every time-dependent component takes an injected `Clock`. `TickGenerator`, `FailureTracker` and
`DeadLetterPublisher` all do.

The related one, from `50684bc`: a test asserting that a second `start()` was ignored has to wait for
at least one tick first, or it asserts against a driver that had not produced anything yet either way.

## Configuring a rate is not measuring one

`market-data-simulator` has a configurable `rate-per-second`. Nothing in the repository evidences the
FR-01.4 ceiling or NFR-01.1, because no sustained-throughput run has been done. A configured number in
a YAML file is not a result.

## The current LocalStack image will not start without a licence

`localstack/localstack` moved to a unified image in March 2026. From that tag onward the container
requires `LOCALSTACK_AUTH_TOKEN` and exits with code 55 when it does not find one, which surfaces
through Testcontainers as `Wait strategy failed. Container exited with code 55`, with nothing about
licensing in the Java stack trace.

`4.14.0` is the last community tag. Both the compose stack and `LocalStackFixture` pin it, and the
reason is written beside each pin so a routine version bump does not quietly make the test suite
require a paid account.

## Turning on the configuration cache found a task reaching across projects

`renderKafkaAcls` used to sit in the root build and take its classpath from
`project(':platform-common').sourceSets.testFixtures.runtimeClasspath`. That works until the
configuration cache is enabled, and then it fails at cache-write time:

```text
Resolution of the configuration ':platform-common:testFixturesRuntimeClasspath' was attempted
without an exclusive lock. This is unsafe and not allowed.
```

Reading another project's source set resolves that project's configuration without the lock Gradle
holds for it. The task now lives in `platform-common/build.gradle`, where the classpath is its own, and
writes to the root build directory where `scripts/local-stack.sh` reads it. Running
`./gradlew renderKafkaAcls` from the root still works, because an unqualified task name matches in
every project that defines it.

## Spotless's default unused-import engine cannot read this toolchain

`removeUnusedImports()` defaults to the google-java-format engine, which reaches into `jdk.compiler`
internals that are not open here. The first module to run it throws `ExceptionInInitializerError`, and
every module after it reports the misleading
`NoClassDefFoundError: com/google/googlejavaformat/java/RemoveUnusedImports` against every file it was
asked to check.

`removeUnusedImports('cleanthat-javaparser-unnecessaryimport')` parses the source directly and needs no
compiler internals.

## Testcontainers' `withListener` silently makes a Kafka listener plaintext

Adding an in-network listener to the secure broker looks like a one-liner:
`KafkaContainer.withListener("kafka:19092")`. It is not. That method names the listener `TC-0` and
writes `TC-0:PLAINTEXT` into the security protocol map unconditionally, at container start, after any
value you set yourself.

On this fixture that would have been silently catastrophic. `ANONYMOUS` is a super user, so a service
container reaching a plaintext listener authenticates as nobody, passes every ACL, and every denial
assertion in the repository keeps passing while proving nothing. `SecureKafkaStack` configures
`KAFKA_LISTENERS`, the protocol map and the advertised listeners by hand instead, and overrides
`containerIsStarting` rather than calling `super`, because the superclass writes a starter script that
drops the in-network advertised entry.

The same reasoning applies to the listeners that were already there. `BROKER` and `CONTROLLER` are
plaintext by necessity, and were harmless while the broker sat on no shared network. The moment it
joined one, they became a way in, so both are bound to `127.0.0.1` and a test asserts a client on the
network cannot reach them.

## A Testcontainers log consumer can lag the wait strategy that just matched

`Wait.forLogMessage` attaches its own consumer on a separate Docker follow-stream from the one
`withLogConsumer` feeds. When `start()` returns, the accumulated `ToStringConsumer` string is not
guaranteed to contain the line the wait just matched, let alone anything after it.

That turned a mandated negative assertion into one that could pass on an empty string. The identity
contract now reads `getLogs()`, a synchronous non-follow fetch, while the container is still up, and
keeps the consumer only for the failure path where the container has already closed. It also asserts
positively that each captured log contains the marker it waited for, so a regression to a lagging
capture fails rather than going quiet again.

## A Gradle test task does not know about files the test reads from other modules

`ServiceIdentityNamesTest` lives in `platform-common` and reads every service module's
`application.yml` and `kafka-acls.yml` to check the three names agree. Gradle's up-to-date check
tracks the module's own classpath and resources, not arbitrary runtime file reads, so breaking the
name chain in a service and running `./gradlew build` reported `:platform-common:test` as UP-TO-DATE
and caught nothing.

`platform-common/build.gradle` now declares those files as task inputs. Worth remembering for any
check that reads across module boundaries: the test being correct is not the same as the build running
it.
