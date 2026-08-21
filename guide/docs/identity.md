# Service identity

[Workload authorization](authorization.md) proves what a principal named `trade-producer` is allowed
to do. This page is about the other half: proving that the process calling itself `trade-producer`
really is that principal.

The two are easy to conflate and are not the same claim. A policy file naming
`User:trade-producer` says nothing about which credential a running container presents. Hand that
container an administrator's secret and every functional test in this repository still passes, while
every ACL is defeated at once, because the administrator is a super user the broker never denies.

## The identity is derived, never configured

`platform-common/src/main/java/dev/engnotes/fes/common/kafka/KafkaSecurityConfiguration.java`
builds the SASL login module for the producer and consumer factories Spring Boot autoconfigures, and
the username in it is `spring.application.name`. A Kafka Streams service will need the equivalent
applied to its `StreamsBuilderFactoryBean` once one exists:

```java
@Configuration(proxyBeanMethods = false)
@Profile("secure-kafka")
public class KafkaSecurityConfiguration {

    KafkaSecurityConfiguration(
            @Value("${spring.application.name}") String applicationName,
            @Value("${fes.kafka.sasl-secret}") String secret,
            @Value("${fes.kafka.security-protocol:SASL_PLAINTEXT}") String securityProtocol) {
        this.saslProfile = saslProfile(applicationName, secret, securityProtocol);
    }
```

No service module holds a property that names an identity. The secret, the bootstrap servers and the
security protocol all come from the environment, because those are facts about a deployment. The
identity is not, so it is not configurable (ADR-031).

This is the shape `ProducerDurabilityConfiguration` already takes, for the same reason: what the
platform must guarantee is imposed by shared code, not restated in five service configurations that
are each free to drift.

Two tests hold the rule up rather than merely stating it:

- `should_override_a_jaas_config_a_service_tried_to_set_for_itself` puts a JAAS config naming `admin`
  into `spring.kafka.properties` and asserts the derivation still wins. Without it the derivation
  would be advice rather than a control.
- `should_not_let_a_module_name_a_sasl_username_of_its_own` walks each service module's `src/main`
  and fails if any file mentions a JAAS config at all.

The configuration is active only under the `secure-kafka` profile, so a developer running against a
plaintext broker is not forced to authenticate to something that would not understand it.

## Three names have to agree

The chain has three links, and each lives somewhere different:

| Link | Where it lives | Used for |
| --- | --- | --- |
| Gradle module name | `settings.gradle` | the image, as `fes/<module>:local` |
| `spring.application.name` | the module's `application.yml` | the derived SASL username |
| `SecureKafkaStack.PRINCIPALS` entry | `platform-common` test fixtures | the broker credential and the ACL grant |

A mismatch between any two breaks the binding in a way no module-local test can see, because each
module knows only its own link. `should_name_one_module_one_application_name_and_one_policy_principal_alike`
is a parameterized test over every shipped principal that checks all three agree.

Because that test reads files in other modules, `platform-common/build.gradle` declares them as task
inputs. Without that, Gradle reports `:platform-common:test` as up to date when a service's
`application.yml` changes, and the check quietly stops running. A gate the build skips is not a gate.

## The proof: denied, then granted

Each service module carries a `ServiceIdentityStackTest` extending
`platform-common/src/testFixtures/java/dev/engnotes/fes/testing/ServiceIdentityContract.java`. A
subclass implements three values, and overrides two more only where the service needs them:

```java
class TradeProducerServiceIdentityStackTest extends ServiceIdentityContract {

    protected String principal()                { return "trade-producer"; }
    protected String authorizationErrorMarker() { return "TopicAuthorizationException"; }
    protected String successMarker()            { return "Published trade"; }

    protected Map<String, String> extraEnvironment() { ... }   // driver gate, log level
    protected void prepareBroker() { ... }                     // register the Avro subject
}
```

The contract runs the service's own image twice against the secure broker.

**Ungranted, it must be denied.** No ACL is applied for the principal. The service starts, tries to
work, and must log an authorization error. A super user is never denied, so a denial rules out the
administrator.

**Granted, it must work.** The module's own committed `kafka-acls.yml` is applied, granting
`User:<service>` and nothing else. A second container of the same image runs, and must log the line
it only writes after a successful publish.

Between them the two runs pin the principal, with no need to read the broker's own logs. The broker
runs `allow.everyone.if.no.acl.found=false`, so no other non-super identity would be un-denied by
that one grant.

Two container runs rather than one because deciding that a failure has *stopped* inside a live
container means reasoning about log offsets. Over a fresh log both assertions are positive.

## Why the obvious version of this test is worthless

A test that just asserted a successful publish would pass exactly as happily for a service running as
the administrator, which is the state this whole mechanism exists to rule out. The denial is the
load-bearing half.

Three further details are there to stop the test passing for the wrong reason.

**A timeout is a failure, not a pass.** A mis-wired secret never reaches an authorization check at
all, so "the service failed" is not enough. The denied run waits for a specific exception name, and
if it never arrives the test fails with the captured output attached.

**The captured output must not contain `SaslAuthenticationException`.** That is the backstop against
the same case, in the event the service fails the handshake in a way that still produces output.

**The log is read synchronously.** The wait strategy and the log consumer are separate Docker
follow-streams, so the accumulated consumer string can lag behind what the wait already matched. The
contract calls `getLogs()` instead, and asserts positively that each captured log holds the marker it
waited for, which makes the completeness of the capture a tested property rather than an assumption.

## Watching it fail

A proof nobody has watched fail is a proof nobody has verified. Hardcoding `admin` and its secret
into `KafkaSecurityConfiguration` in place of the derivation makes `TradeProducerServiceIdentityStackTest`
fail, and the shape of the failure is the point: the denied run times out having logged 11,880
`Published trade` lines and zero authentication errors. That is a service publishing happily as a
super user, which is exactly the state the test exists to catch.

## The broker the test runs against

`SecureKafkaStack` gained an authenticated in-network listener so a service container can reach it:

```text
kafka:19092    INTERNAL     SASL_PLAINTEXT   in-network, for service containers
<host>:<port>  PLAINTEXT    SASL_PLAINTEXT   host-mapped, for the authorization tests
127.0.0.1:9093 BROKER       PLAINTEXT        inter-broker, loopback only
127.0.0.1:9094 CONTROLLER   PLAINTEXT        KRaft quorum, loopback only
```

The last two lines matter more than they look. `ANONYMOUS` is a super user on this broker, because
the broker's own inter-broker traffic authenticates as nobody. That was harmless while the container
sat on no shared network. Once it joined one so services could reach it, any sibling container could
have dialled `kafka:9093`, authenticated as `ANONYMOUS`, and bypassed every ACL, which would have
made every denial assertion in the repository pass without being true. Binding those two listeners to
loopback closes it, and `should_not_expose_the_inter_broker_listener_to_a_client_on_the_network`
asserts a client on the network cannot reach them at all, rather than merely being denied.

Testcontainers' `KafkaContainer.withListener` is the obvious API here and is a trap: it names the
listener `TC-0` and maps it to plain `PLAINTEXT` unconditionally. The fixture configures the listener
by hand instead. See [Gotchas](gotchas.md).

The four Avro producers serialise against a Schema Registry before a byte reaches the broker, and
they ship `auto.register.schemas: false`, so the fixture also runs a registry on the same network and
registers each subject through its REST API. Overriding the property in the test would have meant the
artifact under test was no longer the artifact that ships.

## The images

Images come from `bootBuildImage`, the Spring Boot plugin's Paketo integration. There is no
Dockerfile. The root `build.gradle` names them and makes the tests depend on them:

```groovy
plugins.withId('org.springframework.boot') {
    tasks.named('bootBuildImage') {
        imageName = "fes/${project.name}:local"
    }
    tasks.named('integrationTest') {
        dependsOn tasks.named('bootBuildImage')
    }
}
```

That dependency is what stops the identity proof running against a stale or absent image.

## What this does not prove

The strict-security compose profile still does not run the services themselves. This makes the
binding provable in CI; showing the whole stack running live to a developer is separate work.

Cloud deployments authorise with MSK IAM rather than ACLs (ADR-009), and nothing here touches that
path.
