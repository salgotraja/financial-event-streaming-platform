package dev.engnotes.fes.testing;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The binding every shipped service must satisfy: it authenticates as its own principal, and not as
 * an administrator (ADR-031, NFR-05.4).
 *
 * <p>Each service's committed {@code kafka-acls.yml} is already proven by its
 * {@code *AuthorizationTest}, which applies the policy to a real broker and asserts its denials.
 * What that cannot reach is which principal a running service presents. A policy naming
 * {@code User:trade-producer} says nothing about a process that might be holding the
 * administrator's credential, and on this broker the administrator is a super user who passes every
 * ACL at once.
 *
 * <p>The proof is a pair of runs of the service's own image.
 *
 * <p><strong>Ungranted, it must be denied.</strong> A super user is never denied, so a denial rules
 * out the administrator. The wait for the authorization marker has an explicit timeout and the
 * timeout is a failure, because a mis-wired secret never reaches an authorization check at all. The
 * captured output is then checked for {@code SaslAuthenticationException}, as a backstop against
 * exactly that.
 *
 * <p><strong>Granted, it must work.</strong> The module's own policy grants
 * {@code User:<service>} and nothing else, and the broker runs
 * {@code allow.everyone.if.no.acl.found=false}, so no other non-super identity would be un-denied
 * by that one grant. Between them the two runs pin the principal, with no need to read the broker's
 * own logs.
 *
 * <p>Two runs rather than one, because deciding that a failure has stopped inside a live container
 * means reasoning about log offsets. Over a fresh log both assertions are positive. ACLs here are
 * only ever added, so the ungranted-then-granted direction holds within one class.
 *
 * <p>A test that merely asserted a successful publish would pass just as happily for a service
 * running as an administrator, which is precisely the state this exists to rule out.
 *
 * <p><strong>Writing a subclass.</strong> Name it {@code <Service>ServiceIdentityStackTest} so the
 * root build routes it to {@code integrationTest}, which requires Docker and forks one class per
 * JVM. Implement {@link #principal()}, {@link #authorizationErrorMarker()} and
 * {@link #successMarker()}; override {@link #extraEnvironment()} and {@link #prepareBroker()} only
 * as those two document.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ServiceIdentityContract {

    private static final Duration DENIAL_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration SUCCESS_TIMEOUT = Duration.ofMinutes(2);

    /** The workload identity under test, matching the principal in its committed policy. */
    protected abstract String principal();

    /** The image built from this service's own module, as {@code fes/<service>:local}. */
    protected String imageName() {
        return "fes/" + principal() + ":local";
    }

    /**
     * A substring of the exception this service raises when the broker denies it. Named per service
     * rather than as a shared superclass: a producer sees {@code TopicAuthorizationException} and a
     * consumer's group join may surface {@code GroupAuthorizationException} instead.
     *
     * <p>Both markers are used as regular expressions, so one containing a metacharacter must be
     * wrapped in {@link java.util.regex.Pattern#quote}. An exception's simple name never needs it.
     */
    protected abstract String authorizationErrorMarker();

    /**
     * A substring the service logs only after it has actually done its work.
     *
     * <p>Not a startup line. The granted run's wait matches on this, so a marker the service logs
     * before it touches Kafka would pass for a service that never reached the broker at all. Where
     * the only such line is at DEBUG, raise that logger's level through {@link #extraEnvironment()}
     * rather than settling for the startup line.
     */
    protected abstract String successMarker();

    /**
     * Environment beyond the identity, such as a driver gate or a log level.
     *
     * <p>It may not restate anything {@link SecureKafkaStack#serviceEnvironment} already sets: a
     * subclass that could supply its own secret, profile or bootstrap address would be testing its
     * own wiring rather than the service's. Collisions are rejected rather than merged.
     */
    protected Map<String, String> extraEnvironment() {
        return Map.of();
    }

    /**
     * Anything the broker must hold before the service runs: registered Avro subjects for a
     * producer, a seeded record for a consumer. Runs once, before the denied run, and what it
     * creates is still there for the granted one.
     *
     * <p>The two are not interchangeable. {@link SecureKafkaStack#registerSubject} is also the
     * registry container's only lazy start; {@link SecureKafkaStack#serviceEnvironment} hands the
     * service a network alias for it, but nothing else brings the container up. A subclass that
     * only seeds a record without ever registering a subject points its service at an alias with
     * nothing listening behind it.
     */
    protected void prepareBroker() {
        // Overridden by services that need a subject registered or a record to read.
    }

    @Test
    @DisplayName("should be denied while ungranted, then work once its own principal is granted")
    void should_be_denied_while_ungranted_then_work_once_its_own_principal_is_granted() {
        SecureKafkaStack.start();
        KafkaAclPolicy policy = KafkaAclPolicy.load();
        assertThat(policy.principal())
                .as("the test and the committed policy must name the same identity")
                .isEqualTo(principal());

        // Topics exist before the service runs, created by the super user, because a
        // least-privilege identity holds no CREATE and auto-creation would grant a permission the
        // policy denies.
        SecureKafkaStack.createTopics(policy.topics().toArray(String[]::new));
        prepareBroker();

        String deniedOutput = runAndCapture(authorizationErrorMarker(), DENIAL_TIMEOUT);

        // The wait has already matched this. Asserting it again makes the capture's completeness a
        // tested property rather than an assumption, and the three doesNotContain assertions below
        // are only as strong as the log they run over.
        assertThat(deniedOutput)
                .as("the captured log must hold the line the wait matched, or every absence "
                        + "asserted over it is vacuous")
                .contains(authorizationErrorMarker());
        assertThat(deniedOutput)
                .as("a mis-wired secret fails the handshake and never reaches an authorization "
                        + "check, which would make this whole test pass for the wrong reason")
                .doesNotContain("SaslAuthenticationException");
        assertThat(deniedOutput)
                .as("the ungranted run must have been stopped by the broker, not merely have "
                        + "logged an error on its way to succeeding anyway")
                .doesNotContain(successMarker());

        SecureKafkaStack.apply(policy);

        String grantedOutput = runAndCapture(successMarker(), SUCCESS_TIMEOUT);

        assertThat(grantedOutput)
                .as("the captured log must hold the line the wait matched, or every absence "
                        + "asserted over it is vacuous")
                .contains(successMarker());
        assertThat(grantedOutput)
                .as("the only grant applied names User:%s, so work that now succeeds could only "
                        + "have been done by that principal", principal())
                .doesNotContain(authorizationErrorMarker());
    }

    private String runAndCapture(String marker, Duration timeout) {
        if (marker == null || marker.isBlank()) {
            throw new AssertionError("A blank marker matches the first line of startup output, so "
                    + "an unset one would make this test pass on noise. Name a line the service "
                    + "logs only after doing its work.");
        }

        Map<String, String> environment = environment();
        // Only for the failure path. The wait strategy follows its own log stream, so this consumer
        // is not synchronised with it and may still be behind when start() returns.
        ToStringConsumer output = new ToStringConsumer();

        try (GenericContainer<?> service =
                     new GenericContainer<>(DockerImageName.parse(imageName()))
                             .withNetwork(SecureKafkaStack.network())
                             .withEnv(environment)
                             .withLogConsumer(output)
                             // The surrounding .* are load-bearing: the wait strategy full-matches
                             // the line rather than searching it.
                             .waitingFor(Wait.forLogMessage(".*" + marker + ".*", 1)
                                     .withStartupTimeout(timeout))) {
            service.start();
            // A synchronous fetch of the whole log, not the followed stream. Every assertion the
            // caller makes is an absence, and an absence read from a log that lags the wait would
            // pass without being true.
            return service.getLogs();
        } catch (RuntimeException e) {
            // The container is closed by now, so getLogs() is no longer available and the consumer
            // is the only record of what happened. Lagging or not, it is what there is to report.
            throw new AssertionError("The service never logged \"" + marker + "\" within "
                    + timeout + ". A timeout here is a failure, not a pass. Output was:\n"
                    + output.toUtf8String(), e);
        }
    }

    private Map<String, String> environment() {
        Map<String, String> identity = SecureKafkaStack.serviceEnvironment(principal());
        Map<String, String> extra = extraEnvironment();

        TreeSet<String> collisions = new TreeSet<>(identity.keySet());
        collisions.retainAll(extra.keySet());
        if (!collisions.isEmpty()) {
            throw new AssertionError("extraEnvironment() may not restate the identity environment, "
                    + "which would let this test hand the service a credential of its own choosing "
                    + "and prove nothing. Remove " + collisions + " from "
                    + getClass().getSimpleName() + ".");
        }

        Map<String, String> merged = new HashMap<>(identity);
        merged.putAll(extra);
        return merged;
    }
}
