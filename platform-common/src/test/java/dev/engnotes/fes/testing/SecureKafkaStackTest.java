package dev.engnotes.fes.testing;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the fixture denies rather than merely fails.
 *
 * <p>Every per-service DENY test rests on one assumption: that an authenticated identity with no
 * grant is refused by the authorizer, and that the refusal is distinguishable from a broker that is
 * unreachable, misconfigured or slow. If an absent grant produced a timeout instead, fifteen denial
 * assertions would pass without proving anything. This test is the discriminator.
 */
@DisplayName("SecureKafkaStack denial semantics")
class SecureKafkaStackTest {

    private static final String TOPIC = "zero-grant-probe";

    @BeforeAll
    static void prepare() {
        SecureKafkaStack.start();
        SecureKafkaStack.createTopics(TOPIC);
    }

    @Test
    @DisplayName("should deny an authenticated identity that holds no grant, with an authorization error")
    void should_deny_an_authenticated_identity_that_holds_no_grant_with_an_authorization_error() {
        // Authenticated as a real service identity, but no ACL names this topic. The broker runs
        // with allow.everyone.if.no.acl.found=false, so absence of a grant is a denial.
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig("trade-producer"))) {
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>(TOPIC, "K", new byte[]{1})).get())
                    .isInstanceOf(ExecutionException.class)
                    .as("a timeout here would mean the denial tests cannot tell denied from broken")
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }
}
