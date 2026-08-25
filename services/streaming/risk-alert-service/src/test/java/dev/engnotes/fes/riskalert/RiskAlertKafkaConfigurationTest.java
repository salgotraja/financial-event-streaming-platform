package dev.engnotes.fes.riskalert;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.common.kafka.FailureTracker;
import dev.engnotes.fes.riskalert.governance.RuleTimelineLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A direct, broker-free proof that {@link RiskAlertKafkaConfiguration#openTheReadinessGate} is
 * load-bearing.
 *
 * <p>{@code RuleTimelineLoaderIntegrationTest} cannot prove this on its own: it constructs a
 * {@link RuleTimelineLoader} directly and calls {@code loadInitialSnapshot()} itself, in the test
 * thread, so it never touches {@link RiskAlertKafkaConfiguration} or a Spring context at all.
 * Deleting {@code openTheReadinessGate} has no effect on it (this was confirmed by running the
 * Step 7 probe from the Task 7 brief and observing no failure, see the Task 7 fix-round-1 report).
 *
 * <p>This test calls the real {@link RiskAlertKafkaConfiguration#openTheReadinessGate} method,
 * wraps it as the only {@code @Bean} in a minimal {@link AnnotationConfigApplicationContext}, and
 * asserts that refreshing that context actually invokes {@code loadInitialSnapshot()} on the
 * injected {@link RuleTimelineLoader}, mocked here so no broker is needed. The real
 * {@code ruleTimelineLoader(...)} factory bean, which builds a hand-rolled {@code KafkaConsumer}
 * and would need a broker to construct meaningfully, is never registered; only the gate method
 * itself is exercised, matching the same broker-free, direct-mechanism style as
 * {@code RawTradeConsumerGroupIdTest} in {@code trade-enrichment-service}.
 */
@DisplayName("RiskAlertKafkaConfiguration.openTheReadinessGate")
class RiskAlertKafkaConfigurationTest {

    @Test
    @DisplayName("should call loadInitialSnapshot on the loader during context refresh")
    void should_call_load_initial_snapshot_on_the_loader_during_context_refresh() {
        RuleTimelineLoader mockLoader = mock(RuleTimelineLoader.class);

        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean(RuleTimelineLoader.class, () -> mockLoader);
            context.register(GateOnlyConfiguration.class);
            context.refresh();
        }

        verify(mockLoader).loadInitialSnapshot();
    }

    @Configuration(proxyBeanMethods = false)
    static class GateOnlyConfiguration {

        // The real production instance, not a reimplementation: this is what makes the Step 7
        // probe (deleting openTheReadinessGate and expecting this test to fail) meaningful. A
        // locally rewritten copy of the bean's body would keep passing after the real method was
        // deleted, exactly the flaw found in RuleTimelineLoaderIntegrationTest.
        private final RiskAlertKafkaConfiguration delegate = new RiskAlertKafkaConfiguration();

        @Bean
        SmartInitializingSingleton openTheReadinessGate(RuleTimelineLoader loader) {
            return delegate.openTheReadinessGate(loader);
        }
    }

    /**
     * The two failure classes ADR-027 separates, wired for {@code trades.enriched}. This service
     * calls no external datastore, so unlike {@code EnrichmentKafkaConfiguration} there is no
     * dependency-outage branch and no {@code ContainerPausingBackOffHandler}: the error handler
     * takes three arguments, not five.
     */
    @Nested
    @DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
    class RiskAlertErrorHandlerTest {

        private final DeadLetterPublisher deadLetterPublisher = mock(DeadLetterPublisher.class);
        private final RiskAlertMetrics metrics = mock(RiskAlertMetrics.class);

        private DefaultErrorHandler errorHandler() {
            when(deadLetterPublisher.publish(any(), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            return new RiskAlertKafkaConfiguration().riskAlertErrorHandler(
                    deadLetterPublisher, new FailureTracker(), metrics);
        }

        @Test
        void a_deserialization_failure_is_not_retried() {
            // The bytes do not decode differently on a second attempt, so retrying only delays the
            // quarantine and holds up the partition behind it.
            assertThat(errorHandler().removeClassification(DeserializationException.class))
                    .isFalse();
        }

        @Test
        void an_invalid_argument_is_not_retried() {
            // A non-finite priceDeviation is a verdict on the payload and it will not change.
            assertThat(errorHandler().removeClassification(IllegalArgumentException.class))
                    .isFalse();
        }

        @Test
        void the_recovered_records_offset_is_acknowledged_so_the_partition_keeps_moving() {
            // Per record quarantine (ADR-027). Without this one poison payload blocks every record
            // behind it on the same partition.
            assertThat(errorHandler().isAckAfterHandle()).isTrue();
        }

        @Test
        void the_quarantined_payload_comes_from_the_exception_not_the_null_record_value() {
            byte[] delivered = {0, 0, 0, 0, 1, 42};
            ConsumerRecord<String, Object> failed =
                    new ConsumerRecord<>("trades.enriched", 0, 7L, "RELIANCE", null);
            DeserializationException cause = new DeserializationException(
                    "cannot decode", delivered, false, new IllegalStateException("bad bytes"));

            errorHandler().handleRemaining(cause, List.of(failed), mock(
                    org.apache.kafka.clients.consumer.Consumer.class), mock(
                    org.springframework.kafka.listener.MessageListenerContainer.class));

            // ErrorHandlingDeserializer nulls the record value and carries the delivered bytes on
            // the exception, and DeadLetterPublisher substitutes an empty array for a null payload.
            // Passing record.value() through would quarantine nothing at all and lose the only
            // evidence there is of what was actually published.
            ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
            verify(deadLetterPublisher).publish(any(), payload.capture(), any());
            assertThat(payload.getValue()).isEqualTo(delivered);
        }
    }
}
