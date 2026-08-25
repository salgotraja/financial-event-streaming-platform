package dev.engnotes.fes.riskalert;

import dev.engnotes.fes.riskalert.governance.RuleTimelineLoader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
}
