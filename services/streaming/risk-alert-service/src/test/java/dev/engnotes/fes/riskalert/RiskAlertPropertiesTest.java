package dev.engnotes.fes.riskalert;

import java.time.Duration;

import dev.engnotes.fes.riskalert.governance.BootstrapRuleProperties;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property binding only, against an explicit minimal configuration rather than the full
 * application context.
 *
 * <p>{@link RiskAlertKafkaConfiguration} adds a blocking {@code SmartInitializingSingleton} that
 * folds {@code risk-rules.events} from a real broker before the context is allowed to finish
 * refreshing (Task 7). A full {@code @SpringBootTest} would component-scan that class and hang or
 * fail on a metadata timeout here, where no broker runs. Naming
 * {@link PropertiesOnlyConfiguration} as the only configuration class keeps
 * {@link RiskAlertKafkaConfiguration} out of this context entirely, while {@code @SpringBootTest}
 * still bootstraps through {@code SpringApplication}, so {@code application.yml} on the classpath
 * is still processed and these three assertions still prove real property binding.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        classes = RiskAlertPropertiesTest.PropertiesOnlyConfiguration.class,
        properties = {
                "management.otlp.metrics.export.enabled=false",
                "management.otlp.tracing.export.enabled=false"
        })
@ActiveProfiles("test")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RiskAlertPropertiesTest {

    @Autowired
    RiskAlertProperties properties;

    @Autowired
    BootstrapRuleProperties bootstrapRuleProperties;

    @Test
    void the_topics_bind_from_application_yml() {
        assertThat(properties.topic()).isEqualTo("trades.enriched");
        assertThat(properties.ruleTopic()).isEqualTo("risk-rules.events");
        assertThat(properties.outputTopic()).isEqualTo("notifications.alerts");
    }

    @Test
    void the_fold_timeout_binds_as_a_duration() {
        assertThat(properties.ruleTimelineTimeout()).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void the_volume_window_horizon_binds_from_application_yml() {
        assertThat(properties.volumeWindowSeconds()).isEqualTo(3_600L);
    }

    @Test
    void the_recent_trade_horizon_and_cap_bind_from_application_yml() {
        assertThat(properties.recentTradeHorizonSeconds()).isEqualTo(3_600L);
        assertThat(properties.recentTradeCandidateCap()).isEqualTo(200);
    }

    @Test
    void the_bootstrap_rule_set_binds_from_application_yml() {
        assertThat(bootstrapRuleProperties.rules()).hasSize(4);

        assertThat(bootstrapRuleProperties.rules())
                .filteredOn(rule -> rule.ruleId().equals("price-deviation"))
                .singleElement()
                .satisfies(rule -> {
                    assertThat(rule.ruleType()).isEqualTo("price-deviation");
                    assertThat(rule.parameters())
                            .containsEntry("warn-deviation-percent", "2.0")
                            .containsEntry("critical-deviation-percent", "5.0");
                });

        assertThat(bootstrapRuleProperties.rules())
                .filteredOn(rule -> rule.ruleId().equals("position-limit"))
                .singleElement()
                .satisfies(rule -> {
                    assertThat(rule.ruleType()).isEqualTo("position-limit");
                    assertThat(rule.parameters())
                            .containsEntry("warn-position-quantity", "10000")
                            .containsEntry("critical-position-quantity", "50000");
                });

        assertThat(bootstrapRuleProperties.rules())
                .filteredOn(rule -> rule.ruleId().equals("unusual-volume"))
                .singleElement()
                .satisfies(rule -> {
                    assertThat(rule.ruleType()).isEqualTo("unusual-volume");
                    assertThat(rule.parameters())
                            .containsEntry("warn-sigma-multiplier", "3.0")
                            .containsEntry("critical-sigma-multiplier", "5.0")
                            .containsEntry("min-sample-count", "30");
                });

        assertThat(bootstrapRuleProperties.rules())
                .filteredOn(rule -> rule.ruleId().equals("self-cross"))
                .singleElement()
                .satisfies(rule -> {
                    assertThat(rule.ruleType()).isEqualTo("self-cross");
                    assertThat(rule.parameters())
                            .containsEntry("window-seconds", "300")
                            .containsEntry("quantity-tolerance-percent", "1.0")
                            .containsEntry("price-tolerance-percent", "1.0");
                });
    }

    @Test
    void a_candidate_cap_of_zero_is_rejected_rather_than_silencing_the_self_cross_rule() {
        // The store asks for cap + 1 rows so truncation is detectable. A cap of zero therefore
        // finds one row, calls the set truncated, and hands the rule nothing: the rule stops
        // alerting and says so only in a counter.
        assertThatThrownBy(() -> new RiskAlertProperties("t", "r", "o", "i",
                Duration.ofSeconds(60), 3_600L, 3_600L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recent-trade-candidate-cap");
    }

    @Test
    void a_non_positive_window_horizon_is_rejected() {
        assertThatThrownBy(() -> new RiskAlertProperties("t", "r", "o", "i",
                Duration.ofSeconds(60), 0L, 3_600L, 200))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("volume-window-seconds");

        assertThatThrownBy(() -> new RiskAlertProperties("t", "r", "o", "i",
                Duration.ofSeconds(60), 3_600L, -1L, 200))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recent-trade-horizon-seconds");
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({RiskAlertProperties.class, BootstrapRuleProperties.class})
    static class PropertiesOnlyConfiguration {
    }
}
