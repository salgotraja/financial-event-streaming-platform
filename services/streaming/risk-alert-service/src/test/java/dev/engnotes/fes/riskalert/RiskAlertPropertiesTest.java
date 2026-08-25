package dev.engnotes.fes.riskalert;

import java.time.Duration;

import dev.engnotes.fes.riskalert.governance.BootstrapRuleProperties;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
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
    void the_bootstrap_rule_set_binds_from_application_yml() {
        assertThat(bootstrapRuleProperties.rules()).singleElement()
                .satisfies(rule -> {
                    assertThat(rule.ruleId()).isEqualTo("price-deviation");
                    assertThat(rule.ruleType()).isEqualTo("price-deviation");
                    assertThat(rule.parameters())
                            .containsEntry("warn-deviation-percent", "2.0")
                            .containsEntry("critical-deviation-percent", "5.0");
                });
    }
}
