package dev.engnotes.fes.riskalert;

import java.util.List;
import javax.sql.DataSource;

import dev.engnotes.fes.testing.KafkaAvroStack;
import dev.engnotes.fes.testing.PostgresStack;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Flyway migration applies against the same PostgreSQL the deployed system runs, not H2.
 *
 * <p>Kafka is started too, because {@code @SpringBootTest} brings up the whole context including
 * the rule-fold readiness gate, which polls a real broker before the context reports ready.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
class RiskWindowSchemaIntegrationTest {

    private static final String RULE_TOPIC = "ras-schema-rules-" + java.util.UUID.randomUUID();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        PostgresStack.start();
        // RuleTimelineLoader assigns partitions directly rather than subscribing, so the rule topic
        // must exist before the context starts.
        RiskAlertTestKafka.createTopic(RULE_TOPIC, 6);
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("fes.risk-alert-service.rule-topic", () -> RULE_TOPIC);
        registry.add("spring.datasource.url", PostgresStack::jdbcUrl);
        registry.add("spring.datasource.username", PostgresStack::username);
        registry.add("spring.datasource.password", PostgresStack::password);
    }

    @Autowired
    private DataSource dataSource;

    @Test
    void v2_creates_the_four_window_and_correlation_tables_in_the_risk_alert_schema() {
        JdbcClient jdbc = JdbcClient.create(dataSource);

        List<String> tables = jdbc.sql("""
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = current_schema() ORDER BY table_name
                        """)
                .query(String.class)
                .list();

        assertThat(tables).contains(
                "risk_recent_trade", "risk_volume_applied_trade",
                "risk_volume_bucket", "risk_volume_ticker");
    }

    @Test
    void the_volume_totals_are_exact_integers_rather_than_floating_point() {
        // An hour of a busy ticker can push the sum of squares past the exact-integer range of a
        // double, where the naive variance form cancels to a small negative number and sqrt returns
        // NaN. NUMERIC keeps the stored totals exact so the only rounding is the one the rule does
        // deliberately, in BigDecimal.
        JdbcClient jdbc = JdbcClient.create(dataSource);

        List<String> types = jdbc.sql("""
                        SELECT data_type FROM information_schema.columns
                        WHERE table_schema = current_schema() AND table_name = 'risk_volume_bucket'
                          AND column_name IN ('quantity_sum', 'quantity_sumsq')
                        """)
                .query(String.class)
                .list();

        assertThat(types).containsOnly("numeric");
    }

    @Test
    void applied_seq_is_a_unique_arrival_order_key_with_its_own_sequence() {
        JdbcClient jdbc = JdbcClient.create(dataSource);

        Long unique = jdbc.sql("""
                        SELECT count(*) FROM information_schema.table_constraints
                        WHERE table_schema = current_schema() AND table_name = 'risk_recent_trade'
                          AND constraint_type = 'UNIQUE'
                        """)
                .query(Long.class)
                .single();

        assertThat(unique).isEqualTo(1L);
    }
}
