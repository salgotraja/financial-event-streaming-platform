package dev.engnotes.fes.riskalert;

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
class RiskPositionSchemaIntegrationTest {

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
    void the_migration_creates_both_position_tables_with_their_primary_keys() {
        JdbcClient jdbc = JdbcClient.create(dataSource);

        assertThat(tableExists(jdbc, "risk_position")).isTrue();
        assertThat(tableExists(jdbc, "risk_position_applied_trade")).isTrue();

        assertThat(primaryKeyColumns(jdbc, "risk_position"))
                .containsExactly("trader_id", "ticker");
        assertThat(primaryKeyColumns(jdbc, "risk_position_applied_trade"))
                .containsExactly("trade_id");
    }

    @Test
    void every_risk_position_column_is_not_null() {
        JdbcClient jdbc = JdbcClient.create(dataSource);

        assertThat(jdbc.sql("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name = 'risk_position' AND is_nullable = 'YES'
                        """)
                .query(String.class).list())
                .as("every risk_position column is declared NOT NULL explicitly")
                .isEmpty();
    }

    private static boolean tableExists(JdbcClient jdbc, String table) {
        return jdbc.sql("SELECT to_regclass(?) IS NOT NULL")
                .param(table)
                .query(Boolean.class)
                .single();
    }

    private static java.util.List<String> primaryKeyColumns(JdbcClient jdbc, String table) {
        return jdbc.sql("""
                        SELECT a.attname
                        FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                        WHERE i.indrelid = ?::regclass AND i.indisprimary
                        ORDER BY array_position(i.indkey, a.attnum)
                        """)
                .param(table)
                .query(String.class)
                .list();
    }
}
