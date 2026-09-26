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
 *
 * <p>{@link PostgresStack} is a dev-shaped superuser connected to whatever schema its search_path
 * resolves to, not necessarily {@code risk_alert}: these assertions check {@code current_schema()}
 * rather than the literal schema name. {@link RiskPositionFlywayLeastPrivilegeIntegrationTest} is
 * what proves the tables land in {@code risk_alert} specifically, under the least-privilege role.
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
    void v2_creates_the_four_window_and_correlation_tables() {
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

        // containsOnly alone would not catch a dropped column, since a shorter list containing
        // only "numeric" still satisfies it. The explicit size pins both columns as present.
        assertThat(types).hasSize(2).containsOnly("numeric");
    }

    @Test
    void applied_seq_is_a_unique_arrival_order_key_backed_by_its_own_sequence() {
        JdbcClient jdbc = JdbcClient.create(dataSource);

        // Not just "some UNIQUE constraint exists on this table": that would still pass if the
        // constraint moved to a different column. key_column_usage ties the constraint to the
        // specific column applied_seq, which is what the replay-determinism property in Task 5
        // actually depends on.
        List<String> uniqueColumns = jdbc.sql("""
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON kcu.constraint_name = tc.constraint_name
                         AND kcu.table_schema = tc.table_schema
                        WHERE tc.table_schema = current_schema() AND tc.table_name = 'risk_recent_trade'
                          AND tc.constraint_type = 'UNIQUE'
                        """)
                .query(String.class)
                .list();

        assertThat(uniqueColumns).containsExactly("applied_seq");

        // BIGSERIAL is sugar for a BIGINT column defaulted from a dedicated sequence: this is what
        // makes applied_seq an arrival-order key rather than a plain unique column the application
        // would have to populate itself.
        String columnDefault = jdbc.sql("""
                        SELECT column_default FROM information_schema.columns
                        WHERE table_schema = current_schema() AND table_name = 'risk_recent_trade'
                          AND column_name = 'applied_seq'
                        """)
                .query(String.class)
                .single();

        assertThat(columnDefault).contains("nextval(");
    }

    @Test
    void the_candidate_index_orders_columns_for_the_trader_ticker_lookup() {
        // Column order matters for this index: equality on trader_id and ticker, then a descending
        // range scan on applied_seq. Task 5's candidate query relies on exactly this order to avoid
        // a sort at query time.
        JdbcClient jdbc = JdbcClient.create(dataSource);

        String indexDef = jdbc.sql("""
                        SELECT indexdef FROM pg_indexes
                        WHERE schemaname = current_schema()
                          AND tablename = 'risk_recent_trade'
                          AND indexname = 'ix_risk_recent_trade_candidates'
                        """)
                .query(String.class)
                .single();

        assertThat(indexDef).contains("(trader_id, ticker, applied_seq DESC)");
    }
}
