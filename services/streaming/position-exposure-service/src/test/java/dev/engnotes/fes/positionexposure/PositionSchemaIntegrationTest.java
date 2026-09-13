package dev.engnotes.fes.positionexposure;

import java.util.List;
import javax.sql.DataSource;

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
 * <p>{@link PostgresStack} is a dev-shaped superuser connected to whatever schema its search_path
 * resolves to, not necessarily {@code position_exposure}: these assertions check
 * {@code current_schema()} rather than the literal schema name. The least-privilege placement into
 * a named schema is proven separately in Task 5, by a different test against a different fixture.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
class PositionSchemaIntegrationTest {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresStack.start();
        registry.add("spring.datasource.url", PostgresStack::jdbcUrl);
        registry.add("spring.datasource.username", PostgresStack::username);
        registry.add("spring.datasource.password", PostgresStack::password);
    }

    @Autowired
    private DataSource dataSource;

    @Test
    void v1_creates_the_read_model_and_its_idempotency_ledger() {
        JdbcClient jdbc = JdbcClient.create(dataSource);

        List<String> tables = jdbc.sql("""
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = current_schema() ORDER BY table_name
                        """)
                .query(String.class)
                .list();

        assertThat(tables).contains("position", "position_applied_trade");
    }

    @Test
    void the_ledger_pins_every_figure_the_snapshot_publishes() {
        // Not just the net. The whole snapshot is this service's output, so a redelivery must
        // reproduce all four figures or it differs from the original in a field nobody watched.
        JdbcClient jdbc = JdbcClient.create(dataSource);

        List<String> columns = jdbc.sql("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = current_schema() AND table_name = 'position_applied_trade'
                        """)
                .query(String.class)
                .list();

        assertThat(columns).contains("net_quantity_after", "gross_buy_after",
                "gross_sell_after", "market_value_after");
    }

    @Test
    void the_grain_is_the_three_part_position_key() {
        JdbcClient jdbc = JdbcClient.create(dataSource);

        List<String> key = jdbc.sql("""
                        SELECT c.column_name FROM information_schema.table_constraints t
                        JOIN information_schema.key_column_usage c
                          ON c.constraint_name = t.constraint_name
                        WHERE t.table_schema = current_schema() AND t.table_name = 'position'
                          AND t.constraint_type = 'PRIMARY KEY'
                        ORDER BY c.ordinal_position
                        """)
                .query(String.class)
                .list();

        assertThat(key).containsExactly("account_id", "trader_id", "ticker");
    }
}
