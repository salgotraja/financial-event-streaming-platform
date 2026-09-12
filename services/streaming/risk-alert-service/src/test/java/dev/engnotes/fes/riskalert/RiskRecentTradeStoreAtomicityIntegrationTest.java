package dev.engnotes.fes.riskalert;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.correlation.RiskRecentTradeStore;
import dev.engnotes.fes.riskalert.rules.EnrichedTrades;
import dev.engnotes.fes.testing.PostgresStack;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves {@link RiskRecentTradeStore#apply} is genuinely transactional. The method inserts the
 * claim row into {@code risk_recent_trade} (the ledger and the candidate state are the same row
 * here) and then runs the retention cleanup {@code DELETE} before reading the candidate set. If
 * the {@code DELETE} fails and the claim row survives anyway, a redelivery would see the trade as
 * already applied and answer from state the transaction never actually committed.
 *
 * <p>The failure is a genuine PostgreSQL constraint, not a mock: a foreign key from an auxiliary
 * table is pointed at the one row the retention cleanup will try to delete, so that {@code DELETE}
 * fails with a real foreign-key violation.
 *
 * <p>Deliberately narrow, the same way {@link RiskAlertPropertiesTest} is: {@link
 * RiskAlertKafkaConfiguration} runs a blocking Kafka fold at startup that hangs with no broker
 * running, so {@link StoreOnlyConfiguration} imports auto-configuration directly to get a real
 * {@code DataSource}, {@code JdbcClient} and transaction manager without dragging that class in.
 * The store bean is resolved through this context rather than constructed with {@code new}, because
 * {@code @Transactional} is applied by a proxy that only a container-managed bean carries.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        classes = RiskRecentTradeStoreAtomicityIntegrationTest.StoreOnlyConfiguration.class,
        properties = {
                "management.otlp.metrics.export.enabled=false",
                "management.otlp.tracing.export.enabled=false"
        })
class RiskRecentTradeStoreAtomicityIntegrationTest {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresStack.start();
        registry.add("spring.datasource.url", PostgresStack::jdbcUrl);
        registry.add("spring.datasource.username", PostgresStack::username);
        registry.add("spring.datasource.password", PostgresStack::password);
    }

    @Autowired
    private DataSource dataSource;

    @Autowired
    private RiskRecentTradeStore store;

    private JdbcClient jdbc;

    @BeforeEach
    void clearState() {
        jdbc = JdbcClient.create(dataSource);
        // Dropped first: a row left referenced by this table from a previous run would make the
        // TRUNCATE below fail on the same foreign key this test relies on.
        jdbc.sql("DROP TABLE IF EXISTS risk_recent_trade_pin").update();
        jdbc.sql("TRUNCATE TABLE risk_recent_trade").update();
    }

    @Test
    void a_retention_delete_failure_after_the_claim_insert_rolls_back_the_whole_apply() {
        String traderId = "trader-pin";
        String ticker = "PIN";
        Instant newTradeTimestamp = Instant.parse("2026-09-12T10:00:00Z");
        // Eight days back: outside the store's 7-day retention horizon, so apply's own cleanup
        // DELETE targets this row.
        Instant oldTradeTimestamp = newTradeTimestamp.minus(Duration.ofDays(8));

        jdbc.sql("""
                        INSERT INTO risk_recent_trade
                            (trade_id, trader_id, ticker, side, quantity, price, event_timestamp)
                        VALUES (?, ?, ?, 'BUY', 100, 10.00, ?)
                        """)
                .params("t-old", traderId, ticker, Timestamp.from(oldTradeTimestamp))
                .update();

        // A real foreign key, not a mock: referencing the row above means deleting it fails with a
        // genuine constraint violation, after the claim insert for the new trade already ran in
        // the same transaction.
        jdbc.sql("""
                        CREATE TABLE risk_recent_trade_pin (
                            id BIGSERIAL PRIMARY KEY,
                            trade_id VARCHAR(64) NOT NULL REFERENCES risk_recent_trade (trade_id)
                        )
                        """)
                .update();
        jdbc.sql("INSERT INTO risk_recent_trade_pin (trade_id) VALUES (?)")
                .param("t-old")
                .update();

        EnrichedTradeEvent trade = EnrichedTrades.withPosition(
                "t-new", traderId, ticker, Side.BUY, 100L, newTradeTimestamp);

        assertThatThrownBy(() -> store.apply(trade))
                .isInstanceOf(DataAccessException.class);

        Long claimRows = jdbc.sql("SELECT count(*) FROM risk_recent_trade WHERE trade_id = ?")
                .param("t-new")
                .query(Long.class)
                .single();

        assertThat(claimRows)
                .as("the claim row must not survive a later statement's failure")
                .isZero();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class StoreOnlyConfiguration {

        @Bean
        RiskRecentTradeStore riskRecentTradeStore(JdbcClient jdbcClient) {
            return new RiskRecentTradeStore(jdbcClient, 3_600L, 200);
        }
    }
}
