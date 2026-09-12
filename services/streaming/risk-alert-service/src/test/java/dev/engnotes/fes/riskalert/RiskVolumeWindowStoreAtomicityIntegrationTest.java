package dev.engnotes.fes.riskalert;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import javax.sql.DataSource;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.rules.EnrichedTrades;
import dev.engnotes.fes.riskalert.window.RiskVolumeWindowStore;
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
 * Proves {@link RiskVolumeWindowStore#apply} is genuinely transactional. The method inserts a
 * claim row into {@code risk_volume_applied_trade} and then mutates {@code risk_volume_ticker} and
 * {@code risk_volume_bucket} before pinning the computed window back onto the claim row. If a
 * later statement fails and the claim row survives anyway, a redelivery would read a pinned answer
 * that no statement ever produced.
 *
 * <p>The failure is a genuine PostgreSQL constraint, not a mock: the bucket this trade lands in is
 * primed with the maximum value a {@code NUMERIC(38,0)} column can hold, so the store's own bucket
 * upsert overflows it.
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
        classes = RiskVolumeWindowStoreAtomicityIntegrationTest.StoreOnlyConfiguration.class,
        properties = {
                "management.otlp.metrics.export.enabled=false",
                "management.otlp.tracing.export.enabled=false"
        })
class RiskVolumeWindowStoreAtomicityIntegrationTest {

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
    private RiskVolumeWindowStore store;

    private JdbcClient jdbc;

    @BeforeEach
    void clearState() {
        jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE risk_volume_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_volume_bucket").update();
        jdbc.sql("TRUNCATE TABLE risk_volume_ticker").update();
    }

    @Test
    void a_bucket_write_failure_after_the_claim_insert_rolls_back_the_whole_apply() {
        String ticker = "OVERFLOW";
        Instant bucketStart = Instant.parse("2026-09-12T10:00:00Z");
        Instant eventTimestamp = Instant.parse("2026-09-12T10:00:30Z");
        BigDecimal maxNumeric38 = new BigDecimal("99999999999999999999999999999999999999");

        // Primes the bucket this trade will land in with the maximum NUMERIC(38,0) value, so the
        // store's own bucket upsert (adding this trade's small quantity to it) overflows the
        // column: a real constraint failure, not a mock.
        jdbc.sql("""
                        INSERT INTO risk_volume_bucket
                            (ticker, bucket_start, trade_count, quantity_sum, quantity_sumsq)
                        VALUES (?, ?, 1, ?, ?)
                        """)
                .params(ticker, Timestamp.from(bucketStart), maxNumeric38, maxNumeric38)
                .update();

        EnrichedTradeEvent trade = EnrichedTrades.withPosition(
                "t-overflow", "trader-1", ticker, Side.BUY, 1L, eventTimestamp);

        assertThatThrownBy(() -> store.apply(trade))
                .isInstanceOf(DataAccessException.class);

        Long ledgerRows = jdbc.sql("SELECT count(*) FROM risk_volume_applied_trade WHERE trade_id = ?")
                .param("t-overflow")
                .query(Long.class)
                .single();

        assertThat(ledgerRows)
                .as("the claim row must not survive a later statement's failure")
                .isZero();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class StoreOnlyConfiguration {

        @Bean
        RiskVolumeWindowStore riskVolumeWindowStore(JdbcClient jdbcClient) {
            return new RiskVolumeWindowStore(jdbcClient, 3_600L);
        }
    }
}
