package dev.engnotes.fes.positionexposure;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

import dev.engnotes.fes.positionexposure.snapshot.SnapshotPublishException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two failure classes must not be merged. A poison bound is for bytes that cannot improve; an
 * outage bound is for a dependency that comes back. Giving a database outage the poison bound would
 * dead-letter good trades during a restart (ADR-027, ADR-036), following
 * {@code RiskAlertBackOffTest} in risk-alert-service.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PositionExposureBackOffTest {

    @Test
    void a_lost_database_connection_pauses_indefinitely_rather_than_quarantining() {
        FixedBackOff backOff = (FixedBackOff) PositionExposureKafkaConfiguration.backOffFor(
                new CannotGetJdbcConnectionException("connection refused",
                        new SQLTransientConnectionException("refused")));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void a_database_that_accepted_the_socket_and_stopped_answering_also_pauses() {
        FixedBackOff backOff = (FixedBackOff) PositionExposureKafkaConfiguration.backOffFor(
                new QueryTimeoutException("statement timed out"));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void an_outage_wrapped_several_causes_deep_is_still_recognised() {
        FixedBackOff backOff = (FixedBackOff) PositionExposureKafkaConfiguration.backOffFor(
                new IllegalStateException("listener failed",
                        new RuntimeException("store failed",
                                new CannotGetJdbcConnectionException("connection refused"))));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void a_pool_timeout_at_transaction_begin_pauses_rather_than_quarantining() {
        // PositionStore.apply is @Transactional, so the connection is taken in
        // DataSourceTransactionManager.doBegin, which wraps Hikari's pool timeout this way rather
        // than as a CannotGetJdbcConnectionException.
        FixedBackOff backOff = (FixedBackOff) PositionExposureKafkaConfiguration.backOffFor(
                new CannotCreateTransactionException("Could not open JDBC Connection for transaction",
                        new SQLTransientConnectionException("Connection is not available, request timed out")));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void a_connection_lost_mid_statement_pauses_rather_than_quarantining() {
        FixedBackOff backOff = (FixedBackOff) PositionExposureKafkaConfiguration.backOffFor(
                new DataAccessResourceFailureException("An I/O error occurred while sending to the backend",
                        new SQLException("An I/O error occurred while sending to the backend", "08006")));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void an_untranslated_sql_exception_in_the_connection_state_class_pauses() {
        FixedBackOff backOff = (FixedBackOff) PositionExposureKafkaConfiguration.backOffFor(
                new SQLException("connection failure", "08006"));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void a_failed_snapshot_publish_pauses_rather_than_quarantining_an_applied_trade() {
        FixedBackOff backOff = (FixedBackOff) PositionExposureKafkaConfiguration.backOffFor(
                new SnapshotPublishException("send failed", new TimeoutException("broker timeout")));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void a_sql_exception_outside_the_connection_state_class_keeps_the_poison_back_off() {
        assertThat(PositionExposureKafkaConfiguration.backOffFor(
                new SQLException("numeric field overflow", "22003")))
                .isInstanceOf(ExponentialBackOff.class);
    }

    @Test
    void a_plain_runtime_exception_keeps_the_bounded_poison_back_off() {
        assertThat(PositionExposureKafkaConfiguration.backOffFor(new RuntimeException("unexpected")))
                .isInstanceOf(ExponentialBackOff.class);
    }

    @Test
    void any_other_failure_keeps_the_bounded_poison_back_off() {
        // PoisonRecordPolicy.poisonBackOff() returns an ExponentialBackOff, never a FixedBackOff,
        // so the type alone distinguishes the bounded path from the outage path.
        assertThat(PositionExposureKafkaConfiguration.backOffFor(new IllegalArgumentException("bad payload")))
                .isInstanceOf(ExponentialBackOff.class);
    }
}
