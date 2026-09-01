package dev.engnotes.fes.riskalert;

import java.sql.SQLTransientConnectionException;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two failure classes must not be merged. A poison bound is for bytes that cannot improve; an
 * outage bound is for a dependency that comes back. Giving a database outage the poison bound would
 * dead-letter good trades during a restart (ADR-027, ADR-036).
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RiskAlertBackOffTest {

    @Test
    void a_lost_database_connection_pauses_indefinitely_rather_than_quarantining() {
        FixedBackOff backOff = (FixedBackOff) RiskAlertKafkaConfiguration.backOffFor(
                new CannotGetJdbcConnectionException("connection refused",
                        new SQLTransientConnectionException("refused")));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void a_database_that_accepted_the_socket_and_stopped_answering_also_pauses() {
        FixedBackOff backOff = (FixedBackOff) RiskAlertKafkaConfiguration.backOffFor(
                new QueryTimeoutException("statement timed out"));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void an_outage_wrapped_several_causes_deep_is_still_recognised() {
        FixedBackOff backOff = (FixedBackOff) RiskAlertKafkaConfiguration.backOffFor(
                new IllegalStateException("listener failed",
                        new RuntimeException("store failed",
                                new CannotGetJdbcConnectionException("connection refused"))));

        assertThat(backOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Test
    void any_other_failure_keeps_the_bounded_poison_back_off() {
        // PoisonRecordPolicy.poisonBackOff() returns an ExponentialBackOff, never a FixedBackOff,
        // so the type alone distinguishes the bounded path from the outage path.
        assertThat(RiskAlertKafkaConfiguration.backOffFor(new IllegalArgumentException("bad payload")))
                .isInstanceOf(ExponentialBackOff.class);
    }
}
