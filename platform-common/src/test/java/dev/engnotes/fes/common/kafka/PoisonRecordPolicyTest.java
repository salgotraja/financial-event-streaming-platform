package dev.engnotes.fes.common.kafka;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PoisonRecordPolicyTest {

    private static final byte[] RAW = "raw-avro-bytes".getBytes(StandardCharsets.UTF_8);

    private static ConsumerRecord<String, Object> recordWith(Object value) {
        return new ConsumerRecord<>("trades.enriched", 0, 0L, "key", value);
    }

    @Test
    void a_decode_failure_yields_the_bytes_the_deserializer_rejected() {
        // The record value is null on a decode failure, so without unwrapping the exception the
        // dead letter would carry no payload and the quarantined bytes would be unrecoverable.
        DeserializationException failure =
                new DeserializationException("bad", RAW, false, new IllegalStateException());

        assertThat(PoisonRecordPolicy.originalPayload(recordWith(null), failure)).isEqualTo(RAW);
    }

    @Test
    void a_decode_failure_is_found_however_deeply_it_is_wrapped() {
        Throwable wrapped = new IllegalStateException("listener",
                new RuntimeException("container",
                        new DeserializationException("bad", RAW, false, new IllegalStateException())));

        assertThat(PoisonRecordPolicy.originalPayload(recordWith(null), wrapped)).isEqualTo(RAW);
    }

    @Test
    void a_self_referential_cause_chain_terminates() {
        // Some container exceptions return themselves from getCause(). Walking the chain without a
        // guard would spin forever and hold the consumer thread past max.poll.interval.ms.
        Throwable selfReferential = new RuntimeException("loop") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        assertThat(PoisonRecordPolicy.originalPayload(recordWith(RAW), selfReferential)).isEqualTo(RAW);
    }

    @Test
    void a_processing_failure_falls_back_to_the_record_value() {
        assertThat(PoisonRecordPolicy.originalPayload(recordWith(RAW), new IllegalStateException()))
                .isEqualTo(RAW);
    }

    @Test
    void a_value_that_is_not_bytes_yields_no_payload() {
        // A deserialized record that fails downstream has no original bytes to preserve, and
        // inventing some from toString would put a fabricated payload in the dead letter.
        assertThat(PoisonRecordPolicy.originalPayload(recordWith("already-deserialized"),
                new IllegalStateException())).isNull();
    }

    @Test
    void a_null_failure_falls_back_to_the_record_value() {
        assertThat(PoisonRecordPolicy.originalPayload(recordWith(RAW), null)).isEqualTo(RAW);
    }

    @Test
    void the_back_off_is_bounded_at_three_attempts_in_total() {
        // Two retries after the first delivery. An unbounded sequence would hold the partition
        // behind a record whose bytes cannot improve.
        BackOffExecution execution = PoisonRecordPolicy.poisonBackOff().start();

        assertThat(execution.nextBackOff()).isEqualTo(100L);
        assertThat(execution.nextBackOff()).isEqualTo(200L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void the_back_off_is_configured_the_same_way_for_every_service() {
        BackOff backOff = PoisonRecordPolicy.poisonBackOff();

        assertThat(backOff).isInstanceOfSatisfying(ExponentialBackOff.class, exponential -> {
            assertThat(exponential.getInitialInterval()).isEqualTo(100L);
            assertThat(exponential.getMultiplier()).isEqualTo(2.0);
            assertThat(exponential.getMaxInterval()).isEqualTo(5_000L);
            assertThat(exponential.getMaxElapsedTime()).isEqualTo(5_000L);
            assertThat(exponential.getMaxAttempts()).isEqualTo(2L);
        });
    }

    @Test
    void every_call_returns_a_fresh_back_off() {
        // BackOff is stateful once started. A shared instance would carry one listener's exhausted
        // attempt count into the next.
        assertThat(PoisonRecordPolicy.poisonBackOff())
                .isNotSameAs(PoisonRecordPolicy.poisonBackOff());
    }
}
