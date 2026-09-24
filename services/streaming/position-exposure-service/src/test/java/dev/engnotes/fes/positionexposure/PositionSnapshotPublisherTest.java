package dev.engnotes.fes.positionexposure;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.PositionSnapshotEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.positionexposure.position.Position;
import dev.engnotes.fes.positionexposure.snapshot.PositionSnapshotPublisher;
import dev.engnotes.fes.positionexposure.snapshot.SnapshotPublishException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PositionSnapshotPublisherTest {

    @Mock
    private KafkaTemplate<String, PositionSnapshotEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, PositionSnapshotEvent>> recordCaptor;

    private PositionSnapshotPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new PositionSnapshotPublisher(kafkaTemplate, "positions.snapshots");
        // Lenient because the separator test fails before any send is attempted.
        lenient().when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private static EnrichedTradeEvent trade() {
        TradeEvent tradeEvent = TradeEvent.newBuilder()
                .setTradeId("t-1")
                .setCorrelationId("corr-1")
                .setTicker("RELIANCE")
                .setQuantity(60L)
                .setPrice(2500.0)
                .setSide(Side.BUY)
                .setTraderId("trader-1")
                .setAccountId("acc-1")
                .setEventTimestamp(Instant.ofEpochMilli(2_000L))
                .setProducedAt(Instant.ofEpochMilli(2_001L))
                .setTraceContext(Map.of())
                .build();

        return EnrichedTradeEvent.newBuilder()
                .setTrade(tradeEvent)
                .setMidPriceAtExecution(2500.0)
                .setSpreadAtExecution(0.5)
                .setVwap5Min(2500.0)
                .setMarketCap(1_700_000.0)
                .setPriceDeviation(0.0)
                .setEnrichedAt(Instant.ofEpochMilli(2_002L))
                .setEnrichmentLatencyMs(1L)
                .setMarketDataAgeMs(50L)
                .build();
    }

    private static ConsumerRecord<String, EnrichedTradeEvent> source() {
        return new ConsumerRecord<>("trades.enriched", 0, 0L, "RELIANCE", trade());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Position position() {
        return new Position("acc-1", "trader-1", "RELIANCE", 60L, 60L, 0L,
                new BigDecimal("150000.0000"));
    }

    private ProducerRecord<String, PositionSnapshotEvent> captured() {
        org.mockito.Mockito.verify(kafkaTemplate).send(recordCaptor.capture());
        return recordCaptor.getValue();
    }

    @Test
    void the_snapshot_carries_the_position_and_the_trade_that_produced_it() {
        publisher.publish(source(), position());

        PositionSnapshotEvent published = captured().value();

        // asOf is the trade's event time, never the wall clock, so a replayed trade republishes an
        // identical snapshot.
        assertThat(published.getAsOf()).isEqualTo(Instant.ofEpochMilli(2_000L));
        assertThat(published.getLastProcessedTradeId()).hasToString("t-1");
        assertThat(published.getNetQuantity()).isEqualTo(60L);
        assertThat(published.getMarketValue()).isEqualTo(150_000.0);
    }

    @Test
    void the_record_is_keyed_on_a_hash_of_the_composite_position_key() {
        publisher.publish(source(), position());

        String sentKey = captured().key();

        // Keying on ticker would put two different positions' snapshots on one partition with no
        // ordering between them, and a consumer folding the topic would be at the mercy of
        // interleaving. Hashed because accountId and traderId are RESTRICTED, and a record key is
        // printed by Kafka tooling, logs and the dead-letter topic.
        assertThat(sentKey)
                .isEqualTo(IdempotencyKeys.deterministic("acc-1", "trader-1", "RELIANCE").toString())
                .doesNotContain("acc-1")
                .doesNotContain("trader-1");
    }

    @Test
    void the_trace_headers_of_the_consumed_record_are_copied_onto_the_snapshot() {
        ConsumerRecord<String, EnrichedTradeEvent> source = source();
        source.headers().add("traceparent", bytes("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
        source.headers().add("tracestate", bytes("vendor=value"));
        source.headers().add("correlationId", bytes("corr-1"));
        source.headers().add("unrelated", bytes("not-propagated"));

        publisher.publish(source, position());

        Headers sent = captured().headers();
        assertThat(sent.lastHeader("traceparent").value())
                .isEqualTo(bytes("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
        assertThat(sent.lastHeader("tracestate").value()).isEqualTo(bytes("vendor=value"));
        assertThat(sent.lastHeader("correlationId").value()).isEqualTo(bytes("corr-1"));
        assertThat(sent.lastHeader("unrelated")).isNull();
    }

    @Test
    void an_absent_trace_header_is_not_invented_on_the_snapshot() {
        ConsumerRecord<String, EnrichedTradeEvent> source = source();
        source.headers().add("correlationId", bytes("corr-1"));

        publisher.publish(source, position());

        Headers sent = captured().headers();
        assertThat(sent.lastHeader("traceparent")).isNull();
        assertThat(sent.lastHeader("tracestate")).isNull();
        assertThat(sent.lastHeader("correlationId").value()).isEqualTo(bytes("corr-1"));
        assertThat(sent.toArray()).hasSize(1);
    }

    @Test
    void the_snapshot_is_sent_to_the_configured_topic() {
        publisher.publish(source(), position());

        assertThat(captured().topic()).isEqualTo("positions.snapshots");
    }

    @Test
    void the_snapshot_id_is_derived_so_a_replay_republishes_the_same_identity() {
        publisher.publish(source(), position());

        PositionSnapshotEvent published = captured().value();

        assertThat(published.getSnapshotId()).hasToString(
                IdempotencyKeys.deterministic("t-1", "acc-1", "trader-1", "RELIANCE").toString());
    }

    @Test
    void a_send_that_throws_is_reported_as_a_publish_failure() {
        SerializationException cause = new SerializationException("Error retrieving Avro schema");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(cause);

        assertThatThrownBy(() -> publisher.publish(source(), position()))
                .isInstanceOf(SnapshotPublishException.class)
                .hasCause(cause);
    }

    @Test
    void a_send_that_completes_exceptionally_is_reported_as_a_publish_failure() {
        TimeoutException cause = new TimeoutException("Expiring 1 record(s)");
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(cause));

        assertThatThrownBy(() -> publisher.publish(source(), position()))
                .isInstanceOf(SnapshotPublishException.class)
                .hasRootCause(cause);
    }

    @Test
    void a_separator_reaching_the_publisher_is_rejected_before_the_send_rather_than_wrapped_as_a_publish_failure() {
        // EnrichedTradeConsumer rejects such a trade before the apply, so in the service this is
        // unreachable. The boundary still matters: wrapped as SnapshotPublishException, it would
        // pause the container forever on a record no retry can fix.
        Position position = new Position("acc\u001F1", "trader-1", "RELIANCE", 60L, 60L, 0L,
                new BigDecimal("150000.0000"));

        assertThatThrownBy(() -> publisher.publish(source(), position))
                .isExactlyInstanceOf(IllegalArgumentException.class);
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }
}
