package dev.engnotes.fes.events;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("TradeEvent contract")
class TradeEventContractTest {

    private static final Instant EXECUTED_AT = Instant.parse("2026-08-16T09:15:00Z");
    private static final Instant PRODUCED_AT = Instant.parse("2026-08-16T09:15:00.004Z");

    @Test
    @DisplayName("should omit CDC provenance when built by a native live producer")
    void should_omit_cdc_provenance_when_built_by_a_native_live_producer() {
        TradeEvent trade = nativeTrade();

        assertThat(trade.getSourceSystem()).isNull();
        assertThat(trade.getSourceRecordKey()).isNull();
        assertThat(trade.getSourceChangePosition()).isNull();
        assertThat(trade.getMigrationBatchId()).isNull();
    }

    @Test
    @DisplayName("should carry CDC provenance when built by the migration normalizer")
    void should_carry_cdc_provenance_when_built_by_the_migration_normalizer() {
        TradeEvent migrated = TradeEvent.newBuilder(nativeTrade())
                .setSourceSystem("legacy-trading-core")
                .setSourceRecordKey("legacy_trade:88213")
                .setSourceChangePosition("0/1A2B3C4D")
                .setMigrationBatchId("backfill-2026-08-16-001")
                .build();

        assertThat(migrated.getSourceSystem()).isEqualTo("legacy-trading-core");
        assertThat(migrated.getSourceChangePosition()).isEqualTo("0/1A2B3C4D");
    }

    @Test
    @DisplayName("should preserve every field across a binary round trip")
    void should_preserve_every_field_across_a_binary_round_trip() throws IOException {
        TradeEvent original = nativeTrade();

        assertThat(roundTrip(original)).isEqualTo(original);
    }

    @Test
    @DisplayName("should map timestamp-millis fields to Instant")
    void should_map_timestamp_millis_fields_to_instant() {
        assertThat(nativeTrade().getEventTimestamp()).isEqualTo(EXECUTED_AT);
    }

    @Test
    @DisplayName("should reject a trade missing a mandatory field")
    void should_reject_a_trade_missing_a_mandatory_field() {
        assertThatThrownBy(() -> TradeEvent.newBuilder()
                .setTradeId("TRD-1")
                .build())
                .isInstanceOf(org.apache.avro.AvroRuntimeException.class);
    }

    private static TradeEvent nativeTrade() {
        return TradeEvent.newBuilder()
                .setTradeId("TRD-000001")
                .setCorrelationId("550e8400-e29b-41d4-a716-446655440000")
                .setTicker("RELIANCE")
                .setQuantity(1_500L)
                .setPrice(2_847.55d)
                .setSide(Side.BUY)
                .setTraderId("TRADER-014")
                .setAccountId("ACC-9931")
                .setEventTimestamp(EXECUTED_AT)
                .setProducedAt(PRODUCED_AT)
                .setTraceContext(Map.of("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
                .build();
    }

    private static TradeEvent roundTrip(TradeEvent event) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(TradeEvent.class).write(event, encoder);
        encoder.flush();

        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null);
        return new SpecificDatumReader<>(TradeEvent.class).read(null, decoder);
    }
}
