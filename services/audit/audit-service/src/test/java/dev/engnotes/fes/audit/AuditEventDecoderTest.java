package dev.engnotes.fes.audit;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AuditEventDecoder")
class AuditEventDecoderTest {

    private static final String TOPIC = "trades.raw";
    private static final Map<String, Object> SERDE_CONFIG = Map.of(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://audit",
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

    private KafkaAvroSerializer serializer;
    private AuditEventDecoder decoder;

    @BeforeEach
    void setUp() {
        SchemaRegistryClient registry = new MockSchemaRegistryClient();
        serializer = new KafkaAvroSerializer(registry);
        serializer.configure(SERDE_CONFIG, false);

        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(registry);
        deserializer.configure(SERDE_CONFIG, false);
        decoder = new AuditEventDecoder(deserializer);
    }

    @Test
    @DisplayName("should name the event type from the writer schema without a compile-time dependency on it")
    void should_name_the_event_type_from_the_writer_schema_without_a_compile_time_dependency_on_it() {
        byte[] payload = serializer.serialize(TOPIC, Trades.trade("T-1"));

        assertThat(decoder.eventType(TOPIC, payload)).isEqualTo("TradeEvent");
    }

    @Test
    @DisplayName("should reject a payload that cannot be decoded against the registry")
    void should_reject_a_payload_that_cannot_be_decoded_against_the_registry() {
        byte[] garbage = "this is not an avro payload".getBytes();

        assertThatThrownBy(() -> decoder.eventType(TOPIC, garbage))
                .isInstanceOf(AuditDecodeException.class)
                .hasMessageContaining(TOPIC);
    }

    @Test
    @DisplayName("should classify a tombstone rather than fail on it")
    void should_classify_a_tombstone_rather_than_fail_on_it() {
        // reference-data.instruments is compacted, so a null value is a legitimate record with no
        // schema to name. Treating it as poison would quarantine a valid deletion.
        assertThat(decoder.eventType("reference-data.instruments", null)).isEqualTo("Tombstone");
    }
}
