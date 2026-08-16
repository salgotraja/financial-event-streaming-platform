package dev.engnotes.fes.audit;

import org.apache.avro.generic.GenericContainer;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * Derives the event type of a payload without taking a compile-time dependency on it.
 *
 * <p>The audit service archives every evidence topic, so it cannot deserialise into a specific
 * generated class per topic without becoming a module that depends on every schema in the platform
 * and needs a redeploy each time one is added. It reads the writer schema from the registry into a
 * generic container instead and takes the schema name, which is what the S3 key's
 * {@code event_type=} partition needs.
 *
 * <p>Decoding does not produce the archived bytes. Archival keeps the delivered payload; this class
 * exists to classify it, and to make an undecodable payload fail here rather than reach the archive
 * as evidence nothing can read.
 */
@Component
public class AuditEventDecoder {

    private final KafkaAvroDeserializer deserializer;

    public AuditEventDecoder(KafkaAvroDeserializer deserializer) {
        this.deserializer = deserializer;
    }

    public String eventType(String topic, byte[] payload) {
        if (payload == null) {
            // A null value is a tombstone on a compacted topic, which is a legitimate record with no
            // schema to name. reference-data.instruments is compacted, so this is reachable.
            return "Tombstone";
        }
        Object decoded;
        try {
            decoded = deserializer.deserialize(topic, payload);
        } catch (Exception e) {
            throw new AuditDecodeException(
                    "Payload on %s could not be decoded against the registry".formatted(topic), e);
        }
        if (decoded instanceof GenericContainer container) {
            return container.getSchema().getName();
        }
        throw new AuditDecodeException(
                "Payload on %s decoded to %s, which carries no schema"
                        .formatted(topic, decoded == null ? "null" : decoded.getClass().getName()),
                null);
    }
}
