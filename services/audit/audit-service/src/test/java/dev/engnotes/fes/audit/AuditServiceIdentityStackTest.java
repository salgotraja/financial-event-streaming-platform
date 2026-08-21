package dev.engnotes.fes.audit;

import java.util.Map;

import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.testing.SecureKafkaStack;
import dev.engnotes.fes.testing.ServiceIdentityContract;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.jupiter.api.DisplayName;

@DisplayName("audit-service service identity")
class AuditServiceIdentityStackTest extends ServiceIdentityContract {

    @Override
    protected String principal() {
        return "audit-service";
    }

    @Override
    protected String authorizationErrorMarker() {
        // Resolved empirically from a captured denied run: the container logs "Not authorized to
        // access topics: [...]" via a TopicAuthorizationException, so the topic check decides
        // before any group check does.
        return "TopicAuthorizationException";
    }

    @Override
    protected String successMarker() {
        // LoggingAuditSink logs this at INFO once a record has actually reached the archive
        // boundary; no earlier startup line says it.
        return "Archive boundary";
    }

    @Override
    protected void prepareBroker() {
        // AuditEventDecoder classifies every evidence payload against the registry, so a one-byte
        // seed would be quarantined rather than archived and successMarker() would never appear.
        // registerSubject also performs the registry container's only lazy start; serviceEnvironment
        // hands the service a network alias for it, but nothing else brings the container up.
        String subject = "trades.raw-value";
        SecureKafkaStack.registerSubject(subject, TradeEvent.getClassSchema().toString());
        SecureKafkaStack.seed("trades.raw", "AAPL", avroTrade());
    }

    private static byte[] avroTrade() {
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    SecureKafkaStack.schemaRegistryUrl(),
                    AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false), false);
            return serializer.serialize("trades.raw", Trades.trade("T-IDENTITY"));
        }
    }
}
