package dev.engnotes.fes.marketdatacacheprojector;

import java.time.Instant;
import java.util.Map;

import dev.engnotes.fes.events.MarketDataTickEvent;
import dev.engnotes.fes.testing.SecureKafkaStack;
import dev.engnotes.fes.testing.ServiceIdentityContract;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.jupiter.api.DisplayName;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@DisplayName("market-data-cache-projector service identity")
class MarketDataCacheProjectorServiceIdentityStackTest extends ServiceIdentityContract {

    private static final String REDIS_ALIAS = "redis";
    private static final String TOPIC = "market-data.ticks";

    private static GenericContainer<?> redis;

    @Override
    protected String principal() {
        return "market-data-cache-projector";
    }

    @Override
    protected String authorizationErrorMarker() {
        // Resolved empirically from a captured denied run, as every other subclass's marker was.
        // A consumer with no grant at all may be stopped by the group check or the topic check
        // first; see Step 2 for how to read which.
        return "TopicAuthorizationException";
    }

    @Override
    protected String successMarker() {
        // MarketDataTickConsumer logs this only after a tick has been projected into Redis. It is
        // at DEBUG because an INFO line per record is the dominant cost at the platform's target
        // rate, so extraEnvironment raises that one logger.
        return "Projected tick";
    }

    @Override
    protected Map<String, String> extraEnvironment() {
        return Map.of(
                "LOGGING_LEVEL_DEV_ENGNOTES_FES_MARKETDATACACHEPROJECTOR", "DEBUG",
                "SPRING_DATA_REDIS_HOST", REDIS_ALIAS,
                "SPRING_DATA_REDIS_PORT", "6379");
    }

    @Override
    protected void prepareBroker() {
        // Redis joins the stack's network so the service container reaches it by alias. Without it
        // the granted run authenticates, reads its tick, and then fails to project, so the success
        // marker never appears and the test reports a denial that did not happen.
        redis = new GenericContainer<>(DockerImageName.parse("redis:8.10.1-alpine"))
                .withNetwork(SecureKafkaStack.network())
                .withNetworkAliases(REDIS_ALIAS)
                .withExposedPorts(6379);
        redis.start();

        // registerSubject also performs the registry container's only lazy start; serviceEnvironment
        // hands the service a network alias for it, but nothing else brings the container up.
        SecureKafkaStack.registerSubject(TOPIC + "-value",
                MarketDataTickEvent.getClassSchema().toString());
        SecureKafkaStack.seed(TOPIC, "RELIANCE", avroTick());
    }

    private static byte[] avroTick() {
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    SecureKafkaStack.schemaRegistryUrl(),
                    AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false), false);
            return serializer.serialize(TOPIC, MarketDataTickEvent.newBuilder()
                    .setTicker("RELIANCE")
                    .setBidPrice(101.0)
                    .setAskPrice(102.0)
                    .setLastTradedPrice(101.5)
                    .setVolume(100L)
                    .setEventTimestamp(Instant.ofEpochMilli(1_000L))
                    .setProducedAt(Instant.ofEpochMilli(1_005L))
                    .setCorrelationId("identity-probe")
                    .build());
        }
    }
}
