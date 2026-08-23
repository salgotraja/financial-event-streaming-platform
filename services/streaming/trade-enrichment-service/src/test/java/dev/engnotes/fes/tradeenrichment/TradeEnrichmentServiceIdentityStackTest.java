package dev.engnotes.fes.tradeenrichment;

import java.time.Instant;
import java.util.Map;

import dev.engnotes.fes.common.cache.MarketCacheKeys;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.InstrumentReferenceEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.testing.SecureKafkaStack;
import dev.engnotes.fes.testing.ServiceIdentityContract;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.jupiter.api.DisplayName;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@DisplayName("trade-enrichment-service service identity")
class TradeEnrichmentServiceIdentityStackTest extends ServiceIdentityContract {

    private static final String REDIS_ALIAS = "redis";
    private static final String TRADE_TOPIC = "trades.raw";
    private static final String REFERENCE_TOPIC = "reference-data.instruments";
    private static final String OUTPUT_TOPIC = "trades.enriched";
    private static final String TICKER = "RELIANCE";
    private static final long FIXED_EVENT_TIMESTAMP_MILLIS = 1_000L;

    private static GenericContainer<?> redis;

    @Override
    protected String principal() {
        return "trade-enrichment-service";
    }

    @Override
    protected String authorizationErrorMarker() {
        // Not resolved to a single exception class, deliberately. This service makes two independent
        // Kafka calls on startup: InstrumentCacheLoader's readiness gate calls
        // partitionsFor(reference-data.instruments), and the trade listener joins the
        // "trade-enrichment-service" group. Either can be the one the broker denies first, which is
        // the "stopped by the group check or the topic check first" ambiguity this contract's class
        // javadoc already warns about, so pinning the marker to one of TopicAuthorizationException or
        // GroupAuthorizationException would make this test depend on an ordering nothing guarantees.
        // Both are subclasses of org.apache.kafka.common.errors.AuthorizationException, so matching
        // that shared substring is correct regardless of which check wins, and it still excludes
        // SaslAuthenticationException.
        return "AuthorizationException";
    }

    @Override
    protected String successMarker() {
        // RawTradeConsumer logs this only after a trade has actually been enriched. It is at DEBUG
        // because an INFO line per record is the dominant cost at the platform's target rate, so
        // extraEnvironment raises that one logger.
        return "Enriched trade";
    }

    @Override
    protected Map<String, String> extraEnvironment() {
        return Map.of(
                "LOGGING_LEVEL_DEV_ENGNOTES_FES_TRADEENRICHMENT", "DEBUG",
                "SPRING_DATA_REDIS_HOST", REDIS_ALIAS,
                "SPRING_DATA_REDIS_PORT", "6379");
    }

    @Override
    protected void prepareBroker() {
        // Redis joins the stack's network so the service container reaches it by alias. Without it
        // the granted run authenticates, reads its trade, and then fails to enrich, so the success
        // marker never appears and the test reports a denial that did not happen.
        redis = new GenericContainer<>(DockerImageName.parse("redis:8.10.1-alpine"))
                .withNetwork(SecureKafkaStack.network())
                .withNetworkAliases(REDIS_ALIAS)
                .withExposedPorts(6379);
        redis.start();

        // All three, because trade-enrichment-service both consumes and produces Avro records and
        // ships auto.register.schemas: false. Missing the output subject lets the granted run
        // consume and enrich the trade, then fail the publish, which reports as a denial that never
        // happened.
        SecureKafkaStack.registerSubject(TRADE_TOPIC + "-value", TradeEvent.getClassSchema().toString());
        SecureKafkaStack.registerSubject(REFERENCE_TOPIC + "-value",
                InstrumentReferenceEvent.getClassSchema().toString());
        SecureKafkaStack.registerSubject(OUTPUT_TOPIC + "-value",
                EnrichedTradeEvent.getClassSchema().toString());

        SecureKafkaStack.seed(REFERENCE_TOPIC, "INE-" + TICKER, avroInstrument());
        SecureKafkaStack.seed(TRADE_TOPIC, TICKER, avroTrade());
        seedMarketState();
    }

    /**
     * Both the seeded trade's and the seeded tick's {@code eventTimestamp} are the same fixed value,
     * so the freshness policy (ADR-034) sees an age of exactly zero regardless of when the test runs.
     */
    private static void seedMarketState() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379)));
        factory.afterPropertiesSet();
        try {
            StringRedisTemplate template = new StringRedisTemplate(factory);
            template.afterPropertiesSet();
            template.opsForHash().putAll(MarketCacheKeys.tickKey(TICKER), Map.of(
                    "eventTimestamp", String.valueOf(FIXED_EVENT_TIMESTAMP_MILLIS),
                    "bidPrice", "100.0",
                    "askPrice", "101.0",
                    "lastTradedPrice", "100.5"));
            long bucket = MarketCacheKeys.bucketFor(FIXED_EVENT_TIMESTAMP_MILLIS);
            template.opsForHash().putAll(MarketCacheKeys.windowKey(TICKER), Map.of(
                    bucket + ":pv", "1000.0",
                    bucket + ":v", "10.0"));
        } finally {
            factory.destroy();
        }
    }

    private static byte[] avroTrade() {
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    SecureKafkaStack.schemaRegistryUrl(),
                    AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false), false);
            return serializer.serialize(TRADE_TOPIC, TradeEvent.newBuilder()
                    .setTradeId("identity-probe-trade")
                    .setCorrelationId("identity-probe")
                    .setTicker(TICKER)
                    .setQuantity(10L)
                    .setPrice(100.5)
                    .setSide(Side.BUY)
                    .setTraderId("TR-1")
                    .setAccountId("AC-1")
                    .setEventTimestamp(Instant.ofEpochMilli(FIXED_EVENT_TIMESTAMP_MILLIS))
                    .setProducedAt(Instant.ofEpochMilli(FIXED_EVENT_TIMESTAMP_MILLIS + 5))
                    .build());
        }
    }

    private static byte[] avroInstrument() {
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    SecureKafkaStack.schemaRegistryUrl(),
                    AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false), false);
            return serializer.serialize(REFERENCE_TOPIC, InstrumentReferenceEvent.newBuilder()
                    .setInstrumentId("INE-" + TICKER)
                    .setTicker(TICKER)
                    .setExchange("NSE")
                    .setIsin("INE-" + TICKER)
                    .setSecurityType("EQUITY")
                    .setCurrency("INR")
                    .setSector("ENERGY")
                    .setSharesOutstanding(1_000_000L)
                    .setReferenceVersion(1L)
                    .setEffectiveAt(Instant.ofEpochMilli(1_000L))
                    .setProducerIdentity("reference-data-service")
                    .build());
        }
    }
}
