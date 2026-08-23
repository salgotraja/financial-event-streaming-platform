package dev.engnotes.fes.tradeenrichment;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import dev.engnotes.fes.common.cache.MarketCacheKeys;
import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.InstrumentReferenceEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.testing.KafkaAvroStack;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The consumer against a real broker, a real Schema Registry and a real Redis: enrichment,
 * quarantine on each of the five {@link UnavailableReason}s and on a malformed payload, a Redis
 * outage that pauses rather than dead-letters, and the instrument-master readiness gate.
 *
 * <p>Every topic is unique to this class, including the output topic, so another module's tests, or
 * a future test in this module, cannot race these assertions.
 *
 * <p>The reference master is seeded once, in {@link #seedTheWorldBeforeTheContextStarts()}, before
 * any {@code @Test} method runs and therefore before the Spring context is created. The
 * {@code @SpringBootTest} context is shared across every test in this class, so the instrument
 * master is loaded exactly once; every ticker any test needs, other than the one that must be
 * absent, is seeded up front.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("RawTradeConsumer against a real broker, registry and Redis")
class RawTradeConsumerIntegrationTest {

    private static final String TRADE_TOPIC = "tes-it-" + UUID.randomUUID();
    private static final String REFERENCE_TOPIC = "tes-ref-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "tes-out-it-" + UUID.randomUUID();
    private static final String DLQ_TOPIC = TRADE_TOPIC + ".dlq";

    private static final int REFERENCE_PARTITIONS = 6;
    // Large enough that the catch-up loop spans several polls (as in
    // InstrumentCacheLoaderIntegrationTest), which is what gives an ungated listener a real window
    // to get ahead of the fold in the gate test below.
    private static final int FILLER_INSTRUMENTS = 2000;

    private static final byte[] POISON = {0, 0, 0, 0, 1, 42};

    private static final String ENRICH_TICKER = "ENRICH-OK";
    private static final String HEADERS_TICKER = "HEADERS-OK";
    private static final String TICK_ABSENT_TICKER = "TICK-ABSENT";
    private static final String STALE_TICKER = "STALE-TICK";
    private static final String FUTURE_TICKER = "FUTURE-TICK";
    private static final String WINDOW_EMPTY_TICKER = "WINDOW-EMPTY-TICK";
    private static final String INSTRUMENT_MISSING_TICKER = "INSTRUMENT-MISSING-TICK";
    private static final String OUTAGE_TICKER = "OUTAGE-TICK";
    private static final String GATE_TICKER = "GATE-TICKER";

    private static final long SHARES_OUTSTANDING = 1_000_000L;

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:8.10.1-alpine")).withExposedPorts(6379);

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        REDIS.start();
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("fes.trade-enrichment-service.topic", () -> TRADE_TOPIC);
        registry.add("fes.trade-enrichment-service.reference-topic", () -> REFERENCE_TOPIC);
        registry.add("fes.trade-enrichment-service.output-topic", () -> OUTPUT_TOPIC);
    }

    /**
     * Everything the gate test needs is prepared here, in {@code @BeforeAll}, which JUnit runs
     * before the first {@code @Test} instance is created and therefore before Spring's
     * {@code @DynamicPropertySource}-driven context creation. The trades published for
     * {@link #GATE_TICKER} sit on {@link #TRADE_TOPIC} untouched until the context, and the
     * readiness gate inside it, has started.
     */
    @BeforeAll
    static void seedTheWorldBeforeTheContextStarts() throws Exception {
        KafkaAvroStack.start();
        REDIS.start();

        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(TRADE_TOPIC, 1, (short) 1),
                    new NewTopic(REFERENCE_TOPIC, REFERENCE_PARTITIONS, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 1, (short) 1),
                    new NewTopic(DLQ_TOPIC, 1, (short) 1))).all().get();
        }

        try (CachedSchemaRegistryClient client =
                     new CachedSchemaRegistryClient(KafkaAvroStack.schemaRegistryUrl(), 10)) {
            client.register(TRADE_TOPIC + "-value", new AvroSchema(TradeEvent.getClassSchema()));
            client.register(REFERENCE_TOPIC + "-value",
                    new AvroSchema(InstrumentReferenceEvent.getClassSchema()));
            client.register(OUTPUT_TOPIC + "-value", new AvroSchema(EnrichedTradeEvent.getClassSchema()));
            client.register(DLQ_TOPIC + "-value", new AvroSchema(DeadLetterEvent.getClassSchema()));
        }

        seedReferenceMaster();
        seedRedisBeforeContextStarts();
        publishGateTradesBeforeContextStarts();
    }

    private static void seedReferenceMaster() {
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(referenceProducerProperties())) {
            // Filler across every partition, so the catch-up loop cannot finish in one poll. An
            // ungated listener therefore has a genuine multi-poll window to consume GATE_TICKER's
            // trades before the loader has folded anything close to the whole master.
            for (int i = 0; i < FILLER_INSTRUMENTS; i++) {
                producer.send(new ProducerRecord<>(REFERENCE_TOPIC, "FILLER-INE-" + i,
                        instrument("FILLER-INE-" + i, "FILLER-" + i)));
            }
            for (String ticker : List.of(ENRICH_TICKER, HEADERS_TICKER, TICK_ABSENT_TICKER,
                    STALE_TICKER, FUTURE_TICKER, WINDOW_EMPTY_TICKER, OUTAGE_TICKER)) {
                producer.send(new ProducerRecord<>(REFERENCE_TOPIC, "INE-" + ticker,
                        instrument("INE-" + ticker, ticker)));
            }
            // Sent last, and after 2000 filler records spread over 6 partitions, so whichever
            // partition it lands on still has filler ahead of it in the log. INSTRUMENT_MISSING_TICKER
            // is deliberately never sent at all.
            producer.send(new ProducerRecord<>(REFERENCE_TOPIC, "INE-" + GATE_TICKER,
                    instrument("INE-" + GATE_TICKER, GATE_TICKER)));
            producer.flush();
        }
    }

    private static void seedRedisBeforeContextStarts() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)));
        factory.afterPropertiesSet();
        try {
            StringRedisTemplate template = new StringRedisTemplate(factory);
            template.afterPropertiesSet();
            seedMarketState(template, GATE_TICKER, 5_000L, 100.0, 101.0);
        } finally {
            factory.destroy();
        }
    }

    private static void publishGateTradesBeforeContextStarts() {
        try (KafkaProducer<String, TradeEvent> producer = new KafkaProducer<>(tradeProducerProperties())) {
            // Several trades, not one, so a listener that started even briefly early has more than a
            // single-record chance to prove the race, and so a positive assertion after the gate
            // opens has more than one record to find.
            for (int i = 0; i < 20; i++) {
                producer.send(new ProducerRecord<>(TRADE_TOPIC, GATE_TICKER,
                        trade("GATE-" + i, GATE_TICKER, 5_000L)));
            }
            producer.flush();
        }
    }

    @Test
    @DisplayName("should enrich a trade whose ticker has fresh market state and a known instrument")
    void should_enrich_a_trade_whose_ticker_has_fresh_market_state_and_a_known_instrument() {
        seedMarketState(redis, ENRICH_TICKER, 5_000L, 100.0, 101.0);

        publish(trade("T-ENRICH-1", ENRICH_TICKER, 5_000L));

        ConsumerRecord<String, EnrichedTradeEvent> enriched = readOneEnriched(ENRICH_TICKER);
        assertThat(enriched.value().getMidPriceAtExecution()).isEqualTo(100.5);
        assertThat(enriched.value().getVwap5Min()).isGreaterThan(0.0);
        assertThat(enriched.value().getMarketCap()).isGreaterThan(0.0);
        assertThat(enriched.value().getMarketDataAgeMs()).isEqualTo(0L);
    }

    @Test
    @DisplayName("should preserve the ticker key and the trace headers on the enriched record")
    void should_preserve_the_ticker_key_and_the_trace_headers_on_the_enriched_record() {
        seedMarketState(redis, HEADERS_TICKER, 5_000L, 100.0, 101.0);

        ProducerRecord<String, TradeEvent> record =
                new ProducerRecord<>(TRADE_TOPIC, HEADERS_TICKER, trade("T-HEADERS-1", HEADERS_TICKER, 5_000L));
        record.headers().add(new RecordHeader("traceparent", "trace-abc".getBytes()));
        record.headers().add(new RecordHeader("correlationId", "corr-abc".getBytes()));
        publish(record);

        ConsumerRecord<String, EnrichedTradeEvent> enriched = readOneEnriched(HEADERS_TICKER);
        assertThat(enriched.key()).isEqualTo(HEADERS_TICKER);
        assertThat(new String(enriched.headers().lastHeader("traceparent").value())).isEqualTo("trace-abc");
        assertThat(new String(enriched.headers().lastHeader("correlationId").value())).isEqualTo("corr-abc");
    }

    @Test
    @DisplayName("should quarantine a trade whose ticker has no projected market state")
    void should_quarantine_a_trade_whose_ticker_has_no_projected_market_state() {
        publish(trade("T-TICK-ABSENT-1", TICK_ABSENT_TICKER, 5_000L));

        assertThat(readOneDeadLetter(TICK_ABSENT_TICKER).value().getFailureReason())
                .contains("tick_absent");
    }

    @Test
    @DisplayName("should quarantine a trade whose cached tick is older than the freshness limit")
    void should_quarantine_a_trade_whose_cached_tick_is_older_than_the_freshness_limit() {
        // The service's configured limit is 30s; 60s old is unambiguously stale.
        seedMarketState(redis, STALE_TICKER, 5_000L, 100.0, 101.0);

        publish(trade("T-STALE-1", STALE_TICKER, 65_000L));

        assertThat(readOneDeadLetter(STALE_TICKER).value().getFailureReason()).contains("stale");
    }

    @Test
    @DisplayName("should quarantine a trade whose cached tick postdates it")
    void should_quarantine_a_trade_whose_cached_tick_postdates_it() {
        seedMarketState(redis, FUTURE_TICKER, 10_000L, 100.0, 101.0);

        publish(trade("T-FUTURE-1", FUTURE_TICKER, 5_000L));

        assertThat(readOneDeadLetter(FUTURE_TICKER).value().getFailureReason()).contains("future");
    }

    @Test
    @DisplayName("should quarantine a trade whose rolling window carries no volume")
    void should_quarantine_a_trade_whose_rolling_window_carries_no_volume() {
        // Tick key seeded, window key deliberately absent: windowVolume folds to zero.
        redis.opsForHash().putAll(MarketCacheKeys.tickKey(WINDOW_EMPTY_TICKER), Map.of(
                "eventTimestamp", "5000",
                "bidPrice", "100.0",
                "askPrice", "101.0",
                "lastTradedPrice", "100.5"));

        publish(trade("T-WINDOW-EMPTY-1", WINDOW_EMPTY_TICKER, 5_000L));

        assertThat(readOneDeadLetter(WINDOW_EMPTY_TICKER).value().getFailureReason())
                .contains("window_empty");
    }

    @Test
    @DisplayName("should quarantine a trade for a ticker the instrument master does not carry")
    void should_quarantine_a_trade_for_a_ticker_the_instrument_master_does_not_carry() {
        seedMarketState(redis, INSTRUMENT_MISSING_TICKER, 5_000L, 100.0, 101.0);

        publish(trade("T-INSTRUMENT-MISSING-1", INSTRUMENT_MISSING_TICKER, 5_000L));

        assertThat(readOneDeadLetter(INSTRUMENT_MISSING_TICKER).value().getFailureReason())
                .contains("instrument_missing");
    }

    @Test
    @DisplayName("should quarantine a malformed payload with the bytes the broker delivered")
    void should_quarantine_a_malformed_payload_with_the_bytes_the_broker_delivered() {
        publishRaw("POISON", POISON);

        ConsumerRecord<String, DeadLetterEvent> quarantined = readOneDeadLetter("POISON");

        // ErrorHandlingDeserializer nulls the record value and carries the delivered bytes on the
        // exception. A recoverer that passes record.value() through would quarantine an empty
        // payload, which is the same defect as no quarantine record at all.
        assertThat(quarantined.value().getOriginalPayload().array()).isEqualTo(POISON);
    }

    @Test
    @DisplayName("should pause the container during a Redis outage rather than dead-letter a good trade")
    void should_pause_the_container_during_a_redis_outage_rather_than_dead_letter_a_good_trade() {
        seedMarketState(redis, OUTAGE_TICKER, 5_000L, 100.0, 101.0);

        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            publish(trade("T-OUTAGE-1", OUTAGE_TICKER, 5_000L));

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertThat(listenerContainer().isContainerPaused())
                            .as("only ContainerPausingBackOffHandler pauses the container, and the "
                                    + "error handler reaches it only by classifying a real "
                                    + "connection failure or command timeout as a redis outage")
                            .isTrue());

            assertThat(readDeadLetterKeys())
                    .as("a connection failure is not a poison record, and a dead letter here would "
                            + "be a lie about a trade that was never bad")
                    .doesNotContain("T-OUTAGE-1");
        } finally {
            REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
        }

        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(readEnrichedTradeIds()).contains("T-OUTAGE-1"));
    }

    @Test
    @DisplayName("should not deliver any trade before the instrument master has been folded")
    void should_not_deliver_any_trade_before_the_instrument_master_has_been_folded() {
        // The 20 GATE_TICKER trades and the GATE_TICKER instrument were published in @BeforeAll,
        // before this class's Spring context, and therefore the readiness gate inside it, ever
        // started. If the trade listener had started before the loader finished folding the
        // 2000-plus-record master, it would have consumed these with an empty or partial map and
        // dead-lettered every one of them with instrument_missing. Touching @Autowired here is what
        // forces the context, and therefore the gate, to have already run to completion by the time
        // this assertion executes.
        assertThat(listenerRegistry).isNotNull();

        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(readEnrichedTradeIds()).as("the gate must let every GATE_TICKER trade "
                                + "through once the master is loaded, not just avoid dead-lettering it")
                        .contains("GATE-0", "GATE-19"));

        assertThat(readDeadLetterKeys())
                .as("a dead letter for a GATE_TICKER trade here means the listener consumed at "
                        + "least one of them before the instrument master was fully folded")
                .noneMatch(key -> key != null && key.equals(GATE_TICKER));
    }

    private static void seedMarketState(StringRedisTemplate template, String ticker,
                                        long eventTimestampMillis, double bid, double ask) {
        template.opsForHash().putAll(MarketCacheKeys.tickKey(ticker), Map.of(
                "eventTimestamp", String.valueOf(eventTimestampMillis),
                "bidPrice", String.valueOf(bid),
                "askPrice", String.valueOf(ask),
                "lastTradedPrice", String.valueOf((bid + ask) / 2.0)));
        long bucket = MarketCacheKeys.bucketFor(eventTimestampMillis);
        template.opsForHash().putAll(MarketCacheKeys.windowKey(ticker), Map.of(
                bucket + ":pv", "1000.0",
                bucket + ":v", "10.0"));
    }

    private MessageListenerContainer listenerContainer() {
        return listenerRegistry.getListenerContainer(RawTradeConsumer.LISTENER_ID);
    }

    private static TradeEvent trade(String tradeId, String ticker, long eventTimestampMillis) {
        return TradeEvent.newBuilder()
                .setTradeId(tradeId)
                .setCorrelationId("corr-" + tradeId)
                .setTicker(ticker)
                .setQuantity(10L)
                .setPrice(100.5)
                .setSide(Side.BUY)
                .setTraderId("TR-1")
                .setAccountId("AC-1")
                .setEventTimestamp(Instant.ofEpochMilli(eventTimestampMillis))
                .setProducedAt(Instant.ofEpochMilli(eventTimestampMillis + 5))
                .build();
    }

    private static InstrumentReferenceEvent instrument(String instrumentId, String ticker) {
        return InstrumentReferenceEvent.newBuilder()
                .setInstrumentId(instrumentId)
                .setTicker(ticker)
                .setExchange("NSE")
                .setIsin(instrumentId)
                .setSecurityType("EQUITY")
                .setCurrency("INR")
                .setSector("ENERGY")
                .setSharesOutstanding(SHARES_OUTSTANDING)
                .setReferenceVersion(1L)
                .setEffectiveAt(Instant.ofEpochMilli(1_000L))
                .setProducerIdentity("reference-data-service")
                .build();
    }

    private static void publish(TradeEvent trade) {
        publish(new ProducerRecord<>(TRADE_TOPIC, trade.getTicker().toString(), trade));
    }

    private static void publish(ProducerRecord<String, TradeEvent> record) {
        try (KafkaProducer<String, TradeEvent> producer = new KafkaProducer<>(tradeProducerProperties())) {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish the trade", e);
        }
    }

    private static void publishRaw(String key, byte[] payload) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(TRADE_TOPIC, key, payload)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish the poison record", e);
        }
    }

    /**
     * Filters by key rather than returning the first record on the topic. The 20 GATE_TICKER
     * trades seeded in {@code @BeforeAll} land on this same shared {@link #OUTPUT_TOPIC} as soon as
     * the readiness gate opens, ahead of whichever test happens to trigger context startup, so the
     * first record on the topic is not reliably this test's own.
     */
    private static ConsumerRecord<String, EnrichedTradeEvent> readOneEnriched(String key) {
        try (KafkaConsumer<String, EnrichedTradeEvent> consumer = enrichedConsumer()) {
            consumer.subscribe(List.of(OUTPUT_TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, EnrichedTradeEvent> record :
                        consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No record keyed " + key + " reached " + OUTPUT_TOPIC + " within 30s");
    }

    private static List<String> readEnrichedTradeIds() {
        try (KafkaConsumer<String, EnrichedTradeEvent> consumer = enrichedConsumer()) {
            consumer.subscribe(List.of(OUTPUT_TOPIC));
            List<String> ids = new java.util.ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, EnrichedTradeEvent> record :
                        consumer.poll(Duration.ofMillis(500))) {
                    ids.add(record.value().getTrade().getTradeId().toString());
                }
            }
            return ids;
        }
    }

    /**
     * Filters by key rather than returning the first record on the topic. Every quarantine test in
     * this class shares {@link #DLQ_TOPIC}, so without filtering, whichever test's dead letter
     * happens to land first is the one every later test reads, regardless of which trade it published.
     */
    private static ConsumerRecord<String, DeadLetterEvent> readOneDeadLetter(String key) {
        try (KafkaConsumer<String, DeadLetterEvent> consumer = deadLetterConsumer()) {
            consumer.subscribe(List.of(DLQ_TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, DeadLetterEvent> record :
                        consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No record keyed " + key + " reached " + DLQ_TOPIC + " within 30s");
    }

    private static List<String> readDeadLetterKeys() {
        try (KafkaConsumer<String, DeadLetterEvent> consumer = deadLetterConsumer()) {
            consumer.subscribe(List.of(DLQ_TOPIC));
            List<String> keys = new java.util.ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, DeadLetterEvent> record :
                        consumer.poll(Duration.ofMillis(500))) {
                    keys.add(record.key());
                }
            }
            return keys;
        }
    }

    private static KafkaConsumer<String, EnrichedTradeEvent> enrichedConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "enriched-reader-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new KafkaConsumer<>(properties);
    }

    private static KafkaConsumer<String, DeadLetterEvent> deadLetterConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-reader-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new KafkaConsumer<>(properties);
    }

    private static Properties tradeProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        return properties;
    }

    private static Properties referenceProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        return properties;
    }
}
