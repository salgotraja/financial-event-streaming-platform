package dev.engnotes.fes.marketdatacacheprojector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.events.MarketDataTickEvent;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The consumer against a real broker, a real Schema Registry and a real Redis: a healthy tick is
 * projected, a redelivered tick has one effect, a malformed record is quarantined with its bytes
 * intact, and a Redis outage never produces a dead letter.
 *
 * <p>The topic is unique to this class. The service's real topic is shared with the simulator's
 * tests, and consuming their traffic here would race every assertion against records this test
 * never published.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("MarketDataTickConsumer against a real broker, registry and Redis")
class MarketDataTickConsumerIntegrationTest {

    private static final String TOPIC = "mdcp-it-" + UUID.randomUUID();
    private static final String DLQ_TOPIC = TOPIC + ".dlq";
    private static final byte[] POISON = {0, 0, 0, 0, 1, 42};

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
        registry.add("fes.market-data-cache-projector.topic", () -> TOPIC);
    }

    @BeforeAll
    static void prepareBroker() throws Exception {
        KafkaAvroStack.start();
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(TOPIC, 1, (short) 1),
                    new NewTopic(DLQ_TOPIC, 1, (short) 1))).all().get();
        }
        try (CachedSchemaRegistryClient client =
                     new CachedSchemaRegistryClient(KafkaAvroStack.schemaRegistryUrl(), 10)) {
            client.register(TOPIC + "-value", new AvroSchema(MarketDataTickEvent.getClassSchema()));
            client.register(DLQ_TOPIC + "-value", new AvroSchema(DeadLetterEvent.getClassSchema()));
        }
    }

    @Test
    @DisplayName("should project a healthy tick into redis")
    void should_project_a_healthy_tick_into_redis() {
        publish(tick("PROJECT-OK", 5_000L, 101.5));

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(redis.opsForHash().entries(MarketStateProjection.KEY_PREFIX + "PROJECT-OK"))
                        .containsEntry("lastTradedPrice", "101.5"));
    }

    @Test
    @DisplayName("should have one effect when the same tick is delivered twice")
    void should_have_one_effect_when_the_same_tick_is_delivered_twice() {
        publish(tick("IDEMPOTENT", 5_000L, 101.5));
        publish(tick("IDEMPOTENT", 5_000L, 999.0));
        // The topic is single-partition, so this sentinel is processed strictly after both
        // IDEMPOTENT records above. Waiting for it in redis before asserting on IDEMPOTENT proves
        // the duplicate was already handled rather than racing whichever record lands first: the
        // straightforward "await lastTradedPrice=101.5" form is satisfied the moment the first
        // record is projected, possibly before the second has even been polled.
        publish(tick("IDEMPOTENT-SENTINEL", 5_000L, 1.0));

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(redis.opsForHash().entries(MarketStateProjection.KEY_PREFIX + "IDEMPOTENT-SENTINEL"))
                        .containsEntry("lastTradedPrice", "1.0"));

        assertThat(redis.opsForHash().entries(MarketStateProjection.KEY_PREFIX + "IDEMPOTENT"))
                .as("the second IDEMPOTENT record must have zero effect, having already been "
                        + "applied once with this timestamp")
                .containsEntry("lastTradedPrice", "101.5");
    }

    @Test
    @DisplayName("should quarantine a malformed record with its original bytes and keep the partition moving")
    void should_quarantine_a_malformed_record_with_its_original_bytes_and_keep_the_partition_moving() {
        publishRaw("POISON", POISON);
        publish(tick("AFTER-POISON", 6_000L, 202.5));

        ConsumerRecord<String, DeadLetterEvent> quarantined = readOneDeadLetter();

        assertThat(quarantined.value().getOriginalTopic()).hasToString(TOPIC);
        // ErrorHandlingDeserializer nulls the record value and carries the delivered bytes on the
        // exception. A recoverer that passes record.value() through quarantines an empty payload,
        // and the only copy of the evidence is gone.
        assertThat(quarantined.value().getOriginalPayload().array())
                .as("an empty quarantine record is the same defect as no quarantine record")
                .isEqualTo(POISON);

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(redis.opsForHash().entries(MarketStateProjection.KEY_PREFIX + "AFTER-POISON"))
                        .as("the offset must advance past the poison record")
                        .containsEntry("lastTradedPrice", "202.5"));
    }

    @Test
    @DisplayName("should not quarantine a valid tick when redis is unreachable")
    void should_not_quarantine_a_valid_tick_when_redis_is_unreachable() {
        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            publish(tick("OUTAGE", 7_000L, 303.5));

            // 15s against a 2s redis command timeout is ample for a QueryTimeoutException to fire
            // and reach isRedisOutage, and for that classification to actually pause the container:
            // a bounded retry sequence would instead have exhausted within the 5s poisonBackOff
            // elapsed cap and quarantined the record.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertThat(listenerContainer().isContainerPaused())
                            .as("only ContainerPausingBackOffHandler pauses the container, and the "
                                    + "error handler reaches it only by classifying a real "
                                    + "QueryTimeoutException as a redis outage; a container that "
                                    + "never pauses means that branch never engaged")
                            .isTrue());

            assertThat(readDeadLetterKeys())
                    .as("a connection failure is not a poison record, and a dead letter here would "
                            + "be a lie about a record that was never bad")
                    .doesNotContain("OUTAGE");
        } finally {
            REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
        }

        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(redis.opsForHash().entries(MarketStateProjection.KEY_PREFIX + "OUTAGE"))
                        .as("the unlimited backoff keeps retrying the uncommitted record, and it is "
                                + "redelivered successfully once redis answers again")
                        .containsEntry("lastTradedPrice", "303.5"));
    }

    private MessageListenerContainer listenerContainer() {
        return listenerRegistry.getListenerContainers().iterator().next();
    }

    private static MarketDataTickEvent tick(String ticker, long eventTimestampMillis, double price) {
        return MarketDataTickEvent.newBuilder()
                .setTicker(ticker)
                .setBidPrice(price - 0.5)
                .setAskPrice(price + 0.5)
                .setLastTradedPrice(price)
                .setVolume(100L)
                .setEventTimestamp(Instant.ofEpochMilli(eventTimestampMillis))
                .setProducedAt(Instant.ofEpochMilli(eventTimestampMillis + 5))
                .setCorrelationId("corr-" + ticker)
                .build();
    }

    private static void publish(MarketDataTickEvent tick) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        try (KafkaProducer<String, MarketDataTickEvent> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(TOPIC, tick.getTicker().toString(), tick)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish the tick", e);
        }
    }

    private static void publishRaw(String key, byte[] payload) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(TOPIC, key, payload)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish the poison record", e);
        }
    }

    private static ConsumerRecord<String, DeadLetterEvent> readOneDeadLetter() {
        try (KafkaConsumer<String, DeadLetterEvent> consumer = deadLetterConsumer()) {
            consumer.subscribe(List.of(DLQ_TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, DeadLetterEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, DeadLetterEvent> record : records) {
                    return record;
                }
            }
        }
        throw new AssertionError("No record reached " + DLQ_TOPIC + " within 30s");
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
}
