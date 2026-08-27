package dev.engnotes.fes.riskalert;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.testing.KafkaAvroStack;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared broker helpers for the risk-alert-service round-trip tests, topic-parameterised so
 * {@code EnrichedTradeConsumerIntegrationTest} and {@code QuarantineIntegrationTest} (Task 9) can
 * each use their own topics without racing one another.
 */
final class RiskAlertTestKafka {

    private RiskAlertTestKafka() {
    }

    /**
     * Creates a topic before the context starts. {@code RuleTimelineLoader} assigns partitions
     * directly rather than subscribing, so it fails fast with "has no partitions" if the rule
     * topic does not exist yet at startup; auto-creation on first metadata lookup is not
     * synchronous enough to beat that check. {@code RuleTimelineLoaderIntegrationTest} in Task 7
     * creates its topic the same way for the same reason.
     *
     * <p>Idempotent: a second {@code createTopics} call for a topic that already exists fails
     * with {@code TopicExistsException} wrapped in an {@code ExecutionException}. That outcome
     * means the precondition this method exists to guarantee already holds, so it is swallowed
     * rather than treated as a setup failure, which is what lets {@code @DynamicPropertySource}
     * call it freely without tracking whether a given topic name has already been created.
     */
    static void createTopic(String topic, int partitions) {
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        try (Admin admin = Admin.create(adminProperties)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                return;
            }
            throw new IllegalStateException("Could not create " + topic, e);
        } catch (Exception e) {
            throw new IllegalStateException("Could not create " + topic, e);
        }
    }

    /**
     * Registers a subject before the context starts. {@code application.yml} sets
     * {@code auto.register.schemas: false} for this service's own producer (a deliberate production
     * setting, not relaxed for tests), so a subject the service's own {@code KafkaTemplate} writes to
     * must already exist in the registry or the first send fails with a schema-not-found
     * {@code SerializationException}. Only the output topic needs this: the test's own producer,
     * from {@link #producer()}, uses the Confluent client's default {@code auto.register.schemas=true}
     * and self-registers whatever it writes to the trade and rule topics.
     * {@code RawTradeConsumerIntegrationTest} in {@code trade-enrichment-service} registers its
     * output subject the same way for the same reason.
     */
    static void registerSchema(String topic, org.apache.avro.Schema schema) {
        try (CachedSchemaRegistryClient client =
                     new CachedSchemaRegistryClient(KafkaAvroStack.schemaRegistryUrl(), 10)) {
            client.register(topic + "-value", new AvroSchema(schema));
        } catch (Exception e) {
            throw new IllegalStateException("Could not register schema for " + topic, e);
        }
    }

    static <T> KafkaProducer<String, T> producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        return new KafkaProducer<>(properties);
    }

    /**
     * Positioned at the topic's current end, not its beginning. Every test in
     * {@code EnrichedTradeConsumerIntegrationTest} shares one Spring context and one output topic,
     * so an earliest-offset consumer here would also read every alert an earlier test in the class
     * already published, and {@code drain}'s exact {@code hasSize(expected)} would then never
     * settle. {@code seekToEnd} is lazy until the position is actually read, so {@code position} is
     * called on each partition immediately to resolve it before this consumer is handed back to a
     * test that may publish within milliseconds.
     */
    static KafkaConsumer<String, RiskAlertEvent> alertConsumer(String outputTopic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "assert-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put("specific.avro.reader", true);
        KafkaConsumer<String, RiskAlertEvent> consumer = new KafkaConsumer<>(properties);

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(outputTopic);
        List<TopicPartition> partitions = partitionInfos.stream()
                .map(info -> new TopicPartition(outputTopic, info.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position);
        return consumer;
    }

    static EnrichedTradeEvent trade(String tradeId, String ticker, double deviation,
                                    Instant eventTimestamp) {
        TradeEvent source = TradeEvent.newBuilder()
                .setTradeId(tradeId)
                .setCorrelationId("corr-" + tradeId)
                .setTicker(ticker)
                .setQuantity(100L)
                .setPrice(2500.0)
                .setSide(Side.BUY)
                .setTraderId("trader-1")
                .setAccountId("account-1")
                .setEventTimestamp(eventTimestamp)
                .setProducedAt(eventTimestamp)
                .build();

        return EnrichedTradeEvent.newBuilder()
                .setTrade(source)
                .setMidPriceAtExecution(2450.0)
                .setSpreadAtExecution(0.5)
                .setVwap5Min(2460.0)
                .setMarketCap(1_700_000.0)
                .setPriceDeviation(deviation)
                .setEnrichedAt(eventTimestamp)
                .setEnrichmentLatencyMs(1L)
                .setMarketDataAgeMs(50L)
                .build();
    }

    static List<RiskAlertEvent> drain(KafkaConsumer<String, RiskAlertEvent> consumer,
                                      int expected, Duration within) {
        List<RiskAlertEvent> alerts = new ArrayList<>();
        Awaitility.await().atMost(within).untilAsserted(() -> {
            ConsumerRecords<String, RiskAlertEvent> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(record -> alerts.add(record.value()));
            assertThat(alerts).hasSize(expected);
        });
        return alerts;
    }
}
