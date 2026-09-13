package dev.engnotes.fes.positionexposure;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.PositionSnapshotEvent;
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
 * Shared broker helpers for the position-exposure-service round-trip tests, topic-parameterised so
 * each integration test class can use its own topics without racing another one, following
 * {@code RiskAlertTestKafka} in risk-alert-service.
 */
final class PositionExposureTestKafka {

    private PositionExposureTestKafka() {
    }

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
     * {@code auto.register.schemas: false} for this service's own producer, so a subject the
     * service's own {@code KafkaTemplate} writes to must already exist in the registry or the first
     * send fails with a schema-not-found {@code SerializationException}.
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
     * Positioned at the topic's current end, not its beginning, following
     * {@code RiskAlertTestKafka.alertConsumer}: every test sharing one Spring context and one output
     * topic must not read snapshots an earlier test already published.
     */
    static KafkaConsumer<String, PositionSnapshotEvent> snapshotConsumer(String outputTopic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "assert-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put("specific.avro.reader", true);
        KafkaConsumer<String, PositionSnapshotEvent> consumer = new KafkaConsumer<>(properties);

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(outputTopic);
        List<TopicPartition> partitions = partitionInfos.stream()
                .map(info -> new TopicPartition(outputTopic, info.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position);
        return consumer;
    }

    static EnrichedTradeEvent trade(String tradeId, String accountId, String traderId, String ticker,
                                    Side side, long quantity, double midPrice, Instant eventTimestamp) {
        TradeEvent source = TradeEvent.newBuilder()
                .setTradeId(tradeId)
                .setCorrelationId("corr-" + tradeId)
                .setTicker(ticker)
                .setQuantity(quantity)
                .setPrice(midPrice)
                .setSide(side)
                .setTraderId(traderId)
                .setAccountId(accountId)
                .setEventTimestamp(eventTimestamp)
                .setProducedAt(eventTimestamp)
                .build();

        return EnrichedTradeEvent.newBuilder()
                .setTrade(source)
                .setMidPriceAtExecution(midPrice)
                .setSpreadAtExecution(0.5)
                .setVwap5Min(midPrice)
                .setMarketCap(1_700_000.0)
                .setPriceDeviation(0.0)
                .setEnrichedAt(eventTimestamp)
                .setEnrichmentLatencyMs(1L)
                .setMarketDataAgeMs(50L)
                .build();
    }

    static List<PositionSnapshotEvent> drain(KafkaConsumer<String, PositionSnapshotEvent> consumer,
                                             int expected, Duration within) {
        List<PositionSnapshotEvent> snapshots = new ArrayList<>();
        Awaitility.await().atMost(within).untilAsserted(() -> {
            ConsumerRecords<String, PositionSnapshotEvent> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(record -> snapshots.add(record.value()));
            assertThat(snapshots).hasSize(expected);
        });
        return snapshots;
    }

    static List<org.apache.kafka.clients.consumer.ConsumerRecord<String, PositionSnapshotEvent>> drainRecords(
            KafkaConsumer<String, PositionSnapshotEvent> consumer, int expected, Duration within) {
        List<org.apache.kafka.clients.consumer.ConsumerRecord<String, PositionSnapshotEvent>> records =
                new ArrayList<>();
        Awaitility.await().atMost(within).untilAsserted(() -> {
            ConsumerRecords<String, PositionSnapshotEvent> polled = consumer.poll(Duration.ofMillis(500));
            polled.forEach(records::add);
            assertThat(records).hasSize(expected);
        });
        return records;
    }
}
