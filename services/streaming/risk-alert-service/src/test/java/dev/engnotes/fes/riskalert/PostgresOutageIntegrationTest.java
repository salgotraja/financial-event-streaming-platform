package dev.engnotes.fes.riskalert;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import javax.sql.DataSource;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.testing.KafkaAvroStack;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A PostgreSQL outage against a real broker and a real, container-local PostgreSQL (ADR-027,
 * ADR-036): the container pauses rather than dead-letters a good trade while the database is
 * unreachable, and resumes processing once it comes back.
 *
 * <p>A container local to this class, not {@code PostgresStack}. That stack is started once per
 * JVM and shared by every other integration test in this module, and Docker-pausing it would freeze
 * the database out from under any other test class running in the same JVM. Pausing a container
 * this class alone owns is what makes the outage safe to simulate.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("RiskAlertKafkaConfiguration against a real broker and a real PostgreSQL outage")
class PostgresOutageIntegrationTest {

    private static final String TRADE_TOPIC = "ras-outage-it-" + UUID.randomUUID();
    private static final String RULE_TOPIC = "ras-outage-rules-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "ras-outage-out-it-" + UUID.randomUUID();
    private static final String DLQ_TOPIC = TRADE_TOPIC + DeadLetterPublisher.DLQ_SUFFIX;

    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("risk_alert")
                    .withUsername("risk_alert_service")
                    .withPassword("risk_alert_service");

    @Autowired
    private DataSource dataSource;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        POSTGRES.start();
        RiskAlertTestKafka.createTopic(TRADE_TOPIC, 1);
        RiskAlertTestKafka.createTopic(RULE_TOPIC, 6);
        RiskAlertTestKafka.createTopic(OUTPUT_TOPIC, 1);
        RiskAlertTestKafka.createTopic(DLQ_TOPIC, 1);
        RiskAlertTestKafka.registerSchema(OUTPUT_TOPIC, RiskAlertEvent.getClassSchema());
        RiskAlertTestKafka.registerSchema(DLQ_TOPIC, DeadLetterEvent.getClassSchema());
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("fes.risk-alert-service.topic", () -> TRADE_TOPIC);
        registry.add("fes.risk-alert-service.rule-topic", () -> RULE_TOPIC);
        registry.add("fes.risk-alert-service.output-topic", () -> OUTPUT_TOPIC);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        // A paused container keeps its TCP connections open but stops answering on them, so a
        // query already in flight over an idle pooled connection would otherwise hang until the
        // OS's own TCP retransmission timeout, tens of minutes away, rather than surface as the
        // QueryTimeoutException isPostgresOutage classifies. socketTimeout bounds that read.
        registry.add("spring.datasource.hikari.data-source-properties.socketTimeout", () -> "3");
        // Bounds acquiring a new physical connection while the container is paused and none of the
        // pool's existing connections are idle and usable, so CannotGetJdbcConnectionException
        // surfaces within the test's own await window rather than Hikari's 30s default.
        registry.add("spring.datasource.hikari.connection-timeout", () -> "3000");
    }

    @BeforeEach
    void clearPositions() {
        JdbcClient jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE risk_position_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_position").update();
    }

    private static KafkaConsumer<String, DeadLetterEvent> dlqConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "assert-outage-dlq-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put("specific.avro.reader", true);
        KafkaConsumer<String, DeadLetterEvent> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(DLQ_TOPIC));
        return consumer;
    }

    private static List<String> deadLetters(KafkaConsumer<String, DeadLetterEvent> dlq) {
        ConsumerRecords<String, DeadLetterEvent> polled = dlq.poll(Duration.ofMillis(500));
        List<String> keys = new ArrayList<>();
        polled.forEach(record -> keys.add(record.key()));
        return keys;
    }

    private MessageListenerContainer listenerContainer() {
        return listenerRegistry.getListenerContainer(EnrichedTradeConsumer.LISTENER_ID);
    }

    @Test
    void should_pause_the_container_during_a_postgres_outage_rather_than_dead_letter_a_good_trade() {
        POSTGRES.getDockerClient().pauseContainerCmd(POSTGRES.getContainerId()).exec();
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, DeadLetterEvent> dlq = dlqConsumer()) {

            producer.send(new ProducerRecord<>(TRADE_TOPIC, "OUTAGE-TICK",
                    RiskAlertTestKafka.trade("T-OUTAGE-1", "OUTAGE-TICK", 1.0,
                            Instant.ofEpochMilli(1_000L))));
            producer.flush();

            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertThat(listenerContainer().isContainerPaused())
                            .as("only ContainerPausingBackOffHandler pauses the container, and the "
                                    + "error handler reaches it only by classifying a real "
                                    + "connection failure or statement timeout as a postgres outage")
                            .isTrue());

            assertThat(deadLetters(dlq))
                    .as("a database outage must never dead-letter a trade that was never bad (ADR-027)")
                    .isEmpty();
        } finally {
            POSTGRES.getDockerClient().unpauseContainerCmd(POSTGRES.getContainerId()).exec();
        }

        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(JdbcClient.create(dataSource)
                        .sql("SELECT net_quantity_after FROM risk_position_applied_trade WHERE trade_id = ?")
                        .param("T-OUTAGE-1")
                        .query(Long.class)
                        .optional())
                        .as("the recoverer is only reached by way of the poison branch, so a row "
                                + "here proves the trade was applied through the ordinary success "
                                + "path once the container resumed, not quarantined")
                        .isPresent());
    }
}
