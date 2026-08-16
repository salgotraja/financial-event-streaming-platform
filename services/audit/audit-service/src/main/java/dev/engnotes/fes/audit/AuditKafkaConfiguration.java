package dev.engnotes.fes.audit;

import java.util.Map;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.common.kafka.FailureTracker;
import dev.engnotes.fes.events.DeadLetterEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Consumer-side wiring: what a failure costs, and where a record goes when it cannot be archived.
 *
 * <p>Retry is bounded at three attempts because the failures worth retrying here are transient sink
 * and registry errors, and a longer sequence holds up the partition without changing the outcome. A
 * decode failure is not retried at all: the bytes do not improve.
 *
 * <p>The recoverer waits for the dead-letter send to complete. Quarantine failing silently while the
 * offset advances is the one outcome an evidence archive cannot have, so a failed send propagates
 * and the container stops rather than skipping a record that was archived nowhere.
 */
@Configuration(proxyBeanMethods = false)
public class AuditKafkaConfiguration {

    // Three attempts total, so two retries after the first failure.
    private static final long MAX_RETRIES = 2;
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 5_000;
    private static final long MAX_ELAPSED_MS = 5_000;

    @Bean
    KafkaAvroDeserializer auditPayloadDeserializer(
            @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {

        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                // Generic, not specific: this service classifies every evidence topic and must not
                // need a compile-time dependency on each schema to do it.
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false), false);
        return deserializer;
    }

    @Bean
    FailureTracker failureTracker() {
        return new FailureTracker();
    }

    @Bean
    DeadLetterPublisher deadLetterPublisher(KafkaTemplate<String, DeadLetterEvent> kafkaTemplate,
                                            FailureTracker failureTracker,
                                            AuditProperties properties,
                                            @Value("${spring.kafka.consumer.group-id}") String consumerGroup) {

        return new DeadLetterPublisher(kafkaTemplate, failureTracker, consumerGroup,
                properties.consumerInstance());
    }

    @Bean
    DefaultErrorHandler auditErrorHandler(DeadLetterPublisher deadLetterPublisher,
                                          FailureTracker failureTracker) {

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> quarantine(deadLetterPublisher, record, exception),
                backOff());
        errorHandler.addNotRetryableExceptions(AuditDecodeException.class);
        errorHandler.setRetryListeners(failureTracker);
        // The container commits the recovered record's offset, so one poison payload does not block
        // the partition behind it.
        errorHandler.setAckAfterHandle(true);
        return errorHandler;
    }

    @SuppressWarnings("unchecked")
    private static void quarantine(DeadLetterPublisher publisher,
                                   ConsumerRecord<?, ?> record,
                                   Exception exception) {

        ConsumerRecord<String, byte[]> failed = (ConsumerRecord<String, byte[]>) record;
        publisher.publish(failed, failed.value(), exception).join();
    }

    private static BackOff backOff() {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(INITIAL_BACKOFF_MS);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(MAX_BACKOFF_MS);
        backOff.setMaxElapsedTime(MAX_ELAPSED_MS);
        backOff.setMaxAttempts(MAX_RETRIES);
        return backOff;
    }
}
