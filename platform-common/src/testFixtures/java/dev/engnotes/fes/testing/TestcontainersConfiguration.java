package dev.engnotes.fes.testing;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.grafana.LgtmStackContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared container wiring for integration tests across every service module.
 *
 * <p>Testcontainers rather than {@code @EmbeddedKafka} and rather than H2, so tests run against the
 * same Kafka and PostgreSQL implementations as the deployed system.
 *
 * <p>Image tags are pinned. A floating {@code :latest} makes a green build depend on when it ran.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    private static final String KAFKA_IMAGE = "apache/kafka-native:4.1.0";
    private static final String POSTGRES_IMAGE = "postgres:16-alpine";
    private static final String LGTM_IMAGE = "grafana/otel-lgtm:0.11.4";
    private static final String ZIPKIN_IMAGE = "openzipkin/zipkin:3.4";

    @Bean
    @ServiceConnection
    LgtmStackContainer grafanaLgtmContainer() {
        return new LgtmStackContainer(DockerImageName.parse(LGTM_IMAGE));
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        // withStartupAttempts for the same reason KafkaAvroStack carries it: this is the GraalVM
        // native broker build, and it can segfault during launch on a CI runner.
        return new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withStartupAttempts(3);
    }

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer(DockerImageName.parse(POSTGRES_IMAGE));
    }

    @Bean
    @ServiceConnection(name = "openzipkin/zipkin")
    GenericContainer<?> zipkinContainer() {
        return new GenericContainer<>(DockerImageName.parse(ZIPKIN_IMAGE)).withExposedPorts(9411);
    }
}
