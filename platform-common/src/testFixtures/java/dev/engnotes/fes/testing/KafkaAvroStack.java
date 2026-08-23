package dev.engnotes.fes.testing;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A real Kafka broker and a real Confluent Schema Registry, shared by every service's integration
 * tests.
 *
 * <p>Schema Registry is not optional scaffolding here. The platform serialises financial payloads
 * with Avro against a registry (ADR-002), and a producer test that skips the registry proves the
 * object graph works while leaving the part that actually breaks in production, schema resolution
 * and subject compatibility, untested.
 *
 * <p>Containers are started once per JVM and deliberately never stopped. Ryuk reaps them when the
 * run ends, and reusing one broker across test classes costs seconds rather than tens of seconds.
 *
 * <p>The broker publishes two listeners. Tests on the host connect through the mapped port from
 * {@link #bootstrapServers()}; Schema Registry, which lives on the container network, connects to
 * {@code kafka:19092}. Without the second listener the registry would receive a {@code localhost}
 * advertised address and fail to reach the broker.
 */
public final class KafkaAvroStack {

    private static final String KAFKA_IMAGE = "apache/kafka-native:4.1.0";
    private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.9.1";

    private static final String KAFKA_ALIAS = "kafka";
    private static final int KAFKA_INTERNAL_PORT = 19092;
    private static final int SCHEMA_REGISTRY_PORT = 8081;

    private static final Network NETWORK = Network.newNetwork();

    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases(KAFKA_ALIAS)
            .withListener(KAFKA_ALIAS + ":" + KAFKA_INTERNAL_PORT)
            // The broker image is a GraalVM native build, and it occasionally segfaults during
            // launch on a CI runner. Testcontainers then times out waiting for the RECOVERY to
            // RUNNING line and fails the whole class with an initializationError, which reads like
            // a test failure and is not one. startupAttempts defaults to 1, so a single crash ends
            // the run; each retry discards the dead container and starts a fresh one. Observed once
            // on a pull request whose next run against identical code passed.
            .withStartupAttempts(3);

    private static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("schema-registry")
                    .withExposedPorts(SCHEMA_REGISTRY_PORT)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            "PLAINTEXT://" + KAFKA_ALIAS + ":" + KAFKA_INTERNAL_PORT)
                    .waitingFor(Wait.forHttp("/subjects").forPort(SCHEMA_REGISTRY_PORT));

    static {
        KAFKA.start();
        SCHEMA_REGISTRY.start();
    }

    private KafkaAvroStack() {
    }

    /** Forces class initialisation, and therefore container startup, before properties are read. */
    public static void start() {
        // Static initialiser does the work.
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    public static String schemaRegistryUrl() {
        return "http://%s:%d".formatted(
                SCHEMA_REGISTRY.getHost(), SCHEMA_REGISTRY.getMappedPort(SCHEMA_REGISTRY_PORT));
    }
}
