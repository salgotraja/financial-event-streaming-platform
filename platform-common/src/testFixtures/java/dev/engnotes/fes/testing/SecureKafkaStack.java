package dev.engnotes.fes.testing;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The local {@code strict-security} broker: authenticated clients and per-service Kafka ACLs, so
 * service-identity failures can be tested without AWS (NFR-05.5).
 *
 * <p>Cloud deployments authorise with MSK IAM; ACLs exist for exactly this profile (ADR-009). What a
 * test here proves is the policy, not the deployment: that a workload identity can do only what its
 * committed {@code kafka-acls.yml} allows. The fixture also carries a second, authenticated listener
 * on a Docker network, so a service container can connect and prove it runs as that identity too.
 *
 * <p><strong>Never configure the in-network listener with {@code withListener}.</strong> That method
 * names the listener {@code TC-0} and maps it to PLAINTEXT unconditionally. An unauthenticated
 * in-network listener would let a client authenticate as ANONYMOUS, a super user on this broker, and
 * every denial assertion in the repository would then pass without being true.
 *
 * <p><strong>The image is the JVM one, deliberately.</strong> {@link KafkaAvroStack} uses
 * {@code apache/kafka-native} because it starts faster, but the native build cannot act as a SASL
 * server: {@code SaslServerAuthenticator.createSaslServer} fails to load the {@code Subject} methods
 * it needs under GraalVM, and the client sees only a connection dropped mid-authentication. Do not
 * "align" this back to the native image.
 *
 * <p>The broker denies by default. {@code allow.everyone.if.no.acl.found=false} is what makes an
 * absent grant a denial rather than a pass, and it is the single setting this whole fixture rests on.
 *
 * <p><strong>Grants are additive and nothing revokes them, so one authorization test class per JVM.</strong>
 * {@link #apply} only ever creates ACLs; the container is static and never reset. Gradle forks a test
 * JVM per module, so today each module's single authorization class gets a clean broker. Put two
 * such classes in one JVM and the second inherits the first's grants, which turns a denial assertion
 * into a pass without failing anything. If a module ever needs two, add a revoke step rather than
 * assuming isolation.
 */
public final class SecureKafkaStack {

    /** Pinned: an upstream release must not change what the authorization tests run against. */
    private static final String KAFKA_IMAGE = "apache/kafka:4.1.0";

    private static final String SUPER_USER = "admin";

    private static final Duration BLOCK_TIMEOUT = Duration.ofSeconds(20);

    /**
     * Every shipped service identity. The broker needs all credentials at startup, so this list is
     * here rather than per module; a service still only ever authenticates as its own principal.
     */
    private static final List<String> PRINCIPALS = List.of(
            "trade-producer",
            "market-data-simulator",
            "corporate-action-producer",
            "reference-data-service",
            "audit-service");

    private static final String KAFKA_ALIAS = "kafka";
    private static final int IN_NETWORK_PORT = 19092;
    private static final String IN_NETWORK_LISTENER = "INTERNAL";

    private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.9.1";
    private static final String SCHEMA_REGISTRY_ALIAS = "schema-registry";
    private static final int SCHEMA_REGISTRY_PORT = 8081;

    /** The starter script path Testcontainers' KafkaContainer writes. Private upstream. */
    private static final String STARTER_SCRIPT = "/tmp/testcontainers_start.sh";

    private static final Network NETWORK = Network.newNetwork();

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)) {

                /**
                 * Configured by hand rather than with {@code withListener}, which names the
                 * listener {@code TC-0} and maps it to PLAINTEXT unconditionally. An
                 * unauthenticated in-network listener would let a client authenticate as
                 * ANONYMOUS, a super user here, and every denial assertion in the repository
                 * would pass without being true.
                 */
                @Override
                protected void configure() {
                    super.configure();
                    // BROKER and CONTROLLER bind to loopback only: a single-node fixture never
                    // needs either reachable from a sibling container, and 0.0.0.0 here would let
                    // any container on NETWORK dial in as ANONYMOUS, a super user on this broker.
                    getEnvMap().put("KAFKA_LISTENERS",
                            "PLAINTEXT://0.0.0.0:9092,BROKER://127.0.0.1:9093,"
                                    + "CONTROLLER://127.0.0.1:9094,"
                                    + IN_NETWORK_LISTENER + "://0.0.0.0:" + IN_NETWORK_PORT);
                    getEnvMap().put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                            "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT,"
                                    + IN_NETWORK_LISTENER + ":SASL_PLAINTEXT");
                }

                /**
                 * Deliberately does not call super. The superclass writes the same starter script
                 * without the in-network advertised entry, and a listener Kafka does not advertise
                 * is one an in-network client is redirected away from.
                 */
                @Override
                protected void containerIsStarting(InspectContainerResponse containerInfo) {
                    // BROKER's advertised entry matches its loopback bind: the broker only ever
                    // connects to itself over this listener, so no other address is needed.
                    String advertised = String.join(",",
                            "PLAINTEXT://" + getBootstrapServers(),
                            "BROKER://127.0.0.1:9093",
                            IN_NETWORK_LISTENER + "://" + KAFKA_ALIAS + ":" + IN_NETWORK_PORT);
                    String command = "#!/bin/bash\n"
                            + "export KAFKA_ADVERTISED_LISTENERS=" + advertised + "\n"
                            + "/etc/kafka/docker/run \n";
                    copyFileToContainer(Transferable.of(command, 0777), STARTER_SCRIPT);
                }
            }
                    .withNetwork(NETWORK)
                    .withNetworkAliases(KAFKA_ALIAS)
                    .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                            "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT")
                    .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                    .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", brokerJaas())
                    .withEnv("KAFKA_LISTENER_NAME_" + IN_NETWORK_LISTENER
                            + "_PLAIN_SASL_JAAS_CONFIG", brokerJaas())
                    .withEnv("KAFKA_AUTHORIZER_CLASS_NAME",
                            "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
                    // ANONYMOUS covers the inter-broker PLAINTEXT listener only. Client tests always
                    // connect through an authenticated listener, and the zero-grant test proves an
                    // unauthorised client is denied rather than merely unable to connect.
                    .withEnv("KAFKA_SUPER_USERS", "User:" + SUPER_USER + ";User:ANONYMOUS")
                    .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false");

    static {
        KAFKA.start();
    }

    private SecureKafkaStack() {
    }

    /** Forces class initialisation, and therefore container startup. */
    public static void start() {
        // Static initialiser does the work.
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    /** The network the broker is on, so a test can start a service container beside it. */
    public static Network network() {
        return NETWORK;
    }

    /** Bootstrap address for a client on {@link #network()}, not for one on the host. */
    public static String inNetworkBootstrapServers() {
        return KAFKA_ALIAS + ":" + IN_NETWORK_PORT;
    }

    /**
     * Started on first use rather than with the broker. Only the four Avro producers need a
     * registry, and the five authorization tests and audit-service should not pay for one.
     */
    private static final class SchemaRegistry {

        private static final GenericContainer<?> CONTAINER =
                new GenericContainer<>(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(SCHEMA_REGISTRY_ALIAS)
                        .withExposedPorts(SCHEMA_REGISTRY_PORT)
                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", SCHEMA_REGISTRY_ALIAS)
                        .withEnv("SCHEMA_REGISTRY_LISTENERS",
                                "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                                "SASL_PLAINTEXT://" + inNetworkBootstrapServers())
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM", "PLAIN")
                        // The registry runs as the super user because its _schemas topic is
                        // infrastructure, not a workload's data. No service principal holds
                        // CREATE, and granting one would widen a policy this repository tests.
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG",
                                loginModule(SUPER_USER, secretFor(SUPER_USER)))
                        .waitingFor(Wait.forHttp("/subjects").forPort(SCHEMA_REGISTRY_PORT));

        static {
            start();
            CONTAINER.start();
        }
    }

    public static String schemaRegistryUrl() {
        return "http://%s:%d".formatted(
                SchemaRegistry.CONTAINER.getHost(),
                SchemaRegistry.CONTAINER.getMappedPort(SCHEMA_REGISTRY_PORT));
    }

    /** Registry address for a client on {@link #network()}. */
    public static String inNetworkSchemaRegistryUrl() {
        return "http://%s:%d".formatted(SCHEMA_REGISTRY_ALIAS, SCHEMA_REGISTRY_PORT);
    }

    /**
     * Registers a schema under a subject, the way a deployment would.
     *
     * <p>Done here rather than by letting the container register its own, because every service
     * ships {@code auto.register.schemas: false} and overriding that in the test would mean the
     * artifact under test is no longer the artifact that ships.
     */
    public static void registerSubject(String subject, String avroSchema) {
        String body = "{\"schema\": %s}".formatted(quoteJson(avroSchema));
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(schemaRegistryUrl() + "/subjects/" + subject + "/versions"))
                            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Could not register " + subject + ": "
                        + response.statusCode() + " " + response.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted registering " + subject, e);
        } catch (IOException e) {
            throw new IllegalStateException("Could not register " + subject, e);
        }
    }

    private static String quoteJson(String value) {
        StringBuilder quoted = new StringBuilder("\"");
        value.chars().forEach(c -> {
            switch (c) {
                case '"' -> quoted.append("\\\"");
                case '\\' -> quoted.append("\\\\");
                case '\n' -> quoted.append("\\n");
                default -> quoted.append((char) c);
            }
        });
        return quoted.append('"').toString();
    }

    /** A client-properties file authenticating as the super user, for CLI containers. */
    public static String adminClientProperties() {
        return """
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=PLAIN
                sasl.jaas.config=%s
                """.formatted(loginModule(SUPER_USER, secretFor(SUPER_USER)));
    }

    /** Client configuration authenticating as one service identity, and nothing more. */
    public static Map<String, Object> clientConfig(String principal) {
        if (!PRINCIPALS.contains(principal) && !SUPER_USER.equals(principal)) {
            throw new IllegalArgumentException("Unknown principal " + principal
                    + ". Add it to SecureKafkaStack.PRINCIPALS when the service ships.");
        }
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(SaslConfigs.SASL_JAAS_CONFIG, loginModule(principal, secretFor(principal)));
        return config;
    }

    /**
     * A byte-array producer authenticating as one identity. Payload shape is irrelevant to an
     * authorization test: the broker decides before it looks at the bytes.
     */
    public static Map<String, Object> producerConfig(String principal) {
        Map<String, Object> config = new HashMap<>(clientConfig(principal));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        // Without a bounded block, a denial that arrives as metadata failure looks like a hang.
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int) BLOCK_TIMEOUT.toMillis());
        return config;
    }

    /**
     * A byte-array consumer authenticating as one identity.
     *
     * <p>A null group leaves {@code group.id} unset entirely, which is how a test proves a topic
     * read is denied without the group ACL deciding first. Merely passing an unused group name is
     * not enough: an assigned consumer still fetches committed offsets for its group, so the broker
     * may answer with a group denial instead, and which of the two errors surfaces is a race.
     */
    public static Map<String, Object> consumerConfig(String principal, String groupId) {
        Map<String, Object> config = new HashMap<>(clientConfig(principal));
        if (groupId != null) {
            config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) BLOCK_TIMEOUT.toMillis());
        return config;
    }

    /** Publishes as the super user, so a consumer test has something to read. */
    public static void seed(String topic, String key, byte[] payload) {
        try (org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> producer =
                     new org.apache.kafka.clients.producer.KafkaProducer<>(producerConfig(SUPER_USER))) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, payload)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted seeding " + topic, e);
        } catch (Exception e) {
            throw new IllegalStateException("Could not seed " + topic, e);
        }
    }

    /**
     * Applies a module's committed policy and creates the topics it names.
     *
     * <p>Topics are created by the super user because a least-privilege identity holds no
     * {@code CREATE}: letting auto-creation stand in would grant every producer a permission its
     * policy denies, and the ALLOW tests would then pass for the wrong reason.
     */
    public static void apply(KafkaAclPolicy policy) {
        try (Admin admin = Admin.create(adminConfig())) {
            createTopics(admin, policy.topics());
            List<AclBinding> bindings = policy.allowed().stream()
                    .flatMap(grant -> grant.operations().stream().map(operation -> new AclBinding(
                            new ResourcePattern(grant.resourceType(), grant.name(), PatternType.LITERAL),
                            new AccessControlEntry("User:" + policy.principal(), "*", operation,
                                    AclPermissionType.ALLOW))))
                    .toList();
            admin.createAcls(bindings).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted applying the ACL policy", e);
        } catch (Exception e) {
            throw new IllegalStateException("Could not apply the ACL policy for " + policy.principal(), e);
        }
    }

    /** Creates topics a test needs but the policy does not name, such as another service's topic. */
    public static void createTopics(String... topics) {
        try (Admin admin = Admin.create(adminConfig())) {
            createTopics(admin, List.of(topics));
        }
    }

    private static void createTopics(Admin admin, List<String> topics) {
        List<NewTopic> requested = topics.stream()
                .distinct()
                .map(topic -> new NewTopic(topic, 1, (short) 1))
                .collect(Collectors.toCollection(ArrayList::new));
        if (requested.isEmpty()) {
            return;
        }
        try {
            admin.createTopics(requested).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted creating topics", e);
        } catch (Exception e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("Could not create topics " + topics, e);
            }
        }
    }

    private static Map<String, Object> adminConfig() {
        Map<String, Object> config = clientConfig(SUPER_USER);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        return config;
    }

    private static String brokerJaas() {
        StringBuilder jaas = new StringBuilder("org.apache.kafka.common.security.plain.PlainLoginModule required ")
                .append("username=\"").append(SUPER_USER).append("\" ")
                .append("password=\"").append(secretFor(SUPER_USER)).append("\" ")
                .append("user_").append(SUPER_USER).append("=\"").append(secretFor(SUPER_USER)).append("\" ");
        PRINCIPALS.forEach(principal -> jaas
                .append("user_").append(principal).append("=\"").append(secretFor(principal)).append("\" "));
        return jaas.append(";").toString();
    }

    private static String loginModule(String principal, String secret) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";"
                .formatted(principal, secret);
    }

    /**
     * Test credentials for a throwaway container. These are not secrets and are deliberately not
     * read from the environment: a fixture that needs configuring to run is a fixture that gets
     * skipped.
     */
    private static String secretFor(String principal) {
        return principal + "-local-secret";
    }
}
