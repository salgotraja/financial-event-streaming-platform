package dev.engnotes.fes.testing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The local {@code strict-security} broker: authenticated clients and per-service Kafka ACLs, so
 * service-identity failures can be tested without AWS (NFR-05.5).
 *
 * <p>Cloud deployments authorise with MSK IAM; ACLs exist for exactly this profile (ADR-009). What a
 * test here proves is the policy, not the deployment: that a workload identity can do only what its
 * committed {@code kafka-acls.yml} allows. It does not prove the deployed service runs as that
 * identity.
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

    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", brokerJaas())
            .withEnv("KAFKA_AUTHORIZER_CLASS_NAME",
                    "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
            // ANONYMOUS covers the inter-broker PLAINTEXT listener only. Client tests always connect
            // through the authenticated listener, and the zero-grant test proves an unauthorised
            // client is denied rather than merely unable to connect.
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
