package dev.engnotes.fes.testing;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.yaml.snakeyaml.Yaml;

/**
 * A service's committed least-privilege Kafka policy, loaded from its own module.
 *
 * <p>The policy is a source artifact rather than something a test builds inline, for the reason the
 * security rules give: a control the test invents is a control the test is asserting against itself.
 * The same file is what the strict-security stack's ACL bootstrap consumes, so the permissions a
 * test proves and the permissions a local stack applies cannot drift apart.
 *
 * <p>Only the ALLOW half lives here. Everything else is denied by the broker's
 * {@code allow.everyone.if.no.acl.found=false}, and which denials a service must prove is a test
 * expectation, not a provisioning input.
 */
public record KafkaAclPolicy(String principal, List<Grant> allowed) {

    /** {@code DESCRIBE} is implied by {@code READ} and {@code WRITE} and is not listed. */
    public record Grant(ResourceType resourceType, String name, List<AclOperation> operations) {
    }

    public static final String DEFAULT_LOCATION = "/security/kafka-acls.yml";

    /** Loads the calling module's own policy from its main resources. */
    public static KafkaAclPolicy load() {
        return load(DEFAULT_LOCATION);
    }

    public static KafkaAclPolicy load(String location) {
        try (InputStream source = KafkaAclPolicy.class.getResourceAsStream(location)) {
            if (source == null) {
                throw new IllegalStateException("No Kafka ACL policy at " + location
                        + ". Every service commits one; see docs/security/identity-trust-matrix.md");
            }
            return parse(source);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Could not read the Kafka ACL policy at " + location, e);
        }
    }

    /**
     * Loads a policy by path rather than from the classpath, so the local stack's ACL provisioning
     * reads every service's file with the same parser the tests use. Two parsers would be two
     * chances for the applied permissions and the proven ones to diverge.
     */
    public static KafkaAclPolicy loadFile(java.nio.file.Path path) {
        try (InputStream source = java.nio.file.Files.newInputStream(path)) {
            return parse(source);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Could not read the Kafka ACL policy at " + path, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static KafkaAclPolicy parse(InputStream source) {
        Map<String, Object> document = new Yaml().load(source);
        List<Map<String, Object>> grants =
                (List<Map<String, Object>>) document.getOrDefault("allowed", List.of());
        return new KafkaAclPolicy(
                (String) document.get("principal"),
                grants.stream().map(KafkaAclPolicy::toGrant).toList());
    }

    @SuppressWarnings("unchecked")
    private static Grant toGrant(Map<String, Object> grant) {
        return new Grant(
                ResourceType.valueOf((String) grant.get("resourceType")),
                (String) grant.get("name"),
                ((List<String>) grant.get("operations")).stream().map(AclOperation::valueOf).toList());
    }

    /** Every topic this policy names, so the fixture can create them before the test runs. */
    public List<String> topics() {
        return allowed.stream()
                .filter(grant -> grant.resourceType() == ResourceType.TOPIC)
                .map(Grant::name)
                .toList();
    }
}
