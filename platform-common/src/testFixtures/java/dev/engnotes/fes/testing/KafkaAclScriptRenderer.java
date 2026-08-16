package dev.engnotes.fes.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Renders every service's committed policy into {@code kafka-acls.sh} arguments for the local stack.
 *
 * <p>The point is that the strict-security stack and the authorization tests apply the same
 * permissions. Both read the same {@code kafka-acls.yml} files through {@link KafkaAclPolicy}, so a
 * grant cannot be proven in a test and missing from the stack, or the reverse.
 *
 * <p>Arguments only, not whole commands: the bootstrap address and the command-config belong to the
 * script that knows which profile is running.
 *
 * <p>Invoked by the {@code renderKafkaAcls} Gradle task. Usage: output file, then policy files.
 */
public final class KafkaAclScriptRenderer {

    private KafkaAclScriptRenderer() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("usage: <output-file> <policy-file>...");
        }
        Path output = Path.of(args[0]);
        List<Path> policies = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            policies.add(Path.of(args[i]));
        }

        List<String> lines = new ArrayList<>();
        lines.add("# Generated from each service's src/main/resources/security/kafka-acls.yml.");
        lines.add("# Do not edit: run ./gradlew renderKafkaAcls. One ACL per line, arguments only.");

        for (Path policyFile : policies.stream().sorted().toList()) {
            KafkaAclPolicy policy = KafkaAclPolicy.loadFile(policyFile);
            lines.add("");
            lines.add("# " + policy.principal());
            for (KafkaAclPolicy.Grant grant : policy.allowed()) {
                for (var operation : grant.operations()) {
                    lines.add("--allow-principal User:%s --operation %s --%s %s".formatted(
                            policy.principal(),
                            operation.name(),
                            resourceFlag(grant.resourceType()),
                            grant.name()));
                }
            }
        }

        Files.createDirectories(output.getParent());
        Files.write(output, lines);

        System.out.println("Rendered " + (lines.size()) + " lines from " + policies.size() + " policies to " + output);
    }

    /**
     * Kafka's resource flags are hyphenated where the enum name is not: {@code TRANSACTIONAL_ID} is
     * {@code --transactional-id}. Lowercasing alone would emit a flag {@code kafka-acls.sh} rejects,
     * and only for resource types no policy uses yet, which is the kind of break that surfaces in
     * someone else's session.
     */
    private static String resourceFlag(org.apache.kafka.common.resource.ResourceType resourceType) {
        return resourceType.name().toLowerCase().replace('_', '-');
    }
}
