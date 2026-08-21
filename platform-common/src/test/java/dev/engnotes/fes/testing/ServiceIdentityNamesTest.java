package dev.engnotes.fes.testing;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three-link name chain, checked rather than asserted in prose.
 *
 * <p>A service's image is built from its Gradle module, its SASL username is derived from
 * {@code spring.application.name}, and its ACL grant names the {@code PRINCIPALS} entry. A
 * mismatch between any two of the three breaks the identity binding in a way no module-local test
 * can see, because each module only knows its own link.
 */
@DisplayName("the service identity name chain")
class ServiceIdentityNamesTest {

    private static final Path REPO_ROOT = Path.of("..").toAbsolutePath().normalize();

    static java.util.List<String> principals() {
        return SecureKafkaStack.principals();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("principals")
    @DisplayName("should name one module, one application name and one policy principal alike")
    void should_name_one_module_one_application_name_and_one_policy_principal_alike(String principal)
            throws Exception {
        Path module = moduleFor(principal);

        assertThat(module)
                .as("no Gradle module is named %s, so no image could be built for it", principal)
                .isDirectory();

        String applicationYml = Files.readString(
                module.resolve("src/main/resources/application.yml"));
        // A substring check would also pass for "trade-producer-x": strip and compare whole
        // lines so a principal that is merely a prefix of the configured name still fails.
        assertThat(applicationYml.lines().map(String::strip).toList())
                .as("the SASL username is derived from spring.application.name, so a module whose "
                        + "application name differs from its principal authenticates as something "
                        + "no ACL grants")
                .contains("name: " + principal);

        String policy = Files.readString(
                module.resolve("src/main/resources/security/kafka-acls.yml"));
        assertThat(policy.lines().map(String::strip).toList())
                .as("the committed policy must grant the principal the service presents")
                .contains("principal: " + principal);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("principals")
    @DisplayName("should not let a module name a SASL username of its own")
    void should_not_let_a_module_name_a_sasl_username_of_its_own(String principal) throws Exception {
        Path module = moduleFor(principal);

        try (var sources = Files.walk(module.resolve("src/main"))) {
            var offenders = sources
                    .filter(Files::isRegularFile)
                    .filter(file -> {
                        try {
                            return Files.readString(file).contains("sasl.jaas.config")
                                    || Files.readString(file).contains("sasl-jaas-config");
                        } catch (java.io.IOException e) {
                            throw new IllegalStateException("Could not read " + file, e);
                        }
                    })
                    .toList();

            assertThat(offenders)
                    .as("the SASL identity is derived by platform-common (ADR-031). A module that "
                            + "can state a jaas config can state the administrator's.")
                    .isEmpty();
        }
    }

    private static Path moduleFor(String principal) throws Exception {
        String settings = Files.readString(REPO_ROOT.resolve("settings.gradle"));
        return settings.lines()
                .map(String::trim)
                .filter(line -> line.startsWith("include '"))
                .map(line -> line.substring("include '".length(), line.length() - 1))
                .filter(path -> path.endsWith(":" + principal))
                .map(path -> REPO_ROOT.resolve(path.replace(':', '/')))
                .findFirst()
                .orElse(REPO_ROOT.resolve("no-module-named-" + principal));
    }
}
