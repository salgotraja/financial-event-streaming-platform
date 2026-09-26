package dev.engnotes.fes.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yaml.snakeyaml.Yaml;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every service keeps its API docs off by default and turns them on only under the dev profile,
 * on a port of its own. Checked across modules because each module only sees its own file, and a
 * new service added without the stanza would otherwise ship the docs on by springdoc's default.
 */
@DisplayName("the API docs profile of every service")
class ApiDocsProfileTest {

    private static final Path REPO_ROOT = Path.of("..").toAbsolutePath().normalize();
    private static final Path SERVICES = REPO_ROOT.resolve("services");

    // Enumerated from settings.gradle rather than the file tree, so a service whose application.yml is
    // missing or renamed fails here instead of silently dropping out of the parameter set.
    static List<String> services() throws IOException {
        return Files.readString(REPO_ROOT.resolve("settings.gradle")).lines()
                .map(String::trim)
                .filter(line -> line.startsWith("include 'services:"))
                .map(line -> line.substring("include 'services:".length(), line.length() - 1))
                .map(path -> path.replace(':', '/'))
                .sorted()
                .toList();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("services")
    @DisplayName("should keep API docs off by default and on under the dev profile with a unique port")
    void should_keep_api_docs_off_by_default_and_on_under_dev_with_a_unique_port_when_service_is_configured(
            String service) throws IOException {
        assertThat(applicationYml(SERVICES.resolve(service)))
                .as("every service declares its API docs profile in application.yml")
                .isRegularFile();
        List<Map<String, Object>> documents = documentsOf(SERVICES.resolve(service));

        Map<String, Object> base = documents.getFirst();
        assertThat(valueAt(base, "spring", "profiles"))
                .as("the base document must not activate or include a profile, dev included")
                .isNull();
        assertThat(valueAt(base, "springdoc", "api-docs", "enabled"))
                .as("the OpenAPI document must be off unless a profile turns it on")
                .isEqualTo(false);
        assertThat(valueAt(base, "scalar", "enabled"))
                .as("the Scalar UI must be off unless a profile turns it on")
                .isEqualTo(false);

        assertThat(documents.stream().skip(1)
                        .filter(document -> valueAt(document, "spring", "config", "activate", "on-profile") == null)
                        .filter(document -> document.containsKey("springdoc") || document.containsKey("scalar")))
                .as("a document without a profile activation applies everywhere, so it must not touch the docs")
                .isEmpty();

        Map<String, Object> dev = devDocument(documents);
        assertThat(dev).as("no document is activated on-profile: dev").isNotNull();
        assertThat(valueAt(dev, "springdoc", "api-docs", "enabled")).isEqualTo(true);
        assertThat(valueAt(dev, "scalar", "enabled")).isEqualTo(true);
        assertThat(valueAt(dev, "scalar", "telemetry")).isEqualTo(false);
        assertThat(valueAt(dev, "spring", "kafka", "bootstrap-servers"))
                .as("the dev profile must point Kafka at the local stack")
                .isEqualTo("localhost:29092,localhost:29093,localhost:29094");

        Object port = valueAt(dev, "server", "port");
        assertThat(port).as("the dev profile must pin server.port").isInstanceOf(Integer.class);

        List<Object> allPorts = services().stream()
                .map(other -> devDocument(documentsOf(SERVICES.resolve(other))))
                .filter(Objects::nonNull)
                .map(document -> valueAt(document, "server", "port"))
                .toList();
        assertThat(allPorts)
                .as("dev ports must not collide, since the services run side by side")
                .filteredOn(port::equals)
                .hasSize(1);
    }

    private static Path applicationYml(Path module) {
        return module.resolve("src/main/resources/application.yml");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> documentsOf(Path module) {
        try {
            String yaml = Files.readString(applicationYml(module));
            return StreamSupport.stream(new Yaml().loadAll(yaml).spliterator(), false)
                    .map(document -> (Map<String, Object>) document)
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Map<String, Object> devDocument(List<Map<String, Object>> documents) {
        return documents.stream()
                .filter(document -> "dev".equals(
                        valueAt(document, "spring", "config", "activate", "on-profile")))
                .findFirst()
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private static Object valueAt(Map<String, Object> document, String... path) {
        Object current = document;
        for (String key : path) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = ((Map<String, Object>) map).get(key);
        }
        return current;
    }
}
