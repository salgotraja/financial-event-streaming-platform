package dev.engnotes.fes.events;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The FR-02.2 schema evolution gate.
 *
 * <p>Every schema in {@code src/main/avro} must be mutually readable with the accepted baseline in
 * {@code src/test/resources/schema-baseline}: new readers must read old data, and old readers must
 * read new data. That is Confluent's FULL compatibility.
 *
 * <p>FULL rather than BACKWARD is deliberate. The architecture's stated evolution policy is "new
 * optional fields with defaults, no field removal, no type changes". BACKWARD alone permits field
 * removal, because a new reader simply ignores the dropped field. Only FULL enforces the policy as
 * written, and only FULL is safe during the rolling upgrades FR-02.3 requires, where two schema
 * versions are live at once and an old consumer must still read a new producer's output.
 *
 * <p>The check runs offline against a committed baseline so the gate is deterministic and does not
 * depend on the current contents of a shared registry (ADR-029). A deliberate breaking change is
 * accepted by running {@code ./gradlew updateSchemaBaseline}, which produces a reviewable diff
 * rather than a silent pass.
 */
@DisplayName("Avro schema compatibility gate")
class SchemaCompatibilityTest {

    private static final Path CURRENT = Path.of("src/main/avro");
    private static final Path BASELINE = Path.of("src/test/resources/schema-baseline");

    private static final SchemaValidator FULLY_COMPATIBLE_WITH_BASELINE =
            new SchemaValidatorBuilder().mutualReadStrategy().validateLatest();

    @ParameterizedTest(name = "{0}")
    @MethodSource("schemaNames")
    @DisplayName("should stay fully compatible with the accepted baseline")
    void should_stay_fully_compatible_with_the_accepted_baseline(String schemaName) {
        Schema current = parseAll(CURRENT).get(schemaName);
        Schema baseline = parseAll(BASELINE).get(schemaName);

        assertThat(current)
                .as("schema %s exists in src/main/avro", schemaName)
                .isNotNull();

        try {
            FULLY_COMPATIBLE_WITH_BASELINE.validate(current, List.of(baseline));
        } catch (SchemaValidationException e) {
            fail("""
                    %s is not fully compatible with its accepted baseline.

                    Allowed: add an optional field with a default, widen a union.
                    Not allowed: remove a field, rename a field, change a type, add a mandatory
                    field without a default, make an optional field mandatory, remove an enum symbol.

                    If the break is intentional, publish a new topic version or run
                    ./gradlew updateSchemaBaseline and explain the change in the pull request.

                    %s""".formatted(schemaName, e.getMessage()));
        }
    }

    @Test
    @DisplayName("should not silently drop a schema from the baseline")
    void should_not_silently_drop_a_schema_from_the_baseline() {
        assertThat(parseAll(CURRENT).keySet())
                .as("removing a schema breaks every consumer still reading that topic")
                .containsAll(parseAll(BASELINE).keySet());
    }

    @Test
    @DisplayName("should keep every schema in the dev.engnotes.fes.events namespace")
    void should_keep_every_schema_in_the_dev_engnotes_fes_events_namespace() {
        assertThat(parseAll(CURRENT).values())
                .allSatisfy(schema -> assertThat(schema.getNamespace())
                        .isEqualTo("dev.engnotes.fes.events"));
    }

    static Stream<String> schemaNames() {
        return parseAll(BASELINE).keySet().stream().sorted();
    }

    /**
     * Parses every schema in a directory with one shared parser so cross-schema references such as
     * EnrichedTradeEvent embedding TradeEvent resolve. Files are retried until the set stops
     * shrinking, which removes any dependency on filename ordering.
     */
    private static Map<String, Schema> parseAll(Path directory) {
        List<Path> pending = listSchemas(directory);
        Map<String, Schema> parsed = new LinkedHashMap<>();
        Schema.Parser parser = new Schema.Parser();

        while (!pending.isEmpty()) {
            List<Path> deferred = new ArrayList<>();
            for (Path file : pending) {
                try {
                    Schema schema = parser.parse(file.toFile());
                    parsed.put(schema.getName(), schema);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (RuntimeException e) {
                    // Unresolved cross-schema reference. Retry once its dependency is parsed.
                    deferred.add(file);
                }
            }
            if (deferred.size() == pending.size()) {
                throw new IllegalStateException(
                        "Unresolvable schema references in " + directory + ": " + deferred);
            }
            pending = deferred;
        }
        return parsed;
    }

    private static List<Path> listSchemas(Path directory) {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(p -> p.toString().endsWith(".avsc")).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list schemas in " + directory, e);
        }
    }

}
