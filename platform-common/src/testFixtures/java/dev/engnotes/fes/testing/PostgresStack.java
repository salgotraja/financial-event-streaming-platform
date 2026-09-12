package dev.engnotes.fes.testing;

import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A real PostgreSQL, shared by every service's integration tests.
 *
 * <p>Testcontainers rather than H2, so tests run against the same implementation the deployed
 * system runs. The migrations use {@code ON CONFLICT ... RETURNING} and {@code GREATEST}, and H2's
 * PostgreSQL compatibility mode would pass or fail on its own terms rather than PostgreSQL's.
 *
 * <p>Started once per JVM and deliberately never stopped, exactly like {@link KafkaAvroStack}.
 * Ryuk reaps the container when the run ends.
 *
 * <p>A static stack rather than the {@code PostgreSQLContainer} bean in
 * {@code TestcontainersConfiguration}: that class is imported by no source file in this repository,
 * and importing it into a test that also needs {@link KafkaAvroStack} would start a second broker.
 */
public final class PostgresStack {

    private static final String POSTGRES_IMAGE = "postgres:16-alpine";

    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse(POSTGRES_IMAGE))
                    .withDatabaseName("risk_alert")
                    .withUsername("risk_alert_service")
                    .withPassword("risk_alert_service");

    static {
        POSTGRES.start();
    }

    private PostgresStack() {
    }

    /** Forces class initialisation, and therefore container startup, before properties are read. */
    public static void start() {
        // Static initialiser does the work.
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    public static String username() {
        return POSTGRES.getUsername();
    }

    public static String password() {
        return POSTGRES.getPassword();
    }
}
