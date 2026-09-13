package dev.engnotes.fes.positionexposure;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the migration applies under the strict-security overlay, where the service connects as
 * {@code position_exposure_service}: {@code search_path} set to {@code position_exposure},
 * {@code PUBLIC} revoked from schema {@code public}, and only {@code USAGE, CREATE ON SCHEMA
 * position_exposure} granted, exactly as {@code deploy/compose/postgres/init-position-exposure-role.sql}
 * sets it up. The grant looks sufficient on inspection, and this test exists to confirm that rather
 * than assume it.
 *
 * <p>This launches a plain PostgreSQL container, applies {@code init-position-exposure-role.sql}
 * read from disk (the file that ships, not a re-authored copy), connects to it as
 * {@code position_exposure_service} and nothing more privileged, and runs Flyway against
 * {@code classpath:db/migration} the same way the service does at startup. TLS is out of scope here:
 * {@link PositionExposureDatabaseSecurityIntegrationTest} already proves the TLS-only listener and
 * role privileges live; this test's only question is whether the least-privilege grant is sufficient
 * for the migration DDL itself.
 *
 * <p>Unlike its risk-alert-service counterpart, {@code POSTGRES_DB} is left at the image default
 * ({@code postgres}), not {@code position_exposure}: the init script itself issues
 * {@code CREATE DATABASE position_exposure}, and {@code postgres:16-alpine} has no
 * {@code CREATE DATABASE IF NOT EXISTS}, so the two must not collide.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PositionFlywayLeastPrivilegeIntegrationTest {

    private static final String POSTGRES_IMAGE = "postgres:16-alpine";
    private static final String BOOTSTRAP_PASSWORD = "bootstrap-" + UUID.randomUUID();
    private static final String APP_PASSWORD = "position-exposure-" + UUID.randomUUID();

    private static PostgresContainer postgres;

    @BeforeAll
    static void startPostgresWithTheLeastPrivilegeRole() {
        Path initRoleSql = Path.of("../../../deploy/compose/postgres/init-position-exposure-role.sql")
                .toAbsolutePath();
        assertThat(initRoleSql).as("the committed file this test is meant to exercise").exists();

        postgres = new PostgresContainer(DockerImageName.parse(POSTGRES_IMAGE))
                .withExposedPorts(5432)
                .withEnv("POSTGRES_USER", "postgres")
                .withEnv("POSTGRES_PASSWORD", BOOTSTRAP_PASSWORD)
                .withEnv("POSITION_EXPOSURE_SERVICE_PASSWORD", APP_PASSWORD)
                .withCopyFileToContainer(MountableFile.forHostPath(initRoleSql),
                        "/docker-entrypoint-initdb.d/init-position-exposure-role.sql")
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        postgres.start();
    }

    @AfterAll
    static void stopPostgres() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void the_migration_applies_and_creates_all_three_tables_under_the_least_privilege_role() throws Exception {
        String url = "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432)
                + "/position_exposure";

        Flyway.configure()
                .dataSource(url, "position_exposure_service", APP_PASSWORD)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        try (Connection connection = connect(url);
                ResultSet result = connection.createStatement().executeQuery(
                        "SELECT table_name FROM information_schema.tables "
                                + "WHERE table_schema = 'position_exposure' ORDER BY table_name")) {
            List<String> tables = new ArrayList<>();
            while (result.next()) {
                tables.add(result.getString("table_name"));
            }

            assertThat(tables)
                    .as("the migration, and Flyway's own bookkeeping table, must land in the "
                            + "position_exposure schema under the least-privilege role, not public")
                    .containsExactlyInAnyOrder("flyway_schema_history", "position", "position_applied_trade");
        }
    }

    private static Connection connect(String url) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "position_exposure_service");
        properties.setProperty("password", APP_PASSWORD);
        return DriverManager.getConnection(url, properties);
    }

    /** Named rather than an anonymous {@code GenericContainer<>}: the self-bounded generic on
     * {@link GenericContainer} cannot be inferred through a diamond at an anonymous subclass. */
    private static final class PostgresContainer extends GenericContainer<PostgresContainer> {
        PostgresContainer(DockerImageName image) {
            super(image);
        }
    }
}
