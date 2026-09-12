package dev.engnotes.fes.riskalert;

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
 * {@link RiskPositionSchemaIntegrationTest} proves Flyway migrates against {@link
 * dev.engnotes.fes.testing.PostgresStack}, which is a dev-shaped superuser connected to schema
 * {@code public}. It does not prove the migration applies under the strict-security overlay, where
 * the service connects as {@code risk_alert_service}: {@code search_path} set to {@code risk_alert},
 * {@code PUBLIC} revoked from schema {@code public}, and only {@code USAGE, CREATE ON SCHEMA
 * risk_alert} granted, exactly as {@code deploy/compose/postgres/init-risk-alert-role.sql} sets it
 * up. The grant looks sufficient on inspection, and this test exists to confirm that rather than
 * assume it.
 *
 * <p>This launches a plain PostgreSQL container, applies {@code init-risk-alert-role.sql} read from
 * disk (the file that ships, not a re-authored copy), connects to it as {@code risk_alert_service}
 * and nothing more privileged, and runs Flyway against {@code classpath:db/migration} the same way
 * the service does at startup. TLS is out of scope here: {@link
 * RiskAlertDatabaseSecurityIntegrationTest} already proves the TLS-only listener and role
 * privileges live; this test's only question is whether the least-privilege grant is sufficient for
 * the migration DDL itself.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RiskPositionFlywayLeastPrivilegeIntegrationTest {

    private static final String POSTGRES_IMAGE = "postgres:16-alpine";
    private static final String BOOTSTRAP_PASSWORD = "bootstrap-" + UUID.randomUUID();
    private static final String APP_PASSWORD = "risk-alert-" + UUID.randomUUID();

    private static PostgresContainer postgres;

    @BeforeAll
    static void startPostgresWithTheLeastPrivilegeRole() {
        Path initRoleSql =
                Path.of("../../../deploy/compose/postgres/init-risk-alert-role.sql").toAbsolutePath();
        assertThat(initRoleSql).as("the committed file this test is meant to exercise").exists();

        postgres = new PostgresContainer(DockerImageName.parse(POSTGRES_IMAGE))
                .withExposedPorts(5432)
                .withEnv("POSTGRES_DB", "risk_alert")
                .withEnv("POSTGRES_USER", "postgres")
                .withEnv("POSTGRES_PASSWORD", BOOTSTRAP_PASSWORD)
                .withEnv("RISK_ALERT_SERVICE_PASSWORD", APP_PASSWORD)
                .withCopyFileToContainer(MountableFile.forHostPath(initRoleSql),
                        "/docker-entrypoint-initdb.d/init-risk-alert-role.sql")
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
    void the_migration_applies_and_creates_both_tables_under_the_least_privilege_role() throws Exception {
        String url = "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432)
                + "/risk_alert";

        Flyway.configure()
                .dataSource(url, "risk_alert_service", APP_PASSWORD)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        try (Connection connection = connect(url);
                ResultSet result = connection.createStatement().executeQuery(
                        "SELECT table_name FROM information_schema.tables "
                                + "WHERE table_schema = 'risk_alert' ORDER BY table_name")) {
            List<String> tables = new ArrayList<>();
            while (result.next()) {
                tables.add(result.getString("table_name"));
            }

            assertThat(tables)
                    .as("the migration, and Flyway's own bookkeeping table, must land in the "
                            + "risk_alert schema under the least-privilege role, not public")
                    .containsExactlyInAnyOrder(
                            "flyway_schema_history", "risk_position", "risk_position_applied_trade");
        }
    }

    private static Connection connect(String url) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "risk_alert_service");
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
