package dev.engnotes.fes.riskalert;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The live counterpart to {@link RiskAlertDatabaseIdentityTest}. That test is a fast, no-Docker
 * guard against someone deleting {@code ssl=on} from the file; it cannot prove the file's claims
 * are actually true of a running server, only that the text is still there. NFR-05.1 is a control,
 * and {@code .claude/rules/security.md} requires every control to have an automated test including
 * its negative case, the same way {@code RiskAlertServiceAuthorizationTest} proves Kafka ACLs
 * against a real broker rather than against the rendered policy file.
 *
 * <p>This test launches a PostgreSQL container configured exactly the way
 * {@code docker-compose.strict-security.yml} configures the real one: the same
 * {@code pg_hba.conf} and {@code init-risk-alert-role.sql} committed under
 * {@code deploy/compose/postgres/}, read from disk rather than re-authored here, so the file this
 * test exercises is the file that ships. Only the TLS material differs from the real stack: rather
 * than depending on {@code scripts/generate-dev-security-material.sh} having been run first (which
 * would make this test silently exercise stale or absent material, or fail for a reason that has
 * nothing to do with the control under test), this class generates its own throwaway CA and server
 * certificate in {@link #BeforeAll}, the same way the generator script does, so the test is
 * self-contained and runs the same way in CI as on a workstation that has never run the script.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RiskAlertDatabaseSecurityIntegrationTest {

    private static final String POSTGRES_IMAGE = "postgres:16-alpine";
    private static final String BOOTSTRAP_PASSWORD = "bootstrap-" + UUID.randomUUID();
    private static final String APP_PASSWORD = "risk-alert-" + UUID.randomUUID();

    // The entrypoint wrapper the overlay itself uses (docker-compose.strict-security.yml): the
    // TLS key is copied from the read-only mount into the container's own writable filesystem and
    // chowned there, because a bind-mounted file keeps whichever ownership Docker's file-sharing
    // layer presents, and postgres (uid 70 in this image) refuses to read a key it does not own.
    private static final String ENTRYPOINT_SCRIPT = """
            set -e
            mkdir -p /var/lib/postgresql/tls
            cp /etc/postgresql/tls/postgres.keystore.pem /var/lib/postgresql/tls/postgres.keystore.pem
            chown postgres:postgres /var/lib/postgresql/tls/postgres.keystore.pem
            chmod 600 /var/lib/postgresql/tls/postgres.keystore.pem
            exec docker-entrypoint.sh "$@"
            """;

    private static Path caCert;
    private static PostgresContainer postgres;

    @BeforeAll
    static void startPostgresConfiguredLikeTheStrictOverlay() throws Exception {
        Path certDir = Files.createTempDirectory("risk-alert-db-security-it");
        caCert = certDir.resolve("ca.pem");
        Path serverKeystore = generateThrowawayCertificates(certDir);

        Path pgHba = Path.of("../../../deploy/compose/postgres/pg_hba.conf").toAbsolutePath();
        Path initRoleSql =
                Path.of("../../../deploy/compose/postgres/init-risk-alert-role.sql").toAbsolutePath();
        assertThat(pgHba).as("the committed file this test is meant to exercise").exists();
        assertThat(initRoleSql).as("the committed file this test is meant to exercise").exists();

        postgres = new PostgresContainer(DockerImageName.parse(POSTGRES_IMAGE))
                .withExposedPorts(5432)
                .withEnv("POSTGRES_DB", "risk_alert")
                .withEnv("POSTGRES_USER", "postgres")
                .withEnv("POSTGRES_PASSWORD", BOOTSTRAP_PASSWORD)
                .withEnv("RISK_ALERT_SERVICE_PASSWORD", APP_PASSWORD)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(serverKeystore),
                        "/etc/postgresql/tls/postgres.keystore.pem")
                .withCopyFileToContainer(MountableFile.forHostPath(pgHba), "/etc/postgresql/pg_hba.conf")
                .withCopyFileToContainer(MountableFile.forHostPath(initRoleSql),
                        "/docker-entrypoint-initdb.d/init-risk-alert-role.sql")
                .withCreateContainerCmdModifier(
                        cmd -> cmd.withEntrypoint("sh", "-c", ENTRYPOINT_SCRIPT, "--"))
                .withCommand("postgres",
                        "-c", "ssl=on",
                        "-c", "ssl_cert_file=/var/lib/postgresql/tls/postgres.keystore.pem",
                        "-c", "ssl_key_file=/var/lib/postgresql/tls/postgres.keystore.pem",
                        "-c", "hba_file=/etc/postgresql/pg_hba.conf")
                // The startup log line "database system is ready to accept connections" appears twice:
                // once for docker-entrypoint.sh's own temporary init server (Unix socket only, no
                // TLS, used to run init-risk-alert-role.sql), and again for the final server this
                // test actually talks to. Waiting for the first occurrence races the init script.
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
    void a_plaintext_connection_is_rejected_by_the_tls_only_listener() {
        // The negative case that matters most. pg_hba.conf has only hostssl and local lines, so a
        // client that negotiates no TLS must be refused at the connection, not merely discouraged.
        assertThatThrownBy(() -> connect("sslmode=disable", "risk_alert_service", APP_PASSWORD))
                .as("a plaintext client must be rejected by the strict overlay's listener")
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("no encryption");
    }

    @Test
    void a_tls_connection_verified_against_the_ca_succeeds() throws Exception {
        try (Connection connection = connect(
                "sslmode=verify-ca&sslrootcert=" + caCert, "risk_alert_service", APP_PASSWORD)) {
            try (ResultSet result = connection.createStatement().executeQuery("SELECT 1")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getInt(1)).isEqualTo(1);
            }
        }
    }

    @Test
    void the_risk_alert_service_role_holds_no_superuser_createrole_or_createdb_privilege()
            throws Exception {
        // A live catalog query, not a substring check on the SQL file: a stray comment mentioning
        // NOSUPERUSER, or a file that granted nothing at all, cannot fool a query against pg_roles.
        try (Connection connection = connect(
                "sslmode=verify-ca&sslrootcert=" + caCert, "risk_alert_service", APP_PASSWORD)) {
            try (ResultSet result = connection.createStatement().executeQuery(
                    "SELECT rolsuper, rolcreaterole, rolcreatedb FROM pg_roles "
                            + "WHERE rolname = 'risk_alert_service'")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getBoolean("rolsuper")).isFalse();
                assertThat(result.getBoolean("rolcreaterole")).isFalse();
                assertThat(result.getBoolean("rolcreatedb")).isFalse();
            }
        }
    }

    @Test
    void the_bootstrap_superuser_cannot_connect_over_the_network_at_all() {
        // pg_hba.conf grants "postgres" no line of any kind: only risk_alert_service has a hostssl
        // entry, and the bootstrap role's own init-time work happens over the local socket instead.
        assertThatThrownBy(() ->
                connect("sslmode=verify-ca&sslrootcert=" + caCert, "postgres", BOOTSTRAP_PASSWORD))
                .as("the bootstrap superuser must have no route in from the network")
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("no pg_hba.conf entry");
    }

    private static Connection connect(String query, String user, String password)
            throws SQLException {
        String url = "jdbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(5432) + "/risk_alert?" + query;
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return DriverManager.getConnection(url, properties);
    }

    /**
     * A throwaway CA and server certificate, generated the same way
     * {@code scripts/generate-dev-security-material.sh} generates the real one, so this test does
     * not depend on that script having been run and never touches its output.
     */
    private static Path generateThrowawayCertificates(Path dir) throws IOException {
        Path caKey = dir.resolve("ca.key");
        Path caCertPath = dir.resolve("ca.pem");
        run(dir, "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", caKey.toString(), "-out", caCertPath.toString(),
                "-days", "2", "-subj", "/CN=fes-test-ca");

        Path serverCnf = dir.resolve("server.cnf");
        Files.writeString(serverCnf, """
                [req]
                distinguished_name = dn
                req_extensions = ext
                prompt = no

                [dn]
                CN = postgres

                [ext]
                subjectAltName = DNS:localhost, IP:127.0.0.1
                extendedKeyUsage = serverAuth
                """);

        Path serverKey = dir.resolve("server.key");
        Path serverCsr = dir.resolve("server.csr");
        run(dir, "openssl", "req", "-newkey", "rsa:2048", "-nodes",
                "-keyout", serverKey.toString(), "-out", serverCsr.toString(),
                "-config", serverCnf.toString());

        Path serverCrt = dir.resolve("server.crt");
        run(dir, "openssl", "x509", "-req", "-in", serverCsr.toString(),
                "-CA", caCertPath.toString(), "-CAkey", caKey.toString(), "-CAcreateserial",
                "-out", serverCrt.toString(), "-days", "2",
                "-extfile", serverCnf.toString(), "-extensions", "ext");

        Path keystore = dir.resolve("postgres.keystore.pem");
        Files.writeString(keystore, Files.readString(serverCrt) + Files.readString(serverKey));
        return keystore;
    }

    /** Named rather than an anonymous {@code GenericContainer<>}: the self-bounded generic on
     * {@link GenericContainer} cannot be inferred through a diamond at an anonymous subclass. */
    private static final class PostgresContainer extends GenericContainer<PostgresContainer> {
        PostgresContainer(DockerImageName image) {
            super(image);
        }
    }

    private static void run(Path dir, String... command) throws IOException {
        try {
            Process process = new ProcessBuilder(command)
                    .directory(dir.toFile())
                    .redirectErrorStream(true)
                    .start();
            String output = new String(process.getInputStream().readAllBytes());
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IllegalStateException(
                        "Command failed (" + exitCode + "): " + List.of(command) + "\n" + output);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException("Interrupted running " + List.of(command), e));
        }
    }
}
