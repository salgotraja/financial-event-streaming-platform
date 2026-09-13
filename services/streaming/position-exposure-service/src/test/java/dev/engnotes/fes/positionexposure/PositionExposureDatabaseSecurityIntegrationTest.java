package dev.engnotes.fes.positionexposure;

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
 * The live counterpart to a text-only check on the compose file: NFR-05.1 is a control, and
 * {@code .claude/rules/security.md} requires every control to have an automated test including its
 * negative case, the same way {@code RiskAlertDatabaseSecurityIntegrationTest} proves it for
 * risk-alert-service.
 *
 * <p>This test launches a PostgreSQL container configured exactly the way
 * {@code docker-compose.strict-security.yml} configures the real one: the same {@code pg_hba.conf}
 * and {@code init-position-exposure-role.sql} committed under {@code deploy/compose/postgres/}, read
 * from disk rather than re-authored here, so the file this test exercises is the file that ships.
 * Only the TLS material differs from the real stack: rather than depending on
 * {@code scripts/generate-dev-security-material.sh} having been run first, this class generates its
 * own throwaway CA and server certificate in {@link #startPostgresConfiguredLikeTheStrictOverlay()},
 * the same way the generator script does, so the test is self-contained.
 *
 * <p>{@code POSTGRES_DB} is left at the image default ({@code postgres}), not
 * {@code position_exposure}: the init script itself issues {@code CREATE DATABASE
 * position_exposure}, and {@code postgres:16-alpine} has no {@code CREATE DATABASE IF NOT EXISTS},
 * so the two must not collide.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PositionExposureDatabaseSecurityIntegrationTest {

    private static final String POSTGRES_IMAGE = "postgres:16-alpine";
    private static final String BOOTSTRAP_PASSWORD = "bootstrap-" + UUID.randomUUID();
    private static final String APP_PASSWORD = "position-exposure-" + UUID.randomUUID();

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
        Path certDir = Files.createTempDirectory("position-exposure-db-security-it");
        caCert = certDir.resolve("ca.pem");
        Path serverKeystore = generateThrowawayCertificates(certDir);

        Path pgHba = Path.of("../../../deploy/compose/postgres/pg_hba.conf").toAbsolutePath();
        Path initRoleSql = Path.of("../../../deploy/compose/postgres/init-position-exposure-role.sql")
                .toAbsolutePath();
        assertThat(pgHba).as("the committed file this test is meant to exercise").exists();
        assertThat(initRoleSql).as("the committed file this test is meant to exercise").exists();

        postgres = new PostgresContainer(DockerImageName.parse(POSTGRES_IMAGE))
                .withExposedPorts(5432)
                .withEnv("POSTGRES_USER", "postgres")
                .withEnv("POSTGRES_PASSWORD", BOOTSTRAP_PASSWORD)
                .withEnv("POSITION_EXPOSURE_SERVICE_PASSWORD", APP_PASSWORD)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(serverKeystore),
                        "/etc/postgresql/tls/postgres.keystore.pem")
                .withCopyFileToContainer(MountableFile.forHostPath(pgHba), "/etc/postgresql/pg_hba.conf")
                .withCopyFileToContainer(MountableFile.forHostPath(initRoleSql),
                        "/docker-entrypoint-initdb.d/init-position-exposure-role.sql")
                .withCreateContainerCmdModifier(
                        cmd -> cmd.withEntrypoint("sh", "-c", ENTRYPOINT_SCRIPT, "--"))
                .withCommand("postgres",
                        "-c", "ssl=on",
                        "-c", "ssl_cert_file=/var/lib/postgresql/tls/postgres.keystore.pem",
                        "-c", "ssl_key_file=/var/lib/postgresql/tls/postgres.keystore.pem",
                        "-c", "hba_file=/etc/postgresql/pg_hba.conf")
                // The startup log line "database system is ready to accept connections" appears twice:
                // once for docker-entrypoint.sh's own temporary init server (Unix socket only, no
                // TLS, used to run init-position-exposure-role.sql), and again for the final server
                // this test actually talks to. Waiting for the first occurrence races the init script.
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
        assertThatThrownBy(() -> connect("sslmode=disable", "position_exposure_service", APP_PASSWORD))
                .as("a plaintext client must be rejected by the strict overlay's listener")
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("no encryption");
    }

    @Test
    void a_tls_connection_verified_against_the_ca_succeeds() throws Exception {
        try (Connection connection = connect(
                "sslmode=verify-ca&sslrootcert=" + caCert, "position_exposure_service", APP_PASSWORD)) {
            try (ResultSet result = connection.createStatement().executeQuery("SELECT 1")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getInt(1)).isEqualTo(1);
            }
        }
    }

    @Test
    void the_position_exposure_service_role_holds_no_superuser_createrole_or_createdb_privilege()
            throws Exception {
        // A live catalog query, not a substring check on the SQL file: a stray comment mentioning
        // NOSUPERUSER, or a file that granted nothing at all, cannot fool a query against pg_roles.
        try (Connection connection = connect(
                "sslmode=verify-ca&sslrootcert=" + caCert, "position_exposure_service", APP_PASSWORD)) {
            try (ResultSet result = connection.createStatement().executeQuery(
                    "SELECT rolsuper, rolcreaterole, rolcreatedb FROM pg_roles "
                            + "WHERE rolname = 'position_exposure_service'")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getBoolean("rolsuper")).isFalse();
                assertThat(result.getBoolean("rolcreaterole")).isFalse();
                assertThat(result.getBoolean("rolcreatedb")).isFalse();
            }
        }
    }

    @Test
    void the_role_cannot_create_a_table_in_the_public_schema() throws Exception {
        // The database stays owned by the bootstrap role rather than by position_exposure_service,
        // specifically so this fails: owning the database would make position_exposure_service the
        // owner of its own public schema too (PostgreSQL 15+ owns public via pg_database_owner),
        // which would grant CREATE there despite the comment's claim of "no access to any other
        // schema".
        try (Connection connection = connect(
                "sslmode=verify-ca&sslrootcert=" + caCert, "position_exposure_service", APP_PASSWORD)) {
            assertThatThrownBy(() ->
                    connection.createStatement().execute("CREATE TABLE public.x (id INT)"))
                    .as("the role must hold no CREATE privilege on schema public")
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("permission denied for schema public");
        }
    }

    @Test
    void the_bootstrap_superuser_cannot_connect_over_the_network_at_all() {
        // pg_hba.conf grants "postgres" no line of any kind: neither hostssl line (risk_alert's or
        // position_exposure's) matches the postgres role, so the implicit final deny rejects it,
        // and the bootstrap role's own init-time work happens over the local socket instead.
        assertThatThrownBy(() ->
                connect("sslmode=verify-ca&sslrootcert=" + caCert, "postgres", BOOTSTRAP_PASSWORD))
                .as("the bootstrap superuser must have no route in from the network")
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("no pg_hba.conf entry");
    }

    private static Connection connect(String query, String user, String password)
            throws SQLException {
        String url = "jdbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(5432) + "/position_exposure?" + query;
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
