package dev.engnotes.fes.riskalert;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * NFR-05.1 as a test rather than a comment. Someone removing {@code ssl=on} from the cloud
 * stand-in should fail a build, not discover it in a review.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RiskAlertDatabaseIdentityTest {

    private static final Path OVERLAY =
            Path.of("../../../deploy/compose/docker-compose.strict-security.yml");

    @Test
    void the_cloud_stand_in_requires_tls_on_the_database_listener() throws Exception {
        String overlay = Files.readString(OVERLAY);

        // The postgres command is a YAML list (one flag and its value per entry), not a single
        // string, so "ssl=on" rather than "-c ssl=on" is the substring that survives either form.
        assertThat(overlay)
                .as("NFR-05.1 forbids plaintext database traffic in cloud profiles")
                .contains("ssl=on");
    }

    @Test
    void the_service_does_not_connect_as_the_database_superuser_in_the_cloud_stand_in()
            throws Exception {
        String roleBootstrap = Files.readString(
                Path.of("../../../deploy/compose/postgres/init-risk-alert-role.sql"));

        // PostgreSQL never lets the initdb bootstrap role strip its own SUPERUSER attribute, so
        // the overlay bootstraps as "postgres" (see docker-compose.strict-security.yml) and this
        // file creates risk_alert_service as a genuinely separate, non-superuser role. Proving
        // that means asserting the strip is present, not merely that the grant is absent: a file
        // that granted nothing at all would pass a bare doesNotContain("SUPERUSER") vacuously,
        // and NOSUPERUSER itself contains "SUPERUSER" as a substring.
        assertThat(roleBootstrap)
                .as("the service owns its own schema and nothing else (ADR-028, ADR-030)")
                .contains("AUTHORIZATION risk_alert_service")
                .contains("NOSUPERUSER")
                .contains("NOCREATEROLE")
                .contains("NOCREATEDB");
    }
}
