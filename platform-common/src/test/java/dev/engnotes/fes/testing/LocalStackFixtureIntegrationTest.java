package dev.engnotes.fes.testing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the LocalStack fixture starts and that both services the audit evidence path will need are
 * actually reachable, rather than merely configured.
 *
 * <p>This is the fixture's only caller. No module writes to S3 or signs with KMS yet (ADR-012,
 * Phase 3), so without this test the container wiring would sit unexercised until the day someone
 * needs it to work.
 */
@DisplayName("LocalStackFixture")
class LocalStackFixtureIntegrationTest {

    @Test
    @DisplayName("should accept a bucket on the emulated S3 endpoint")
    void should_accept_a_bucket_on_the_emulated_s3_endpoint() {
        LocalStackFixture.awslocal("s3", "mb", "s3://fes-audit-evidence-fixture");

        String buckets = LocalStackFixture.awslocal("s3", "ls");

        assertThat(buckets).contains("fes-audit-evidence-fixture");
    }

    @Test
    @DisplayName("should expose KMS, which the manifest signature will depend on")
    void should_expose_kms_which_the_manifest_signature_will_depend_on() {
        String keys = LocalStackFixture.awslocal("kms", "list-keys");

        assertThat(keys).contains("Keys");
    }

    @Test
    @DisplayName("should report the endpoint and region a client would be configured with")
    void should_report_the_endpoint_and_region_a_client_would_be_configured_with() {
        assertThat(LocalStackFixture.endpoint().getPort()).isPositive();
        assertThat(LocalStackFixture.region()).isEqualTo("us-east-1");
    }
}
