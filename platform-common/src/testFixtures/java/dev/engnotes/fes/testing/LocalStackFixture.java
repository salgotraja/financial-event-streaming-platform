package dev.engnotes.fes.testing;

import java.net.URI;

import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A real S3 and KMS endpoint for tests, emulated by LocalStack.
 *
 * <p>These are the two AWS services the audit evidence path needs: an object store the archives are
 * written to under Object Lock, and a customer-managed key the sidecar manifest is signed with
 * (ADR-012). None of that is built. The audit service writes through an {@code AuditSink} port whose
 * only implementation logs, so nothing in the source tree calls this fixture except the test that
 * proves the fixture itself works.
 *
 * <p>It exists ahead of its first caller because the alternative is discovering the container wiring
 * and the endpoint shape while also writing the Parquet writer, the manifest and the signature. The
 * same endpoint is in the compose stack, so a service and a test reach AWS the same way.
 *
 * <p>Deliberately no AWS SDK dependency. The fixture hands out an endpoint, a region and the
 * emulated credentials; the first module that actually calls S3 brings its own client. Adding the
 * SDK here would put it on every service's test classpath for no current caller.
 *
 * <p>The container starts once per JVM and is never stopped, matching {@link KafkaAvroStack}. Ryuk
 * reaps it when the run ends.
 */
public final class LocalStackFixture {

    /**
     * The last tag that runs without a licence. From the March 2026 unified image onward the
     * container requires {@code LOCALSTACK_AUTH_TOKEN} and exits with code 55 without one, which
     * would make this a test only a token holder could run. Same tag as the compose stack.
     */
    private static final String LOCALSTACK_IMAGE = "localstack/localstack:4.14.0";

    /** Emulated, so these are arbitrary rather than secret. Cloud workloads use no static key at all (ADR-010). */
    private static final String ACCESS_KEY = "test";

    private static final String SECRET_KEY = "test";

    /** us-east-1 sidesteps the CreateBucket LocationConstraint special case. Not a deployment choice. */
    private static final String REGION = "us-east-1";

    private static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse(LOCALSTACK_IMAGE))
                    .withEnv("SERVICES", "s3,kms")
                    .withEnv("AWS_DEFAULT_REGION", REGION)
                    .withEnv("AWS_ACCESS_KEY_ID", ACCESS_KEY)
                    .withEnv("AWS_SECRET_ACCESS_KEY", SECRET_KEY);

    static {
        LOCALSTACK.start();
    }

    private LocalStackFixture() {
    }

    /** The single edge endpoint. LocalStack fronts every emulated service on one port. */
    public static URI endpoint() {
        return URI.create("http://" + LOCALSTACK.getHost() + ":" + LOCALSTACK.getMappedPort(4566));
    }

    public static String region() {
        return REGION;
    }

    public static String accessKey() {
        return ACCESS_KEY;
    }

    public static String secretKey() {
        return SECRET_KEY;
    }

    /**
     * Runs an {@code awslocal} command inside the container and returns its stdout.
     *
     * <p>The container ships the CLI pre-pointed at its own endpoint, which is how a test can assert
     * against S3 or KMS before any module has an AWS client on its classpath.
     */
    public static String awslocal(String... arguments) {
        String[] command = new String[arguments.length + 1];
        command[0] = "awslocal";
        System.arraycopy(arguments, 0, command, 1, arguments.length);

        try {
            var result = LOCALSTACK.execInContainer(command);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(
                        "awslocal " + String.join(" ", arguments) + " failed: " + result.getStderr());
            }
            return result.getStdout();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted running awslocal", e);
        } catch (Exception e) {
            throw new IllegalStateException("could not run awslocal in the LocalStack container", e);
        }
    }
}
