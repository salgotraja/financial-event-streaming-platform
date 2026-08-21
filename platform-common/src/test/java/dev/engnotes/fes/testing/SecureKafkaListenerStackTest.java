package dev.engnotes.fes.testing;

import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("the secure stack's in-network listener")
class SecureKafkaListenerStackTest {

    private static final String CLI_IMAGE = "apache/kafka:4.1.0";

    @BeforeAll
    static void startBroker() {
        SecureKafkaStack.start();
    }

    @Test
    @DisplayName("should refuse an in-network client that presents no credentials")
    void should_refuse_an_in_network_client_that_presents_no_credentials() throws Exception {
        try (GenericContainer<?> cli = cliContainer()) {
            cli.start();
            Container.ExecResult result = cli.execInContainer("/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server", SecureKafkaStack.inNetworkBootstrapServers(),
                    "--list");

            assertThat(result.getExitCode())
                    .as("an unauthenticated in-network client would authenticate as ANONYMOUS, "
                            + "which is a super user on this broker, and every denial assertion "
                            + "in the repository would pass vacuously")
                    .isNotZero();
        }
    }

    @Test
    @DisplayName("should admit an in-network client that presents credentials")
    void should_admit_an_in_network_client_that_presents_credentials() throws Exception {
        try (GenericContainer<?> cli = cliContainer()) {
            cli.start();
            cli.copyFileToContainer(
                    org.testcontainers.images.builder.Transferable.of(
                            SecureKafkaStack.adminClientProperties()),
                    "/tmp/admin.properties");

            Container.ExecResult result = cli.execInContainer("/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server", SecureKafkaStack.inNetworkBootstrapServers(),
                    "--command-config", "/tmp/admin.properties",
                    "--list");

            assertThat(result.getExitCode())
                    .as("stderr was: %s", result.getStderr())
                    .isZero();
        }
    }

    @Test
    @DisplayName("should not expose the inter-broker listener to a client on the network")
    void should_not_expose_the_inter_broker_listener_to_a_client_on_the_network() throws Exception {
        try (GenericContainer<?> cli = cliContainer()) {
            cli.start();
            String alias = SecureKafkaStack.inNetworkBootstrapServers().split(":")[0];
            cli.copyFileToContainer(
                    org.testcontainers.images.builder.Transferable.of("request.timeout.ms=5000\n"),
                    "/tmp/short-timeout.properties");

            for (String port : java.util.List.of("9093", "9094")) {
                Container.ExecResult result = cli.execInContainer("/opt/kafka/bin/kafka-broker-api-versions.sh",
                        "--bootstrap-server", alias + ":" + port,
                        "--command-config", "/tmp/short-timeout.properties");

                assertThat(result.getExitCode())
                        .as("the inter-broker listener on port %s must be unreachable from a sibling "
                                + "container, not merely deny an unauthenticated client", port)
                        .isNotZero();
                assertThat(result.getStderr())
                        .as("must fail to connect (refused or timed out), not be denied after "
                                + "authenticating: stderr was %s", result.getStderr())
                        .doesNotContain("SaslAuthenticationException")
                        .doesNotContain("Authentication failed")
                        .containsAnyOf("TimeoutException", "Connection refused", "Errno 111",
                                "Connection to node");
            }
        }
    }

    private static GenericContainer<?> cliContainer() {
        return new GenericContainer<>(DockerImageName.parse(CLI_IMAGE))
                .withNetwork(SecureKafkaStack.network())
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sleep"))
                .withCommand("300")
                .withStartupTimeout(Duration.ofMinutes(2))
                .waitingFor(Wait.forSuccessfulCommand("true"));
    }
}
