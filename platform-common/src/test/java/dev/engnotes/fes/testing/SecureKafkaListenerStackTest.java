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

    private static GenericContainer<?> cliContainer() {
        return new GenericContainer<>(DockerImageName.parse(CLI_IMAGE))
                .withNetwork(SecureKafkaStack.network())
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sleep"))
                .withCommand("300")
                .withStartupTimeout(Duration.ofMinutes(2))
                .waitingFor(Wait.forSuccessfulCommand("true"));
    }
}
