package dev.engnotes.fes.testing;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("the secure stack's schema registry")
class SecureSchemaRegistryStackTest {

    @Test
    @DisplayName("should serve a subject registered against the authenticated broker")
    void should_serve_a_subject_registered_against_the_authenticated_broker() throws Exception {
        String schema = """
                {"type":"record","name":"Probe","namespace":"dev.engnotes.fes.testing",\
                "fields":[{"name":"value","type":"string"}]}""";

        SecureKafkaStack.registerSubject("identity-probe-value", schema);

        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .uri(URI.create(SecureKafkaStack.schemaRegistryUrl() + "/subjects"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body())
                .as("the registry stores its state in a topic on the secure broker, so a "
                        + "readable subject also proves it authenticated to that broker")
                .contains("identity-probe-value");
    }
}
