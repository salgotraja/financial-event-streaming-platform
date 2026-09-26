package dev.engnotes.fes.tradeproducer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * API docs are a local convenience, not a deployed surface. Without a profile neither the OpenAPI
 * document nor the Scalar UI may be served; the dev profile turns both on.
 *
 * <p>No broker runs here. The context starts because nothing in this service touches Kafka until a
 * record is sent.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@AutoConfigureMockMvc
@DisplayName("API docs exposure")
class ApiDocsExposureTest {

    @Autowired
    private MockMvcTester mvc;

    @Test
    @DisplayName("should serve neither the OpenAPI document nor the Scalar UI when no profile is active")
    void should_serve_neither_openapi_nor_scalar_when_no_profile_is_active() {
        assertThat(mvc.get().uri("/v3/api-docs")).hasStatus(404);
        assertThat(mvc.get().uri("/scalar")).hasStatus(404);
    }

    @Nested
    @ActiveProfiles("dev")
    @DisplayName("under the dev profile")
    class UnderDevProfile {

        @Autowired
        private MockMvcTester mvc;

        @Autowired
        @Qualifier("requestMappingHandlerMapping") private RequestMappingHandlerMapping handlerMapping;

        @Test
        @DisplayName("should serve an OpenAPI document that includes the actuator health path")
        void should_serve_openapi_document_including_actuator_health_when_dev_profile_is_active() {
            assertThat(mvc.get().uri("/v3/api-docs"))
                    .hasStatusOk()
                    .hasContentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                    .bodyText().contains("/actuator/health");
        }

        @Test
        @DisplayName("should serve the Scalar UI from springdoc's controller, pointed at the OpenAPI document")
        void should_serve_scalar_ui_from_springdoc_controller_when_dev_profile_is_active() {
            var scalarHandlers = handlerMapping.getHandlerMethods().entrySet().stream()
                    .filter(entry -> servesPath(entry.getKey(), "/scalar"))
                    .map(entry -> entry.getValue().getBeanType().getName())
                    .toList();
            assertThat(scalarHandlers)
                    .as("scalar-webmvc registers a standalone /scalar controller of its own; exactly "
                            + "one handler, springdoc's, must own the path")
                    .containsExactly("org.springdoc.webmvc.scalar.ScalarWebMvcController");

            assertThat(mvc.get().uri("/scalar"))
                    .hasStatusOk()
                    .hasContentTypeCompatibleWith(MediaType.TEXT_HTML)
                    .bodyText().contains("/v3/api-docs");
        }

        private static boolean servesPath(RequestMappingInfo info, String path) {
            return info.getPatternValues().contains(path);
        }
    }
}
