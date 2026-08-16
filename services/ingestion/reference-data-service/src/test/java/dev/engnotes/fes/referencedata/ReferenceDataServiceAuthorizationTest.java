package dev.engnotes.fes.referencedata;

import dev.engnotes.fes.testing.KafkaProducerAuthorizationContract;
import org.junit.jupiter.api.DisplayName;

@DisplayName("reference-data-service workload identity")
class ReferenceDataServiceAuthorizationTest extends KafkaProducerAuthorizationContract {

    @Override
    protected String principal() {
        return "reference-data-service";
    }

    @Override
    protected String ownTopic() {
        return "reference-data.instruments";
    }

    @Override
    protected String foreignTopic() {
        return "corporate-actions";
    }
}
