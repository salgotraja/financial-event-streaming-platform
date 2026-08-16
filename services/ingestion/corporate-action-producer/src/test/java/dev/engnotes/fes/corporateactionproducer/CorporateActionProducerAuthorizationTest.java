package dev.engnotes.fes.corporateactionproducer;

import dev.engnotes.fes.testing.KafkaProducerAuthorizationContract;
import org.junit.jupiter.api.DisplayName;

@DisplayName("corporate-action-producer workload identity")
class CorporateActionProducerAuthorizationTest extends KafkaProducerAuthorizationContract {

    @Override
    protected String principal() {
        return "corporate-action-producer";
    }

    @Override
    protected String ownTopic() {
        return "corporate-actions";
    }

    @Override
    protected String foreignTopic() {
        // Instrument reference data is compacted, so a write from the wrong identity does not merely
        // add a record, it replaces the row every downstream cache rebuilds from.
        return "reference-data.instruments";
    }
}
