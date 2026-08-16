package dev.engnotes.fes.corporateactionproducer;

import java.util.Map;

import dev.engnotes.fes.events.CorporateActionEvent;
import dev.engnotes.fes.events.CorporateActionType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("CorporateActionValidator")
class CorporateActionValidatorTest {

    @Test
    @DisplayName("should accept a well formed action of each type")
    void should_accept_a_well_formed_action_of_each_type() {
        assertThatCode(() -> {
            CorporateActionValidator.validate(CorporateActions.split("CA-1", "RELIANCE"));
            CorporateActionValidator.validate(CorporateActions.dividend("CA-2", "TCS"));
            CorporateActionValidator.validate(CorporateActions.rightsIssue("CA-3", "INFY"));
            CorporateActionValidator.validate(CorporateActions.earnings("CA-4", "WIPRO"));
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should reject a stock split carrying no split ratio")
    void should_reject_a_stock_split_carrying_no_split_ratio() {
        CorporateActionEvent noRatio = CorporateActions.action(
                "CA-5", "RELIANCE", CorporateActionType.STOCK_SPLIT, Map.of());

        assertThatThrownBy(() -> CorporateActionValidator.validate(noRatio))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("splitRatio");
    }

    @Test
    @DisplayName("should reject a dividend carrying no amount per share")
    void should_reject_a_dividend_carrying_no_amount_per_share() {
        CorporateActionEvent noAmount = CorporateActions.action(
                "CA-6", "TCS", CorporateActionType.DIVIDEND_DECLARATION, Map.of("exDate", "2026-09-01"));

        assertThatThrownBy(() -> CorporateActionValidator.validate(noAmount))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dividendPerShare");
    }

    @Test
    @DisplayName("should report every missing attribute rather than only the first")
    void should_report_every_missing_attribute_rather_than_only_the_first() {
        CorporateActionEvent bare = CorporateActions.action(
                "CA-7", "INFY", CorporateActionType.RIGHTS_ISSUE, Map.of());

        assertThatThrownBy(() -> CorporateActionValidator.validate(bare))
                .hasMessageContaining("ratio")
                .hasMessageContaining("subscriptionPrice");
    }

    @Test
    @DisplayName("should treat a blank attribute value as missing")
    void should_treat_a_blank_attribute_value_as_missing() {
        CorporateActionEvent blank = CorporateActions.action(
                "CA-8", "RELIANCE", CorporateActionType.STOCK_SPLIT, Map.of("splitRatio", "  "));

        assertThatThrownBy(() -> CorporateActionValidator.validate(blank))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("splitRatio");
    }

    @Test
    @DisplayName("should accept an earnings announcement with no attributes")
    void should_accept_an_earnings_announcement_with_no_attributes() {
        // An earnings announcement carries its meaning in effectiveAt. Requiring an attribute would
        // invent a contract the requirement does not state.
        assertThatCode(() -> CorporateActionValidator.validate(CorporateActions.earnings("CA-9", "TCS")))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should reject an action that takes effect before it was announced")
    void should_reject_an_action_that_takes_effect_before_it_was_announced() {
        CorporateActionEvent backdated = CorporateActionEvent.newBuilder(
                        CorporateActions.split("CA-10", "WIPRO"))
                .setEffectiveAt(CorporateActions.ANNOUNCED_AT.minusSeconds(1))
                .build();

        assertThatThrownBy(() -> CorporateActionValidator.validate(backdated))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("before it was announced");
    }

    @Test
    @DisplayName("should allow an action that takes effect the instant it is announced")
    void should_allow_an_action_that_takes_effect_the_instant_it_is_announced() {
        CorporateActionEvent immediate = CorporateActionEvent.newBuilder(
                        CorporateActions.split("CA-11", "WIPRO"))
                .setEffectiveAt(CorporateActions.ANNOUNCED_AT)
                .build();

        assertThatCode(() -> CorporateActionValidator.validate(immediate)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should pin the derived attribute key names for each action type")
    void should_pin_the_derived_attribute_key_names_for_each_action_type() {
        // These key names are derived from the schema doc, not specified by any document, so they are
        // pinned here: a silent edit to the switch breaks a test rather than a downstream consumer.
        // Exhaustiveness itself needs no test, the compiler rejects a new enum symbol.
        assertThat(CorporateActionValidator.requiredAttributes(CorporateActionType.STOCK_SPLIT))
                .containsExactly("splitRatio");
        assertThat(CorporateActionValidator.requiredAttributes(CorporateActionType.DIVIDEND_DECLARATION))
                .containsExactly("dividendPerShare");
        assertThat(CorporateActionValidator.requiredAttributes(CorporateActionType.RIGHTS_ISSUE))
                .containsExactlyInAnyOrder("ratio", "subscriptionPrice");
        assertThat(CorporateActionValidator.requiredAttributes(CorporateActionType.EARNINGS_ANNOUNCEMENT))
                .isEmpty();
    }

    @Test
    @DisplayName("should reject a null attribute map by name rather than in the serializer")
    void should_reject_a_null_attribute_map_by_name_rather_than_in_the_serializer() {
        CorporateActionEvent nullAttributes = CorporateActionEvent.newBuilder(
                        CorporateActions.earnings("CA-12", "TCS"))
                .build();
        nullAttributes.setAttributes(null);

        assertThatThrownBy(() -> CorporateActionValidator.validate(nullAttributes))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("attributes");
    }
}
