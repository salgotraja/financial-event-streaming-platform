package dev.engnotes.fes.common.idempotency;

import java.util.UUID;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class IdempotencyKeysTest {

    @Test
    void the_same_components_always_produce_the_same_key() {
        UUID first = IdempotencyKeys.deterministic("trade-1", "price-deviation", "3");
        UUID second = IdempotencyKeys.deterministic("trade-1", "price-deviation", "3");

        assertThat(first).isEqualTo(second);
    }

    @Test
    void a_different_component_produces_a_different_key() {
        UUID version3 = IdempotencyKeys.deterministic("trade-1", "price-deviation", "3");
        UUID version4 = IdempotencyKeys.deterministic("trade-1", "price-deviation", "4");

        assertThat(version3).isNotEqualTo(version4);
    }

    @Test
    void component_boundaries_are_not_ambiguous() {
        // Without a separator that cannot appear in a component, ("ab", "c") and ("a", "bc") would
        // hash identical bytes and collide, so two different trades could share one alertId.
        assertThat(IdempotencyKeys.deterministic("ab", "c"))
                .isNotEqualTo(IdempotencyKeys.deterministic("a", "bc"));
    }

    @Test
    void the_key_is_a_name_based_uuid() {
        UUID key = IdempotencyKeys.deterministic("trade-1", "price-deviation", "3");

        assertThat(key.version()).isEqualTo(5);
        assertThat(key.variant()).isEqualTo(2);
    }

    @Test
    void a_null_component_is_rejected_rather_than_silently_stringified() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> IdempotencyKeys.deterministic("trade-1", null))
                .withMessageContaining("null");
    }

    @Test
    void no_components_is_rejected() {
        assertThatIllegalArgumentException()
                .isThrownBy(IdempotencyKeys::deterministic);
    }
}
