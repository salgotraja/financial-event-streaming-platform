package dev.engnotes.fes.corporateactionproducer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import dev.engnotes.fes.events.CorporateActionType;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic destination topic. The workload identity for this service is authorised to write
 *              this topic and nothing else, so changing it requires an IAM policy change too.
 * @param seed  the startup announcement, off unless switched on. FR-01.3 requires this service to
 *              publish corporate actions; it does not require it to announce a fixed set on every
 *              boot, so a default deployment stays a publisher and nothing else.
 */
@ConfigurationProperties(prefix = "fes.corporate-action-producer")
public record CorporateActionProducerProperties(
        @DefaultValue("corporate-actions") String topic,
        Seed seed) {

    public CorporateActionProducerProperties {
        seed = seed == null ? Seed.defaults() : seed;
    }

    /**
     * @param enabled whether the seeder runs at all
     * @param actions the actions announced at startup
     */
    public record Seed(boolean enabled, List<Action> actions) {

        /**
         * The shipped default: one dividend and one split, so two action types with different
         * required attributes are both exercised. Named here rather than only in a
         * {@code @DefaultValue} annotation so a test can assert against the same list the binder
         * uses, instead of a second copy free to drift from it.
         */
        public static Seed defaults() {
            return new Seed(false, List.of(
                    new Action("RELIANCE", CorporateActionType.DIVIDEND_DECLARATION,
                            Map.of("dividendPerShare", "9.00"), Duration.ofDays(14)),
                    new Action("TCS", CorporateActionType.STOCK_SPLIT,
                            Map.of("splitRatio", "1:2"), Duration.ofDays(30))));
        }

        public Seed {
            actions = actions == null || actions.isEmpty() ? defaults().actions() : List.copyOf(actions);
        }
    }

    /**
     * @param ticker      the instrument affected
     * @param actionType  which action, deciding which attributes the validator requires
     * @param attributes  the type-specific detail. A dividend needs {@code dividendPerShare}, a
     *                    split needs {@code splitRatio}, a rights issue needs {@code ratio} and
     *                    {@code subscriptionPrice}, and an earnings announcement needs none.
     * @param effectiveIn how far after the announcement the action takes effect. The validator
     *                    rejects an action that takes effect before it was announced, so this is
     *                    never negative.
     */
    public record Action(String ticker,
                         CorporateActionType actionType,
                         @DefaultValue Map<String, String> attributes,
                         @DefaultValue("7d") Duration effectiveIn) {
    }
}
