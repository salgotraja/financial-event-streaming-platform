package dev.engnotes.fes.riskalert;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic               the enriched trade topic, overridable so integration tests do not share
 *                            a topic with another module's tests
 * @param ruleTopic           the governed rule lifecycle topic, not compacted (ADR-035)
 * @param outputTopic         the alert topic
 * @param consumerInstance    carried into every DeadLetterEvent
 * @param ruleTimelineTimeout how long startup waits for the initial fold before failing
 * @param volumeWindowSeconds the rolling volume distribution's horizon, in seconds
 * @param recentTradeHorizonSeconds the self-cross candidate window's horizon, in seconds
 * @param recentTradeCandidateCap the maximum prior trades returned as self-cross candidates
 */
@ConfigurationProperties(prefix = "fes.risk-alert-service")
public record RiskAlertProperties(String topic,
                                  String ruleTopic,
                                  String outputTopic,
                                  String consumerInstance,
                                  Duration ruleTimelineTimeout,
                                  @DefaultValue("3600") long volumeWindowSeconds,
                                  @DefaultValue("3600") long recentTradeHorizonSeconds,
                                  @DefaultValue("200") int recentTradeCandidateCap) {

    /**
     * Rejects the three window settings at startup rather than letting them silence a rule at
     * runtime.
     *
     * <p>These are operator configuration, not governed rule parameters, so ADR-035's rejection
     * slugs do not apply: there is no metric tag to keep bounded and nothing downstream to degrade
     * to. Failing the context is the right response, because every bad value here fails the same
     * way, silently.
     *
     * <p>A {@code recentTradeCandidateCap} of zero is the one worth spelling out.
     * {@code RiskRecentTradeStore} queries {@code cap + 1} rows so truncation is detectable, so a
     * cap of zero asks for one row, finds it, reports the set as truncated, and hands the rule an
     * empty candidate list. {@code SelfCrossRule} would then never alert while
     * {@code risk.self.cross.candidates.truncated} climbed on every trade: a rule switched off by a
     * configuration typo, announcing it only in a counter nobody is watching yet.
     *
     * <p>A non-positive horizon is the same shape of failure. The volume fold would cover an empty
     * or inverted range and every window would look empty, and the self-cross candidate query would
     * admit nothing.
     */
    public RiskAlertProperties {
        if (volumeWindowSeconds <= 0L) {
            throw new IllegalArgumentException(
                    "fes.risk-alert-service.volume-window-seconds must be above zero, was " + volumeWindowSeconds);
        }
        if (recentTradeHorizonSeconds <= 0L) {
            throw new IllegalArgumentException("fes.risk-alert-service.recent-trade-horizon-seconds "
                    + "must be above zero, was " + recentTradeHorizonSeconds);
        }
        if (recentTradeCandidateCap <= 0) {
            throw new IllegalArgumentException("fes.risk-alert-service.recent-trade-candidate-cap "
                    + "must be above zero, was " + recentTradeCandidateCap
                    + ". A cap of zero silences the self-cross rule rather than disabling it.");
        }
    }
}
