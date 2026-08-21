package dev.engnotes.fes.tradeproducer;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic      destination topic. The workload identity for this service is authorised to
 *                   write this topic and nothing else, so changing it requires an IAM policy change
 *                   too.
 * @param generation synthetic trade generation, off unless switched on. FR-01.1 requires this
 *                   service to publish trades; it does not require it to invent them on boot, so a
 *                   default deployment stays a publisher and nothing else.
 */
@ConfigurationProperties(prefix = "fes.trade-producer")
public record TradeProducerProperties(@DefaultValue("trades.raw") String topic,
                                      @DefaultValue Generation generation) {

    /**
     * @param enabled       whether the driver runs at all
     * @param ratePerSecond target trades per second. A configured rate is not a measured rate; the
     *                      NFR-01.1 result is a Phase 8 load test and nothing here substitutes for
     *                      it.
     * @param batchInterval how often the driver wakes. The JVM cannot reliably park for less than a
     *                      millisecond, so the driver emits a whole batch per wake.
     * @param tickers       the symbols trades are drawn from
     */
    public record Generation(@DefaultValue("false") boolean enabled,
                             @DefaultValue("100") int ratePerSecond,
                             @DefaultValue("50ms") Duration batchInterval,
                             @DefaultValue({"AAPL", "MSFT", "GOOGL", "AMZN"}) List<String> tickers) {
    }
}
