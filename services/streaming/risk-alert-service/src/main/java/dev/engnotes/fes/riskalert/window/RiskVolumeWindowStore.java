package dev.engnotes.fes.riskalert.window;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The rolling per-ticker volume distribution backing {@code UNUSUAL_VOLUME}, over a fixed
 * {@code windowSeconds} horizon.
 *
 * <p>Placeholder for Task 2: the constructor carries its final shape so Tasks 3 and 5 change only
 * the method body, not every test that builds a {@code TradeStateStores}. {@link #apply} is filled
 * in by Task 3.
 */
public class RiskVolumeWindowStore {

    private final JdbcClient jdbc;
    private final long windowSeconds;

    public RiskVolumeWindowStore(JdbcClient jdbc, long windowSeconds) {
        this.jdbc = jdbc;
        this.windowSeconds = windowSeconds;
    }

    public VolumeWindow apply(EnrichedTradeEvent event) {
        throw new UnsupportedOperationException("implemented in task 3");
    }
}
