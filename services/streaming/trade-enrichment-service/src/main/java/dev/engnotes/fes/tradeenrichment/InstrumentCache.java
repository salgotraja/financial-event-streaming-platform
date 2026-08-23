package dev.engnotes.fes.tradeenrichment;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import dev.engnotes.fes.events.InstrumentReferenceEvent;

/**
 * The instrument master, folded from a compacted topic and looked up by ticker.
 *
 * <p>The topic is keyed by {@code instrumentId} but every lookup arrives as a ticker off a
 * {@code TradeEvent}, so the map is keyed by ticker and a second map remembers which ticker each
 * instrument id currently occupies. Without that second map a ticker change would leave the old
 * symbol enriching trades forever, and a tombstone, which carries no value and therefore no ticker,
 * could not find what to remove.
 *
 * <p>Trade listener threads read this while one loader thread writes it, with no synchronization
 * beyond {@code ConcurrentHashMap}'s. That is sufficient because of what a stale read costs, not
 * because of the map: a trade reading a {@code sharesOutstanding} one version behind produces a
 * slightly stale {@code marketCap}, not a wrong one, and the next trade for that ticker picks up the
 * new value. Nothing here accumulates, so a stale read cannot compound.
 */
public class InstrumentCache {

    private final Map<String, Instrument> byTicker = new ConcurrentHashMap<>();
    private final Map<String, String> tickerByInstrumentId = new ConcurrentHashMap<>();

    /** @param event the record's value, or null for a compaction tombstone */
    public void apply(String instrumentId, InstrumentReferenceEvent event) {
        if (event == null) {
            String previous = tickerByInstrumentId.remove(instrumentId);
            if (previous != null) {
                byTicker.remove(previous);
            }
            return;
        }

        String ticker = event.getTicker().toString();
        String previous = tickerByInstrumentId.put(instrumentId, ticker);
        if (previous != null && !previous.equals(ticker)) {
            byTicker.remove(previous);
        }
        byTicker.put(ticker, new Instrument(ticker, event.getSharesOutstanding()));
    }

    public Optional<Instrument> find(String ticker) {
        return Optional.ofNullable(byTicker.get(ticker));
    }

    public int size() {
        return byTicker.size();
    }
}
