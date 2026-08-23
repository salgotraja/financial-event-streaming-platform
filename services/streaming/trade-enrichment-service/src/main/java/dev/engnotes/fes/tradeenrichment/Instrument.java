package dev.engnotes.fes.tradeenrichment;

/**
 * What enrichment uses from the instrument master, and nothing else.
 *
 * <p>The event carries isin, sector, exchange and security type too. None of them is read here, and
 * holding the whole record would put reference data in the process for no reader.
 */
public record Instrument(String ticker, long sharesOutstanding) {
}
