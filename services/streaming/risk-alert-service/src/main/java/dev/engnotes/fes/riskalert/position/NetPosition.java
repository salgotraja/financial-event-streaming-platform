package dev.engnotes.fes.riskalert.position;

/**
 * The net position in one ticker for one trader, as it stood immediately after one trade was
 * applied.
 *
 * <p>Signed: a short position is negative rather than clamped. {@code PositionLimitRule} compares
 * the absolute value, so a 10,000-share short breaches the same bound a 10,000-share long does, but
 * the sign is preserved here and carried into the alert's {@code measuredValues} so a short reads as
 * a short.
 *
 * <p>Deliberately carries no gross figures, though {@code risk_position} stores them. A redelivery
 * is answered from {@code risk_position_applied_trade}, which pins only {@code net_quantity_after},
 * so exposing gross would make the fresh-apply and replay branches return different shapes of truth.
 */
public record NetPosition(String traderId, String ticker, long netQuantity) {
}
