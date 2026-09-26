package dev.engnotes.fes.positionexposure.position;

import java.math.BigDecimal;

/**
 * One position in one ticker for one trader on one account, as it stood immediately after one trade
 * was applied. This is the whole payload of a {@code PositionSnapshotEvent} bar its identifiers.
 *
 * <p>Signed: a short position is negative rather than clamped, and {@code marketValue} is negative
 * with it. This is a read model reporting direction as well as size.
 *
 * <p><strong>{@code marketValue} is mark-to-last-trade.</strong> It is {@code netQuantity} times the
 * mid price carried on the trade that last moved this position, never a live price. Reading a live
 * price would add a Plane 2 datastore dependency this service does not have and would make a
 * replayed trade produce a different value than the original (ADR-038). A position whose ticker has
 * not traded recently therefore carries a stale valuation, and this record must never be described
 * as a mark-to-market figure.
 *
 * <p>{@code BigDecimal} rather than {@code double} for the value, matching the column: a money
 * figure that is published and later compared for reconciliation (FR-11.5) must not carry binary
 * floating-point error into that comparison.
 */
public record Position(String accountId,
                        String traderId,
                        String ticker,
                        long netQuantity,
                        long grossBuyQuantity,
                        long grossSellQuantity,
                        BigDecimal marketValue) {
}
