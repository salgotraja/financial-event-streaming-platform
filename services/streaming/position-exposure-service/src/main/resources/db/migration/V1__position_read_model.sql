-- Creates the position-exposure-service read model (ADR-017, ADR-038).
--
-- The grain is (account_id, trader_id, ticker), which is FR-11.1's "by account, trader, and ticker"
-- and exactly the key PositionSnapshotEvent carries. trades.enriched is keyed on ticker, and ticker
-- is a component of this key, so every row here is written by one consumer in offset order: that
-- single-writer property is what lets the upsert's row lock be the concurrency control rather than
-- optimistic locking, the same resolution ADR-036 reached for risk_position.
--
-- This schema belongs to position-exposure-service alone (ADR-028). It deliberately duplicates
-- numbers risk-alert-service also holds, and nothing reconciles the two: ADR-017 accepted that, and
-- FR-11.5's rebuild-and-reconcile compares this service's live and rebuilt models, not the two
-- services.

CREATE TABLE position (
    account_id           VARCHAR(64)  NOT NULL,
    trader_id            VARCHAR(64)  NOT NULL,
    ticker               VARCHAR(32)  NOT NULL,
    net_quantity         BIGINT       NOT NULL,
    gross_buy_quantity   BIGINT       NOT NULL,
    gross_sell_quantity  BIGINT       NOT NULL,
    -- Mark-to-last-trade, not a live mark: net_quantity times the mid price carried on the trade
    -- that last moved this row. A live mark would need a Redis read this service deliberately does
    -- not make, and would make a replayed trade produce a different value than the original.
    -- A position whose ticker has not traded recently therefore carries a stale valuation.
    market_value         NUMERIC(19,4) NOT NULL,
    last_event_timestamp TIMESTAMPTZ  NOT NULL,
    updated_at           TIMESTAMPTZ  NOT NULL,
    CONSTRAINT pk_position PRIMARY KEY (account_id, trader_id, ticker)
);

-- The idempotency ledger FR-11.3 requires, and the replay pin.
--
-- It stores all four published figures, where risk-alert-service's counterpart stores only the net.
-- That difference is deliberate: there, the net is the only figure the rule reads. Here the whole
-- snapshot is the output, so every field has to replay identically or a redelivered snapshot
-- differs from the original in a field nobody was watching.
--
-- Nothing prunes this table. trades.enriched retains 7 days, so older rows cannot be redelivered
-- and are dead weight rather than a correctness need. It carries no event-time column a prune could
-- safely use, which is the same recorded deferral risk-alert-service carries for its two ledgers.
CREATE TABLE position_applied_trade (
    trade_id           VARCHAR(64)   NOT NULL,
    account_id         VARCHAR(64)   NOT NULL,
    trader_id          VARCHAR(64)   NOT NULL,
    ticker             VARCHAR(32)   NOT NULL,
    -- Nullable for exactly one instant: row 1 of the apply transaction inserts it, the last
    -- statement of the same transaction fills them in. No committed row carries NULL here.
    net_quantity_after  BIGINT,
    gross_buy_after     BIGINT,
    gross_sell_after    BIGINT,
    market_value_after  NUMERIC(19,4),
    applied_at          TIMESTAMPTZ  NOT NULL,
    CONSTRAINT pk_position_applied_trade PRIMARY KEY (trade_id)
);

-- Point lookups only, never a scan (.claude/rules/database.md). The primary keys serve every query
-- this increment issues, so no secondary index is created.
