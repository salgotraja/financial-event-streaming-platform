-- Creates the risk-alert-service position state that POSITION_LIMIT_BREACH evaluates against.
--
-- Why two tables. risk_position carries the running total. risk_position_applied_trade is the
-- idempotency ledger FR-11.3 requires, and it does a second job: net_quantity_after pins the
-- position a given tradeId produced, so an at-least-once redelivery re-evaluates against the net as
-- it was rather than the net as it is now (ADR-019, ADR-036).
--
-- This schema belongs to risk-alert-service alone (ADR-028). It is not FR-11's position read model,
-- which position-exposure-service will own; the duplication is deliberate and recorded in ADR-036.

CREATE TABLE risk_position (
    trader_id            VARCHAR(64)  NOT NULL,
    ticker               VARCHAR(32)  NOT NULL,
    net_quantity         BIGINT       NOT NULL,
    gross_buy            BIGINT       NOT NULL,
    gross_sell           BIGINT       NOT NULL,
    last_event_timestamp TIMESTAMPTZ  NOT NULL,
    -- Incremented on every update for the audit intent of ADR-008. It is not a compare-and-swap:
    -- the ON CONFLICT DO UPDATE row lock is the concurrency control, and trades.enriched is keyed
    -- on ticker, so each (trader_id, ticker) is single-writer within the consumer group anyway.
    version              BIGINT       NOT NULL,
    updated_at           TIMESTAMPTZ  NOT NULL,
    CONSTRAINT pk_risk_position PRIMARY KEY (trader_id, ticker)
);

CREATE TABLE risk_position_applied_trade (
    trade_id           VARCHAR(64)  NOT NULL,
    trader_id          VARCHAR(64)  NOT NULL,
    ticker             VARCHAR(32)  NOT NULL,
    -- Nullable for exactly one instant: row 1 of the apply transaction inserts it, statement 3 of
    -- the same transaction fills it in. No committed row ever carries NULL here.
    net_quantity_after BIGINT,
    applied_at         TIMESTAMPTZ  NOT NULL,
    CONSTRAINT pk_risk_position_applied_trade PRIMARY KEY (trade_id)
);

-- Point lookups only, never a scan (.claude/rules/database.md). The primary keys serve every query
-- this increment issues, so no secondary index is created. Pruning this ledger is a recorded
-- deferral: trades.enriched retains 7 days, so older rows cannot be redelivered.
