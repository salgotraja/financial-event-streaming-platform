-- Creates the risk-alert-service windowed and correlation state for increment 3.
--
-- Four tables, two stores. risk_volume_bucket plus risk_volume_ticker are the rolling trade-size
-- distribution UNUSUAL_VOLUME folds; risk_volume_applied_trade pins what each trade evaluated
-- against so a redelivery reproduces its verdict (ADR-019, ADR-035). risk_recent_trade is the
-- self-cross candidate source, bounded by arrival order rather than event time.
--
-- These belong to risk-alert-service alone (ADR-028). The role owns the schema, so Flyway creates
-- the BIGSERIAL sequence below as that role and no extra GRANT is required.

-- The rolling distribution, as additive (n, sum, sumsq) rollups. Bucket width is 60 seconds, so a
-- 60-minute fold reads at most 60 rows per ticker. Width changes the fold cost and the pruning
-- granularity, never the statistic: (n, Sx, Sx2) folds to exactly the mean and standard deviation
-- of the underlying individual trade quantities.
--
-- NUMERIC rather than BIGINT for the two totals: an hour of a busy ticker can carry a sum of
-- squares past the exact range of a double, and the naive variance form then cancels to a small
-- negative value whose square root is NaN, which reads as a rule that silently stopped alerting.
CREATE TABLE risk_volume_bucket (
    ticker         VARCHAR(32)   NOT NULL,
    bucket_start   TIMESTAMPTZ   NOT NULL,
    trade_count    BIGINT        NOT NULL,
    quantity_sum   NUMERIC(38,0) NOT NULL,
    quantity_sumsq NUMERIC(38,0) NOT NULL,
    CONSTRAINT pk_risk_volume_bucket PRIMARY KEY (ticker, bucket_start)
);

-- The per-ticker prune cutoff. RiskPositionStore uses GREATEST on last_event_timestamp because
-- records arrive in offset order but eventTimestamp can go backwards; the bucket prune faces the
-- same fact with worse consequences. A cutoff taken from the current trade would let a late older
-- trade resurrect a pruned bucket, changing every later fold for that ticker. This high-water mark
-- only ever advances.
CREATE TABLE risk_volume_ticker (
    ticker          VARCHAR(32) NOT NULL,
    high_water_mark TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_risk_volume_ticker PRIMARY KEY (ticker)
);

-- The replay pin. A redelivery answers from here rather than re-folding a window that has moved on.
--
-- Nothing prunes this table today, and that is a deferral rather than a design, the same one V1
-- recorded for risk_position_applied_trade. It could only safely be pruned at the trades.enriched
-- retention horizon of 7 days, never at the window horizon, because a row still inside retention is
-- a row a redelivery needs. The one column that looks like a cutoff, applied_at, is wall-clock, and
-- ADR-035 keeps wall-clock values out of decisions that must replay identically, so the prune needs
-- an event-time column this table does not yet carry.
CREATE TABLE risk_volume_applied_trade (
    trade_id     VARCHAR(64)   NOT NULL,
    ticker       VARCHAR(32)   NOT NULL,
    window_n     BIGINT        NOT NULL,
    window_sum   NUMERIC(38,0) NOT NULL,
    window_sumsq NUMERIC(38,0) NOT NULL,
    applied_at   TIMESTAMPTZ   NOT NULL,
    CONSTRAINT pk_risk_volume_applied_trade PRIMARY KEY (trade_id)
);

-- The self-cross candidate source.
--
-- applied_seq, not event_timestamp, bounds the candidate set. Event-time bounding diverges on
-- replay: a trade at t=100 arriving after a trade at t=200 is invisible to the second trade's first
-- delivery and visible to its replay, so the verdict changes. applied_seq is fixed at first insert,
-- ON CONFLICT DO NOTHING preserves it, and rows below it survive because this table prunes only at
-- the 7-day retention horizon. That is why this rule needs no pinned-match column.
--
-- BIGSERIAL is non-transactional, so two concurrent transactions can in general commit out of
-- sequence order. Within one (trader_id, ticker) they cannot: trades.enriched is keyed on ticker,
-- so each key is single-writer within the consumer group. That is the same assumption
-- RiskPositionStore's row-lock concurrency control already rests on.
CREATE TABLE risk_recent_trade (
    trade_id        VARCHAR(64)   NOT NULL,
    applied_seq     BIGSERIAL     NOT NULL,
    trader_id       VARCHAR(64)   NOT NULL,
    ticker          VARCHAR(32)   NOT NULL,
    side            VARCHAR(4)    NOT NULL,
    quantity        BIGINT        NOT NULL,
    price           NUMERIC(19,4) NOT NULL,
    event_timestamp TIMESTAMPTZ   NOT NULL,
    CONSTRAINT pk_risk_recent_trade PRIMARY KEY (trade_id),
    CONSTRAINT uq_risk_recent_trade_applied_seq UNIQUE (applied_seq)
);

-- Serves the candidate query exactly: equality on the first two columns, descending range on the
-- third. A bounded index range with a hard row cap, recorded in ADR-037 as a deliberate departure
-- from the point-lookup rule in .claude/rules/database.md.
CREATE INDEX ix_risk_recent_trade_candidates
    ON risk_recent_trade (trader_id, ticker, applied_seq DESC);
