# Golden Dataset v1 — Design Notes

15 cases, not hundreds, per FR-E1. Provenance and category coverage matter
more than volume at this stage.

## Category breakdown (why these 15, specifically)

| Category | Cases | Purpose |
|---|---|---|
| Clear anomalies | CASE-001, 002, 003 | Baseline true positives across price, volume, and settlement — sanity checks that detection works at all |
| False-positive traps | CASE-004, 005, 006 | The highest-value cases. Each looks statistically anomalous but has a legitimate explanation (systemic move, scheduled event, or an already-dismissed precedent). These are what separate a real detector from a threshold-triggered alarm |
| Baseline normal | CASE-007, 008, 009 | Ordinary activity that must *not* get flagged — the direct measure of false-positive rate on the most common case type |
| Ambiguous | CASE-010, 011, 015 | Deliberately unresolved. Scored on whether the agent expresses calibrated uncertainty instead of forcing a confident answer either way |
| Tool failure | CASE-012 | Tests NFR-A3 (tool-failure handling) inside the eval harness itself, not just the happy path |
| Reconciliation-specific | CASE-013, 014, 015 | Drawn from real-world payment-system failure modes. Ledger-vs-external drift (013), mutually exclusive state transitions on one entity (014), and incomplete multi-party settlement data (015). These test detection over *entity state and relationships*, not statistical outliers on a single event, which is a materially different reasoning path |

This directly satisfies NFR-E2: false-negative risk (CASE-001/002/003 catch
misses), false-positive risk (CASE-004/005/006/007/008/009 catch
over-flagging), and they're scored and reported separately, never blended
into one accuracy number.

## Provenance of CASE-013 to 015
Adapted from documented real-world payment-system failure modes (duplicate
and out-of-order event handling, gateway-versus-ledger reconciliation
drift, competing reversal paths, incomplete multi-party settlement).
Adapted as *anomaly scenarios to detect*, not as payment-processing
features to build. The platform observes financial events; it does not
move money, and nothing here implies it should.

Two of these test something the original 12 did not: CASE-013 and 014
require reasoning over an entity's state and its relationships to other
records, rather than judging one event against a statistical baseline.
That is the reasoning path FR-A7 precedent retrieval and the Neo4j graph
layer exist to support, so these cases also serve as the first real
exercise of that layer.

## How this grows (FR-E2)
Every case here was hand-constructed. From day one of real operation, the
dataset grows from actual reviewed decisions in the human-feedback memory
store — a human's approve/reject verdict on a real flagged event becomes
a new golden-dataset entry, following the same shape as the 12 above.
Target the same category balance over time (don't let it silently become
95% baseline-normal cases just because that's what production mostly
looks like) — false-positive traps and ambiguous cases are rarer in
practice but are exactly the ones worth deliberately preserving and
adding to as they occur.

## Using this with an eval framework
The JSON shape here maps cleanly onto DeepEval, Promptfoo, or Langfuse
dataset formats with minor field renaming — deliberately kept
framework-agnostic until a specific tool is chosen for the CI gate (FR-E5).

## A known gap, stated plainly
The `event` fields here are a reasonable representative shape, not
pulled from the actual Avro schema in the original spec (that file isn't
available in this session). Reconcile field names against the real
schema before wiring this into CI — the categories and expected labels
will hold, the exact field names may need adjusting.
