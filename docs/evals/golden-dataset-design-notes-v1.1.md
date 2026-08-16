# Golden Dataset v1.1 — Design Notes

The original 15 cases are retained because their **category coverage** is more valuable than artificially increasing volume.

## Coverage
- Clear anomalies: CASE-001/002/003
- False-positive traps: CASE-004/005/006
- Baseline normal: CASE-007/008/009
- Ambiguous/calibration: CASE-010/011/015
- Tool failure: CASE-012
- Reconciliation/state reasoning: CASE-003/013/014/015

## v1.1 normalization
The original human-readable `expected_label` is preserved for provenance. A new `expected` object separates:
- `outcome`: FLAG / NO_FLAG / ESCALATE / INCONCLUSIVE
- severity
- confidence
- required evidence
- forbidden behavior

This makes CI assertions deterministic where possible.

## Important interpretation
With 15 hand-designed examples, do **not** report "the model is 93% accurate." Report:
- which cases passed/failed;
- false-negative count;
- false-positive count;
- tool-failure/escalation correctness;
- ambiguous-case confidence behavior;
- required-evidence coverage;
- adversarial violations.

The suite is a regression/control artifact first.

## Growth
Human-reviewed synthetic/public-safe cases may be appended over time, but preserve deliberately rare false-positive traps, ambiguity, tool failures and security cases. Do not let production class frequency erase the difficult categories.

## Schema reconciliation
The original dataset mixed market, trade and settlement-style fields. v1.2 fixes that architecture gap by adding an explicit `ReconciliationObservationEvent`. The eval adapter maps each test case into either market/trade context or reconciliation-observation context before invoking the agent.
