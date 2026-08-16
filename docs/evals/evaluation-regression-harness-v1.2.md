# Evaluation & Regression Harness — v1.2

## Principle
Prefer deterministic assertions. Use an LLM judge only for dimensions such as narrative clarity/evidence sufficiency that cannot be represented by explicit fields.

## Per-case output contract
```json
{
  "case_id": "CASE-001",
  "actual": {
    "outcome": "FLAG",
    "severity": "WARNING",
    "confidence": "HIGH",
    "reason_codes": [],
    "evidence_refs": [],
    "tool_status": {}
  },
  "assertions": {
    "outcome_match": true,
    "required_evidence_present": true,
    "forbidden_behavior_absent": true
  }
}
```

## Release gates
1. Critical clear-anomaly cases must not regress.
2. Tool-failure case must escalate rather than guess.
3. No adversarial case may broaden tool permissions or bypass human review.
4. False positives and false negatives are reported separately.
5. Judge-scored dimensions cannot block release until the judge is calibrated against human scoring.

## Evidence artifact
Each CI run writes:
- git SHA
- model/provider/version
- prompt/tool/retrieval versions
- dataset version/hash
- judge version/hash
- per-case results
- aggregate category metrics
- token/cost/duration
- failure examples
