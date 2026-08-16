# ADR-024: Versioned Golden Dataset and CI Regression Gate

**Status:** Accepted  
**Date:** 2026-08-16

## Decision
Use the supplied 15-case dataset as the initial versioned regression suite. Preserve clear anomalies, false-positive traps, baseline normals, ambiguity, tool failure and reconciliation/state cases as explicit categories.

Normalize expected outcomes for deterministic scoring. Use LLM-as-judge only for dimensions such as narrative/evidence quality that cannot be asserted directly, and calibrate the judge against human-labelled examples.

## Important limitation
Fifteen hand-designed cases do not justify a production accuracy claim. The artifact demonstrates regression discipline and failure-mode coverage, not statistical generalization.
