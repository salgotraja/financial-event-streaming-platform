# ADR-006: Geometric Brownian Motion and Pareto Volume for the Synthetic Market

**Status:** Accepted
**Date:** 2026-08-16

## Context
The platform needs a market-data source that sustains 50,000 events/sec and produces statistically
plausible prices and volumes. Licensed real-time feeds are out of scope and would make load testing
impossible.

## Decision
Generate prices in-process with Geometric Brownian Motion (configurable drift and volatility) and
trade volumes with a Pareto distribution (shape 1.5).

## Alternatives
- **Uniform or Gaussian random walk.** Trivial. Rejected: symmetric, thin-tailed volume never
  triggers the FR-04.2 unusual-volume rule realistically, so the risk rules would be tested against
  data that cannot exercise them.
- **Replayed historical market data.** Most realistic. Rejected: licensing, storage, and the
  inability to dial throughput to 50,000 events/sec on demand.

## Consequences
Heavy-tailed volume genuinely exercises the three-sigma rule and the false-positive traps in the
golden dataset. The model is explicitly not a claim about market microstructure fidelity, and no
result from it may be presented as a market observation.
