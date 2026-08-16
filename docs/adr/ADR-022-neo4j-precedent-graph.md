# ADR-022: Neo4j as a Derived Precedent Graph

**Status:** Accepted  
**Date:** 2026-08-16

## Context
At the initial case volume, PostgreSQL or flat retrieval is sufficient. Some precedent questions are nevertheless graph-shaped (counterparty/entity linkage and multi-hop reviewed-case relationships), and the project intentionally includes graph/GraphRAG learning value.

## Decision
Use Neo4j Community locally (and a rebuildable short-lived hosted validation option) for the reviewed-case precedent graph. PostgreSQL is authoritative for case/review state.

## Guardrail
If Neo4j disappears, the platform remains correct: the graph can be rebuilt from review events and the agent can degrade to a no-graph/flat fallback.

## Honesty in positioning
Neo4j is not claimed to be required by current data volume; it is chosen for graph semantics and deliberate skill development.
