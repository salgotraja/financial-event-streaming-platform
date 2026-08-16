# ADR-022: Graph Layer for Reconciliation-Agent Precedent Retrieval

**Status:** Accepted  
**Date:** 2026-08-16  
**Decider:** Project architect

## Context
At tens to low hundreds of reviewed cases, PostgreSQL or flat/vector retrieval is sufficient for most lookup needs. Some useful precedent queries are inherently relationship-shaped: same counterparty across multiple cases, multi-hop related entities, and repeated anomaly-pattern/resolution paths.

The project also deliberately values hands-on graph/GraphRAG learning. That learning objective is stated openly rather than disguised as a scaling necessity.

## Decision
Use Neo4j for a bounded **derived precedent graph**. PostgreSQL remains authoritative for jobs, cases, decisions and human feedback.

Graph scope:
- Event
- Instrument
- Counterparty
- ReviewCase
- AnomalyPattern
- Resolution

The graph is updated from `review.decisions` through an idempotent projection consumer.

## Alternatives
### PostgreSQL edge table
Lowest complexity and sufficient scale, but weaker as a graph-learning artifact.

### Neo4j — chosen
Adds operational surface but provides native relationship traversal and direct graph tooling experience.

### Amazon Neptune
Valid AWS-native alternative, but not selected for this portfolio because the graph is derived/rebuildable and the project specifically wants portable Cypher/Neo4j practice.

## Guardrails
- Neo4j is not a source of truth.
- Agent investigation must degrade when Neo4j is unavailable.
- Graph can be rebuilt from authoritative review history.
- No hidden production data is required.
