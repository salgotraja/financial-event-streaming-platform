# ADR-026: Separate Deterministic and Agentic SLO/Cost Planes

**Status:** Accepted  
**Date:** 2026-08-16

## Decision
The 50,000-events/sec and sub-200ms objectives remain deterministic-streaming SLOs. The agent plane has independent candidate throughput, queue, time-to-case, provider availability and budget SLOs.

## Consequence
A successful portfolio result can demonstrate both high-throughput event engineering and controlled AI investigation without pretending the LLM can or should process the entire stream.
