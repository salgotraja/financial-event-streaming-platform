# ADR-020: Real Debezium CDC for the Legacy-to-Streaming Migration Exercise

**Status:** Accepted  
**Date:** 2026-08-16

## Context
A migration-shaped producer is valuable, but simulating CDC in application code would avoid the hard parts of capture, offsets, snapshots and restart behavior.

## Decision
Use a mock legacy PostgreSQL source plus the Debezium PostgreSQL connector. Publish CDC envelopes to `legacy.trades.cdc`; normalize them into the canonical Avro `TradeEvent`.

## Why
This keeps the migration story technically honest and demonstrates snapshot/backfill, dual-run, schema mapping, connector restart, source provenance and cutover evidence.

## Security
The connector receives a dedicated least-privilege database identity and writes only to its CDC topic.
