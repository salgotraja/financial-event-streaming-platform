---
name: flyway-migration
description: Use when a database schema change is needed, a new table must be created, a column needs adding or modifying, or a persistence class has structural changes. Scaffolds the next Flyway migration file with the correct version number in the owning service module.
allowed-tools: Read, Bash, Edit, Glob
---

Existing migrations across all service modules:
!find . -path "*/resources/db/migration/V*.sql" -not -path "./build/*" 2>/dev/null | sort -V | tail -10 || echo "No migrations found"

Today:
!date +%Y-%m-%d

## Context

Each service owns its own schema (ADR-028). Migration versions are **per module**, so two services
may both have a `V1__`. Never place a migration in a module that does not own the table.

Store ownership, for deciding which module the change belongs to:

| State | Owning service |
| --- | --- |
| Risk rule evaluation state | `risk-alert-service` |
| Position and exposure read model | `position-exposure-service` |
| Alert cases | `alert-case-service` |
| Risk rule versions and lifecycle | `risk-rule-governance-service` |
| Agent decisions and decision traces | `agent-investigation-service` |
| Human review cases and verdicts | `human-review-service` |
| Mock legacy source tables | the legacy CDC source, seeded by SQL not Flyway |

Neo4j and Redis are not migrated here. Neo4j is a derived rebuildable graph (ADR-022); Redis is a
projection (ADR-027).

## Steps

1. Identify the owning service module from the table above. If the change spans two services, stop:
   that is a schema-sharing violation and needs a design conversation, not a migration.
2. Locate the module's migration directory: `{module}/src/main/resources/db/migration/`
3. Parse the highest version in **that module only**
4. Calculate the next version by incrementing the rightmost integer
5. Ask the user to describe the schema change if it is not clear from context
6. Generate migration SQL with explicit PostgreSQL types, NOT NULL constraints, and indexes on
   foreign key columns and on any column used for a point lookup
7. Create the file at `{module}/src/main/resources/db/migration/V{next}__{Description_In_Snake_Case}.sql`
8. Show the full content and path. Do not write until confirmed.

## File Header (every migration)

```sql
-- Migration  : V{version}__{Description}
-- Module     : {service module}
-- Description: {what and why}
-- Date       : {today}
```

## Critical Rules

- NEVER modify an existing migration file. Always add a new one.
- Audit, governance and security tables are append-only. No UPDATE, no DELETE, no soft-delete column.
  A correction is a new row plus a new lifecycle event (ADR-016).
- Any table holding a running total needs an optimistic locking version column (ADR-008).
- Any table written from a Kafka consumer needs a natural idempotency key with a unique constraint,
  because delivery is at-least-once (ADR-019).
- If the change could lose data, warn explicitly and require confirmation.
