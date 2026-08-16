---
description: Database and migration rules for PostgreSQL + Flyway
paths: "src/**/repository/**,src/**/entity/**,db/migration/**,src/**/migration/**,**/*Repository.java,**/*Entity.java"
---

## Migration Rules
- Tool: Flyway
- Location: db/migration/ or src/main/resources/db/migration/
- Naming: V{version}__{Description_with_underscores}.sql
- NEVER modify an existing migration — always create a new one

## Critical: Invoice / Sequential Number Generation
- PostgreSQL sequences VIOLATE CGST Rule 46 for invoice numbering
- Use a transactional counter table:
  CREATE TABLE invoice_counters (series VARCHAR(10) PRIMARY KEY, last_value BIGINT NOT NULL);
  UPDATE invoice_counters SET last_value = last_value + 1 WHERE series = ? RETURNING last_value;

## Entity Rules
- Use @Column(nullable = false) explicitly
- Audit fields via @EntityListeners(AuditingEntityListener)
- No bidirectional @OneToMany without explicit fetch = LAZY
- Soft deletes via is_deleted + deleted_at — never hard delete user-facing data

## Query Rules
- Pagination: always use Pageable — never fetch unbounded collections
- Complex queries: @Query with JPQL, not native SQL unless performance-justified
