---
name: flyway-migration
description: Use when a database schema change is needed, a new table must be created, a column needs adding or modifying, or an entity class has structural changes. Scaffolds the next Flyway migration file with the correct version number.
allowed-tools: Read, Bash, Edit, Glob
---

Latest migration:
!find . \( -path "*/db/migration/V*.sql" -o -path "*/resources/db/migration/V*.sql" \) 2>/dev/null | sort -V | tail -3 || echo "No migrations found"

Today:
!date +%Y-%m-%d

## Steps
1. Find migration directory: `find . -type d -name "migration" | grep -v target | head -1`
2. Parse the highest version number from existing files
3. Calculate next version (increment rightmost integer)
4. Ask user to describe the schema change if not clear from context
5. Generate migration SQL with explicit PostgreSQL types, NOT NULL constraints, indexes on FK columns
6. Create file at `{dir}/V{next}__{Description_In_Snake_Case}.sql`
7. Show full content and path — do not write until confirmed

## File Header (every migration)
```sql
-- Migration  : V{version}__{Description}
-- Description: {what and why}
-- Date       : {today}
```

## Critical Rules
- NEVER modify an existing migration file
- For invoice/order sequence numbers: use transactional counter table, NOT PostgreSQL sequence (CGST Rule 46)
- If change could lose data, warn explicitly and require confirmation
