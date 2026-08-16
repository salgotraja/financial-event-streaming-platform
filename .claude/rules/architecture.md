---
description: System architecture context for backend changes
paths: "src/main/**/*.java,src/main/**/*.kt"
---

## Layer Rules
- Controller: validates input, maps to/from DTOs, delegates to Service
- Service: business logic only, no HTTP or persistence concerns
- Repository: persistence only, no business logic
- No @Transactional on controllers — services own transaction boundaries

## Naming Conventions
- DTOs: {Entity}Request, {Entity}Response
- Services: {Domain}Service
- Repositories: {Entity}Repository
- Kafka consumers: {Topic}Consumer, producers: {Topic}Producer

## Architecture
# EDIT: Fill in after first planning session.
# Key component relationships, external integrations, known constraints.
