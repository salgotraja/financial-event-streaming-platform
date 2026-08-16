---
description: Security constraints for auth, payments, and PII-adjacent code
paths: "src/**/auth/**,src/**/security/**,src/**/payment/**,src/**/subscription/**,src/**/invoice/**,src/**/user/**"
---

## PII Fields
# EDIT: List actual PII fields for this project.
# These must never appear in logs or unmasked in API responses.

## Auth Rules
- Validate JWT expiry, signature, and scope on every protected endpoint
- Use @PreAuthorize for resource-level access control
- Always verify the principal owns the resource before returning it

## Payment Rules
- Every payment state transition logged to audit table with timestamp and actor
- Idempotency key required on all payment initiation endpoints
- Failed payment webhooks: verify signature before processing

## Input Validation
- Validate at controller boundary using Jakarta Bean Validation (@Valid)
- Reject requests with extra unexpected fields

## Compliance
- GSTIN validation required on all B2B invoice endpoints
- Invoice number sequence: transactional counter table (not PostgreSQL sequences) per CGST Rule 46
- Audit log retention: 7 years minimum per RBI guidelines
