---
description: Kafka topology and consumer/producer patterns
paths: "src/**/kafka/**,src/**/consumer/**,src/**/producer/**,src/**/event/**,**/*Consumer.java,**/*Producer.java,**/*Event.java"
---

## Non-Negotiable Rules
- All consumers MUST be idempotent — duplicate messages will arrive
- Commit offsets only after successful processing
- Dead letter topic pattern: {original-topic}.DLT
- Consumer group naming: {service-name}-{purpose}-cg

## Consumer Pattern
- Catch and log exceptions; rethrow only for transient failures
- For permanent failures: write to DLT, do not block the partition
- Include correlation-id in logs for every message processed

## Producer Pattern
- Use KafkaTemplate with explicit error callback
- Always set message key for ordering guarantees
- Serialize events to JSON via Jackson

## Topics
# EDIT: Fill in your actual topic inventory.
# | Topic | Partitions | Consumer Group | Purpose |
