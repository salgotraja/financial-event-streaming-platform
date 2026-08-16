# Threat Model Starter — v1.2

## Trust boundaries
1. human browser -> control plane;
2. workload -> MSK/AWS services;
3. Kafka event -> agent candidate plane;
4. agent -> tool gateway;
5. tool gateway -> data stores/graph;
6. human review -> synthetic remediation intent;
7. CDC connector -> legacy source and Kafka;
8. CI/CD -> artifact registry/deployment.

## Priority abuse cases
- stolen workload identity attempts cross-topic access;
- agent event payload contains prompt-injection text;
- graph precedent contains instruction-like content;
- model requests an undeclared tool;
- tool result is unavailable but model returns confident `NO_FLAG`;
- reviewer tries to approve a policy/case they are not allowed to approve;
- replay endpoint is invoked without ticket/reason;
- Debezium identity is used to read unrelated tables;
- audit writer attempts retention bypass/delete;
- compromised dependency/image reaches deployment;
- Neo4j outage causes false "no precedent" without disclosure;
- agent provider outage backpressures deterministic stream.

Every material threat must map to a prevention/detection control and a repeatable negative test.
