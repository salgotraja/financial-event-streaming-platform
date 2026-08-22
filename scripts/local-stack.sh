#!/usr/bin/env bash
# The single command for the local stack (FR-09.1, NFR-07.1).
#
# Compose alone cannot bring this up correctly: topics need explicit partition counts, schema
# subjects need registering because auto.register.schemas is off, and the strict-security profile
# needs certificates, credentials and ACLs applied in order. This script owns that sequence.
#
# Usage:
#   scripts/local-stack.sh up [dev|strict-security]   bring the stack up and provision it
#   scripts/local-stack.sh down                       stop it, keeping volumes
#   scripts/local-stack.sh destroy                    stop it and delete the volumes
#   scripts/local-stack.sh status                     what is running, and what is provisioned
#
# The two profiles are not interchangeable at runtime: they differ in listener security protocol, so
# switching profiles needs destroy first. dev is plaintext and is never evidence of production
# security; strict-security is where anything about identity gets tested.
#
# Requires docker, docker compose and python3. Python is used only to build JSON for the Schema
# Registry API, where hand-rolled quoting would be a bug waiting to happen.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="$REPO_ROOT/deploy/compose"
AVRO_DIR="$REPO_ROOT/contracts/src/main/avro"
REGISTRY_URL="http://localhost:8081"
ACL_ARGS_FILE="$REPO_ROOT/build/kafka-acls.args"

BOOTSTRAP_INTERNAL="kafka1:19092"
ADMIN_CONFIG="/etc/kafka/secrets/client-admin.properties"

# Set by resolve_profile. The running stack's profile is recorded so down/status do not need it.
PROFILE=""
PROFILE_MARKER="$COMPOSE_DIR/.active-profile"

log()  { printf '\033[1m==>\033[0m %s\n' "$*"; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

compose() {
  local files=(-f "$COMPOSE_DIR/docker-compose.yml")
  [ "$PROFILE" = "strict-security" ] && files+=(-f "$COMPOSE_DIR/docker-compose.strict-security.yml")
  docker compose "${files[@]}" "$@"
}

# Every Kafka CLI call needs client configuration under strict-security and none under dev.
#
# The ${extra[@]+"${extra[@]}"} form is not decoration. macOS ships bash 3.2, where expanding an
# empty array under `set -u` is an unbound-variable error, so the plain form made every dev-profile
# call fail while strict-security worked.
kafka_cli() {
  local script="$1"; shift
  local extra=()
  [ "$PROFILE" = "strict-security" ] && extra=(--command-config "$ADMIN_CONFIG")
  compose exec -T kafka1 "/opt/kafka/bin/$script" --bootstrap-server "$BOOTSTRAP_INTERNAL" \
    ${extra[@]+"${extra[@]}"} "$@" </dev/null
}

require_tools() {
  for tool in docker python3 curl; do
    command -v "$tool" >/dev/null || die "$tool is required and was not found on PATH"
  done
  docker compose version >/dev/null 2>&1 || die "docker compose v2 or later is required"
}

resolve_profile() {
  PROFILE="${1:-}"
  if [ -z "$PROFILE" ]; then
    PROFILE="$(cat "$PROFILE_MARKER" 2>/dev/null || echo dev)"
  fi
  case "$PROFILE" in
    dev|strict-security) ;;
    *) die "unknown profile '$PROFILE'. Use dev or strict-security" ;;
  esac
}

wait_for_healthy() {
  local service="$1" attempts="${2:-60}"
  log "waiting for $service"
  for _ in $(seq 1 "$attempts"); do
    local state
    state="$(compose ps --format json "$service" 2>/dev/null | python3 -c '
import json, sys
raw = sys.stdin.read().strip()
if not raw:
    print("missing"); raise SystemExit
for line in raw.splitlines():
    entry = json.loads(line)
    print(entry.get("Health") or entry.get("State") or "unknown")
    break
' || echo missing)"
    [ "$state" = "healthy" ] && return 0
    [ "$state" = "exited" ] && die "$service exited during startup; see: docker compose logs $service"
    sleep 2
  done
  die "$service did not become healthy; see: docker compose logs $service"
}

ensure_security_material() {
  if [ ! -f "$COMPOSE_DIR/tls/ca.pem" ] || [ ! -f "$COMPOSE_DIR/.env" ] || [ ! -f "$COMPOSE_DIR/redis/users.acl" ]; then
    log "generating TLS and credential material"
    "$REPO_ROOT/scripts/generate-dev-security-material.sh"
  else
    log "reusing existing TLS and credential material"
  fi
}

create_topics() {
  log "creating topics from deploy/compose/topics.tsv"
  local created=0 existing=0
  while IFS=$'\t' read -r name partitions config; do
    [ -z "${name:-}" ] && continue
    case "$name" in \#*) continue ;; esac

    local args=(--create --if-not-exists --topic "$name" --partitions "$partitions" --replication-factor 3)
    if [ -n "${config:-}" ] && [ "$config" != "-" ]; then
      local settings setting
      IFS=',' read -ra settings <<< "$config"
      for setting in "${settings[@]}"; do
        args+=(--config "$setting")
      done
    fi

    # Failure has to be told apart from "already exists". Counting a failed create as existing is
    # how this script once reported 22 topics present against a broker that had none.
    local output status
    set +e
    output="$(kafka_cli kafka-topics.sh "${args[@]}" 2>&1)"
    status=$?
    set -e
    if [ $status -ne 0 ]; then
      die "could not create topic $name: $output"
    fi
    case "$output" in
      *Created*) created=$((created + 1)) ;;
      *)         existing=$((existing + 1)) ;;
    esac
  done < "$COMPOSE_DIR/topics.tsv"
  log "topics: $created created, $existing already present"

  local actual
  actual="$(kafka_cli kafka-topics.sh --list 2>/dev/null | grep -cv '^_' || true)"
  log "topics on the broker: $actual"
}

register_subjects() {
  log "registering schema subjects from deploy/compose/subjects.tsv"
  local registered=0
  while IFS=$'\t' read -r topic schema references; do
    [ -z "${topic:-}" ] && continue
    case "$topic" in \#*) continue ;; esac

    local schema_file="$AVRO_DIR/$schema"
    [ -f "$schema_file" ] || die "subjects.tsv names $schema, which does not exist in contracts/src/main/avro"

    # A schema that names another record type is registered with a reference to that record's
    # subject, not with the type inlined. Inlining would give the registry two definitions of one
    # type and let them drift.
    local reference_json='[]'
    if [ -n "${references:-}" ] && [ "$references" != "-" ]; then
      local type_name="${references%%=*}" ref_subject="${references#*=}" ref_version
      ref_version="$(curl -sS "$REGISTRY_URL/subjects/$ref_subject/versions/latest" \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"])' 2>/dev/null)" \
        || die "cannot reference $ref_subject: register it before $topic in subjects.tsv"
      reference_json="$(python3 -c 'import json,sys; print(json.dumps([{"name": sys.argv[1], "subject": sys.argv[2], "version": int(sys.argv[3])}]))' \
        "$type_name" "$ref_subject" "$ref_version")"
    fi

    local payload
    payload="$(python3 -c '
import json, sys
print(json.dumps({
    "schema": open(sys.argv[1]).read(),
    "schemaType": "AVRO",
    "references": json.loads(sys.argv[2]),
}))' "$schema_file" "$reference_json")"

    local response
    response="$(curl -sS -X POST \
      -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data "$payload" \
      "$REGISTRY_URL/subjects/${topic}-value/versions")"

    case "$response" in
      *'"id"'*) registered=$((registered + 1)) ;;
      *) die "could not register ${topic}-value: $response" ;;
    esac
  done < "$COMPOSE_DIR/subjects.tsv"
  log "schema subjects: $registered registered or already current"
}

apply_acls() {
  log "rendering ACLs from each service's committed kafka-acls.yml"
  (cd "$REPO_ROOT" && ./gradlew --quiet renderKafkaAcls) \
    || die "could not render ACLs; run ./gradlew renderKafkaAcls to see why"
  [ -f "$ACL_ARGS_FILE" ] || die "$ACL_ARGS_FILE was not produced"

  local applied=0
  while read -r line; do
    [ -z "$line" ] && continue
    case "$line" in \#*) continue ;; esac
    # shellcheck disable=SC2086 # the rendered line is a deliberate argument list
    kafka_cli kafka-acls.sh --add $line >/dev/null
    applied=$((applied + 1))
  done < "$ACL_ARGS_FILE"
  log "ACLs applied: $applied"
}

# The profile's whole claim is that an identity can do only what its policy allows. Asserting it here
# means a broken stack fails at provisioning time rather than looking healthy and denying nothing.
#
# Both halves are checked. A denial on its own does not distinguish an enforced policy from a client
# that cannot connect at all, which would deny everything and prove nothing.
probe_write() {
  compose exec -T kafka1 sh -c "echo probe | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server $BOOTSTRAP_INTERNAL \
    --producer.config /etc/kafka/secrets/client-$1.properties \
    --topic $2" 2>&1 || true
}

verify_least_privilege() {
  log "verifying that least privilege is actually enforced"

  local allowed
  allowed="$(probe_write trade-producer trades.raw)"
  case "$allowed" in
    *Exception*|*"Not authorized"*|*ERROR*)
      die "trade-producer could not write its own topic, so the denial below would prove nothing. Output: $allowed" ;;
    *)
      log "verified: trade-producer can write trades.raw" ;;
  esac
  # A record has to actually be sent. An empty stdin makes the console producer exit successfully
  # without ever contacting the topic, which looks like a pass and proves nothing.
  local output
  output="$(probe_write trade-producer market-data.ticks)"

  case "$output" in
    *TopicAuthorizationException*|*"Not authorized"*)
      log "verified: trade-producer is denied write access to market-data.ticks" ;;
    *)
      die "trade-producer was NOT denied write access to another service's topic. The profile is not enforcing. Output: $output" ;;
  esac
}

cmd_up() {
  resolve_profile "${1:-dev}"
  require_tools

  if [ "$PROFILE" = "strict-security" ]; then
    ensure_security_material
  fi

  local previous
  previous="$(cat "$PROFILE_MARKER" 2>/dev/null || echo "")"
  if [ -n "$previous" ] && [ "$previous" != "$PROFILE" ]; then
    die "the $previous stack is provisioned; the profiles differ in listener security protocol. Run: scripts/local-stack.sh destroy"
  fi

  log "starting the $PROFILE stack"
  compose up -d

  wait_for_healthy kafka1
  wait_for_healthy kafka2
  wait_for_healthy kafka3
  wait_for_healthy schema-registry
  wait_for_healthy localstack
  wait_for_healthy redis

  create_topics
  register_subjects

  if [ "$PROFILE" = "strict-security" ]; then
    apply_acls
    verify_least_privilege
  fi

  echo "$PROFILE" > "$PROFILE_MARKER"
  print_banner
}

print_banner() {
  cat <<BANNER

Local stack is up (profile: $PROFILE).

  Kafka            localhost:29092, localhost:29093, localhost:29094
  Schema Registry  http://localhost:8081
  Kafka UI         http://localhost:8080
  Grafana          http://localhost:3000
  Prometheus       http://localhost:9090
  Loki             http://localhost:3100
  OTLP endpoint    localhost:4317 (grpc), localhost:4318 (http)
  LocalStack       http://localhost:4566 (s3, kms; no service calls it yet)
BANNER

  if [ "$PROFILE" = "strict-security" ]; then
    cat <<'BANNER'

Kafka requires SASL_SSL with SASL/PLAIN. Per-identity client configuration is in
deploy/compose/tls/client-<identity>.properties, and the broker trusts deploy/compose/tls/ca.pem.
Each identity can do only what its service's kafka-acls.yml allows, and that was verified above.
BANNER
  else
    cat <<'BANNER'

This profile is plaintext and unauthenticated. It is a convenience for iteration and is never
evidence of production security; use the strict-security profile for anything about identity.
BANNER
  fi
}

cmd_down() {
  resolve_profile ""
  log "stopping the $PROFILE stack, keeping volumes"
  compose down
}

cmd_destroy() {
  resolve_profile ""
  log "stopping the $PROFILE stack and deleting its volumes"
  compose down --volumes
  rm -f "$PROFILE_MARKER"
}

cmd_status() {
  resolve_profile ""
  echo "profile: $PROFILE"
  compose ps
  echo
  if curl -sf "$REGISTRY_URL/subjects" >/dev/null 2>&1; then
    local count
    count="$(curl -s "$REGISTRY_URL/subjects" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')"
    echo "schema subjects registered: $count"
  else
    echo "schema registry is not reachable at $REGISTRY_URL"
  fi
}

case "${1:-}" in
  up)      shift; cmd_up "$@" ;;
  down)    cmd_down ;;
  destroy) cmd_destroy ;;
  status)  cmd_status ;;
  *)       die "usage: scripts/local-stack.sh {up [dev|strict-security]|down|destroy|status}" ;;
esac
