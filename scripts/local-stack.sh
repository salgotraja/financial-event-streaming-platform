#!/usr/bin/env bash
# The single command for the local stack (FR-09.1, NFR-07.1).
#
# Compose alone cannot bring this up correctly: topics need explicit partition counts, schema
# subjects need registering because auto.register.schemas is off, and the strict-security profile
# needs certificates and ACLs applied in order. This script owns that sequence.
#
# Usage:
#   scripts/local-stack.sh up [dev|strict-security]   bring the stack up and provision it
#   scripts/local-stack.sh down                       stop it, keeping volumes
#   scripts/local-stack.sh destroy                    stop it and delete the volumes
#   scripts/local-stack.sh status                     what is running, and what is provisioned
#
# Requires docker, docker compose and python3. Python is used only to build JSON for the Schema
# Registry API, where hand-rolled quoting would be a bug waiting to happen.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="$REPO_ROOT/deploy/compose"
AVRO_DIR="$REPO_ROOT/contracts/src/main/avro"
REGISTRY_URL="http://localhost:8081"

# One broker is enough to reach the cluster; provisioning talks to all three through it.
BOOTSTRAP_INTERNAL="kafka1:19092"

log()  { printf '\033[1m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarning:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

compose() {
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" "$@"
}

require_tools() {
  for tool in docker python3; do
    command -v "$tool" >/dev/null || die "$tool is required and was not found on PATH"
  done
  docker compose version >/dev/null 2>&1 || die "docker compose v2 or later is required"
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

create_topics() {
  log "creating topics from deploy/compose/topics.tsv"
  local created=0 existing=0
  while IFS=$'\t' read -r name partitions config; do
    [ -z "${name:-}" ] && continue
    case "$name" in \#*) continue ;; esac

    local args=(--bootstrap-server "$BOOTSTRAP_INTERNAL" --create --if-not-exists
                --topic "$name" --partitions "$partitions" --replication-factor 3)
    if [ -n "${config:-}" ] && [ "$config" != "-" ]; then
      local setting
      IFS=',' read -ra settings <<< "$config"
      for setting in "${settings[@]}"; do
        args+=(--config "$setting")
      done
    fi

    # </dev/null matters: docker compose exec reads stdin, and without it the first invocation
    # consumes the rest of topics.tsv and the loop silently creates one topic.
    if compose exec -T kafka1 /opt/kafka/bin/kafka-topics.sh "${args[@]}" </dev/null 2>/dev/null | grep -q Created; then
      created=$((created + 1))
    else
      existing=$((existing + 1))
    fi
  done < "$COMPOSE_DIR/topics.tsv"
  log "topics: $created created, $existing already present"
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

cmd_up() {
  local profile="${1:-dev}"
  case "$profile" in
    dev) ;;
    strict-security) die "the strict-security profile is not wired up yet; see docs/task-status.md" ;;
    *) die "unknown profile '$profile'. Use dev or strict-security" ;;
  esac

  require_tools
  log "starting the $profile stack"
  compose up -d

  wait_for_healthy kafka1
  wait_for_healthy kafka2
  wait_for_healthy kafka3
  wait_for_healthy schema-registry

  create_topics
  register_subjects

  cat <<'BANNER'

Local stack is up (profile: dev).

  Kafka            localhost:29092, localhost:29093, localhost:29094
  Schema Registry  http://localhost:8081
  Kafka UI         http://localhost:8080

This profile is plaintext and unauthenticated. It is a convenience for iteration and is never
evidence of production security; use the strict-security profile for anything about identity.
BANNER
}

cmd_down() {
  log "stopping the stack, keeping volumes"
  compose down
}

cmd_destroy() {
  log "stopping the stack and deleting its volumes"
  compose down --volumes
}

cmd_status() {
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
