#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GRPC_ADDR="${GRPC_ADDR:-localhost:3000}"
DB_PATH="${DB_PATH:-${REPO_ROOT}/torii-data/introspect.db}"
DB_URL="${DB_URL:-${DATABASE_URL:-}}"
LIMIT="${LIMIT:-1}"
RUN_SUBSCRIPTIONS_SMOKE="${RUN_SUBSCRIPTIONS_SMOKE:-1}"

log() {
  printf '[ecs-e2e] %s\n' "$*"
}

fail() {
  printf '[ecs-e2e] error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

sql_one() {
  local query="$1"
  case "$DB_BACKEND" in
    sqlite)
      sqlite3 -batch -noheader "$DB_PATH" "$query" | head -n 1
      ;;
    postgres)
      psql "$DB_URL" -Atqc "$query" | head -n 1
      ;;
    *)
      fail "unsupported DB_BACKEND: $DB_BACKEND"
      ;;
  esac
}

model_name_query() {
  local kind="$1"
  case "$DB_BACKEND" in
    sqlite)
      cat <<EOF
SELECT m.name
FROM torii_ecs_table_kinds k
JOIN dojo_tables m
  ON ltrim(lower(hex(m.owner)), '0') = ltrim(substr(k.world_address, 3), '0')
 AND ltrim(lower(hex(m.id)), '0') = ltrim(substr(k.table_id, 3), '0')
WHERE k.world_address = '$WORLD_ADDRESS'
  AND k.kind = '$kind'
ORDER BY m.name
LIMIT 1;
EOF
      ;;
    postgres)
      cat <<EOF
SELECT m.name
FROM torii_ecs_table_kinds k
JOIN dojo_tables m
  ON ltrim(lower(encode(m.owner, 'hex')), '0') = ltrim(substr(k.world_address, 3), '0')
 AND ltrim(lower(encode(m.id, 'hex')), '0') = ltrim(substr(k.table_id, 3), '0')
WHERE k.world_address = '$WORLD_ADDRESS'
  AND k.kind = '$kind'
ORDER BY m.name
LIMIT 1;
EOF
      ;;
    *)
      fail "unsupported DB_BACKEND: $DB_BACKEND"
      ;;
  esac
}

felt_to_base64() {
  local hex="${1#0x}"
  hex="$(printf '%064s' "$hex" | tr ' ' '0')"
  printf '%s' "$hex" | xxd -r -p | base64 | tr -d '\n'
}

base64_decode() {
  if base64 --help 2>&1 | rg -q '\-d'; then
    base64 -d
  else
    base64 -D
  fi
}

base64_to_felt() {
  local value="$1"
  local hex
  hex="$(printf '%s' "$value" | base64_decode | xxd -p -c 256 | tr -d '\n')"
  hex="$(printf '%s' "$hex" | sed 's/^0*//')"
  if [[ -z "$hex" ]]; then
    hex="0"
  fi
  printf '0x%s' "$hex"
}

grpc_call() {
  local method="$1"
  local payload="$2"
  local output="$3"
  grpcurl -plaintext -d "$payload" "$GRPC_ADDR" "$method" >"$output"
}

assert_contains() {
  local needle="$1"
  local file="$2"
  rg -F -q "$needle" "$file" || fail "expected to find '$needle' in $file"
}

assert_non_empty_entities() {
  local file="$1"
  assert_contains '"entities": [' "$file"
  if rg -q '"entities": \[\]' "$file"; then
    fail "expected non-empty entities response in $file"
  fi
}

assert_non_empty_events() {
  local file="$1"
  assert_contains '"events": [' "$file"
  if rg -q '"events": \[\]' "$file"; then
    fail "expected non-empty events response in $file"
  fi
}

require_cmd grpcurl
require_cmd rg
require_cmd xxd
require_cmd base64
require_cmd mktemp
require_cmd python3

if [[ -n "$DB_URL" && "$DB_URL" =~ ^postgres(ql)?:// ]]; then
  DB_BACKEND="postgres"
else
  DB_BACKEND="sqlite"
fi

if [[ -z "${WORLD_ADDRESS:-}" || -z "${ENTITY_MODEL:-}" || -z "${EVENT_MESSAGE_MODEL:-}" ]]; then
  if [[ "$DB_BACKEND" == "sqlite" ]]; then
    require_cmd sqlite3
    [[ -f "$DB_PATH" ]] || fail "sqlite database not found at $DB_PATH"
  else
    require_cmd psql
    [[ -n "$DB_URL" ]] || fail "postgres DB_URL is required for metadata autodiscovery"
  fi
fi

discovery_dir="$(mktemp -d)"
trap 'rm -rf "$discovery_dir"' EXIT
discovered_worlds_output="$discovery_dir/worlds_discovery.json"
grpc_call "world.World/Worlds" "{}" "$discovered_worlds_output"

if [[ -z "${WORLD_ADDRESS:-}" ]]; then
  world_address_b64="$(
    rg -o '"worldAddress": "[^"]+"' "$discovered_worlds_output" \
      | rg -v '"worldAddress": "[0-9]+"' \
      | sed -E 's/"worldAddress": "([^"]+)"/\1/' \
      | while IFS= read -r candidate; do
          candidate_hex="$(base64_to_felt "$candidate")"
          if [[ "$candidate_hex" != "0x0" ]]; then
            printf '%s\n' "$candidate"
            break
          fi
        done
  )"
  [[ -n "$world_address_b64" ]] || fail "could not discover a world from live Worlds response"
  WORLD_ADDRESS="$(base64_to_felt "$world_address_b64")"
fi

[[ -n "$WORLD_ADDRESS" ]] || fail "could not discover a world address"

ENTITY_MODEL="${ENTITY_MODEL:-$(sql_one "$(model_name_query entity)")}"
EVENT_MESSAGE_MODEL="${EVENT_MESSAGE_MODEL:-$(sql_one "$(model_name_query event_message)")}"

[[ -n "$ENTITY_MODEL" ]] || fail "could not discover an entity model for world $WORLD_ADDRESS"
[[ -n "$EVENT_MESSAGE_MODEL" ]] || fail "could not discover an event_message model for world $WORLD_ADDRESS"

WORLD_ADDRESS_B64="$(felt_to_base64 "$WORLD_ADDRESS")"

tmp_dir="$(mktemp -d)"
if [[ "${KEEP_TMP:-0}" != "1" ]]; then
  trap 'rm -rf "$tmp_dir" "$discovery_dir"' EXIT
else
  trap 'rm -rf "$discovery_dir"' EXIT
fi

list_output="$tmp_dir/list.txt"
describe_output="$tmp_dir/describe.txt"
worlds_output="$tmp_dir/worlds.json"
entities_output="$tmp_dir/entities.json"
event_messages_output="$tmp_dir/event_messages.json"
events_output="$tmp_dir/events.json"

log "gRPC target: $GRPC_ADDR"
log "db backend: $DB_BACKEND"
log "world: $WORLD_ADDRESS"
log "entity model: $ENTITY_MODEL"
log "event message model: $EVENT_MESSAGE_MODEL"

grpcurl -plaintext "$GRPC_ADDR" list >"$list_output"
assert_contains 'world.World' "$list_output"
assert_contains 'torii.Torii' "$list_output"

grpcurl -plaintext "$GRPC_ADDR" describe world.World >"$describe_output"
assert_contains 'rpc RetrieveEntities' "$describe_output"
assert_contains 'rpc RetrieveEventMessages' "$describe_output"
assert_contains 'rpc RetrieveEvents' "$describe_output"

grpc_call "world.World/Worlds" "{\"world_addresses\":[\"$WORLD_ADDRESS_B64\"]}" "$worlds_output"
assert_contains '"worlds": [' "$worlds_output"
assert_contains "\"worldAddress\": \"$WORLD_ADDRESS_B64\"" "$worlds_output"

grpc_call \
  "world.World/RetrieveEntities" \
  "{\"query\":{\"world_addresses\":[\"$WORLD_ADDRESS_B64\"],\"models\":[\"$ENTITY_MODEL\"],\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" \
  "$entities_output"
assert_non_empty_entities "$entities_output"
assert_contains "\"name\": \"$ENTITY_MODEL\"" "$entities_output"

grpc_call \
  "world.World/RetrieveEventMessages" \
  "{\"query\":{\"world_addresses\":[\"$WORLD_ADDRESS_B64\"],\"models\":[\"$EVENT_MESSAGE_MODEL\"],\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" \
  "$event_messages_output"
assert_non_empty_entities "$event_messages_output"
assert_contains "\"name\": \"$EVENT_MESSAGE_MODEL\"" "$event_messages_output"

grpc_call \
  "world.World/RetrieveEvents" \
  "{\"query\":{\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" \
  "$events_output"
assert_non_empty_events "$events_output"

log "verification passed"

if [[ "$RUN_SUBSCRIPTIONS_SMOKE" == "1" ]]; then
  log "running subscriptions smoke checks"
  GRPC_ADDR="$GRPC_ADDR" python3 "$REPO_ROOT/scripts/test-ecs-subscriptions-smoke.py"
fi

if [[ "${KEEP_TMP:-0}" == "1" ]]; then
  log "artifacts kept in $tmp_dir"
fi
