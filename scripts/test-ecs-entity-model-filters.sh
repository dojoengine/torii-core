#!/usr/bin/env bash
set -euo pipefail

GRPC_ADDR="${GRPC_ADDR:-localhost:3000}"
DB_PATH="${DB_PATH:-torii-data/introspect.db}"
WORLD_ADDRESS="${WORLD_ADDRESS:-}"
LIMIT="${LIMIT:-20}"

fail(){ printf '[ecs-model-filter] error: %s\n' "$*" >&2; exit 1; }
log(){ printf '[ecs-model-filter] %s\n' "$*"; }

for cmd in grpcurl sqlite3 rg xxd base64 mktemp; do command -v "$cmd" >/dev/null 2>&1 || fail "missing $cmd"; done
[[ -f "$DB_PATH" ]] || fail "missing db at $DB_PATH"

if [[ -z "$WORLD_ADDRESS" ]]; then
  WORLD_ADDRESS="$(sqlite3 -batch -noheader "$DB_PATH" "SELECT world_address FROM torii_ecs_entity_models WHERE kind='entity' ORDER BY world_address LIMIT 1;")"
fi
[[ -n "$WORLD_ADDRESS" ]] || fail 'cannot discover world'

mapfile -t MODELS < <(sqlite3 -batch -noheader "$DB_PATH" "
SELECT DISTINCT d.name
FROM torii_ecs_table_kinds k
JOIN dojo_tables d
  ON ltrim(lower(hex(d.owner)), '0') = ltrim(substr(k.world_address, 3), '0')
 AND ltrim(lower(hex(d.id)), '0') = ltrim(substr(k.table_id, 3), '0')
WHERE k.world_address='${WORLD_ADDRESS}' AND k.kind='entity'
ORDER BY d.name LIMIT 2;")
[[ ${#MODELS[@]} -ge 2 ]] || fail 'not enough models'

felt_to_b64(){ local h="${1#0x}"; h="$(printf '%064s' "$h" | tr ' ' '0')"; printf '%s' "$h" | xxd -r -p | base64 | tr -d '\n'; }
WORLD_B64="$(felt_to_b64 "$WORLD_ADDRESS")"

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
one="$tmp/one.json"; two="$tmp/two.json"; both="$tmp/both.json"; bad="$tmp/bad.json"

grpcurl -plaintext -d "{\"query\":{\"world_addresses\":[\"$WORLD_B64\"],\"models\":[\"${MODELS[0]}\"],\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" "$GRPC_ADDR" world.World/RetrieveEntities >"$one"
grpcurl -plaintext -d "{\"query\":{\"world_addresses\":[\"$WORLD_B64\"],\"models\":[\"${MODELS[1]}\"],\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" "$GRPC_ADDR" world.World/RetrieveEntities >"$two"
grpcurl -plaintext -d "{\"query\":{\"world_addresses\":[\"$WORLD_B64\"],\"models\":[\"${MODELS[0]}\",\"${MODELS[1]}\"],\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" "$GRPC_ADDR" world.World/RetrieveEntities >"$both"
grpcurl -plaintext -d "{\"query\":{\"world_addresses\":[\"$WORLD_B64\"],\"models\":[\"definitely-not-a-real-model\"],\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" "$GRPC_ADDR" world.World/RetrieveEntities >"$bad"

rg -q 'hashedKeys' "$one" || fail 'single model A returned no entities'
rg -q 'hashedKeys' "$two" || fail 'single model B returned no entities'
rg -q 'hashedKeys' "$both" || fail 'union model query returned no entities'
! rg -q 'hashedKeys' "$bad" || fail 'unknown model should return no entities'

log "world=${WORLD_ADDRESS}"
log "model_a=${MODELS[0]}"
log "model_b=${MODELS[1]}"
log 'entity model filter checks passed'
