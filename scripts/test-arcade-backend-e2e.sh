#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GRPC_ADDR="${GRPC_ADDR:-localhost:3000}"
WORLD_ADDRESS="${WORLD_ADDRESS:-0x2d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb}"
ARCADE_ERC721="${ARCADE_ERC721:-0x1e1c477f2ef896fd638b50caa31e3aa8f504d5c6cb3c09c99cd0b72523f07f7}"
LIMIT="${LIMIT:-1}"

log() {
  printf '[arcade-e2e] %s\n' "$*"
}

fail() {
  printf '[arcade-e2e] error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

felt_to_base64() {
  local hex="${1#0x}"
  hex="$(printf '%064s' "$hex" | tr ' ' '0')"
  printf '%s' "$hex" | xxd -r -p | base64 | tr -d '\n'
}

grpc_call() {
  local method="$1"
  local payload="${2:-{}}"
  local output="$3"
  grpcurl -plaintext -d "$payload" "$GRPC_ADDR" "$method" >"$output"
}

assert_contains() {
  local needle="$1"
  local file="$2"
  rg -F -q "$needle" "$file" || fail "expected to find '$needle' in $file"
}

require_cmd grpcurl
require_cmd rg
require_cmd xxd
require_cmd base64
require_cmd mktemp

WORLD_ADDRESS_B64="$(felt_to_base64 "$WORLD_ADDRESS")"

tmp_dir="$(mktemp -d)"
if [[ "${KEEP_TMP:-0}" != "1" ]]; then
  trap 'rm -rf "$tmp_dir"' EXIT
fi

list_output="$tmp_dir/list.txt"
world_describe_output="$tmp_dir/world_describe.txt"
erc721_describe_output="$tmp_dir/erc721_describe.txt"
worlds_output="$tmp_dir/worlds.json"
entities_output="$tmp_dir/entities.json"
event_messages_output="$tmp_dir/event_messages.json"
events_output="$tmp_dir/events.json"
arcade_describe_output="$tmp_dir/arcade_describe.txt"
arcade_games_output="$tmp_dir/arcade_games.json"
arcade_editions_output="$tmp_dir/arcade_editions.json"
arcade_collections_output="$tmp_dir/arcade_collections.json"
arcade_listings_output="$tmp_dir/arcade_listings.json"
arcade_sales_output="$tmp_dir/arcade_sales.json"
erc20_stats_output="$tmp_dir/erc20_stats.json"
erc721_stats_output="$tmp_dir/erc721_stats.json"

log "gRPC target: $GRPC_ADDR"
log "world: $WORLD_ADDRESS"
log "arcade erc721: $ARCADE_ERC721"

grpcurl -plaintext "$GRPC_ADDR" list >"$list_output"
assert_contains 'torii.Torii' "$list_output"
assert_contains 'world.World' "$list_output"
assert_contains 'arcade.v1.Arcade' "$list_output"
assert_contains 'torii.sinks.erc20.Erc20' "$list_output"
assert_contains 'torii.sinks.erc721.Erc721' "$list_output"

grpcurl -plaintext "$GRPC_ADDR" describe world.World >"$world_describe_output"
assert_contains 'rpc Worlds' "$world_describe_output"
assert_contains 'rpc RetrieveEntities' "$world_describe_output"
assert_contains 'rpc RetrieveEventMessages' "$world_describe_output"
assert_contains 'rpc RetrieveEvents' "$world_describe_output"

grpcurl -plaintext "$GRPC_ADDR" describe torii.sinks.erc721.Erc721 >"$erc721_describe_output"
assert_contains 'rpc GetStats' "$erc721_describe_output"
assert_contains 'rpc GetOwnership' "$erc721_describe_output"

grpcurl -plaintext "$GRPC_ADDR" describe arcade.v1.Arcade >"$arcade_describe_output"
assert_contains 'rpc ListGames' "$arcade_describe_output"
assert_contains 'rpc ListEditions' "$arcade_describe_output"
assert_contains 'rpc ListCollections' "$arcade_describe_output"
assert_contains 'rpc ListListings' "$arcade_describe_output"
assert_contains 'rpc ListSales' "$arcade_describe_output"

grpc_call "world.World/Worlds" "{\"world_addresses\":[\"$WORLD_ADDRESS_B64\"]}" "$worlds_output"
assert_contains "\"worldAddress\": \"$WORLD_ADDRESS_B64\"" "$worlds_output"

grpc_call \
  "world.World/RetrieveEntities" \
  "{\"query\":{\"world_addresses\":[\"$WORLD_ADDRESS_B64\"],\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" \
  "$entities_output"
assert_contains '"entities": [' "$entities_output"

grpc_call \
  "world.World/RetrieveEventMessages" \
  "{\"query\":{\"world_addresses\":[\"$WORLD_ADDRESS_B64\"],\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" \
  "$event_messages_output"
assert_contains '"entities": [' "$event_messages_output"

grpc_call \
  "world.World/RetrieveEvents" \
  "{\"query\":{\"pagination\":{\"limit\":$LIMIT,\"direction\":\"FORWARD\"}}}" \
  "$events_output"
assert_contains '"events": [' "$events_output"

grpc_call "arcade.v1.Arcade/ListGames" "{\"includeUnpublished\":true,\"includeUnwhitelisted\":true}" "$arcade_games_output"
assert_contains '"games": [' "$arcade_games_output"

grpc_call "arcade.v1.Arcade/ListEditions" "{\"includeUnpublished\":true,\"includeUnwhitelisted\":true,\"limit\":$LIMIT}" "$arcade_editions_output"
assert_contains '"editions": [' "$arcade_editions_output"

grpc_call "arcade.v1.Arcade/ListCollections" "{\"limit\":$LIMIT}" "$arcade_collections_output"
assert_contains '"collections": [' "$arcade_collections_output"

grpc_call "arcade.v1.Arcade/ListListings" "{\"limit\":$LIMIT,\"includeInactive\":true}" "$arcade_listings_output"
assert_contains '"listings": [' "$arcade_listings_output"

grpc_call "arcade.v1.Arcade/ListSales" "{\"limit\":$LIMIT}" "$arcade_sales_output"
assert_contains '"sales": [' "$arcade_sales_output"

grpcurl -plaintext "$GRPC_ADDR" torii.sinks.erc20.Erc20/GetStats >"$erc20_stats_output"
assert_contains '"uniqueTokens":' "$erc20_stats_output"

grpcurl -plaintext "$GRPC_ADDR" torii.sinks.erc721.Erc721/GetStats >"$erc721_stats_output"
assert_contains '"uniqueTokens":' "$erc721_stats_output"

log "verification passed"
if [[ "${KEEP_TMP:-0}" == "1" ]]; then
  log "artifacts kept in $tmp_dir"
fi
