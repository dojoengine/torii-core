#!/usr/bin/env bash

set -euo pipefail

GRPC_ADDR="${GRPC_ADDR:-localhost:3000}"
STREAM_TIMEOUT_SEC="${STREAM_TIMEOUT_SEC:-2.5}"
REQUIRE_LIVE_UPDATES="${REQUIRE_LIVE_UPDATES:-0}"
WORLD_ADDRESS_B64="${WORLD_ADDRESS_B64:-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=}"

log() {
  printf '[ecs-sub-smoke] %s\n' "$*"
}

fail() {
  printf '[ecs-sub-smoke] error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

stream_call() {
  local method="$1"
  local payload="$2"
  local out_file="$3"
  local err_file="$4"

  set +e
  grpcurl -plaintext -max-time "$STREAM_TIMEOUT_SEC" -d "$payload" "$GRPC_ADDR" "$method" >"$out_file" 2>"$err_file"
  local status=$?
  set -e

  if [[ $status -ne 0 ]]; then
    if rg -q 'DeadlineExceeded|max-time reached' "$err_file"; then
      return 0
    fi
    fail "stream call failed for $method: $(<"$err_file")"
  fi
}

require_cmd grpcurl
require_cmd rg
require_cmd mktemp

list_out="$(mktemp)"
trap 'rm -f "$list_out" "$entities_out" "$entities_err" "$messages_out" "$messages_err" "$events_out" "$events_err" "$contracts_out" "$contracts_err"' EXIT

grpcurl -plaintext "$GRPC_ADDR" list >"$list_out"
rg -F -q 'world.World' "$list_out" || fail 'world.World service not exposed'

entities_out="$(mktemp)"; entities_err="$(mktemp)"
messages_out="$(mktemp)"; messages_err="$(mktemp)"
events_out="$(mktemp)"; events_err="$(mktemp)"
contracts_out="$(mktemp)"; contracts_err="$(mktemp)"

log "gRPC target: $GRPC_ADDR"
log "world filter: $WORLD_ADDRESS_B64"

stream_call "world.World/SubscribeEntities" "{\"world_addresses\":[\"$WORLD_ADDRESS_B64\"]}" "$entities_out" "$entities_err"
rg -q '"subscriptionId":' "$entities_out" || fail 'SubscribeEntities missing subscriptionId frame'
if [[ "$REQUIRE_LIVE_UPDATES" == "1" ]]; then
  rg -q '"entity": \{' "$entities_out" || fail 'SubscribeEntities did not receive live entity update'
fi
log 'SubscribeEntities setup frame: OK'

stream_call "world.World/SubscribeEventMessages" "{\"world_addresses\":[\"$WORLD_ADDRESS_B64\"]}" "$messages_out" "$messages_err"
rg -q '"subscriptionId":' "$messages_out" || fail 'SubscribeEventMessages missing subscriptionId frame'
if [[ "$REQUIRE_LIVE_UPDATES" == "1" ]]; then
  rg -q '"entity": \{' "$messages_out" || fail 'SubscribeEventMessages did not receive live update'
fi
log 'SubscribeEventMessages setup frame: OK'

stream_call "world.World/SubscribeEvents" '{"keys":[]}' "$events_out" "$events_err"
if ! rg -q '^\{\}' "$events_out" && ! rg -q '"event": \{' "$events_out"; then
  fail 'SubscribeEvents did not emit setup or live frame'
fi
if [[ "$REQUIRE_LIVE_UPDATES" == "1" ]]; then
  rg -q '"event": \{' "$events_out" || fail 'SubscribeEvents did not receive live event'
fi
log 'SubscribeEvents setup frame: OK'

stream_call "world.World/SubscribeContracts" '{"query":{"contract_addresses":["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="]}}' "$contracts_out" "$contracts_err"
if rg -q '"contract": \{' "$contracts_out"; then
  log 'SubscribeContracts initial/live frame: OK'
elif [[ "$REQUIRE_LIVE_UPDATES" == "1" ]]; then
  fail 'SubscribeContracts did not receive contract frame'
else
  log 'SubscribeContracts: no contract frame during window (inconclusive)'
fi

log 'subscription smoke checks passed'
