#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <runs_dir>" >&2
  exit 1
fi

runs_dir="$1"

if [ ! -d "$runs_dir" ]; then
  echo "runs directory not found: $runs_dir" >&2
  exit 1
fi

echo -e "token_type\tduration_ms\tblocks\ttransactions\tevents\tenvelopes\tblocks_per_sec\ttx_per_sec\tevents_per_sec\tenvelopes_per_sec\textract_ms\tdecode_ms\tsink_ms\tcommit_ms"

for report in "$runs_dir"/*/report.json; do
  if [ ! -f "$report" ]; then
    continue
  fi
  
  token_type=$(jq -r '.token_type' "$report")
  duration_ms=$(jq -r '.duration_ms' "$report")
  blocks=$(jq -r '.totals.blocks' "$report")
  transactions=$(jq -r '.totals.transactions' "$report")
  events=$(jq -r '.totals.events' "$report")
  envelopes=$(jq -r '.totals.envelopes' "$report")
  blocks_per_sec=$(jq -r '.throughput.blocks_per_sec' "$report")
  tx_per_sec=$(jq -r '.throughput.tx_per_sec' "$report")
  events_per_sec=$(jq -r '.throughput.events_per_sec' "$report")
  envelopes_per_sec=$(jq -r '.throughput.envelopes_per_sec' "$report")
  extract_ms=$(jq -r '.stage_latency_ms.extract.total_ms' "$report")
  decode_ms=$(jq -r '.stage_latency_ms.decode.total_ms' "$report")
  sink_ms=$(jq -r '.stage_latency_ms.sink.total_ms' "$report")
  commit_ms=$(jq -r '.stage_latency_ms.commit.total_ms' "$report")
  
  echo -e "${token_type}\t${duration_ms}\t${blocks}\t${transactions}\t${events}\t${envelopes}\t${blocks_per_sec}\t${tx_per_sec}\t${events_per_sec}\t${envelopes_per_sec}\t${extract_ms}\t${decode_ms}\t${sink_ms}\t${commit_ms}"
done
