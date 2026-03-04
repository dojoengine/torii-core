#!/usr/bin/env bash
set -euo pipefail

baselines_dir="perf/synthetic-baselines"

if [ ! -d "$baselines_dir" ]; then
  echo "No baselines found at $baselines_dir" >&2
  exit 1
fi

latest_baseline=$(ls -1t "$baselines_dir" | head -1)

if [ -z "$latest_baseline" ]; then
  echo "No baseline found" >&2
  exit 1
fi

baseline_file="$baselines_dir/$latest_baseline/metrics.tsv"

if [ ! -f "$baseline_file" ]; then
  echo "Baseline metrics not found at $baseline_file" >&2
  exit 1
fi

current_file="perf/synthetic-runs/metrics.tsv"

if [ ! -f "$current_file" ]; then
  echo "Current metrics not found at $current_file" >&2
  exit 1
fi

echo "Comparing current run against baseline: $latest_baseline"
echo ""

compare_metrics() {
  local token_type="$1"
  local metric="$2"
  
  baseline_value=$(grep "^${token_type}" "$baseline_file" | cut -f"$metric")
  current_value=$(grep "^${token_type}" "$current_file" | cut -f"$metric")
  
  if [ -z "$baseline_value" ] || [ -z "$current_value" ]; then
    return
  fi
  
  baseline_value=$(echo "$baseline_value" | awk '{printf "%.2f", $0}')
  current_value=$(echo "$current_value" | awk '{printf "%.2f", $0}')
  
  if [ "$baseline_value" != "0" ] && [ "$baseline_value" != "0.00" ]; then
    change=$(echo "scale=2; (($current_value - $baseline_value) / $baseline_value) * 100" | bc)
    change=$(printf "%.2f" "$change")
    
    if (( $(echo "$change > 5" | bc -l) )); then
      echo "⚠️  $token_type $metric: $baseline_value → $current_value (+${change}%)"
    elif (( $(echo "$change < -5" | bc -l) )); then
      echo "✅ $token_type $metric: $baseline_value → $current_value (${change}%)"
    else
      echo "   $token_type $metric: $baseline_value → $current_value (${change}%)"
    fi
  fi
}

echo "=== Throughput Comparison ==="
for token_type in erc20 erc721 erc1155; do
  compare_metrics "$token_type" 7
  compare_metrics "$token_type" 8
  compare_metrics "$token_type" 9
done

echo ""
echo "=== Latency Comparison (total ms) ==="
for token_type in erc20 erc721 erc1155; do
  compare_metrics "$token_type" 11
  compare_metrics "$token_type" 12
  compare_metrics "$token_type" 13
  compare_metrics "$token_type" 14
done

echo ""
echo "Full baseline metrics:"
cat "$baseline_file"
