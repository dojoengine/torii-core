#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <baseline_dir>" >&2
  exit 1
fi

baseline_dir="$1"
baseline_metrics="${baseline_dir}/metrics.tsv"

if [ ! -f "$baseline_metrics" ]; then
  echo "Baseline metrics not found: $baseline_metrics" >&2
  exit 1
fi

SAMPLE_SIZE="${SAMPLE_SIZE:-40}"
MEASUREMENT_TIME="${MEASUREMENT_TIME:-2}"
WARMUP_TIME="${WARMUP_TIME:-1}"

cargo bench -p torii --bench perf_harness -- \
  --sample-size "$SAMPLE_SIZE" \
  --measurement-time "$MEASUREMENT_TIME" \
  --warm-up-time "$WARMUP_TIME"

current_metrics="$(mktemp)"
trap 'rm -f "$current_metrics"' EXIT
scripts/perf/extract_metrics.sh target/criterion > "$current_metrics"

echo "Bench\tBaseline(ns)\tCurrent(ns)\tDelta(%)\tStatus"
awk -F '\t' '
NR==FNR {base[$1]=$2; next}
{
  bench=$1
  cur=$2
  if (!(bench in base)) next
  b=base[bench]
  delta=((b-cur)/b)*100
  status="marginal"
  if (delta >= 10.0) status="IMPROVED"
  else if (delta <= -10.0) status="REGRESSED"
  printf "%s\t%.0f\t%.0f\t%.2f\t%s\n", bench, b, cur, delta, status
}
' "$baseline_metrics" "$current_metrics" | sort
