#!/usr/bin/env bash
set -euo pipefail

SAMPLE_SIZE="${SAMPLE_SIZE:-40}"
MEASUREMENT_TIME="${MEASUREMENT_TIME:-2}"
WARMUP_TIME="${WARMUP_TIME:-1}"

cargo bench -p torii --bench perf_harness -- \
  --sample-size "$SAMPLE_SIZE" \
  --measurement-time "$MEASUREMENT_TIME" \
  --warm-up-time "$WARMUP_TIME"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="perf/baselines/${stamp}"
mkdir -p "$out_dir"

scripts/perf/extract_metrics.sh target/criterion > "$out_dir/metrics.tsv"

cat > "$out_dir/context.txt" <<CTX
commit=$(git rev-parse HEAD)
branch=$(git branch --show-current)
sample_size=${SAMPLE_SIZE}
measurement_time=${MEASUREMENT_TIME}
warmup_time=${WARMUP_TIME}
created_at_utc=${stamp}
CTX

echo "Baseline saved to $out_dir"
