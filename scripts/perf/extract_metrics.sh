#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <criterion_dir>" >&2
  exit 1
fi

criterion_dir="$1"

if [ ! -d "$criterion_dir" ]; then
  echo "criterion directory not found: $criterion_dir" >&2
  exit 1
fi

while IFS= read -r file; do
  rel="${file#${criterion_dir}/}"
  bench="${rel%/new/estimates.json}"
  median_ns="$(jq -r '.median.point_estimate' "$file")"
  printf '%s\t%s\n' "$bench" "$median_ns"
done < <(find "$criterion_dir" -type f -path '*/new/estimates.json' | sort)
