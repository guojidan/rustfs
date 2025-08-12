#!/usr/bin/env bash
set -euo pipefail

# Quick benchmark for ecstore IO with and without io_uring (Linux only).
# Usage: ./scripts/bench_uring.sh

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This benchmark is intended for Linux (io_uring)." >&2
  exit 1
fi

echo "Building without io_uring..."
cargo build -p rustfs-ecstore >/dev/null

echo "Running tests (baseline)..."
TIMEFMT=$'real %3lR\nuser %3lU\nsys  %3lS'
TIMEBASE=$( { time cargo test -p rustfs-ecstore -- --ignored >/dev/null; } 2>&1 | tail -n3 ) || true

echo "Building with io_uring..."
cargo build -p rustfs-ecstore --features uring-io >/dev/null

echo "Running tests (uring-io)..."
TIMEURING=$( { time cargo test -p rustfs-ecstore --features uring-io -- --ignored >/dev/null; } 2>&1 | tail -n3 ) || true

echo "\nBaseline timing:"; echo "$TIMEBASE"
echo "\nuring-io timing:"; echo "$TIMEURING"

echo "\nNote: For meaningful results, add dedicated benches or fio runs on real datasets."
