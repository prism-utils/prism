#!/usr/bin/env bash
# benchmark.sh — systematic prism benchmark over a real input.
#
# It builds prism + prism-bench, generates a pipeline config for the chosen
# scenario, runs the pipeline for a bounded window, and reports execution cost
# (wall/CPU/peak-RSS) alongside a raw-input↔output reconciliation (input records
# vs Parquet rows read from the footer, plus JSON summary group/count totals).
#
# Usage:
#   scripts/benchmark.sh logs    <logfmt-file>        [duration] [parser]
#   scripts/benchmark.sh metrics <exposition-file>    [duration]
#
#   parser defaults to "logfmt"; also accepts "json" (one JSON object per line).
#   duration defaults to 5s.
#
# Examples:
#   scripts/benchmark.sh logs /var/log/app.logfmt 5s
#   scripts/benchmark.sh metrics ./metrics.txt 8s
#
# Output artifacts land under $OUT_ROOT (default: ./bench-out): the encoded
# Parquet/JSON files plus a machine-readable report.json.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/bench-out}"
BIN_DIR="$REPO_ROOT/bin"
PRISM="$BIN_DIR/prism"
BENCH="$BIN_DIR/prism-bench"

scenario="${1:-}"
input="${2:-}"
duration="${3:-5s}"
parser="${4:-logfmt}"

die() { echo "benchmark.sh: $*" >&2; exit 2; }
[ -n "$scenario" ] || die "usage: benchmark.sh <logs|metrics> <input-file> [duration] [parser]"
[ -n "$input" ] && [ -f "$input" ] || die "input file not found: '$input'"

echo ">> building prism + prism-bench"
( cd "$REPO_ROOT" && go build -o "$PRISM" ./cmd/prism && go build -o "$BENCH" ./cmd/prism-bench )

cfg_dir="$(mktemp -d)"
cfg="$cfg_dir/bench.yaml"
trap 'rm -rf "$cfg_dir"' EXIT

case "$scenario" in
logs)
  out="$OUT_ROOT/logs"
  rm -rf "$out"
  cat >"$cfg" <<YAML
pipelines:
  - name: bench-logs
    input:
      type: file
      options: { path: "$input", mode: batch, batch_size: 1000 }
    parser: { type: $parser }
    processors:
      - type: template
        options: { source: msg, target: template }
    buffer: { max_age: "1s", max_bytes: "12MiB" }
    branches:
      - name: data
        encoder: { type: parquet, options: { compression: snappy } }
        output: { type: dir, options: { dir: "$out/data", prefix: "l-" } }
      - name: summary
        processors:
          - type: summary
            options: { group_by: ["level"], aggregates: ["count"] }
        encoder: { type: json }
        output: { type: dir, options: { dir: "$out/summary", prefix: "s-" } }
YAML
  "$BENCH" -label "logs ($parser): $(basename "$input")" \
    -bin "$PRISM" -config "$cfg" -out "$out" -input "$input" \
    -duration "$duration" -json "$OUT_ROOT/logs-report.json"
  ;;

metrics)
  # Serve the exposition snapshot on a local port and scrape it live, so the
  # full prometheus input → parse → buffer path is exercised.
  port="${BENCH_METRICS_PORT:-19099}"
  serve_dir="$(dirname "$input")"
  ( cd "$serve_dir" && exec python3 -m http.server "$port" ) >/dev/null 2>&1 &
  httpd=$!
  trap 'rm -rf "$cfg_dir"; kill "$httpd" 2>/dev/null || true' EXIT
  sleep 1
  url="http://localhost:$port/$(basename "$input")"
  out="$OUT_ROOT/metrics"
  rm -rf "$out"
  cat >"$cfg" <<YAML
pipelines:
  - name: bench-metrics
    input:
      type: prometheus
      options: { targets: ["$url"], interval: "1s" }
    parser: { type: prometheus }
    buffer: { max_age: "2s", max_bytes: "12MiB" }
    branches:
      - name: data
        encoder: { type: parquet, options: { compression: snappy } }
        output: { type: dir, options: { dir: "$out/data", prefix: "m-" } }
      - name: summary
        processors:
          - type: summary
            options:
              group_by: ["__name__"]
              aggregates: ["count", "sum:value", "avg:value", "max:value"]
        encoder: { type: json }
        output: { type: dir, options: { dir: "$out/summary", prefix: "s-" } }
YAML
  # No -input: for a scrape, raw record count is scrapes×series, not a file.
  "$BENCH" -label "metrics: $(basename "$input") @1s" \
    -bin "$PRISM" -config "$cfg" -out "$out" \
    -duration "$duration" -json "$OUT_ROOT/metrics-report.json"
  ;;

*)
  die "unknown scenario '$scenario' (want: logs | metrics)"
  ;;
esac
