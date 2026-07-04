#!/usr/bin/env bash
# benchmark.sh — systematic prism benchmark over a real input.
#
# It builds prism + prism-bench, generates a pipeline config for the chosen
# scenario, runs the pipeline for a bounded window, and reports execution cost
# (wall/CPU/peak-RSS) alongside a raw-input↔output reconciliation (input records
# vs Parquet rows read from the footer) plus log-template metrics read from the
# summary Parquet ("template X → count Y").
#
# Usage:
#   scripts/benchmark.sh logs    <log-file>          [duration] [format]
#   scripts/benchmark.sh metrics <exposition-file>   [duration]
#
#   format defaults to "auto" (the logs parser sniffs k8s|json|syslog|clf|cef and
#   otherwise keeps the raw line); pass an explicit format to force one.
#   duration defaults to 5s.
#
# Examples:
#   scripts/benchmark.sh logs /var/log/app.log 5s
#   scripts/benchmark.sh logs ./access.clf 5s clf
#   scripts/benchmark.sh metrics ./metrics.txt 8s
#
# Output artifacts land under $OUT_ROOT (default: ./bench-out): the encoded
# Parquet files (raw/template/summary phases) plus a machine-readable
# report.json.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/bench-out}"
BIN_DIR="$REPO_ROOT/bin"
PRISM="$BIN_DIR/prism"
BENCH="$BIN_DIR/prism-bench"

scenario="${1:-}"
input="${2:-}"
duration="${3:-5s}"
format="${4:-auto}"

die() { echo "benchmark.sh: $*" >&2; exit 2; }
[ -n "$scenario" ] || die "usage: benchmark.sh <logs|metrics> <input-file> [duration] [format]"
[ -n "$input" ] && [ -f "$input" ] || die "input file not found: '$input'"

echo ">> building prism + prism-bench"
( cd "$REPO_ROOT" && go build -o "$PRISM" ./cmd/prism && go build -o "$BENCH" ./cmd/prism-bench )

cfg_dir="$(mktemp -d)"
cfg="$cfg_dir/bench.yaml"
trap 'rm -rf "$cfg_dir"' EXIT

case "$scenario" in
logs)
  # Three-phase logging, matching configs/logging.yaml: raw + template + summary,
  # each a time-range-named Parquet artifact. The summary groups by the mined
  # template so the harness can report per-template counts.
  out="$OUT_ROOT/logs"
  rm -rf "$out"
  cat >"$cfg" <<YAML
pipelines:
  - name: logs
    input:
      type: file
      options: { path: "$input", mode: batch, batch_size: 1000 }
    parser:
      type: logs
      options: { format: $format }
    buffer: { max_age: "1s", max_bytes: "12MiB" }
    branches:
      - name: raw
        encoder: { type: parquet, options: { compression: snappy } }
        output: { type: dir, options: { dir: "$out/raw" } }
      - name: template
        processors:
          - type: template
            options: { source: message, target: template }
        encoder: { type: parquet, options: { compression: snappy } }
        output: { type: dir, options: { dir: "$out/template" } }
      - name: summary
        processors:
          - type: template
            options: { source: message, target: template }
          - type: summary
            options: { group_by: ["template"], aggregates: ["count"] }
        encoder: { type: parquet, options: { compression: snappy } }
        output: { type: dir, options: { dir: "$out/summary" } }
YAML
  # Reconcile against the raw phase only (template/summary re-count the same
  # records), so row-delta stays meaningful.
  "$BENCH" -label "logs ($format): $(basename "$input")" \
    -bin "$PRISM" -config "$cfg" -out "$out/raw" -input "$input" \
    -duration "$duration" -json "$OUT_ROOT/logs-report.json"
  echo
  echo ">> log-template metrics (all phases under $out):"
  "$BENCH" -inspect -label "logs templates" -out "$out" \
    -json "$OUT_ROOT/logs-templates-report.json"
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
  # No summary branch: server-side analytics aggregate the columnar Parquet
  # directly (matching configs/metrics.yaml), which is cheaper than
  # pre-aggregating unknown series here.
  cat >"$cfg" <<YAML
pipelines:
  - name: metrics
    input:
      type: prometheus
      options: { targets: ["$url"], interval: "1s" }
    parser: { type: prometheus }
    buffer: { max_age: "2s", max_bytes: "12MiB" }
    branches:
      - name: raw
        encoder: { type: parquet, options: { compression: snappy } }
        output: { type: dir, options: { dir: "$out/raw" } }
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
