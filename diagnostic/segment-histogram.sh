#!/usr/bin/env bash
# JSON histogram of on-disk prism-store segments for one tenant.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
BIN="${TMPDIR:-/tmp}/prism-segment-histogram.$$"
cleanup() { rm -f "$BIN"; }
trap cleanup EXIT
CGO_ENABLED=0 go build -o "$BIN" ./diagnostic/segment-histogram

data_dir=""
tenant=""
args=("$@")
i=0
while ((i < ${#args[@]})); do
  case "${args[$i]}" in
    --data-dir)
      data_dir="${args[$((i + 1))]:-}"
      i=$((i + 2))
      continue
      ;;
    --tenant)
      tenant="${args[$((i + 1))]:-}"
      i=$((i + 2))
      continue
      ;;
    --data-dir=*)
      data_dir="${args[$i]#*=}"
      ;;
    --tenant=*)
      tenant="${args[$i]#*=}"
      ;;
  esac
  i=$((i + 1))
done

if [[ -n "$data_dir" && -n "$tenant" && ! -r "$data_dir/$tenant" ]]; then
  sudo -n "$BIN" "$@"
  exit $?
fi
"$BIN" "$@"
