#!/usr/bin/env bash
# run-cluster-bench.sh — deploy prism into a live cluster, run it against real
# in-cluster workloads, then pull the outputs back and reconcile them with
# prism-bench (Parquet row counts + JSON summaries) and record the pod's live
# CPU/memory from metrics-server.
#
# Prereqs: a reachable cluster (kubectl); the prism:bench image available to the
# cluster (for k3d: `k3d image import prism:bench -c <cluster>`); and a real
# logfmt sample file (< 1 MiB — the ConfigMap limit).
#
# The target namespace must be able to reach the exporters named in
# deploy/k8s/prism-bench.yaml. On a default-deny cluster, run it in the
# exporters' own namespace, e.g. NS=live-demo.
#
# Usage:
#   NS=live-demo deploy/k8s/run-cluster-bench.sh <logfmt-sample> [settle-seconds]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NS="${NS:-prism-bench}"
sample="${1:?usage: [NS=ns] run-cluster-bench.sh <logfmt-sample> [settle-seconds]}"
settle="${2:-30}"
OUT="${OUT_ROOT:-$REPO_ROOT/bench-out}/cluster"

[ -f "$sample" ] || { echo "sample not found: $sample" >&2; exit 2; }

echo ">> namespace: $NS"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo ">> applying manifest + log sample ConfigMap"
kubectl apply -n "$NS" -f "$REPO_ROOT/deploy/k8s/prism-bench.yaml"
kubectl -n "$NS" delete configmap prism-logs --ignore-not-found
kubectl -n "$NS" create configmap prism-logs --from-file=sample.logfmt="$sample"
kubectl -n "$NS" label configmap prism-logs app=prism-bench --overwrite >/dev/null
kubectl -n "$NS" rollout restart deploy/prism-bench
kubectl -n "$NS" rollout status deploy/prism-bench --timeout=90s

pod="$(kubectl -n "$NS" get pod -l app=prism-bench -o jsonpath='{.items[0].metadata.name}')"
echo ">> pod: $pod ; letting it scrape/process for ${settle}s"
sleep "$settle"

echo; echo ">> live resource usage (metrics-server, per container):"
kubectl -n "$NS" top pod "$pod" --containers 2>/dev/null || echo "   (metrics-server not ready)"

echo; echo ">> scrape health (last lines):"
kubectl -n "$NS" logs "$pod" -c prism 2>&1 | tail -4

echo; echo ">> copying /out from the inspector sidecar"
rm -rf "$OUT"; mkdir -p "$OUT"
kubectl -n "$NS" cp "$pod:/out" "$OUT" -c inspector

echo; echo ">> in-cluster outputs:"
find "$OUT" -type f | sort

go build -o "$REPO_ROOT/bin/prism-bench" "$REPO_ROOT/cmd/prism-bench"
echo; echo ">> reconciling logs branch (input sample vs in-pod Parquet/JSON):"
"$REPO_ROOT/bin/prism-bench" -inspect -label "cluster-logs (real, in-pod)" \
  -out "$OUT/logs" -input "$sample" || true
echo; echo ">> reconciling metrics branch (in-pod scrape output):"
"$REPO_ROOT/bin/prism-bench" -inspect -label "cluster-metrics (real, in-pod scrape)" \
  -out "$OUT/metrics" || true

echo; echo ">> sample metrics summary rows:"
cat "$OUT"/metrics/summary/s-*.json 2>/dev/null | head -c 600; echo
echo ">> clean up with: kubectl -n $NS delete all,cm -l app=prism-bench"
