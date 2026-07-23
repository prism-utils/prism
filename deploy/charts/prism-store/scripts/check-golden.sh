#!/usr/bin/env bash
# Assert helm template output for the default profile matches the committed golden manifest.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="${ROOT}"
GOLDEN="${ROOT}/testdata/golden/default.yaml"
RELEASE="${HELM_RELEASE_NAME:-golden}"

render() {
  helm template "${RELEASE}" "${CHART}" "$@"
}

if [[ ! -f "${GOLDEN}" ]]; then
  echo "golden manifest missing: ${GOLDEN}" >&2
  echo "Generate with: helm template ${RELEASE} ${CHART} > ${GOLDEN}" >&2
  exit 1
fi

tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT
render > "${tmp}"

if ! diff -u "${GOLDEN}" "${tmp}"; then
  echo "golden manifest drift — re-render and commit if intentional:" >&2
  echo "  helm template ${RELEASE} ${CHART} > ${GOLDEN}" >&2
  exit 1
fi

echo "golden manifest OK (${GOLDEN})"
