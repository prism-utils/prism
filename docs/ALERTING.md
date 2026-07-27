# prism-alert — alerting contract

`cmd/prism-alert` is a per-tenant PromQL ruler with Alertmanager-compatible
webhook notification. It loads Prometheus alerting-rule YAML, evaluates each
rule against `prism-store`'s PromQL read API, runs the
pending→firing→resolved state machine (honoring `for` and `keep_firing_for`)
with `$value`/`$labels` templating, groups alerts Alertmanager-style, and POSTs
the **identical Alertmanager v4 webhook** the prism notifier already consumes.

One instance rules for exactly one tenant. Deploy one release per tenant.

- Configuration reference (env + flags + routes): [`CONFIG.md`](CONFIG.md) §15.
- Design rationale (why a lean in-tree state machine, not `rules.Manager`):
  [`DESIGN.md`](DESIGN.md) §15 → "prism-alert".
- The store's PromQL read API this ruler queries: [`STORE.md`](STORE.md).

## Data flow

```
rule YAML ──► ruler ──(PromQL /{ns}/api/v1/query)──► prism-store
                │                                        │
                │◄────────────── instant vector ─────────┘
                ▼
        for / keep_firing_for / resolve state machine
                ▼
        Alertmanager-style dispatcher (group_by / group_wait /
        group_interval / repeat_interval / resolve_timeout)
                ▼
        v4 webhook (bearer)  ──►  prism notifier /webhook
```

The ruler holds no TSDB. It reads the store over HTTP (optionally with a reader
JWT read fresh per request from `STORE_TOKEN_FILE`, so rotation needs no
restart) and writes to exactly one notifier webhook.

### Hot-only evaluation

By default (`QUERY_HOT_ONLY=true`) the ruler tags **every** query with the
store's `hot_only` extension, so recurring evaluations read only the hot
snapshot and never scan cold Parquet tiers — the ideal ruler scope: cheap,
bounded, and safe to run on a short `EVALUATION_INTERVAL`. This is enforced
end-to-end (`test/e2e` asserts the store receives `hot_only=true`). The param
can only tighten scope, so it is a no-op against a store already globally
hot-only. Set `QUERY_HOT_ONLY=false` only if a rule genuinely needs the full
time range; expect heavier, slower queries against cold storage.

## Rule format

Standard Prometheus **alerting** rule groups. Recording rules are ignored (the
ruler records nothing). Files are globbed as `*.yml` / `*.yaml` under
`RULES_DIR`; a missing directory yields no rules (a not-yet-mounted ConfigMap
does not crash startup).

```yaml
groups:
  - name: node
    rules:
      - alert: NodeDown
        expr: up == 0
        for: 5m                # pending until true for this long; 0s = fire now
        keep_firing_for: 0s    # optional: keep firing this long after it clears
        labels:
          severity: critical   # label values are templated ($labels, $value)
        annotations:
          summary: "{{ $labels.instance }} is down"
          description: "up has been 0 for {{ $value }}"
```

Every expression is validated at load with the canonical
`promql/parser`; a malformed `expr` is a fatal startup error naming the file.

### State machine

| Transition | When |
|---|---|
| (new) → **pending** | expression produces the series and `for > 0` and it has been active `< for` |
| pending → **firing** | the series has been continuously active for `≥ for` (`for: 0s` fires on the first match) |
| firing → **firing** (kept) | series stops matching but `keep_firing_for > 0` and within that window |
| firing → **resolved** | series stops matching (past any `keep_firing_for`); a resolved webhook is sent once |
| pending → (dropped) | series stops matching before it ever fired — no notification |

**Fail-open:** if a store query fails, that rule's alert state is left untouched
(no spurious resolve) and the error is logged, not fatal.

### Templating

Annotations and label **values** are expanded with the Prometheus `template`
package against the result sample, so `{{ $value }}`, `{{ $labels.<name> }}`,
and `{{ $externalURL }}` behave as in Prometheus. The template `query` function
is **disabled** (it returns an error): letting rule YAML issue extra PromQL per
series per evaluation would amplify load on prism-store and the ruler, and a
template expansion error ships a generic `<template error>` marker rather than
leaking internal detail into the webhook.

## Webhook payload (Alertmanager v4)

The dispatcher groups alerts by `GROUP_BY` and emits one webhook per group. The
body is frozen to the notifier contract (`version: "4"`); a resolved alert
carries its real `endsAt`, while a firing alert's `endsAt` is `now +
RESOLVE_TIMEOUT` so a receiver auto-resolves if refreshes stop.

```json
{
  "version": "4",
  "groupKey": "{alertname=\"HighCPU\",severity=\"warning\"}",
  "status": "firing",
  "receiver": "tenant-webhook",
  "groupLabels": { "alertname": "HighCPU", "severity": "warning" },
  "commonLabels": { "alertname": "HighCPU", "severity": "warning" },
  "commonAnnotations": { "summary": "CPU > 90%" },
  "externalURL": "https://prism.example/alerts",
  "alerts": [
    {
      "status": "firing",
      "labels": { "alertname": "HighCPU", "instance": "node-a", "severity": "warning" },
      "annotations": { "summary": "CPU > 90%" },
      "startsAt": "2026-04-19T09:55:00Z",
      "endsAt": "2026-04-19T10:05:00Z",
      "generatorURL": "https://prism.example/alerts/graph?g0.expr=cpu",
      "fingerprint": "58a171d28f29e910"
    }
  ]
}
```

| Field | Meaning |
|---|---|
| `version` | Always `"4"` (schema version the notifier pins). |
| `groupKey` | Deterministic opaque key `{k="v",…}` (group_by keys, sorted). |
| `status` | `firing` if any alert in the group is firing, else `resolved`. |
| `receiver` | `RECEIVER` (stamped on every payload). |
| `groupLabels` | The group_by tuple for this group. |
| `commonLabels` / `commonAnnotations` | Pairs present and identical across every alert in the group. |
| `alerts[].fingerprint` | 64-bit label fingerprint as 16 hex digits; lets a receiver dedupe firing/resolved pairs. |
| `alerts[].generatorURL` | Expression permalink when `EXTERNAL_URL` is set; empty otherwise. |

Timestamps are RFC 3339 UTC. Bodies larger than **256 KiB** (the notifier's
per-request cap) are split into multiple webhooks, each a valid payload for a
subset of the group's alerts. Delivery uses `Authorization: Bearer
<WEBHOOK_SECRET>` and bounded exponential backoff; `5xx`/`429` retry, other
`4xx` are permanent.

### Delivery semantics (best-effort, bounded)

Notification is **best-effort with bounded in-request retry**, not a durable
queue. A single `Send` retries transient failures (`5xx`/`429`/transport) with
exponential backoff up to a capped elapsed time, then gives up. After that:

- An unchanged **firing** group re-notifies on its next change or at
  `REPEAT_INTERVAL`, so a firing alert is eventually re-sent.
- A **resolved** notification dropped after retries is **not** re-sent — the
  series no longer matches, so there is nothing to re-emit.
- A multi-chunk group is delivered chunk-by-chunk with no cross-chunk rollback;
  a failure mid-way can leave the notifier holding a partial group until the
  next re-notify.

This bounded design is deliberate (see [`DESIGN.md`](DESIGN.md) §15 → the lean,
no-TSDB ruler): it never buffers unboundedly and never blocks evaluation on a
down notifier. If you need at-least-once resolve delivery, front the notifier
with a durable receiver.

## Dispatch knobs

| Knob | Effect |
|---|---|
| `GROUP_BY` | Labels alerts are grouped on before notifying. |
| `GROUP_WAIT` | Delay before the first notification for a new group (coalesces related alerts). |
| `GROUP_INTERVAL` | Minimum spacing between notifications for a group once fired, when its alert set changes. |
| `REPEAT_INTERVAL` | How often an unchanged firing group re-notifies. |
| `RESOLVE_TIMEOUT` | `endsAt` horizon on firing alerts for receiver-side auto-resolution. |

## Deployment

Use the [`deploy/charts/prism-alert`](../deploy/charts/prism-alert) Helm chart:
one release per tenant, rules supplied inline (`rules:`) or via an existing
ConfigMap (`rulesConfigMap:`), and `WEBHOOK_SECRET` (plus an optional reader JWT)
sourced from a pre-created Secret (`secrets.existingSecret`). The pod runs as
non-root uid 65532 with a read-only root filesystem and all capabilities
dropped. The image is a signed multi-arch distroless build
(`ghcr.io/elk-utilities/prism-alert`).

## Testing

`test/e2e/alert_e2e_test.go` (build tag `e2e`) drives the full chain — real
PromQL client → ruler state machine → dispatcher → v4 webhook client — against
the canonical `promql` engine serving the store's API shape, into a real HTTP
notifier receiver, asserting a firing→resolved transition with `$value`/`$labels`
expansion. See [`TESTING.md`](TESTING.md).
