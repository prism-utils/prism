# Store diagnostics

Operator tools that read a tenant data directory and print JSON. They do not
mutate store files and they do not require a running `prism-store` process.

## Segment histogram

Snapshot of how a tenant's segments are organized: family (metrics / logs /
rollups / materializations), hot vs cold root, tier, file sizes, and the UTC
calendar days covered by parquet footer min/max (falling back to the window id
in the filename).

```bash
make diagnostic-segments TENANT=user-fqsejat4-apps
```

Defaults: `DATA_DIR=/var/lib/homelab/prism-store`, `COLD_DIR=/data/k8s/prism-store`.
The script builds a CGO-free binary and uses `sudo -n` when the tenant dir is
not readable by the current user (hostPath is mode `750` uid 472).
Override either:

```bash
make diagnostic-segments TENANT=user-fknjdouh-apps DATA_DIR=/var/lib/homelab/prism-store COLD_DIR=/data/k8s/prism-store
```

Or call the script directly:

```bash
./diagnostic/segment-histogram.sh --data-dir /var/lib/homelab/prism-store --cold-dir /data/k8s/prism-store --tenant user-fqsejat4-apps
```

`--compact` emits single-line JSON. `--list` includes every live file (can be
large on log-heavy tenants); the default is histogram + totals only.
