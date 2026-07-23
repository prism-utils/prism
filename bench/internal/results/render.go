// Package results defines the benchmark report schema and markdown renderer.
package results

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/elk-utilities/prism/bench/internal/monitor"
)

// Environment captures host and dependency versions for reproducibility notes.
type Environment struct {
	OS                string    `json:"os"`
	Arch              string    `json:"arch"`
	CPUModel          string    `json:"cpu_model"`
	RAMGiB            float64   `json:"ram_gib"`
	ClickHouseVersion string    `json:"clickhouse_version"`
	DuckDBVersion     string    `json:"duckdb_version"`
	GitCommit         string    `json:"git_commit"`
	MetricsRows       int64     `json:"metrics_rows"`
	LogsRows          int64     `json:"logs_rows"`
	Scale             int       `json:"scale"`
	MeasuredAt        time.Time `json:"measured_at"`
	ResourceCPUs      float64   `json:"resource_cpus_per_system"`
	ResourceMemMiB    int       `json:"resource_mem_mib_per_system"`
	DuckDBThreads     int       `json:"duckdb_threads"`
	DuckDBMemoryLimit string    `json:"duckdb_memory_limit"`
	IdleWindowSec     float64   `json:"idle_window_sec"`
	Profile           string    `json:"profile,omitempty"`
	ChartPaths        []string  `json:"chart_paths,omitempty"`
}

// Workload is one timed benchmark result for a system.
type Workload struct {
	Name        string         `json:"name"`
	System      string         `json:"system"`
	WallSeconds float64        `json:"wall_seconds,omitempty"`
	RowsPerSec  float64        `json:"rows_per_sec,omitempty"`
	Rows        int64          `json:"rows,omitempty"`
	P50Ms       float64        `json:"p50_ms,omitempty"`
	P95Ms       float64        `json:"p95_ms,omitempty"`
	MinMs       float64        `json:"min_ms,omitempty"`
	Usage       *monitor.Usage `json:"usage,omitempty"`
}

// Report is the machine-readable benchmark output written to results.json.
type Report struct {
	Environment            Environment `json:"environment"`
	LikeCountStore         int64       `json:"like_count_store"`
	LikeCountClickHouse    int64       `json:"like_count_clickhouse"`
	MetricsCountStore      int64       `json:"metrics_count_store"`
	MetricsCountClickHouse int64       `json:"metrics_count_clickhouse"`
	Workloads              []Workload  `json:"workloads"`
}

// WriteJSON persists rep to path with mode 0644.
func WriteJSON(path string, rep *Report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return fmt.Errorf("results: marshal: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("results: write json: %w", err)
	}
	return nil
}

// WriteMarkdown persists the rendered table to path.
func WriteMarkdown(path, body string) error {
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return fmt.Errorf("results: write markdown: %w", err)
	}
	return nil
}

// RenderMarkdown formats rep as human-readable markdown for bench/RESULTS.md.
func RenderMarkdown(rep *Report) string {
	return renderMarkdown(rep, true)
}

// RenderMarkdownRoot formats rep for embedding in the repository root README.md.
func RenderMarkdownRoot(rep *Report) string {
	return renderMarkdown(rep, false)
}

func renderMarkdown(rep *Report, benchLocalCharts bool) string {
	env := rep.Environment
	apiProfile := env.Profile == "api"
	apiArrowProfile := env.Profile == "api-arrow"
	var b strings.Builder
	switch {
	case apiArrowProfile:
		b.WriteString("# Arrow transport profile (RBAC on)\n\n")
		b.WriteString("Measured on this host with `make bench-api-arrow` — store queries over the RBAC-guarded HTTP SQL API with **Arrow IPC** transport for count/aggregation and a JSON-vs-Arrow scan comparison.\n\n")
		b.WriteString("*prism-store count/aggregation use Arrow transport (`Accept: application/vnd.apache.arrow.stream`); scan phases compare JSON vs Arrow on the same SQL. ClickHouse uses its native protocol client; logs LIKE remains engine-level (no logs API). JWT/RBAC overhead applies to every store HTTP request.*\n\n")
	case apiProfile:
		b.WriteString("# Benchmark: prism-store (RBAC + HTTP `/sql`) vs ClickHouse\n\n")
		b.WriteString("Measured on this host with `make bench-api` — queries over the RBAC-guarded HTTP SQL API.\n\n")
		b.WriteString("*prism-store count/aggregation are end-to-end HTTP + JWT/RBAC + per-request sandbox (materialize-then-lock); ClickHouse uses its native protocol client; logs LIKE remains engine-level (no logs API).*\n\n")
	default:
		b.WriteString("# Benchmark: prism-store vs ClickHouse\n\n")
		b.WriteString("Measured on this host with `make bench` (default small profile).\n\n")
	}
	b.WriteString("## Environment\n\n")
	fmt.Fprintf(&b, "| Key | Value |\n|-----|-------|\n")
	fmt.Fprintf(&b, "| OS / arch | %s / %s |\n", env.OS, env.Arch)
	fmt.Fprintf(&b, "| CPU | %s |\n", env.CPUModel)
	fmt.Fprintf(&b, "| RAM | %.1f GiB |\n", env.RAMGiB)
	fmt.Fprintf(&b, "| ClickHouse | %s |\n", env.ClickHouseVersion)
	fmt.Fprintf(&b, "| DuckDB | %s |\n", env.DuckDBVersion)
	fmt.Fprintf(&b, "| Dataset | %d metrics + %d logs rows (scale=%d) |\n", env.MetricsRows, env.LogsRows, env.Scale)
	if env.ResourceCPUs > 0 {
		fmt.Fprintf(&b, "| Resource cap (per system) | %.0f vCPU / %d MiB RAM |\n", env.ResourceCPUs, env.ResourceMemMiB)
		fmt.Fprintf(&b, "| DuckDB threads / memory_limit | %d / %s |\n", env.DuckDBThreads, env.DuckDBMemoryLimit)
	}
	if env.IdleWindowSec > 0 {
		fmt.Fprintf(&b, "| Idle baseline window | %.1f s before workloads |\n", env.IdleWindowSec)
	}
	fmt.Fprintf(&b, "| Git commit | `%s` |\n", env.GitCommit)
	if !env.MeasuredAt.IsZero() {
		fmt.Fprintf(&b, "| Measured | %s |\n", env.MeasuredAt.UTC().Format(time.RFC3339))
	}
	b.WriteString("\n## Correctness gates\n\n")
	fmt.Fprintf(&b, "Metrics `COUNT(*)` over the full ingested table: store **%s**, ClickHouse **%s** (must match; equals dataset metrics row count).\n\n",
		formatInt(rep.MetricsCountStore), formatInt(rep.MetricsCountClickHouse))
	fmt.Fprintf(&b, "Logs `LIKE '%%deadline exceeded%%'` count: store **%s**, ClickHouse **%s** (must match).\n\n",
		formatInt(rep.LikeCountStore), formatInt(rep.LikeCountClickHouse))

	b.WriteString("## Results (p50 / p95 / min ms; ingest: wall + rows/s)\n\n")
	if apiArrowProfile {
		b.WriteString("| Workload | prism-store (Arrow) | ClickHouse |\n")
		b.WriteString("|----------|---------------------|------------|\n")
		for _, name := range []string{"ingest", "count", "aggregation", "logs_like"} {
			store := findWorkload(rep.Workloads, name, "prism-store")
			ch := findWorkload(rep.Workloads, name, "clickhouse")
			fmt.Fprintf(&b, "| %s | %s | %s |\n", workloadLabel(name), formatWorkload(store), formatWorkload(ch))
		}
		b.WriteString("\n**Scan transport comparison** (same SQL, store only — not vs ClickHouse):\n\n")
		b.WriteString("| Transport | p50 / p95 / min (ms) | rows returned |\n")
		b.WriteString("|-----------|----------------------|---------------|\n")
		scanJSON := findWorkload(rep.Workloads, monitor.PhaseScanJSON, "prism-store")
		scanArrow := findWorkload(rep.Workloads, monitor.PhaseScanArrow, "prism-store")
		fmt.Fprintf(&b, "| JSON | %s | %s |\n", formatWorkload(scanJSON), formatScanRows(scanJSON))
		fmt.Fprintf(&b, "| Arrow | %s | %s |\n", formatWorkload(scanArrow), formatScanRows(scanArrow))
	} else {
		b.WriteString("| Workload | prism-store | ClickHouse |\n")
		b.WriteString("|----------|-------------|------------|\n")
		for _, name := range []string{"ingest", "count", "aggregation", "logs_like"} {
			store := findWorkload(rep.Workloads, name, "prism-store")
			ch := findWorkload(rep.Workloads, name, "clickhouse")
			fmt.Fprintf(&b, "| %s | %s | %s |\n", workloadLabel(name), formatWorkload(store), formatWorkload(ch))
		}
	}

	b.WriteString("\n## Resource usage (dense series; per-phase aggregates)\n\n")
	b.WriteString("| Workload | System | CPU mean / peak (cores) | Peak RSS (MiB) | Read+write (MiB) | MiB/s | IOPS |\n")
	b.WriteString("|----------|--------|-------------------------|----------------|------------------|-------|------|\n")
	resourcePhases := []string{"idle", "ingest", "count", "aggregation", "logs_like"}
	if apiArrowProfile {
		resourcePhases = append(resourcePhases, monitor.PhaseScanJSON, monitor.PhaseScanArrow)
	}
	for _, name := range resourcePhases {
		if apiArrowProfile && (name == monitor.PhaseScanJSON || name == monitor.PhaseScanArrow) {
			w := findWorkload(rep.Workloads, name, "prism-store")
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n",
				workloadLabel(name), "prism-store",
				formatUsageCPU(w), formatUsageRSS(w), formatUsageIO(w), formatUsageMiBps(w), formatUsageIOPS(w))
			continue
		}
		for _, sys := range []string{"prism-store", "clickhouse"} {
			w := findWorkload(rep.Workloads, name, sys)
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n",
				workloadLabel(name), systemLabel(sys),
				formatUsageCPU(w), formatUsageRSS(w), formatUsageIO(w), formatUsageMiBps(w), formatUsageIOPS(w))
		}
	}
	switch {
	case apiArrowProfile:
		b.WriteString("\n**API-Arrow profile:** store **count**, **aggregation**, **scan_json**, and **scan_arrow** sample the `prism-store` binary (HTTP `/sql` with JWT/RBAC). Store **logs LIKE** samples the benchmark process (embedded engine; server stopped). Store **ingest** and **idle** sample the `prism-store` binary. ClickHouse samples the container cgroup via cumulative counter diffs (~75 ms). **Scan phases** isolate JSON-vs-Arrow transport memory/latency on the same build — not a ClickHouse comparison.\n")
	case apiProfile:
		b.WriteString("\n**API profile:** store **count** and **aggregation** sample the `prism-store` binary (HTTP `/sql` with JWT/RBAC). Store **logs LIKE** samples the benchmark process (embedded engine; server stopped). Store **ingest** and **idle** sample the `prism-store` binary. ClickHouse samples the container cgroup via cumulative counter diffs (~75 ms).\n")
	default:
		b.WriteString("\nStore **count**, **aggregation**, and **logs LIKE** sample the benchmark process (embedded DuckDB engine). Store **ingest** and **idle** sample the `prism-store` binary. ClickHouse samples the container cgroup via cumulative counter diffs (~75 ms).\n")
	}
	if apiArrowProfile || apiProfile {
		b.WriteString("\n**Caveats:** JWT verification and RBAC policy checks run on every store HTTP request. Container I/O/IOPS on Docker Desktop (macOS/Windows) frequently report blkio as 0 — meaningful container I/O needs a native Linux Docker host.\n")
	}

	if len(env.ChartPaths) > 0 {
		b.WriteString("\n## Resource charts\n\n")
		for _, p := range env.ChartPaths {
			base := p
			if i := strings.LastIndex(p, "/"); i >= 0 {
				base = p[i+1:]
			}
			embed := p
			if benchLocalCharts {
				embed = chartEmbedForBenchResults(p, env.Profile)
			}
			fmt.Fprintf(&b, "### %s\n\n![%s](%s)\n\n", strings.TrimSuffix(base, ".svg"), base, embed)
		}
	}

	b.WriteString("\n## Interpretation\n\n")
	if apiArrowProfile {
		b.WriteString(interpretAPIArrow(rep))
	} else {
		b.WriteString(interpret(rep))
	}
	b.WriteString("\n## Reproduce\n\n")
	switch {
	case apiArrowProfile:
		b.WriteString("```bash\nmake bench-api-arrow        # default scale (2M rows total)\nmake bench-api-arrow BENCH_SCALE=2\n```\n")
	case apiProfile:
		b.WriteString("```bash\nmake bench-api        # default scale (2M rows total)\nmake bench-api BENCH_SCALE=2\n```\n")
	default:
		b.WriteString("```bash\nmake bench        # default scale (2M rows total)\nmake bench BENCH_SCALE=2\n```\n")
	}
	b.WriteString("\nSee [`bench/README.md`](README.md) for prerequisites and cleanup.\n")
	return b.String()
}

func chartEmbedForBenchResults(storedPath string, profile string) string {
	switch profile {
	case "api-arrow":
		const prefix = "bench/charts-api-arrow/"
		if strings.HasPrefix(storedPath, prefix) {
			return strings.TrimPrefix(storedPath, "bench/")
		}
		if strings.HasPrefix(storedPath, "charts-api-arrow/") {
			return storedPath
		}
		return storedPath
	case "api":
		const prefix = "bench/charts-api/"
		if strings.HasPrefix(storedPath, prefix) {
			return strings.TrimPrefix(storedPath, "bench/")
		}
		if strings.HasPrefix(storedPath, "charts-api/") {
			return storedPath
		}
		return storedPath
	default:
		const prefix = "bench/charts/"
		if strings.HasPrefix(storedPath, prefix) {
			return strings.TrimPrefix(storedPath, "bench/")
		}
		if strings.HasPrefix(storedPath, "charts/") {
			return storedPath
		}
		return storedPath
	}
}

func interpretAPIArrow(rep *Report) string {
	lines := []string{
		"Both systems ingested the same seeded dataset with JWT auth on store ingest. Each query workload is warmed once before K=5 timed runs.",
		"",
		"**Count and aggregation** use the **Arrow IPC** transport over HTTP `/sql` (lazy-view sandbox + streaming). Compare against the attached JSON `-api` profile (~280–300 ms, ~471–483 MiB peak RSS on the same host class) to see the combined lazy-view (#54) + Arrow transport (#55) impact.",
		"",
		"**Scan JSON vs Arrow** runs the same `SELECT … LIMIT N` over both transports on one build — isolating transport memory/latency (JSON full-buffer vs Arrow streaming). This is **not** compared to ClickHouse.",
		"",
	}
	for _, name := range []string{"ingest", "count", "aggregation", "logs_like"} {
		store := findWorkload(rep.Workloads, name, "prism-store")
		ch := findWorkload(rep.Workloads, name, "clickhouse")
		if store == nil || ch == nil {
			continue
		}
		switch name {
		case "ingest":
			if store.RowsPerSec > ch.RowsPerSec {
				lines = append(lines, fmt.Sprintf("- **%s**: prism-store leads on ingest throughput (%.0f vs %.0f rows/s).", name, store.RowsPerSec, ch.RowsPerSec))
			} else {
				lines = append(lines, fmt.Sprintf("- **%s**: ClickHouse leads on ingest (%.0f vs %.0f rows/s).", name, ch.RowsPerSec, store.RowsPerSec))
			}
		default:
			if store.P50Ms <= ch.P50Ms {
				lines = append(lines, fmt.Sprintf("- **%s** (Arrow): prism-store p50 %.1f ms vs ClickHouse %.1f ms.", name, store.P50Ms, ch.P50Ms))
			} else {
				lines = append(lines, fmt.Sprintf("- **%s** (Arrow): ClickHouse p50 %.1f ms beats prism-store %.1f ms.", name, ch.P50Ms, store.P50Ms))
			}
		}
	}
	scanJSON := findWorkload(rep.Workloads, monitor.PhaseScanJSON, "prism-store")
	scanArrow := findWorkload(rep.Workloads, monitor.PhaseScanArrow, "prism-store")
	if scanJSON != nil && scanArrow != nil {
		lines = append(lines, fmt.Sprintf("- **scan**: JSON p50 %.1f ms (peak RSS %s MiB) vs Arrow p50 %.1f ms (peak RSS %s MiB).",
			scanJSON.P50Ms, formatUsageRSS(scanJSON), scanArrow.P50Ms, formatUsageRSS(scanArrow)))
	}
	return strings.Join(lines, "\n")
}

func interpret(rep *Report) string {
	lines := []string{
		"Both systems ingested the same seeded dataset. Each query workload is warmed once before K=5 timed runs.",
		"",
		"**Metrics count and aggregation** scan the **full ingested metrics table** on both sides — no `ts` range predicate — so both systems read the same N rows (`SELECT count(*)` and `GROUP BY __name__` over every row). Rollup tiers are excluded on the store path to avoid double-counting.",
		"",
		"**Logs LIKE** uses the same dataset-`ts` window on both systems (`ds.LogsQueryRange()`). The store path is **engine-level**: DuckDB `read_parquet` over a logs-shaped zstd Parquet tier in the store on-disk layout — the shipping store has no logs ingest API yet.",
		"",
		"The store metrics path uses real HTTP Parquet ingest, hot→L0 flush, tier compaction, and a fixed-schema union over hot + tier Parquet (no rollups for these workloads).",
		"",
		"ClickHouse uses MergeTree with `LowCardinality` dimensions, day partitioning, batched inserts (50k rows), and a `tokenbf_v1` skip index on `message`.",
		"",
	}
	for _, name := range []string{"ingest", "count", "aggregation", "logs_like"} {
		store := findWorkload(rep.Workloads, name, "prism-store")
		ch := findWorkload(rep.Workloads, name, "clickhouse")
		if store == nil || ch == nil {
			continue
		}
		switch name {
		case "ingest":
			switch {
			case store.RowsPerSec > ch.RowsPerSec:
				lines = append(lines, fmt.Sprintf("- **%s**: prism-store leads on ingest throughput (%.0f vs %.0f rows/s).", name, store.RowsPerSec, ch.RowsPerSec))
			case ch.RowsPerSec > store.RowsPerSec:
				lines = append(lines, fmt.Sprintf("- **%s**: ClickHouse leads on ingest (%.0f vs %.0f rows/s) — native columnar bulk load vs HTTP window ingest + DuckDB hot catalog.", name, ch.RowsPerSec, store.RowsPerSec))
			default:
				lines = append(lines, fmt.Sprintf("- **%s**: ingest throughput is close between systems.", name))
			}
		default:
			if store.P50Ms <= ch.P50Ms {
				lines = append(lines, fmt.Sprintf("- **%s**: prism-store p50 %.1f ms vs ClickHouse %.1f ms.", name, store.P50Ms, ch.P50Ms))
			} else {
				lines = append(lines, fmt.Sprintf("- **%s**: ClickHouse p50 %.1f ms beats prism-store %.1f ms on this host.", name, ch.P50Ms, store.P50Ms))
			}
		}
	}
	return strings.Join(lines, "\n")
}

func findWorkload(all []Workload, name, system string) *Workload {
	for i := range all {
		if all[i].Name == name && all[i].System == system {
			return &all[i]
		}
	}
	return nil
}

func workloadLabel(name string) string {
	switch name {
	case "logs_like":
		return "logs LIKE"
	case "idle":
		return "idle (baseline)"
	case monitor.PhaseScanJSON:
		return "scan (JSON transport)"
	case monitor.PhaseScanArrow:
		return "scan (Arrow transport)"
	default:
		return name
	}
}

func formatScanRows(w *Workload) string {
	if w == nil || w.Rows == 0 {
		return "—"
	}
	return formatInt(w.Rows)
}

func formatWorkload(w *Workload) string {
	if w == nil {
		return "—"
	}
	if w.Name == "ingest" {
		return fmt.Sprintf("%.2fs · %.0f rows/s", w.WallSeconds, w.RowsPerSec)
	}
	return fmt.Sprintf("%.1f / %.1f / %.1f", w.P50Ms, w.P95Ms, w.MinMs)
}

func formatInt(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	if s != "" {
		parts = append([]string{s}, parts...)
	}
	return strings.Join(parts, ",")
}

func systemLabel(system string) string {
	switch system {
	case "prism-store":
		return "prism-store"
	case "clickhouse":
		return "ClickHouse"
	default:
		return system
	}
}

func formatUsageCPU(w *Workload) string {
	if w == nil || w.Usage == nil {
		return "—"
	}
	u := w.Usage
	return fmt.Sprintf("%.2f / %.2f", u.CPUCoresMean, u.CPUCoresPeak)
}

func formatUsageRSS(w *Workload) string {
	if w == nil || w.Usage == nil {
		return "—"
	}
	return fmt.Sprintf("%.1f", w.Usage.RSSPeakMiB())
}

func formatUsageIO(w *Workload) string {
	if w == nil || w.Usage == nil {
		return "—"
	}
	if !w.Usage.IOAvailable() {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", w.Usage.TotalReadWriteMiB())
}

func formatUsageMiBps(w *Workload) string {
	if w == nil || w.Usage == nil {
		return "—"
	}
	if !w.Usage.IOAvailable() {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", w.Usage.TotalMiBPerSec())
}

func formatUsageIOPS(w *Workload) string {
	if w == nil || w.Usage == nil {
		return "—"
	}
	iops, ok := w.Usage.IOPS()
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%.0f", iops)
}
