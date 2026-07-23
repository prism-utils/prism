// Package results defines the benchmark report schema and markdown renderer.
package results

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
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
}

// Workload is one timed benchmark result for a system.
type Workload struct {
	Name        string  `json:"name"`
	System      string  `json:"system"`
	WallSeconds float64 `json:"wall_seconds,omitempty"`
	RowsPerSec  float64 `json:"rows_per_sec,omitempty"`
	Rows        int64   `json:"rows,omitempty"`
	P50Ms       float64 `json:"p50_ms,omitempty"`
	P95Ms       float64 `json:"p95_ms,omitempty"`
	MinMs       float64 `json:"min_ms,omitempty"`
}

// Report is the machine-readable benchmark output written to results.json.
type Report struct {
	Environment         Environment `json:"environment"`
	LikeCountStore      int64       `json:"like_count_store"`
	LikeCountClickHouse int64       `json:"like_count_clickhouse"`
	Workloads           []Workload  `json:"workloads"`
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

// RenderMarkdown formats rep as a human-readable results document.
func RenderMarkdown(rep *Report) string {
	env := rep.Environment
	var b strings.Builder
	b.WriteString("# Benchmark: prism-store vs ClickHouse\n\n")
	b.WriteString("Measured on this host with `make bench` (default small profile).\n\n")
	b.WriteString("## Environment\n\n")
	fmt.Fprintf(&b, "| Key | Value |\n|-----|-------|\n")
	fmt.Fprintf(&b, "| OS / arch | %s / %s |\n", env.OS, env.Arch)
	fmt.Fprintf(&b, "| CPU | %s |\n", env.CPUModel)
	fmt.Fprintf(&b, "| RAM | %.1f GiB |\n", env.RAMGiB)
	fmt.Fprintf(&b, "| ClickHouse | %s |\n", env.ClickHouseVersion)
	fmt.Fprintf(&b, "| DuckDB | %s |\n", env.DuckDBVersion)
	fmt.Fprintf(&b, "| Dataset | %d metrics + %d logs rows (scale=%d) |\n", env.MetricsRows, env.LogsRows, env.Scale)
	fmt.Fprintf(&b, "| Git commit | `%s` |\n", env.GitCommit)
	if !env.MeasuredAt.IsZero() {
		fmt.Fprintf(&b, "| Measured | %s |\n", env.MeasuredAt.UTC().Format(time.RFC3339))
	}
	b.WriteString("\n## Correctness gate\n\n")
	fmt.Fprintf(&b, "Logs `LIKE '%%deadline exceeded%%'` count: store **%s**, ClickHouse **%s** (must match).\n\n",
		formatInt(rep.LikeCountStore), formatInt(rep.LikeCountClickHouse))

	b.WriteString("## Results (p50 / p95 / min ms; ingest: wall + rows/s)\n\n")
	b.WriteString("| Workload | prism-store | ClickHouse |\n")
	b.WriteString("|----------|-------------|------------|\n")
	for _, name := range []string{"ingest", "count", "aggregation", "logs_like"} {
		store := findWorkload(rep.Workloads, name, "prism-store")
		ch := findWorkload(rep.Workloads, name, "clickhouse")
		fmt.Fprintf(&b, "| %s | %s | %s |\n", workloadLabel(name), formatWorkload(store), formatWorkload(ch))
	}

	b.WriteString("\n## Interpretation\n\n")
	b.WriteString(interpret(rep))
	b.WriteString("\n## Reproduce\n\n")
	b.WriteString("```bash\nmake bench        # default scale (2M rows total)\nmake bench BENCH_SCALE=2\n```\n")
	b.WriteString("\nSee [`bench/README.md`](README.md) for prerequisites and cleanup.\n")
	return b.String()
}

func interpret(rep *Report) string {
	lines := []string{
		"Both systems ingested the same seeded dataset over an identical time range with warmed caches (one throwaway query before timing).",
		"",
		"The store metrics path uses real HTTP Parquet ingest, hot→L0 flush, tier compaction, and the fixed-schema union query engine. The logs LIKE path is **engine-level**: DuckDB `read_parquet` over a zstd logs-shaped tier in the store on-disk layout — the shipping store has no logs ingest API yet.",
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
			if name == "count" {
				lines = append(lines, "  Store metrics queries filter on ingest-time `ts`; ClickHouse uses embedded sample timestamps.")
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
	default:
		return name
	}
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
