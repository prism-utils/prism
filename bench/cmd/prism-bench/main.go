// Command prism-bench compares prism-store against ClickHouse on ingest, count,
// aggregation, and logs LIKE workloads with deterministic seeded data.
//
// Usage:
//
//	go run ./bench/cmd/prism-bench [--scale N]
//
// Or: make bench
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/elk-utilities/prism/bench/internal/clickhouse"
	"github.com/elk-utilities/prism/bench/internal/env"
	"github.com/elk-utilities/prism/bench/internal/gen"
	"github.com/elk-utilities/prism/bench/internal/results"
	benchstore "github.com/elk-utilities/prism/bench/internal/store"
	"github.com/elk-utilities/prism/bench/internal/timing"
)

const (
	tenant      = "bench-tenant"
	queryRuns   = 5
	composeFile = "bench/docker-compose.bench.yml"
)

func main() {
	if err := runMain(); err != nil {
		slog.Error("prism-bench", "err", err)
		os.Exit(1)
	}
}

func runMain() error {
	scale := flag.Int("scale", 1, "multiply default row counts")
	workDir := flag.String("workdir", "bench/.work", "ephemeral data directory (relative to repo root unless absolute)")
	flag.Parse()

	ctx := context.Background()

	if err := requireDocker(ctx); err != nil {
		return err
	}

	root, err := env.RepoRoot(ctx)
	if err != nil {
		return err
	}
	composePath := filepath.Join(root, composeFile)

	absWork := *workDir
	if !filepath.IsAbs(absWork) {
		absWork = filepath.Join(root, absWork)
	}
	absWork, err = filepath.Abs(absWork)
	if err != nil {
		return fmt.Errorf("workdir: %w", err)
	}

	cfg := gen.ScaleConfig(*scale)
	ds, err := gen.Generate(cfg)
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	logsStart, logsEnd := ds.LogsQueryRange()

	metricsDir := filepath.Join(absWork, "metrics-windows")
	if err := os.RemoveAll(absWork); err != nil {
		return fmt.Errorf("clean workdir: %w", err)
	}
	windows, err := gen.WriteMetricsWindows(metricsDir, ds.Metrics, 50_000)
	if err != nil {
		return fmt.Errorf("write metrics: %w", err)
	}

	//nolint:gosec // G204: compose file path is derived from the git root, not user input.
	composeUp := exec.CommandContext(ctx, "docker", "compose", "-f", composePath, "up", "-d", "--wait")
	composeUp.Dir = root
	composeUp.Stdout = os.Stderr
	composeUp.Stderr = os.Stderr
	//nolint:gosec // G204: compose file path is derived from the git root, not user input.
	composeDown := exec.CommandContext(context.Background(), "docker", "compose", "-f", composePath, "down", "-v")
	composeDown.Dir = root
	composeDown.Stdout = os.Stderr
	composeDown.Stderr = os.Stderr

	if err := composeUp.Run(); err != nil {
		return fmt.Errorf("compose up: %w", err)
	}
	defer func() {
		if err := composeDown.Run(); err != nil {
			slog.Error("compose down", "err", err)
		}
	}()

	if err := clickhouse.WaitReady("http://127.0.0.1:8123", 2*time.Minute); err != nil {
		return fmt.Errorf("clickhouse ready: %w", err)
	}

	ch, err := clickhouse.Open(clickhouse.Config{Addr: "127.0.0.1:9000"})
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.InitSchema(ctx); err != nil {
		return fmt.Errorf("clickhouse schema: %w", err)
	}
	if err := ch.Truncate(ctx); err != nil {
		return fmt.Errorf("clickhouse truncate: %w", err)
	}

	storeBin := filepath.Join(root, "bin", "prism-store")
	if _, err := os.Stat(storeBin); err != nil {
		//nolint:gosec // G204: storeBin is under the repo bin/ directory from git root.
		build := exec.CommandContext(ctx, "go", "build", "-o", storeBin, "./cmd/prism-store")
		build.Dir = root
		build.Env = append(os.Environ(), "CGO_ENABLED=1")
		build.Stdout = os.Stderr
		build.Stderr = os.Stderr
		if err := build.Run(); err != nil {
			return fmt.Errorf("build prism-store: %w", err)
		}
	}

	dataDir := filepath.Join(absWork, "store-data")
	sd, err := benchstore.New(benchstore.Config{
		DataDir:  dataDir,
		Tenant:   tenant,
		StoreBin: storeBin,
	})
	if err != nil {
		return fmt.Errorf("store driver: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := sd.Start(runCtx); err != nil {
		return fmt.Errorf("store start: %w", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		_ = sd.Stop(stopCtx)
	}()

	host := env.Collect()
	rep := &results.Report{
		Environment: results.Environment{
			OS:          runtime.GOOS,
			Arch:        runtime.GOARCH,
			CPUModel:    host.CPUModel,
			RAMGiB:      host.RAMGiB,
			GitCommit:   env.GitCommit(ctx),
			MetricsRows: cfg.MetricsRows,
			LogsRows:    cfg.LogsRows,
			Scale:       *scale,
			MeasuredAt:  time.Now().UTC(),
		},
	}

	chVer, err := ch.Version(ctx)
	if err != nil {
		return fmt.Errorf("clickhouse version: %w", err)
	}
	rep.Environment.ClickHouseVersion = chVer

	workloads := make([]results.Workload, 0, 8)

	logsPath := filepath.Join(dataDir, tenant, "bench-logs", "segment.parquet")
	logsGlob := logsPath

	storeIngestSec, err := timing.WallRun(func() error {
		if err := sd.IngestMetricsHTTP(ctx, windows); err != nil {
			return err
		}
		return sd.WriteLogsTier(ctx, logsPath, ds.Logs)
	})
	if err != nil {
		return fmt.Errorf("store ingest: %w", err)
	}
	if err := sd.Compact(ctx); err != nil {
		return fmt.Errorf("store compact: %w", err)
	}

	chIngestSec, err := timing.WallRun(func() error {
		if err := ch.IngestMetrics(ctx, ds.Metrics); err != nil {
			return err
		}
		return ch.IngestLogs(ctx, ds.Logs)
	})
	if err != nil {
		return fmt.Errorf("clickhouse ingest: %w", err)
	}

	totalRows := cfg.MetricsRows + cfg.LogsRows
	workloads = append(workloads,
		results.Workload{Name: "ingest", System: "prism-store", WallSeconds: storeIngestSec, Rows: totalRows, RowsPerSec: float64(totalRows) / storeIngestSec},
		results.Workload{Name: "ingest", System: "clickhouse", WallSeconds: chIngestSec, Rows: totalRows, RowsPerSec: float64(totalRows) / chIngestSec},
	)

	duckVer, err := sd.DuckDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("duckdb version: %w", err)
	}
	rep.Environment.DuckDBVersion = duckVer

	expectedLike := gen.ExpectedDeadlineCount(cfg.LogsRows)

	storeLike, err := sd.CountLogsLike(ctx, logsGlob, logsStart, logsEnd)
	if err != nil {
		return fmt.Errorf("store logs like count: %w", err)
	}
	chLike, err := ch.CountLogsLike(ctx, logsStart, logsEnd)
	if err != nil {
		return fmt.Errorf("clickhouse logs like count: %w", err)
	}
	if storeLike != chLike {
		return fmt.Errorf("LIKE count mismatch: store=%d clickhouse=%d", storeLike, chLike)
	}
	if storeLike != expectedLike {
		return fmt.Errorf("LIKE count wrong: got %d want %d", storeLike, expectedLike)
	}
	rep.LikeCountStore = storeLike
	rep.LikeCountClickHouse = chLike

	storeMetricsCount, err := sd.CountMetrics(ctx)
	if err != nil {
		return fmt.Errorf("store metrics count gate: %w", err)
	}
	chMetricsCount, err := ch.CountMetrics(ctx)
	if err != nil {
		return fmt.Errorf("clickhouse metrics count gate: %w", err)
	}
	if storeMetricsCount != chMetricsCount {
		return fmt.Errorf("metrics count mismatch: store=%d clickhouse=%d", storeMetricsCount, chMetricsCount)
	}
	if storeMetricsCount != cfg.MetricsRows {
		return fmt.Errorf("metrics count wrong: got %d want %d", storeMetricsCount, cfg.MetricsRows)
	}
	rep.MetricsCountStore = storeMetricsCount
	rep.MetricsCountClickHouse = chMetricsCount

	storeCountStats, err := timing.RunQuery(queryRuns, func() error {
		n, err := sd.CountMetrics(ctx)
		if err != nil {
			return err
		}
		if n != cfg.MetricsRows {
			return fmt.Errorf("store metrics count %d != %d", n, cfg.MetricsRows)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("store count: %w", err)
	}
	chCountStats, err := timing.RunQuery(queryRuns, func() error {
		n, err := ch.CountMetrics(ctx)
		if err != nil {
			return err
		}
		if n != cfg.MetricsRows {
			return fmt.Errorf("clickhouse metrics count %d != %d", n, cfg.MetricsRows)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("clickhouse count: %w", err)
	}
	workloads = append(workloads,
		results.Workload{Name: "count", System: "prism-store", P50Ms: storeCountStats.P50Ms, P95Ms: storeCountStats.P95Ms, MinMs: storeCountStats.MinMs},
		results.Workload{Name: "count", System: "clickhouse", P50Ms: chCountStats.P50Ms, P95Ms: chCountStats.P95Ms, MinMs: chCountStats.MinMs},
	)

	storeAggStats, err := timing.RunQuery(queryRuns, func() error {
		return sd.AggregateMetrics(ctx)
	})
	if err != nil {
		return fmt.Errorf("store aggregate: %w", err)
	}
	chAggStats, err := timing.RunQuery(queryRuns, func() error {
		return ch.AggregateMetrics(ctx)
	})
	if err != nil {
		return fmt.Errorf("clickhouse aggregate: %w", err)
	}
	workloads = append(workloads,
		results.Workload{Name: "aggregation", System: "prism-store", P50Ms: storeAggStats.P50Ms, P95Ms: storeAggStats.P95Ms, MinMs: storeAggStats.MinMs},
		results.Workload{Name: "aggregation", System: "clickhouse", P50Ms: chAggStats.P50Ms, P95Ms: chAggStats.P95Ms, MinMs: chAggStats.MinMs},
	)

	storeLikeStats, err := timing.RunQuery(queryRuns, func() error {
		n, err := sd.CountLogsLike(ctx, logsGlob, logsStart, logsEnd)
		if err != nil {
			return err
		}
		if n != expectedLike {
			return fmt.Errorf("store logs like %d != %d", n, expectedLike)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("store logs like timing: %w", err)
	}
	chLikeStats, err := timing.RunQuery(queryRuns, func() error {
		n, err := ch.CountLogsLike(ctx, logsStart, logsEnd)
		if err != nil {
			return err
		}
		if n != expectedLike {
			return fmt.Errorf("clickhouse logs like %d != %d", n, expectedLike)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("clickhouse logs like timing: %w", err)
	}
	workloads = append(workloads,
		results.Workload{Name: "logs_like", System: "prism-store", P50Ms: storeLikeStats.P50Ms, P95Ms: storeLikeStats.P95Ms, MinMs: storeLikeStats.MinMs},
		results.Workload{Name: "logs_like", System: "clickhouse", P50Ms: chLikeStats.P50Ms, P95Ms: chLikeStats.P95Ms, MinMs: chLikeStats.MinMs},
	)

	rep.Workloads = workloads

	jsonPath := filepath.Join(root, "bench", "results.json")
	mdPath := filepath.Join(root, "bench", "RESULTS.md")
	if err := results.WriteJSON(jsonPath, rep); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	md := results.RenderMarkdown(rep)
	if err := results.WriteMarkdown(mdPath, md); err != nil {
		return fmt.Errorf("write markdown: %w", err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s and %s\n", jsonPath, mdPath)
	fmt.Print(md)
	return nil
}

func requireDocker(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}")
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("docker not available: %w", err)
	}
	if len(out) == 0 {
		return fmt.Errorf("docker server not running")
	}
	return nil
}
