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

	"github.com/elk-utilities/prism/bench/internal/authgen"
	"github.com/elk-utilities/prism/bench/internal/caps"
	"github.com/elk-utilities/prism/bench/internal/clickhouse"
	"github.com/elk-utilities/prism/bench/internal/env"
	"github.com/elk-utilities/prism/bench/internal/gen"
	"github.com/elk-utilities/prism/bench/internal/monitor"
	"github.com/elk-utilities/prism/bench/internal/results"
	benchstore "github.com/elk-utilities/prism/bench/internal/store"
	"github.com/elk-utilities/prism/bench/internal/timing"
)

const (
	tenant        = "bench-tenant"
	queryRuns     = 5
	composeFile   = "bench/docker-compose.bench.yml"
	clickhouseSvc = "clickhouse"
)

type phaseClock struct {
	phases  []monitor.PhaseSpan
	current string
	start   time.Time
}

func newPhaseClock() *phaseClock {
	return &phaseClock{start: time.Now()}
}

func (p *phaseClock) set(name string) {
	now := time.Now()
	if p.current != "" {
		p.phases = append(p.phases, monitor.PhaseSpan{
			Name:  p.current,
			Start: p.start,
			End:   now,
		})
	}
	p.current = name
	p.start = now
}

func (p *phaseClock) finish() []monitor.PhaseSpan {
	if p.current != "" {
		p.phases = append(p.phases, monitor.PhaseSpan{
			Name:  p.current,
			Start: p.start,
			End:   time.Now(),
		})
		p.current = ""
	}
	return p.phases
}

func main() {
	if err := runMain(); err != nil {
		slog.Error("prism-bench", "err", err)
		os.Exit(1)
	}
}

func runMain() error {
	scale := flag.Int("scale", 1, "multiply default row counts")
	apiProfile := flag.Bool("api", false, "run RBAC + HTTP SQL API profile (writes profile-suffixed results)")
	workDir := flag.String("workdir", "bench/.work", "ephemeral data directory (relative to repo root unless absolute)")
	cpus := flag.Float64("cpus", caps.DefaultCPUs, "vCPU cap per system")
	memMiB := flag.Int("mem-mib", caps.DefaultMemMiB, "memory cap per system in MiB")
	idleSec := flag.Float64("idle-seconds", 5, "quiet idle baseline window before workloads")
	flag.Parse()

	ctx := context.Background()
	budget := caps.Budget{CPUs: *cpus, MemMiB: *memMiB}
	profile := ""
	if *apiProfile {
		profile = "api"
	}

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

	chConfigDir := filepath.Join(absWork, "clickhouse-config")
	if err := clickhouse.WriteBenchConfig(chConfigDir, budget); err != nil {
		return fmt.Errorf("clickhouse config: %w", err)
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
	if err := clickhouse.WriteBenchConfig(chConfigDir, budget); err != nil {
		return fmt.Errorf("clickhouse config: %w", err)
	}
	windows, err := gen.WriteMetricsWindows(metricsDir, ds.Metrics, 50_000)
	if err != nil {
		return fmt.Errorf("write metrics: %w", err)
	}

	composeEnv := append(os.Environ(),
		"BENCH_CPUS="+budget.ComposeCPUs(),
		"BENCH_MEM_LIMIT="+budget.ComposeMemLimit(),
		"BENCH_CH_CONFIG_DIR="+chConfigDir,
	)
	//nolint:gosec // G204: compose file path is derived from the git root, not user input.
	composeUp := exec.CommandContext(ctx, "docker", "compose", "-f", composePath, "up", "-d", "--wait")
	composeUp.Dir = root
	composeUp.Env = composeEnv
	composeUp.Stdout = os.Stderr
	composeUp.Stderr = os.Stderr
	//nolint:gosec // G204: compose file path is derived from the git root, not user input.
	composeDown := exec.CommandContext(context.Background(), "docker", "compose", "-f", composePath, "down", "-v")
	composeDown.Dir = root
	composeDown.Env = composeEnv
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

	chContainerID, err := monitor.ResolveComposeContainerID(ctx, composePath, clickhouseSvc)
	if err != nil {
		return fmt.Errorf("clickhouse container: %w", err)
	}

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
	storeCfg := benchstore.Config{
		DataDir:  dataDir,
		Tenant:   tenant,
		StoreBin: storeBin,
		Budget:   budget,
	}
	if profile == "api" {
		authEnv, err := authgen.New(filepath.Join(absWork, "auth"), tenant)
		if err != nil {
			return fmt.Errorf("auth setup: %w", err)
		}
		tok, err := authEnv.Token()
		if err != nil {
			return fmt.Errorf("auth token: %w", err)
		}
		storeCfg.RBAC = &benchstore.RBACConfig{
			PolicyFile: authEnv.PolicyPath(),
			Issuer:     authEnv.Issuer(),
			JWKSFile:   authEnv.JWKSPath(),
			Audience:   authEnv.Audience(),
		}
		storeCfg.Token = tok
	}
	sd, err := benchstore.New(storeCfg)
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

	chStream, err := monitor.NewDockerStreamSampler(chContainerID)
	if err != nil {
		return fmt.Errorf("clickhouse stream sampler: %w", err)
	}
	storeStream := monitor.NewProcStreamSampler(sd.Pid())
	benchStream := monitor.NewProcStreamSampler(os.Getpid())

	chStream.Start(runCtx)
	storeStream.Start(runCtx)
	benchStream.Start(runCtx)

	phases := newPhaseClock()
	setPhase := func(name string) {
		chStream.ForceSample(runCtx)
		storeStream.ForceSample(runCtx)
		benchStream.ForceSample(runCtx)
		phases.set(name)
		chStream.SetPhase(name)
		storeStream.SetPhase(name)
		benchStream.SetPhase(name)
	}

	host := env.Collect()
	rep := &results.Report{
		Environment: results.Environment{
			OS:                runtime.GOOS,
			Arch:              runtime.GOARCH,
			CPUModel:          host.CPUModel,
			RAMGiB:            host.RAMGiB,
			GitCommit:         env.GitCommit(ctx),
			MetricsRows:       cfg.MetricsRows,
			LogsRows:          cfg.LogsRows,
			Scale:             *scale,
			MeasuredAt:        time.Now().UTC(),
			ResourceCPUs:      budget.CPs(),
			ResourceMemMiB:    budget.MemMiB,
			DuckDBThreads:     budget.Threads(),
			DuckDBMemoryLimit: budget.DuckDBMemoryLimit(),
			IdleWindowSec:     *idleSec,
			Profile:           profile,
		},
	}

	chVer, err := ch.Version(ctx)
	if err != nil {
		return fmt.Errorf("clickhouse version: %w", err)
	}
	rep.Environment.ClickHouseVersion = chVer

	workloads := make([]results.Workload, 0, 10)

	logsPath := filepath.Join(dataDir, tenant, "bench-logs", "segment.parquet")
	logsGlob := logsPath

	setPhase(monitor.PhaseIdle)
	time.Sleep(time.Duration(*idleSec * float64(time.Second)))

	setPhase(monitor.PhaseIngest)
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
	chStream.ForceSample(runCtx)
	storeStream.ForceSample(runCtx)
	benchStream.ForceSample(runCtx)

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

	if profile == "api" {
		if err := runAPIQueryPhase(ctx, sd, ch, cfg, logsGlob, logsStart, logsEnd, expectedLike, setPhase, &workloads, queryRuns, rep); err != nil {
			return err
		}
	} else {
		if err := runBaselineQueryPhase(ctx, sd, ch, cfg, logsGlob, logsStart, logsEnd, expectedLike, setPhase, &workloads, queryRuns, rep); err != nil {
			return err
		}
	}

	phaseSpans := phases.finish()
	storePoints := storeStream.Stop()
	benchPoints := benchStream.Stop()
	chPoints := chStream.Stop()

	storeStitched := monitor.StitchStoreSeries(storePoints, benchPoints, phaseSpans)

	usageFor := func(system, phase string) monitor.Usage {
		switch system {
		case "prism-store":
			switch phase {
			case monitor.PhaseIdle, monitor.PhaseIngest:
				return monitor.AggregatePhaseSpan(storePoints, phase, phaseSpans)
			case monitor.PhaseCount, monitor.PhaseAggregation:
				if profile == "api" {
					return monitor.AggregatePhaseSpan(storePoints, phase, phaseSpans)
				}
				return monitor.AggregatePhaseSpan(benchPoints, phase, phaseSpans)
			default:
				return monitor.AggregatePhaseSpan(benchPoints, phase, phaseSpans)
			}
		case "clickhouse":
			return monitor.AggregatePhaseSpan(chPoints, phase, phaseSpans)
		default:
			return monitor.Usage{}
		}
	}

	attachUsage := func(name, system string) *monitor.Usage {
		u := usageFor(system, name)
		return &u
	}
	idleStore := attachUsage(monitor.PhaseIdle, "prism-store")
	idleCH := attachUsage(monitor.PhaseIdle, "clickhouse")
	workloads = append([]results.Workload{
		{Name: "idle", System: "prism-store", Usage: idleStore},
		{Name: "idle", System: "clickhouse", Usage: idleCH},
	}, workloads...)
	for i := range workloads {
		w := &workloads[i]
		if w.Usage != nil {
			continue
		}
		u := usageFor(w.System, w.Name)
		w.Usage = &u
	}

	rep.Workloads = workloads

	artifacts := results.ArtifactPaths(root, profile)
	if err := os.MkdirAll(artifacts.ChartsDir, 0o750); err != nil {
		return fmt.Errorf("charts dir: %w", err)
	}
	cpuChart := filepath.Join(artifacts.ChartsDir, "cpu-cores.svg")
	memChart := filepath.Join(artifacts.ChartsDir, "memory-rss.svg")
	ioChart := filepath.Join(artifacts.ChartsDir, "disk-io.svg")
	if err := results.WriteCPUChart(cpuChart, storeStitched, chPoints, phaseSpans); err != nil {
		return fmt.Errorf("cpu chart: %w", err)
	}
	if err := results.WriteMemoryChart(memChart, storeStitched, chPoints, phaseSpans); err != nil {
		return fmt.Errorf("memory chart: %w", err)
	}
	chartPaths := []string{
		results.ChartRel(&artifacts, "cpu-cores.svg"),
		results.ChartRel(&artifacts, "memory-rss.svg"),
	}
	if ok, err := results.WriteIOChart(ioChart, storeStitched, chPoints, phaseSpans); err != nil {
		return fmt.Errorf("io chart: %w", err)
	} else if ok {
		chartPaths = append(chartPaths, results.ChartRel(&artifacts, "disk-io.svg"))
	}
	rep.Environment.ChartPaths = chartPaths

	tsPath := artifacts.Timeseries
	if err := results.WriteTimeseriesJSON(tsPath, &results.TimeseriesReport{
		Phases:     phaseSpans,
		Store:      storeStitched,
		ClickHouse: chPoints,
		Series: []results.TimeseriesSeries{
			{System: "prism-store", Target: "prism-store binary", Points: storePoints},
			{System: "prism-store", Target: "benchmark process (embedded engine)", Points: benchPoints},
			{System: "clickhouse", Target: "container", Points: chPoints},
		},
	}); err != nil {
		return fmt.Errorf("write timeseries: %w", err)
	}

	jsonPath := artifacts.JSON
	mdPath := artifacts.Markdown
	if err := results.WriteJSON(jsonPath, rep); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	md := results.RenderMarkdown(rep)
	if err := results.WriteMarkdown(mdPath, md); err != nil {
		return fmt.Errorf("write markdown: %w", err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s, %s, %s, charts under %s\n", jsonPath, tsPath, mdPath, artifacts.ChartsDir)
	fmt.Print(md)
	return nil
}

//nolint:contextcheck // RunQueryMonitored has no ctx param; timed closures use outer ctx by design.
func runBaselineQueryPhase(
	ctx context.Context,
	sd benchstore.Driver,
	ch *clickhouse.Client,
	cfg gen.Config,
	logsGlob string,
	logsStart, logsEnd time.Time,
	expectedLike int64,
	setPhase func(string),
	workloads *[]results.Workload,
	queryRuns int,
	rep *results.Report,
) error {
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

	setPhase(monitor.PhaseCount)
	storeCountOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		n, err := sd.CountMetrics(ctx)
		if err != nil {
			return err
		}
		if n != cfg.MetricsRows {
			return fmt.Errorf("store metrics count %d != %d", n, cfg.MetricsRows)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("store count: %w", err)
	}
	chCountOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		n, err := ch.CountMetrics(ctx)
		if err != nil {
			return err
		}
		if n != cfg.MetricsRows {
			return fmt.Errorf("clickhouse metrics count %d != %d", n, cfg.MetricsRows)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("clickhouse count: %w", err)
	}
	*workloads = append(*workloads,
		results.Workload{Name: "count", System: "prism-store", P50Ms: storeCountOut.Stats.P50Ms, P95Ms: storeCountOut.Stats.P95Ms, MinMs: storeCountOut.Stats.MinMs},
		results.Workload{Name: "count", System: "clickhouse", P50Ms: chCountOut.Stats.P50Ms, P95Ms: chCountOut.Stats.P95Ms, MinMs: chCountOut.Stats.MinMs},
	)

	setPhase(monitor.PhaseAggregation)
	storeAggOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		return sd.AggregateMetrics(ctx)
	}, nil)
	if err != nil {
		return fmt.Errorf("store aggregate: %w", err)
	}
	chAggOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		return ch.AggregateMetrics(ctx)
	}, nil)
	if err != nil {
		return fmt.Errorf("clickhouse aggregate: %w", err)
	}
	*workloads = append(*workloads,
		results.Workload{Name: "aggregation", System: "prism-store", P50Ms: storeAggOut.Stats.P50Ms, P95Ms: storeAggOut.Stats.P95Ms, MinMs: storeAggOut.Stats.MinMs},
		results.Workload{Name: "aggregation", System: "clickhouse", P50Ms: chAggOut.Stats.P50Ms, P95Ms: chAggOut.Stats.P95Ms, MinMs: chAggOut.Stats.MinMs},
	)

	setPhase(monitor.PhaseLogsLike)
	storeLikeOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		n, err := sd.CountLogsLike(ctx, logsGlob, logsStart, logsEnd)
		if err != nil {
			return err
		}
		if n != expectedLike {
			return fmt.Errorf("store logs like %d != %d", n, expectedLike)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("store logs like timing: %w", err)
	}
	chLikeOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		n, err := ch.CountLogsLike(ctx, logsStart, logsEnd)
		if err != nil {
			return err
		}
		if n != expectedLike {
			return fmt.Errorf("clickhouse logs like %d != %d", n, expectedLike)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("clickhouse logs like timing: %w", err)
	}
	*workloads = append(*workloads,
		results.Workload{Name: "logs_like", System: "prism-store", P50Ms: storeLikeOut.Stats.P50Ms, P95Ms: storeLikeOut.Stats.P95Ms, MinMs: storeLikeOut.Stats.MinMs},
		results.Workload{Name: "logs_like", System: "clickhouse", P50Ms: chLikeOut.Stats.P50Ms, P95Ms: chLikeOut.Stats.P95Ms, MinMs: chLikeOut.Stats.MinMs},
	)
	return nil
}

//nolint:contextcheck // RunQueryMonitored has no ctx param; timed closures use outer ctx by design.
func runAPIQueryPhase(
	ctx context.Context,
	sd benchstore.Driver,
	ch *clickhouse.Client,
	cfg gen.Config,
	logsGlob string,
	logsStart, logsEnd time.Time,
	expectedLike int64,
	setPhase func(string),
	workloads *[]results.Workload,
	queryRuns int,
	rep *results.Report,
) error {
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

	setPhase(monitor.PhaseLogsLike)
	storeLikeOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		n, err := sd.CountLogsLike(ctx, logsGlob, logsStart, logsEnd)
		if err != nil {
			return err
		}
		if n != expectedLike {
			return fmt.Errorf("store logs like %d != %d", n, expectedLike)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("store logs like timing: %w", err)
	}
	chLikeOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		n, err := ch.CountLogsLike(ctx, logsStart, logsEnd)
		if err != nil {
			return err
		}
		if n != expectedLike {
			return fmt.Errorf("clickhouse logs like %d != %d", n, expectedLike)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("clickhouse logs like timing: %w", err)
	}
	*workloads = append(*workloads,
		results.Workload{Name: "logs_like", System: "prism-store", P50Ms: storeLikeOut.Stats.P50Ms, P95Ms: storeLikeOut.Stats.P95Ms, MinMs: storeLikeOut.Stats.MinMs},
		results.Workload{Name: "logs_like", System: "clickhouse", P50Ms: chLikeOut.Stats.P50Ms, P95Ms: chLikeOut.Stats.P95Ms, MinMs: chLikeOut.Stats.MinMs},
	)

	stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	if err := sd.StopServer(stopCtx); err != nil {
		cancel()
		return fmt.Errorf("store stop for api restart: %w", err)
	}
	cancel()
	if err := sd.StartServer(ctx); err != nil {
		return fmt.Errorf("store restart for api queries: %w", err)
	}

	storeMetricsCount, err := sd.CountMetricsAPI(ctx)
	if err != nil {
		return fmt.Errorf("store metrics count gate (api): %w", err)
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

	setPhase(monitor.PhaseCount)
	storeCountOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		n, err := sd.CountMetricsAPI(ctx)
		if err != nil {
			return err
		}
		if n != cfg.MetricsRows {
			return fmt.Errorf("store metrics count %d != %d", n, cfg.MetricsRows)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("store count (api): %w", err)
	}
	chCountOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		n, err := ch.CountMetrics(ctx)
		if err != nil {
			return err
		}
		if n != cfg.MetricsRows {
			return fmt.Errorf("clickhouse metrics count %d != %d", n, cfg.MetricsRows)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("clickhouse count: %w", err)
	}
	*workloads = append(*workloads,
		results.Workload{Name: "count", System: "prism-store", P50Ms: storeCountOut.Stats.P50Ms, P95Ms: storeCountOut.Stats.P95Ms, MinMs: storeCountOut.Stats.MinMs},
		results.Workload{Name: "count", System: "clickhouse", P50Ms: chCountOut.Stats.P50Ms, P95Ms: chCountOut.Stats.P95Ms, MinMs: chCountOut.Stats.MinMs},
	)

	setPhase(monitor.PhaseAggregation)
	storeAggOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		return sd.AggregateMetricsAPI(ctx)
	}, nil)
	if err != nil {
		return fmt.Errorf("store aggregate (api): %w", err)
	}
	chAggOut, err := timing.RunQueryMonitored(queryRuns, func() error {
		return ch.AggregateMetrics(ctx)
	}, nil)
	if err != nil {
		return fmt.Errorf("clickhouse aggregate: %w", err)
	}
	*workloads = append(*workloads,
		results.Workload{Name: "aggregation", System: "prism-store", P50Ms: storeAggOut.Stats.P50Ms, P95Ms: storeAggOut.Stats.P95Ms, MinMs: storeAggOut.Stats.MinMs},
		results.Workload{Name: "aggregation", System: "clickhouse", P50Ms: chAggOut.Stats.P50Ms, P95Ms: chAggOut.Stats.P95Ms, MinMs: chAggOut.Stats.MinMs},
	)
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
