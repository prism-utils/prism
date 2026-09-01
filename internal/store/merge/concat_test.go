package merge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestExecuteMergeConcatThreeFiles(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-concat0001-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 3; i++ {
		path := filepath.Join(l0, pathID(i)+".parquet")
		ts := base.Add(time.Duration(i) * time.Minute)
		testparquet.WriteSegmentWithTs(t, path, ts, "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}
	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	now := base.Add(time.Hour)
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, now)
	if err != nil {
		t.Fatalf("ExecuteMerge: %v", err)
	}
	if x.CopyCount != 0 {
		t.Fatalf("CopyCount = %d, want concat (0); dest=%s", x.CopyCount, out.Path)
	}
	got := readMetricRows(t, out.Path)
	if len(got) != 3 {
		t.Fatalf("rows = %d, want 3", len(got))
	}
	for i, row := range got {
		want := base.Add(time.Duration(i) * time.Minute)
		if !row.ts.Equal(want) {
			t.Fatalf("row %d ts = %v, want %v", i, row.ts, want)
		}
		if row.value != float64(i) {
			t.Fatalf("row %d value = %v, want %v", i, row.value, float64(i))
		}
	}
	for _, s := range sources {
		if _, err := os.Stat(s.Path); !os.IsNotExist(err) {
			t.Fatalf("source %s should be deleted", s.Path)
		}
	}
}

func TestExecuteMergeEmptySources(t *testing.T) {
	x, err := NewExecutor(ExecutorConfig{DataDir: t.TempDir(), Tenant: "t", RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	_, err = x.ExecuteMerge(MergeAction{}, time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), "no sources") {
		t.Fatalf("err = %v, want no sources", err)
	}
}

func TestExecuteMergeSingletonIsNoHomogeneousPack(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-concat0002-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(l0, "a.parquet")
	testparquet.WriteSegmentWithTs(t, path, base, "up", 1)
	seg, err := StatSegment(path, 0, DuckDBCaps{})
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	_, err = x.ExecuteMerge(MergeAction{Sources: []Segment{seg}, DestTier: 1}, base)
	if !errors.Is(err, ErrNoHomogeneousPack) {
		t.Fatalf("err = %v, want ErrNoHomogeneousPack", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("singleton source must stay live: %v", err)
	}
}

func TestExecuteMergeSchemaSplitLeavesOtherFamily(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-concat0003-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var familyA []Segment
	for i := 0; i < 5; i++ {
		path := filepath.Join(l0, fmt.Sprintf("a%d.parquet", i))
		testparquet.WriteSegmentWithTs(t, path, base.Add(time.Duration(i)*time.Minute), "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		familyA = append(familyA, seg)
	}
	extraPath := filepath.Join(l0, "extra.parquet")
	writeMetricsExtraColumn(t, extraPath, base.Add(10*time.Minute), "odd")
	extra, err := StatSegment(extraPath, 0, DuckDBCaps{})
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	sources := append(append([]Segment{}, familyA...), extra)
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("ExecuteMerge: %v", err)
	}
	if x.CopyCount != 0 {
		t.Fatalf("CopyCount = %d, want 0", x.CopyCount)
	}
	got := readMetricRows(t, out.Path)
	if len(got) != 5 {
		t.Fatalf("dest rows = %d, want 5 (schema A only)", len(got))
	}
	if _, err := os.Stat(extraPath); err != nil {
		t.Fatalf("schema-B singleton must stay live: %v", err)
	}
	for _, s := range familyA {
		if _, err := os.Stat(s.Path); !os.IsNotExist(err) {
			t.Fatalf("schema-A source %s should be retired", s.Path)
		}
	}
}

func TestExecuteMergeFourteenFilesNoCopy(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-concat0004-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const files, rowsPerFile, labelBytes = 14, 5000, 4096
	var sources []Segment
	var wantRows int
	var sumBytes int64
	for i := 0; i < files; i++ {
		path := filepath.Join(l0, fmt.Sprintf("%02d.parquet", i))
		start := i * rowsPerFile
		writeUniqueMetricsParquet(t, path, base.Add(time.Duration(start)*time.Second), start, rowsPerFile, labelBytes)
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		sumBytes += seg.Bytes
		wantRows += rowsPerFile
		sources = append(sources, seg)
	}
	copyX, err := NewExecutor(ExecutorConfig{
		DataDir:      dataDir,
		Tenant:       tenant,
		RowGroupSize: 1000,
		MemoryLimit:  "256MB",
		Threads:      1,
		FailConcat:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copyX.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, base.Add(time.Hour)); err == nil {
		_ = copyX.Close()
		t.Fatalf("COPY at 256MB succeeded on %d files (%d bytes); fixture is not an OOM case", files, sumBytes)
	} else if !strings.Contains(strings.ToLower(err.Error()), "out of memory") &&
		!strings.Contains(strings.ToLower(err.Error()), "memory_limit") {
		_ = copyX.Close()
		t.Fatalf("COPY fallback error = %v (want DuckDB OOM)", err)
	}
	_ = copyX.Close()
	x, err := NewExecutor(ExecutorConfig{
		DataDir:      dataDir,
		Tenant:       tenant,
		RowGroupSize: 1000,
		MemoryLimit:  "256MB",
		Threads:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, base.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("concat ExecuteMerge: %v", err)
	}
	if x.CopyCount != 0 {
		t.Fatalf("CopyCount = %d, want concat", x.CopyCount)
	}
	if got := countParquetRows(t, out.Path); got != wantRows {
		t.Fatalf("rows = %d, want %d", got, wantRows)
	}
}

func TestExecuteMergeConcatFailFallsBackToCopy(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-concat0005-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 3; i++ {
		path := filepath.Join(l0, pathID(i)+".parquet")
		testparquet.WriteSegmentWithTs(t, path, base.Add(time.Duration(i)*time.Minute), "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}
	x, err := NewExecutor(ExecutorConfig{
		DataDir:      dataDir,
		Tenant:       tenant,
		RowGroupSize: 1000,
		FailConcat:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("ExecuteMerge: %v", err)
	}
	if x.CopyCount != 1 {
		t.Fatalf("CopyCount = %d, want 1 fallback COPY", x.CopyCount)
	}
	if len(readMetricRows(t, out.Path)) != 3 {
		t.Fatal("fallback dest should keep all rows")
	}
	if _, err := os.Stat(layout.MergeSkipMarker(sources[0].Path)); !os.IsNotExist(err) {
		t.Fatal("successful COPY must not leave a skip marker")
	}
}

func TestExecuteMergeRewriteBudgetThenSkip(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-concat0006-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 3; i++ {
		path := filepath.Join(l0, pathID(i)+".parquet")
		testparquet.WriteSegmentWithTs(t, path, base.Add(time.Duration(i)*time.Minute), "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}
	x, err := NewExecutor(ExecutorConfig{
		DataDir:      dataDir,
		Tenant:       tenant,
		RowGroupSize: 1000,
		FailConcat:   true,
		FailCopy:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	for i := 0; i < MergeMaxRewriteAttempts; i++ {
		_, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, base)
		if err == nil {
			t.Fatal("want rewrite failure")
		}
		if recErr := RecordRewriteFailure(sources); recErr != nil {
			t.Fatal(recErr)
		}
	}
	if _, err := os.Stat(layout.MergeSkipMarker(sources[0].Path)); err != nil {
		t.Fatalf("skip marker missing: %v", err)
	}
	live, err := ScanTier(dataDir, tenant, 0, DuckDBCaps{})
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 0 {
		t.Fatalf("ScanTier after skip = %d, want 0 merge inputs", len(live))
	}
	got := readMetricRows(t, sources[0].Path)
	if len(got) != 1 {
		t.Fatalf("skip-marked file must stay readable, rows=%d", len(got))
	}
}

func TestScanTierMergesUnskippedBesideSkipMarker(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-concat0007-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	skipPath := filepath.Join(l0, "skip.parquet")
	testparquet.WriteSegmentWithTs(t, skipPath, base, "up", 1)
	if err := os.WriteFile(layout.MergeSkipMarker(skipPath), []byte("attempts=5\nreason=too-large\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var live []Segment
	for i := 0; i < 6; i++ {
		path := filepath.Join(l0, fmt.Sprintf("live%d.parquet", i))
		testparquet.WriteSegmentWithTs(t, path, base.Add(time.Duration(i+1)*time.Minute), "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		live = append(live, seg)
	}
	found, err := ScanTier(dataDir, tenant, 0, DuckDBCaps{})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 6 {
		t.Fatalf("ScanTier = %d, want 6 live files", len(found))
	}
	p := NewPlanner(PlannerConfig{SegmentsPerTier: 6, MaxSegmentBytes: 1 << 30, FloorBytes: 1})
	actions := p.FindMerges(found)
	if len(actions) != 1 {
		t.Fatalf("FindMerges = %d, want 1", len(actions))
	}
	if len(actions[0].Sources) != 6 {
		t.Fatalf("sources = %d, want 6", len(actions[0].Sources))
	}
	_ = live
}

func TestExecuteLogMergeKwayOrdersByIngestTS(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-kway0001-apps"
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	p0 := filepath.Join(landing, layout.SegmentName(time.Unix(0, 1).UTC()))
	p1 := filepath.Join(landing, layout.SegmentName(time.Unix(0, 2).UTC()))
	writeLogsIngest(t, p0, []logIngestRow{
		{Message: "c", NS: 300},
		{Message: "a", NS: 100},
	})
	writeLogsIngest(t, p1, []logIngestRow{
		{Message: "d", NS: 400},
		{Message: "b", NS: 200},
	})
	s0, err := StatLogSegment(p0, logLandingTier)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := StatLogSegment(p1, logLandingTier)
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000, MemoryLimit: "8MB"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{Sources: []Segment{s0, s1}, DestTier: 0}, time.Unix(0, 9).UTC())
	if err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}
	if x.CopyCount != 0 {
		t.Fatalf("CopyCount = %d, want k-way", x.CopyCount)
	}
	msgs := readLogMessagesOrdered(t, out.Path)
	want := []string{"a", "b", "c", "d"}
	if len(msgs) != 4 {
		t.Fatalf("messages = %v, want %v", msgs, want)
	}
	for i, m := range want {
		if msgs[i] != m {
			t.Fatalf("order = %v, want %v", msgs, want)
		}
	}
}

func TestExecuteLogMergeKwayTiesBreakOnEventTS(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-kway0001b-apps"
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	later := time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)
	earlier := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)
	p0 := filepath.Join(landing, layout.SegmentName(time.Unix(0, 1).UTC()))
	p1 := filepath.Join(landing, layout.SegmentName(time.Unix(0, 2).UTC()))
	writeLogsIngestAndEvent(t, p0, []logEventRow{{Message: "later", NS: 100, Event: later}})
	writeLogsIngestAndEvent(t, p1, []logEventRow{{Message: "earlier", NS: 100, Event: earlier}})
	s0, err := StatLogSegment(p0, logLandingTier)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := StatLogSegment(p1, logLandingTier)
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{Sources: []Segment{s0, s1}, DestTier: 0}, time.Unix(0, 9).UTC())
	if err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}
	if x.CopyCount != 0 {
		t.Fatalf("CopyCount = %d, want k-way", x.CopyCount)
	}
	msgs := readLogMessagesOrdered(t, out.Path)
	want := []string{"earlier", "later"}
	if len(msgs) != 2 || msgs[0] != want[0] || msgs[1] != want[1] {
		t.Fatalf("order = %v, want %v", msgs, want)
	}
}

func TestConcatParquetAllocatorBalance(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 2; i++ {
		path := filepath.Join(dir, fmt.Sprintf("%d.parquet", i))
		testparquet.WriteSegmentWithTs(t, path, base.Add(time.Duration(i)*time.Minute), "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	dest := filepath.Join(dir, "out.parquet")
	if err := concatParquet(dest, sources, mem); err != nil {
		t.Fatalf("concatParquet: %v", err)
	}
	mem.AssertSize(t, 0)
}

func TestKwayMergeLogsAllocatorBalance(t *testing.T) {
	dir := t.TempDir()
	p0 := filepath.Join(dir, "a.parquet")
	p1 := filepath.Join(dir, "b.parquet")
	writeLogsIngest(t, p0, []logIngestRow{{Message: "a", NS: 100}})
	writeLogsIngest(t, p1, []logIngestRow{{Message: "b", NS: 200}})
	s0, err := StatLogSegment(p0, logLandingTier)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := StatLogSegment(p1, logLandingTier)
	if err != nil {
		t.Fatal(err)
	}
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	dest := filepath.Join(dir, "out.parquet")
	if err := kwayMergeLogs(dest, []Segment{s0, s1}, mem); err != nil {
		t.Fatalf("kwayMergeLogs: %v", err)
	}
	mem.AssertSize(t, 0)
}

func TestExecuteLogMergeKwayNullFillsExtraColumn(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-kway0002-apps"
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	p0 := filepath.Join(landing, layout.SegmentName(t0))
	p1 := filepath.Join(landing, layout.SegmentName(t0.Add(time.Minute)))
	testparquet.WriteLogsRawFile(t, p0, []testparquet.LogRow{{Message: "plain", Format: "none"}})
	writeLogsExtraColumn(t, p1, "extra-row", "k8s", "hello")
	s0, err := StatLogSegment(p0, logLandingTier)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := StatLogSegment(p1, logLandingTier)
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{Sources: []Segment{s0, s1}, DestTier: 0}, t0.Add(time.Hour))
	if err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}
	extras := readLogExtraByMessage(t, out.Path)
	if extras["extra-row"] != "hello" {
		t.Fatalf("extra on extra-row = %q", extras["extra-row"])
	}
	if extras["plain"] != "" {
		t.Fatalf("plain row extra = %q, want null/empty", extras["plain"])
	}
}

func TestExecuteLogMergeKwayFailFallsBack(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-kway0003-apps"
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 2; i++ {
		path := filepath.Join(landing, layout.SegmentName(t0.Add(time.Duration(i)*time.Minute)))
		testparquet.WriteLogsRawFile(t, path, []testparquet.LogRow{{Message: fmt.Sprintf("m%d", i), Format: "none"}})
		seg, err := StatLogSegment(path, logLandingTier)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}
	x, err := NewExecutor(ExecutorConfig{
		DataDir:      dataDir,
		Tenant:       tenant,
		RowGroupSize: 1000,
		FailKway:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	if _, err := x.ExecuteLogMerge(artifact, LogMergeAction{Sources: sources, DestTier: 0}, t0); err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}
	if x.CopyCount != 1 {
		t.Fatalf("CopyCount = %d, want 1", x.CopyCount)
	}
}

func TestRecordRewriteFailureLogsThenSkip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.parquet")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := []Segment{{Path: path}}
	for i := 0; i < MergeMaxRewriteAttempts-1; i++ {
		if err := RecordRewriteFailure(src); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(layout.MergeSkipMarker(path)); !os.IsNotExist(err) {
			t.Fatalf("skip marker at attempt %d", i+1)
		}
	}
	if err := RecordRewriteFailure(src); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(layout.MergeSkipMarker(path)); err != nil {
		t.Fatalf("skip marker: %v", err)
	}
}

type metricRow struct {
	ts    time.Time
	value float64
}

func countParquetRows(t *testing.T, path string) int {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	var n int
	q := "SELECT COUNT(*) FROM read_parquet('" + layout.ToSlash(path) + "')"
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func writeUniqueMetricsParquet(t *testing.T, path string, start time.Time, id0, rows, labelBytes int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__name__", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "labels", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "timestamp_ms", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	nameB := b.Field(0).(*array.StringBuilder)
	labB := b.Field(1).(*array.StringBuilder)
	valB := b.Field(2).(*array.Float64Builder)
	msB := b.Field(3).(*array.Int64Builder)
	tsB := b.Field(4).(*array.TimestampBuilder)
	label := make([]byte, labelBytes)
	for i := 0; i < rows; i++ {
		id := id0 + i
		s := uint32(id)*1103515245 + 12345
		for j := range label {
			s = s*1103515245 + 12345
			label[j] = ' ' + byte(s%95)
		}
		nameB.Append("up")
		labB.Append(string(label))
		valB.Append(float64(id))
		msB.Append(0)
		tsB.Append(arrow.Timestamp(start.Add(time.Duration(i) * time.Second).UTC().UnixMicro()))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer tbl.Release()
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // G304: test fixture path
	if err != nil {
		t.Fatal(err)
	}
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	w, err := pqarrow.NewFileWriter(schema, out, props, pqarrow.DefaultWriterProps())
	if err != nil {
		_ = out.Close()
		t.Fatal(err)
	}
	chunk := tbl.NumRows()
	if chunk <= 0 {
		chunk = 1
	}
	if err := w.WriteTable(tbl, chunk); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

type logEventRow struct {
	Message string
	NS      int64
	Event   time.Time
}

func writeLogsIngestAndEvent(t *testing.T, path string, rows []logEventRow) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	parts := make([]string, len(rows))
	for i, r := range rows {
		tsStr := r.Event.UTC().Format("2006-01-02 15:04:05.999999")
		parts[i] = fmt.Sprintf("('%s', 'none', %d::BIGINT, CAST('%s' AS TIMESTAMP))", r.Message, r.NS, tsStr)
	}
	tmp := path + ".tmp"
	q := fmt.Sprintf(`
		COPY (
			SELECT * FROM (VALUES %s) AS t(message, format, "%s", ts)
		) TO '%s' (FORMAT parquet)
	`, strings.Join(parts, ", "), logIngestTSColumn, layout.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func readMetricRows(t *testing.T, path string) []metricRow {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(),
		"SELECT ts, value FROM read_parquet('"+layout.ToSlash(path)+"') ORDER BY ts")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []metricRow
	for rows.Next() {
		var r metricRow
		if err := rows.Scan(&r.ts, &r.value); err != nil {
			t.Fatal(err)
		}
		r.ts = r.ts.UTC()
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func writeMetricsExtraColumn(t *testing.T, path string, ts time.Time, extra string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	tsStr := ts.UTC().Format("2006-01-02 15:04:05.999999")
	tmp := path + ".tmp"
	q := fmt.Sprintf(`
		COPY (
			SELECT 'up' AS "__name__", '{}' AS labels, 1.0 AS value, 0::BIGINT AS timestamp_ms,
			       CAST('%s' AS TIMESTAMP) AS ts, '%s' AS extra_col
		) TO '%s' (FORMAT parquet)
	`, tsStr, extra, layout.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

type logIngestRow struct {
	Message string
	NS      int64
}

func writeLogsIngest(t *testing.T, path string, rows []logIngestRow) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = fmt.Sprintf("('%s', 'none', %d::BIGINT)", r.Message, r.NS)
	}
	tmp := path + ".tmp"
	q := fmt.Sprintf(`
		COPY (
			SELECT * FROM (VALUES %s) AS t(message, format, "%s")
		) TO '%s' (FORMAT parquet)
	`, strings.Join(parts, ", "), logIngestTSColumn, layout.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func writeLogsExtraColumn(t *testing.T, path, message, format, extra string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	tmp := path + ".tmp"
	q := fmt.Sprintf(`
		COPY (
			SELECT '%s' AS message, '%s' AS format, '%s' AS extra
		) TO '%s' (FORMAT parquet)
	`, message, format, extra, layout.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func readLogMessagesOrdered(t *testing.T, path string) []string {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf("SELECT message FROM read_parquet('%s') ORDER BY %s", layout.ToSlash(path), logIngestTSColumn)
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func readLogExtraByMessage(t *testing.T, path string) map[string]string {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(),
		"SELECT message, extra FROM read_parquet('"+layout.ToSlash(path)+"')")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var m string
		var extra sql.NullString
		if err := rows.Scan(&m, &extra); err != nil {
			t.Fatal(err)
		}
		if extra.Valid {
			out[m] = extra.String
		} else {
			out[m] = ""
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
