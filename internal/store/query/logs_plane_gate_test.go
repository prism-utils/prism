package query

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/logmeta"
	"github.com/prism-utils/prism/internal/store/merge"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const gateTenant = "user-gate-77aa"

func TestLokiLogsSQLConstantDepthUnder1100Files(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, gateTenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	const n = 1100
	for i := 0; i < n; i++ {
		name := layout.SegmentName(time.Unix(0, int64(i+1)*int64(time.Second)))
		testparquet.WriteLogsRawFile(t, filepath.Join(dir, name), []testparquet.LogRow{
			{Message: "x", Format: "none"},
		})
	}
	sqlText, files, err := sandboxLokiLogsSQL(tenantRoot, 0, 0, false, 0)
	if err != nil {
		t.Fatalf("sandboxLokiLogsSQL: %v", err)
	}
	if len(files) != n {
		t.Fatalf("open set = %d, want %d", len(files), n)
	}
	if got := strings.Count(strings.ToUpper(sqlText), "UNION ALL"); got >= 50 {
		t.Fatalf("UNION ALL count = %d (want < 50); sql must use list read_parquet, not per-file UNION", got)
	}
	if !strings.Contains(sqlText, "read_parquet([") {
		t.Fatalf("expected list read_parquet, got: %s", truncate(sqlText, 200))
	}
}

func TestLokiOpenSetTimePrunedToRange(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, gateTenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	var overlap []string
	for i := 0; i < 30; i++ {
		at := base.Add(time.Duration(i) * time.Hour)
		name := layout.SegmentName(at)
		path := filepath.Join(dir, name)
		testparquet.WriteLogsRawFile(t, path, []testparquet.LogRow{
			{Message: fmt.Sprintf("h%d", i), Format: "none"},
		})
		if i >= 10 && i <= 12 {
			overlap = append(overlap, path)
		}
	}
	start := base.Add(10 * time.Hour).UnixNano()
	end := base.Add(13 * time.Hour).UnixNano()
	_, files, err := sandboxLokiLogsSQL(tenantRoot, start, end, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) > len(overlap)+1 {
		t.Fatalf("open set = %d paths, want ~%d overlapping; got %#v", len(files), len(overlap), fileBases(files))
	}
	if len(files) < len(overlap) {
		t.Fatalf("open set = %d, want at least %d overlapping", len(files), len(overlap))
	}
	_, all, err := sandboxLokiLogsSQL(tenantRoot, base.UnixNano(), base.Add(40*time.Hour).UnixNano(), false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 30 {
		t.Fatalf("full-range open set = %d, want 30", len(all))
	}
}

func TestLokiAndSQLShareIdenticalLogsRelationSQL(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, gateTenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(time.Unix(int64(i+1), 0))), []testparquet.LogRow{
			{Message: "m", Format: "k8s"},
		})
	}
	sqlFP, err := logsRelationFingerprint(tenantRoot)
	if err != nil {
		t.Fatal(err)
	}
	_, lokiFiles, err := sandboxLokiLogsSQL(tenantRoot, 0, 0, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]string, len(lokiFiles))
	for i, f := range lokiFiles {
		parts[i] = f.Path
	}
	lokiFP := fmt.Sprintf("%d:%s", len(parts), joinComma(parts))
	if sqlFP != lokiFP {
		t.Fatalf("sql fingerprint %q != loki fingerprint %q", sqlFP, lokiFP)
	}
	sqlBody, err := sandboxLogsUnionSQL(tenantRoot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(strings.ToUpper(sqlBody), "UNION ALL") >= 50 {
		t.Fatalf("sql relation still UNION-heavy: %s", truncate(sqlBody, 120))
	}
	if !strings.Contains(sqlBody, "read_parquet([") {
		t.Fatalf("sql relation must use list read_parquet: %s", truncate(sqlBody, 120))
	}
}

func TestLokiLabelAPIsDoNotProjectMessageColumn(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, gateTenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	fat := strings.Repeat("m", 8192)
	testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(time.Now())), []testparquet.LogRow{
		{Message: fat, Format: "k8s"},
	})
	sqlText, _, err := sandboxLokiLogsSQL(tenantRoot, 0, time.Now().Add(time.Hour).UnixNano(), true, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sqlText, "EXCLUDE (message)") && !strings.Contains(sqlText, "EXCLUDE (message,") {
		if !strings.Contains(sqlText, "EXCLUDE ("+lokiMessageColumn+")") {
			t.Fatalf("label view SQL must exclude message: %s", truncate(sqlText, 300))
		}
	}
	full, _, err := sandboxLokiLogsSQL(tenantRoot, 0, time.Now().Add(time.Hour).UnixNano(), false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(full, "EXCLUDE ("+lokiMessageColumn+")") {
		t.Fatalf("query_range view must not exclude message: %s", truncate(full, 200))
	}
}

func TestLogsFileMetaCacheServesSecondLabelsWithoutFullRescan(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, gateTenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(time.Unix(int64(i+1), 0))), []testparquet.LogRow{
			{Message: "x", Format: "none"},
		})
	}
	before := globalLogsMetaCache.rescanCount()
	if _, _, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{}); err != nil {
		t.Fatal(err)
	}
	mid := globalLogsMetaCache.rescanCount()
	if mid != before+1 {
		t.Fatalf("first scan rescans=%d, want %d", mid, before+1)
	}
	if _, _, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{}); err != nil {
		t.Fatal(err)
	}
	if got := globalLogsMetaCache.rescanCount(); got != mid {
		t.Fatalf("second call rescans=%d, want unchanged %d", got, mid)
	}
	testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(time.Unix(1000, 0))), []testparquet.LogRow{
		{Message: "y", Format: "json"},
	})
	InvalidateLogsMetaCache(tenantRoot)
	_, files, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 51 {
		t.Fatalf("after invalidate open set = %d, want 51", len(files))
	}
}

func TestLogsQueryDefaultsToRecentSegmentsNotFullHistory(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, gateTenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	newT := now.Add(-30 * time.Minute)
	testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(old)), []testparquet.LogRow{
		{Message: "old", Format: "old"},
	})
	testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(newT)), []testparquet.LogRow{
		{Message: "new", Format: "new"},
	})
	_, recent, err := sandboxLokiLogsSQL(tenantRoot, 0, now.UnixNano(), false, 6*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 1 || recent[0].MinTsNs != newT.UnixNano() {
		t.Fatalf("recent-only open set = %#v, want only new file", fileBases(recent))
	}
	_, hist, err := sandboxLokiLogsSQL(tenantRoot, old.Add(-time.Hour).UnixNano(), now.UnixNano(), false, 6*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("explicit history open set = %d, want 2", len(hist))
	}
}

func TestLokiLabelValuesUsesCardinalityIndexNotMessageScan(t *testing.T) {
	InvalidateLogsMetaCache("")
	dataDir := t.TempDir()
	tenant := gateTenant
	eng := engine.New(engine.Config{DataDir: dataDir}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	fat := strings.Repeat("m", 8192)
	landRaw := func(format string) {
		t.Helper()
		tmp := filepath.Join(t.TempDir(), "chunk.parquet")
		testparquet.WriteLogsRawFile(t, tmp, []testparquet.LogRow{
			{Message: fat, Format: format},
		})
		b, err := os.ReadFile(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.LandLogWindow(tenant, "logs-raw", bytes.NewReader(b)); err != nil {
			t.Fatalf("land %s: %v", format, err)
		}
	}
	landRaw("k8s")
	landRaw("none")
	// Only refreshed windows are searchable, and the index describes that same set.
	testparquet.PromoteLandedLogsToTier(t, dataDir, tenant, "logs-raw")

	ctx := context.Background()
	rel := &lokiRelation{dataDir: dataDir, tenant: tenant, columns: []string{"format", lokiMessageColumn}}
	vals, err := rel.labelValues(ctx, "format", []string{"1=1"}, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	source, sql := LabelValuesObservationForTest()
	if source != "index" {
		t.Fatalf("label values source = %q, want index", source)
	}
	if labelValuesSQLWouldReferenceMessage(sql) {
		t.Fatalf("indexed label values must not reference message; sql=%q", sql)
	}
	got := map[string]bool{}
	for _, v := range vals {
		got[v] = true
	}
	if !got["k8s"] || !got["none"] {
		t.Fatalf("format values = %v, want k8s and none", vals)
	}

	landRaw("json")
	testparquet.PromoteLandedLogsToTier(t, dataDir, tenant, "logs-raw")
	idxVals, err := logmeta.LabelValues(dataDir, tenant, "format", 0)
	if err != nil {
		t.Fatal(err)
	}
	hasJSON := false
	for _, v := range idxVals {
		if v == "json" {
			hasJSON = true
		}
	}
	if !hasJSON {
		t.Fatalf("index after json land = %v, want json", idxVals)
	}
}

func TestLogsQuerySandboxThreadsIndependentOfMergeThreads(t *testing.T) {
	InvalidateLogsMetaCache("")
	dataDir := t.TempDir()
	tenant := gateTenant
	tenantRoot := filepath.Join(dataDir, tenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	seed := func(n int) {
		for i := 0; i < n; i++ {
			testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(time.Unix(int64(i+1), 0))), []testparquet.LogRow{
				{Message: "x", Format: "none"},
			})
		}
		_ = logmeta.Bump(dataDir, tenant)
	}

	ctx := context.Background()
	seed(10)
	conn, cleanup, err := openLokiSandbox(ctx, tenantRoot, sandboxLimits{Threads: 4}, 0, 0, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn
	cleanup()
	if got := AppliedSandboxThreadsForTest(); got != 4 {
		t.Fatalf("small open set threads = %d, want 4", got)
	}

	seed(600)
	conn2, cleanup2, err := openLokiSandbox(ctx, tenantRoot, sandboxLimits{Threads: 4}, 0, 0, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn2
	cleanup2()
	if got := AppliedSandboxThreadsForTest(); got != 1 {
		t.Fatalf("large open set threads = %d, want 1 fallback", got)
	}

	x, err := merge.NewExecutor(merge.ExecutorConfig{
		DataDir: dataDir, Tenant: tenant, Threads: 1, RowGroupSize: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	var threads string
	if err := x.DB().QueryRowContext(ctx, "SELECT current_setting('threads')").Scan(&threads); err != nil {
		t.Fatal(err)
	}
	if threads != "1" {
		t.Fatalf("merge executor threads = %q, want 1 independent of query", threads)
	}
}

func TestLogsManifestUpdatedOnLandAndReadByPlanner(t *testing.T) {
	InvalidateLogsMetaCache("")
	dataDir := t.TempDir()
	tenant := gateTenant
	tenantRoot := absTenantRoot(t, dataDir, tenant)
	eng := engine.New(engine.Config{DataDir: dataDir}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	land := func(msg string) {
		t.Helper()
		tmp := filepath.Join(t.TempDir(), "w.parquet")
		testparquet.WriteLogsRawFile(t, tmp, []testparquet.LogRow{{Message: msg, Format: "none"}})
		b, err := os.ReadFile(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.LandLogWindow(tenant, "logs-raw", bytes.NewReader(b)); err != nil {
			t.Fatal(err)
		}
	}

	land("one")
	mpath := logmeta.ManifestPath(dataDir, tenant, "logs-raw")
	if _, err := os.Stat(mpath); err != nil {
		t.Fatalf("manifest missing after first land: %v", err)
	}
	m1, err := logmeta.ReadManifest(dataDir, tenant, "logs-raw")
	if err != nil || len(m1.Files) != 1 {
		t.Fatalf("manifest after first land = %+v err=%v", m1, err)
	}

	land("two")
	m2, err := logmeta.ReadManifest(dataDir, tenant, "logs-raw")
	if err != nil || len(m2.Files) != 2 {
		t.Fatalf("manifest after second land = %+v err=%v", m2, err)
	}

	_, buffered, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(buffered) != 0 {
		t.Fatalf("planner open set = %d, want 0 while both windows are still buffered", len(buffered))
	}

	testparquet.PromoteLandedLogsToTier(t, dataDir, tenant, "logs-raw")
	m3, err := logmeta.ReadManifest(dataDir, tenant, "logs-raw")
	if err != nil || len(m3.Files) != 2 {
		t.Fatalf("manifest after refresh = %+v err=%v", m3, err)
	}
	_, openSet, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(openSet) != len(m3.Files) {
		t.Fatalf("planner open set = %d, manifest files = %d", len(openSet), len(m3.Files))
	}

	if err := os.Remove(mpath); err != nil {
		t.Fatal(err)
	}
	InvalidateLogsMetaCache(tenantRoot)
	_, rebuilt, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rebuilt) != 2 {
		t.Fatalf("after manifest delete rebuild open set = %d, want 2", len(rebuilt))
	}

	if err := os.WriteFile(mpath, []byte("{not-json"), 0o640); err != nil {
		t.Fatal(err)
	}
	InvalidateLogsMetaCache(tenantRoot)
	_, corrupt, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatalf("corrupt manifest should fall back to disk walk: %v", err)
	}
	if len(corrupt) != 2 {
		t.Fatalf("corrupt manifest open set = %d, want 2 from disk", len(corrupt))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func fileBases(files []logFileMeta) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = filepath.Base(f.Path)
	}
	return out
}

func absTenantRoot(t *testing.T, dataDir, tenant string) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join(dataDir, tenant))
	if err != nil {
		t.Fatal(err)
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	return root
}

func TestSandboxLogsSkipsDeletedCachedParquet(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, gateTenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dir, layout.SegmentName(time.Unix(1, 0)))
	gone := filepath.Join(dir, layout.SegmentName(time.Unix(2, 0)))
	testparquet.WriteLogsRawFile(t, keep, []testparquet.LogRow{{Message: "keep", Format: "none"}})
	testparquet.WriteLogsRawFile(t, gone, []testparquet.LogRow{{Message: "gone", Format: "none"}})

	if _, files, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{}); err != nil {
		t.Fatal(err)
	} else if len(files) != 2 {
		t.Fatalf("primed open set = %d, want 2", len(files))
	}
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	sqlText, files, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatalf("stale cache must not fail relation build: %v", err)
	}
	if len(files) != 1 || filepath.Base(files[0].Path) != filepath.Base(keep) {
		t.Fatalf("open set after delete = %+v, want only %s", files, keep)
	}
	if strings.Contains(sqlText, filepath.Base(gone)) {
		t.Fatalf("SQL still references deleted file: %s", truncate(sqlText, 240))
	}
}

func TestPrepareMetricsSandboxIgnoresStaleLogCache(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenant := gateTenant
	tenantRoot := filepath.Join(root, tenant)
	metricsPath := filepath.Join(tenantRoot, "tiers", "L0", "seg.parquet")
	if err := os.MkdirAll(filepath.Dir(metricsPath), 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteSegmentRows(t, metricsPath, []testparquet.SegRow{
		{Name: "up", Labels: `job="api"`, Value: 1, Ts: time.Unix(1, 0).UTC()},
	})
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	gone := filepath.Join(dir, layout.SegmentName(time.Unix(2, 0)))
	testparquet.WriteLogsRawFile(t, gone, []testparquet.LogRow{{Message: "x", Format: "none"}})
	if _, _, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	conn, cleanup, err := prepareMetricsSandboxConn(ctx, tenantRoot, false, sandboxLimits{})
	if err != nil {
		t.Fatalf("metrics-only sandbox: %v", err)
	}
	defer cleanup()
	var n int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM "+sandboxMetricsView).Scan(&n); err != nil {
		t.Fatalf("metrics query: %v", err)
	}
	if n < 1 {
		t.Fatalf("metrics rows = %d, want >= 1", n)
	}

	conn2, cleanup2, err := prepareSandboxConn(ctx, tenantRoot, false, sandboxLimits{})
	if err != nil {
		t.Fatalf("full sandbox with stale log path: %v", err)
	}
	cleanup2()
	_ = conn2
}
