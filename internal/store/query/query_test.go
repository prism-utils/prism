package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/lifecycle"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const testTenant = "user-6f3a9c2b-apps"
const otherTenant = "user-99999999-apps"

func TestToJSONExposeSQL(t *testing.T) {
	payload, err := ToJSON([]Row{{Metric: "up", Value: 1}}, true, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"sql"`) {
		t.Fatalf("expected sql field in payload: %s", payload)
	}
	if strings.Contains(string(payload), "union_by_name") {
		t.Fatal("fixture sql should not leak union_by_name")
	}
}

func TestToJSONOmitsSQLByDefault(t *testing.T) {
	payload, err := ToJSON([]Row{{Metric: "up", Value: 1}}, false, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), `"sql"`) {
		t.Fatalf("sql must be omitted when exposeSQL is false: %s", payload)
	}
}

func TestBuildSQLNoUnionByNameOrFilename(t *testing.T) {
	dataDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dataDir, testTenant, "tiers", "L0"), 0o750)
	b := Builder{DataDir: dataDir}
	start := time.Unix(1700000000, 0).UTC()
	sqlText, _, err := b.BuildSQL(&Request{Tenant: testTenant, Start: start, End: start.Add(time.Hour)})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !AssertNoUnionByName(sqlText) {
		t.Fatalf("SQL must not contain union_by_name or filename: %s", sqlText)
	}
	if !strings.Contains(sqlText, "ts >=") {
		t.Fatalf("SQL must filter on ts: %s", sqlText)
	}
	if !strings.Contains(sqlText, "ORDER BY ts") {
		t.Fatalf("SQL must order by ts: %s", sqlText)
	}
}

func TestPickRollupStep(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	cases := []struct {
		step string
		end  time.Time
		want string
	}{
		{"5m", start.Add(48 * time.Hour), "5m"},
		{"", start.Add(8 * 24 * time.Hour), "1h"},
		{"", start.Add(25 * time.Hour), "5m"},
		{"", start.Add(2 * time.Hour), "1m"},
		{"", start.Add(30 * time.Minute), ""},
	}
	for _, tc := range cases {
		got := pickRollupStep(tc.step, start, tc.end)
		if got != tc.want {
			t.Fatalf("pickRollupStep(%q, %v): got %q want %q", tc.step, tc.end.Sub(start), got, tc.want)
		}
	}
}

func TestBuildSQLHotOnlyOmitsParquetAndRollups(t *testing.T) {
	dataDir := t.TempDir()
	tenantRoot := filepath.Join(dataDir, testTenant)
	if err := os.MkdirAll(filepath.Join(tenantRoot, "tiers", "L0"), 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteSegmentWithTs(t, filepath.Join(tenantRoot, "tiers", "L0", "l0.parquet"),
		time.Unix(1700000000, 0).UTC(), "tier", 1)
	rollupDir := layout.RollupDir(dataDir, testTenant, "5m")
	if err := os.MkdirAll(rollupDir, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteRollupBucket(t, filepath.Join(rollupDir, "r.parquet"),
		time.Unix(1700000000, 0).UTC(), "up", 1)

	b := Builder{DataDir: dataDir, HotOnly: true}
	start := time.Unix(1700000000, 0).UTC()
	end := start.Add(48 * time.Hour)
	sqlText, args, err := b.BuildSQL(&Request{Tenant: testTenant, Start: start, End: end})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !strings.Contains(sqlText, hotCurrentTable) || !strings.Contains(sqlText, hotPrevTable) {
		t.Fatalf("hot-only SQL must include hot tables: %s", sqlText)
	}
	if strings.Contains(sqlText, "read_parquet") {
		t.Fatalf("hot-only SQL must not read parquet: %s", sqlText)
	}
	if strings.Contains(sqlText, "rollups") {
		t.Fatalf("hot-only SQL must not include rollups path: %s", sqlText)
	}
	if len(args) != 4 {
		t.Fatalf("hot-only with hot_current+hot_prev wants 4 args, got %d", len(args))
	}
}

func TestBuildSQLHotOnlyFalseIncludesTiersAndRollups(t *testing.T) {
	dataDir := t.TempDir()
	tenantRoot := filepath.Join(dataDir, testTenant)
	if err := os.MkdirAll(filepath.Join(tenantRoot, "tiers", "L0"), 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteSegmentWithTs(t, filepath.Join(tenantRoot, "tiers", "L0", "l0.parquet"),
		time.Unix(1700000000, 0).UTC(), "tier", 1)
	rollupDir := layout.RollupDir(dataDir, testTenant, "5m")
	if err := os.MkdirAll(rollupDir, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteRollupBucket(t, filepath.Join(rollupDir, "r.parquet"),
		time.Unix(1700000000, 0).UTC(), "up", 1)

	b := Builder{DataDir: dataDir}
	start := time.Unix(1700000000, 0).UTC()
	end := start.Add(48 * time.Hour)
	sqlText, args, err := b.BuildSQL(&Request{Tenant: testTenant, Start: start, End: end})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !strings.Contains(sqlText, "read_parquet") || !strings.Contains(sqlText, "rollups/5m") {
		t.Fatalf("full SQL must include tiers and rollups: %s", sqlText)
	}
	if len(args) < 8 {
		t.Fatalf("full union wants at least 8 args (4 parts), got %d", len(args))
	}
}

func TestQueryHotOnlyReturnsOnlyHotRows(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "hot_metric", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := eng.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest hot: %v", err)
	}

	l0Path := layout.TierDir(dataDir, testTenant, 0)
	_ = os.MkdirAll(l0Path, 0o750)
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0Path, "l0.parquet"), start.Add(30*time.Minute), "l0_metric", 2)

	fullQB := Builder{DataDir: dataDir}
	hotQB := Builder{DataDir: dataDir, HotOnly: true}
	req := &Request{Tenant: testTenant, Start: start, End: start.Add(2 * time.Hour)}

	var hotRows, fullRows []Row
	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		hotSQL, hotArgs, err := hotQB.BuildSQLWithDB(context.Background(), req, db)
		if err != nil {
			return err
		}
		if len(hotArgs) != 4 {
			return fmt.Errorf("hot-only args: got %d want 4", len(hotArgs))
		}
		hotRows, err = Execute(context.Background(), db, hotSQL, hotArgs)
		if err != nil {
			return err
		}

		fullSQL, fullArgs, err := fullQB.BuildSQLWithDB(context.Background(), req, db)
		if err != nil {
			return err
		}
		if len(fullArgs) <= len(hotArgs) {
			return fmt.Errorf("full args %d must exceed hot-only %d", len(fullArgs), len(hotArgs))
		}
		fullRows, err = Execute(context.Background(), db, fullSQL, fullArgs)
		return err
	}); err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(hotRows) != 1 || hotRows[0].Metric != "hot_metric" {
		t.Fatalf("hot-only want hot_metric only, got %+v", hotRows)
	}
	if len(fullRows) < 2 {
		t.Fatalf("full mode want hot+L0 union, got %d rows", len(fullRows))
	}
	names := map[string]bool{}
	for _, r := range fullRows {
		names[r.Metric] = true
	}
	if !names["hot_metric"] || !names["l0_metric"] {
		t.Fatalf("full mode missing metrics: %v", names)
	}
}

func TestAggregateSQLRunsOverHotOnly(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "a", Labels: "{}", Value: 10, TimestampMs: 0},
		{Name: "b", Labels: "{}", Value: 20, TimestampMs: 0},
	})
	if _, err := eng.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	l0File := filepath.Join(layout.TierDir(dataDir, testTenant, 0), "l0.parquet")
	testparquet.WriteSegmentWithTs(t, l0File, start.Add(30*time.Minute), "tier_only", 99)

	b := Builder{DataDir: dataDir, HotOnly: true}
	sqlText, args, err := b.BuildSQLWithDB(context.Background(), &Request{
		Tenant: testTenant,
		Start:  start,
		End:    start.Add(time.Hour),
	}, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	aggSQL, err := AggregateSQL(sqlText)
	if err != nil {
		t.Fatalf("aggregate sql: %v", err)
	}

	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		var cnt int64
		var sum sql.NullFloat64
		if err := db.QueryRowContext(context.Background(), aggSQL, args...).Scan(&cnt, &sum); err != nil {
			return err
		}
		if cnt != 2 {
			return fmt.Errorf("hot-only aggregate count = %d want 2", cnt)
		}
		if sum.Float64 != 30 {
			return fmt.Errorf("hot-only aggregate sum = %v want 30", sum.Float64)
		}
		return nil
	}); err != nil {
		t.Fatalf("aggregate execute: %v", err)
	}
}

func TestBuildSQLIncludesRollupForWideRange(t *testing.T) {
	dataDir := t.TempDir()
	rollupDir := layout.RollupDir(dataDir, testTenant, "5m")
	if err := os.MkdirAll(rollupDir, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteRollupBucket(t, filepath.Join(rollupDir, "r.parquet"),
		time.Unix(1700000000, 0).UTC(), "up", 1)

	b := Builder{DataDir: dataDir}
	start := time.Unix(1700000000, 0).UTC()
	sqlText, args, err := b.BuildSQL(&Request{
		Tenant: testTenant,
		Start:  start,
		End:    start.Add(48 * time.Hour),
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !strings.Contains(sqlText, "rollups/5m") {
		t.Fatalf("expected 5m rollup in SQL: %s", sqlText)
	}
	if !strings.Contains(sqlText, "bucket >=") {
		t.Fatalf("rollup part must filter bucket: %s", sqlText)
	}
	if len(args) < 4 {
		t.Fatalf("want bound args for each union part, got %d", len(args))
	}
}

func TestAggregateSQLRunsOverUnionWithRollup(t *testing.T) {
	dataDir := t.TempDir()
	rollupDir := layout.RollupDir(dataDir, testTenant, "1m")
	if err := os.MkdirAll(rollupDir, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteRollupBucket(t, filepath.Join(rollupDir, "r.parquet"),
		time.Unix(1700000000, 0).UTC(), "up", 1)

	b := Builder{DataDir: dataDir}
	start := time.Unix(1700000000, 0).UTC()
	sqlText, args, err := b.BuildSQL(&Request{
		Tenant: testTenant,
		Start:  start,
		End:    start.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	aggSQL, err := AggregateSQL(sqlText)
	if err != nil {
		t.Fatalf("aggregate sql: %v", err)
	}
	if strings.Contains(aggSQL, "ORDER BY") {
		t.Fatalf("aggregate must not retain ORDER BY: %s", aggSQL)
	}

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		var cnt int64
		var sum sql.NullFloat64
		return db.QueryRowContext(context.Background(), aggSQL, args...).Scan(&cnt, &sum)
	}); err != nil {
		t.Fatalf("aggregate execute: %v", err)
	}
}

func TestQueryRangeSpansHotAndTiers(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	now := start
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "hot_metric", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	f, _ := os.Open(path)
	if _, err := eng.Ingest(testTenant, f); err != nil {
		t.Fatalf("ingest hot: %v", err)
	}
	_ = f.Close()

	l0Path := layout.TierDir(dataDir, testTenant, 0)
	_ = os.MkdirAll(l0Path, 0o750)
	l0File := filepath.Join(l0Path, "l0.parquet")
	testparquet.WriteSegmentWithTs(t, l0File, start.Add(30*time.Minute), "l0_metric", 2)

	var rows []Row
	err := eng.WithRead(testTenant, func(db *sql.DB) error {
		qb := Builder{DataDir: dataDir}
		sqlText, args, err := qb.BuildSQLWithDB(context.Background(), &Request{
			Tenant: testTenant,
			Start:  start,
			End:    start.Add(2 * time.Hour),
		}, db)
		if err != nil {
			return err
		}
		rows, err = Execute(context.Background(), db, sqlText, args)
		return err
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("want rows from hot+L0, got %d", len(rows))
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[r.Metric] = true
	}
	if !names["hot_metric"] || !names["l0_metric"] {
		t.Fatalf("missing expected metrics: %v", names)
	}
}

func TestQueryFreshness(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "fresh", Labels: "{}", Value: 42, TimestampMs: 0},
	})
	if _, err := eng.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	var rows []Row
	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		qb := Builder{DataDir: dataDir}
		sqlText, args, err := qb.BuildSQLWithDB(context.Background(), &Request{
			Tenant: testTenant,
			Start:  start.Add(-time.Minute),
			End:    start.Add(time.Minute),
		}, db)
		if err != nil {
			return err
		}
		rows, err = Execute(context.Background(), db, sqlText, args)
		return err
	}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 || rows[0].Metric != "fresh" {
		t.Fatalf("fresh row not visible: %+v", rows)
	}
}

func TestQueryCrossTierGapFree(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "hot", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := eng.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	l0File := filepath.Join(layout.TierDir(dataDir, testTenant, 0), "l0.parquet")
	testparquet.WriteSegmentWithTs(t, l0File, start.Add(30*time.Minute), "l0", 2)

	l1File := filepath.Join(layout.TierDir(dataDir, testTenant, 1), "l1.parquet")
	testparquet.WriteSegmentWithTs(t, l1File, start.Add(time.Hour), "l1", 3)

	var rows []Row
	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		qb := Builder{DataDir: dataDir}
		sqlText, args, err := qb.BuildSQLWithDB(context.Background(), &Request{
			Tenant: testTenant,
			Start:  start.Add(-time.Minute),
			End:    start.Add(2 * time.Hour),
		}, db)
		if err != nil {
			return err
		}
		rows, err = Execute(context.Background(), db, sqlText, args)
		return err
	}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 rows across hot+L0+L1, got %d", len(rows))
	}
	want := []string{"hot", "l0", "l1"}
	for i, r := range rows {
		if r.Metric != want[i] {
			t.Fatalf("row %d: got metric %q want %q", i, r.Metric, want[i])
		}
		if i > 0 && rows[i].Ts.Before(rows[i-1].Ts) {
			t.Fatalf("rows not ordered by ts")
		}
	}
}

func TestQueryRetentionExpiredRangeEmpty(t *testing.T) {
	dataDir := t.TempDir()
	tenant := testTenant
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	expired := filepath.Join(layout.TierDir(dataDir, tenant, 0), "old.parquet")
	testparquet.WriteSegmentWithTs(t, expired, now.Add(-16*24*time.Hour), "old", 1)

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })

	runner := lifecycle.NewRunner(&lifecycle.Config{
		DataDir:       dataDir,
		RetentionDays: 15,
		MaxTier:       8,
	}, eng, func() time.Time { return now })
	if err := runner.TickRetention(); err != nil {
		t.Fatalf("retention: %v", err)
	}

	var rows []Row
	if err := eng.WithRead(tenant, func(db *sql.DB) error {
		qb := Builder{DataDir: dataDir}
		sqlText, args, err := qb.BuildSQLWithDB(context.Background(), &Request{
			Tenant: tenant,
			Start:  now.Add(-17 * 24 * time.Hour),
			End:    now.Add(-16 * 24 * time.Hour),
		}, db)
		if err != nil {
			return err
		}
		rows, err = Execute(context.Background(), db, sqlText, args)
		return err
	}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expired range should return no rows, got %d", len(rows))
	}
}

func TestQueryTenantIsolation(t *testing.T) {
	dataDir := t.TempDir()
	for _, ns := range []string{testTenant, otherTenant} {
		_ = os.MkdirAll(filepath.Join(dataDir, ns, "tiers", "L0"), 0o750)
	}
	aFile := filepath.Join(dataDir, testTenant, "tiers", "L0", "a.parquet")
	bFile := filepath.Join(dataDir, otherTenant, "tiers", "L0", "b.parquet")
	start := time.Unix(1700000000, 0).UTC()
	testparquet.WriteSegmentWithTs(t, aFile, start, "metric_a", 1)
	testparquet.WriteSegmentWithTs(t, bFile, start, "metric_b", 2)

	b := Builder{DataDir: dataDir}
	sqlA, _, err := b.BuildSQL(&Request{Tenant: testTenant, Start: start.Add(-time.Hour), End: start.Add(time.Hour)})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if strings.Contains(sqlA, otherTenant) {
		t.Fatalf("tenant A query must not reference tenant B paths")
	}
	if !strings.Contains(sqlA, testTenant) {
		t.Fatalf("tenant A query must scope to tenant A")
	}

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	var rows []Row
	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		runSQL, args, err := b.BuildSQLWithDB(context.Background(), &Request{Tenant: testTenant, Start: start.Add(-time.Hour), End: start.Add(time.Hour)}, db)
		if err != nil {
			return err
		}
		var execErr error
		rows, execErr = Execute(context.Background(), db, runSQL, args)
		return execErr
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	for _, r := range rows {
		if r.Metric == "metric_b" {
			t.Fatal("tenant A query returned tenant B data")
		}
	}
}

func TestQueryEmptyRangeReturnsEmptyRows(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	var rows []Row
	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		qb := Builder{DataDir: dataDir}
		sqlText, args, err := qb.BuildSQLWithDB(context.Background(), &Request{
			Tenant: testTenant,
			Start:  start,
			End:    start.Add(time.Hour),
		}, db)
		if err != nil {
			return err
		}
		rows, err = Execute(context.Background(), db, sqlText, args)
		return err
	}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if rows == nil || len(rows) != 0 {
		t.Fatalf("want empty slice, got %v", rows)
	}
}

func TestViewSQLNoUnionByName(t *testing.T) {
	dataDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dataDir, testTenant, "tiers", "L0"), 0o750)
	testparquet.WriteSegmentWithTs(t, filepath.Join(dataDir, testTenant, "tiers", "L0", "a.parquet"),
		time.Unix(1700000000, 0).UTC(), "m", 1)

	sqlText, err := ViewSQL(dataDir, testTenant)
	if err != nil {
		t.Fatalf("ViewSQL: %v", err)
	}
	if !AssertNoUnionByName(sqlText) {
		t.Fatalf("view SQL must not use union_by_name or filename: %s", sqlText)
	}
	if !strings.Contains(sqlText, testTenant) {
		t.Fatalf("view SQL must be tenant-scoped: %s", sqlText)
	}
	if !strings.Contains(sqlText, "CREATE OR REPLACE VIEW") {
		t.Fatalf("expected CREATE VIEW: %s", sqlText)
	}
	if !strings.Contains(sqlText, `"__name__"`) {
		t.Fatalf("view must project row shape: %s", sqlText)
	}
}

func TestViewSQLIncludesHotSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	hotDir := filepath.Join(dataDir, testTenant, "hot")
	if err := os.MkdirAll(hotDir, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteSegmentWithTs(t, filepath.Join(hotDir, "current.parquet"),
		time.Unix(1700000000, 0).UTC(), "snap", 1)

	sqlText, err := ViewSQL(dataDir, testTenant)
	if err != nil {
		t.Fatalf("ViewSQL: %v", err)
	}
	if !strings.Contains(sqlText, "hot/current.parquet") {
		t.Fatalf("view must include hot snapshot: %s", sqlText)
	}
}

func readFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

var _ *sql.DB
