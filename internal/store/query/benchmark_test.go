package query

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/lifecycle"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

func BenchmarkQueryAggregateCompactedTenant(b *testing.B) {
	dataDir := b.TempDir()
	tenant := testTenant
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	hotWindow := time.Minute

	now := start
	autoAdvance := false
	clock := func() time.Time {
		if autoAdvance {
			now = now.Add(time.Second)
		}
		return now
	}

	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: hotWindow}, clock)
	b.Cleanup(func() { _ = eng.Close() })

	runner := lifecycle.NewRunner(lifecycle.Config{
		DataDir:         dataDir,
		SegmentsPerTier: 6,
		MaxSegmentBytes: 1 << 30,
		FloorBytes:      lifecycle.FloorBytesFromHotWindow(hotWindow),
		RetentionDays:   15,
		RollupSteps:     "1m,5m,1h",
		MaxTier:         8,
	}, eng, clock)

	dir := b.TempDir()
	for i := 0; i < 6; i++ {
		path := testparquet.WriteWindow(b, dir, "w.parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: float64(i + 1), TimestampMs: 0},
		})
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := eng.Ingest(tenant, f); err != nil {
			_ = f.Close()
			b.Fatalf("ingest %d: %v", i, err)
		}
		_ = f.Close()
		now = now.Add(hotWindow)
		if err := runner.TickFlush(); err != nil {
			b.Fatalf("flush %d: %v", i, err)
		}
	}

	autoAdvance = true
	if err := runner.TickMerge(); err != nil {
		b.Fatalf("merge: %v", err)
	}

	l1Dir := layout.TierDir(dataDir, tenant, 1)
	entries, err := os.ReadDir(l1Dir)
	if err != nil || len(entries) != 1 {
		b.Fatalf("want compacted L1 segment, l1 err=%v entries=%d", err, len(entries))
	}

	qb := Builder{DataDir: dataDir}
	req := &Request{
		Tenant: tenant,
		Start:  start.Add(-time.Hour),
		End:    now.Add(time.Hour),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := eng.WithRead(tenant, func(db *sql.DB) error {
			runSQL, runArgs, buildErr := qb.BuildSQLWithDB(b.Context(), req, db)
			if buildErr != nil {
				return buildErr
			}
			aggSQL := strings.Replace(runSQL, "SELECT * FROM ", "SELECT COUNT(*), SUM(value) FROM ", 1)
			row := db.QueryRowContext(b.Context(), aggSQL, runArgs...)
			var cnt int64
			var sum sql.NullFloat64
			return row.Scan(&cnt, &sum)
		}); err != nil {
			b.Fatalf("aggregate query: %v", err)
		}
	}
}
