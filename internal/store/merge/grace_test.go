package merge

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const graceTenant = "user-grace00001-apps"

func writeSegmentFixture(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("segment\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustNotExist(t *testing.T, path, why string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s still present (%s), stat err = %v", path, why, err)
	}
}

func mustExist(t *testing.T, path, why string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("%s missing (%s): %v", path, why, err)
	}
}

// A reader resolves a path before it opens it, so a merge input that vanishes
// between those two moments fails the reader's whole query. Retiring holds the
// bytes where they are and only records that they are no longer live.
func TestRetireSourcesHoldsBytesAtOriginalPathForGrace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1786140844863329878-aaaaaaaa.parquet")
	writeSegmentFixture(t, path)
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	if err := retireSources([]Segment{{Path: path}}, now, 120*time.Second); err != nil {
		t.Fatalf("retireSources: %v", err)
	}

	mustExist(t, path, "held for the delete grace window")
	deadline, ok, err := readCompactedDeadline(layout.CompactedMarker(path))
	if err != nil {
		t.Fatalf("readCompactedDeadline: %v", err)
	}
	if !ok {
		t.Fatal("marker holds no deadline, want the retire instant plus the grace")
	}
	if want := now.Add(120 * time.Second); !deadline.Equal(want) {
		t.Fatalf("deadline = %s, want %s", deadline, want)
	}
}

func TestRetireSourcesWithoutGraceDeletesImmediately(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1786140844863329878-aaaaaaaa.parquet")
	writeSegmentFixture(t, path)
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	if err := retireSources([]Segment{{Path: path}}, now, 0); err != nil {
		t.Fatalf("retireSources: %v", err)
	}

	mustNotExist(t, path, "zero grace deletes on the spot")
	mustNotExist(t, layout.CompactedMarker(path), "no marker when nothing is held")
}

func TestRetireSourcesWithNegativeGraceDeletesImmediately(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1786140844863329878-aaaaaaaa.parquet")
	writeSegmentFixture(t, path)

	if err := retireSources([]Segment{{Path: path}}, time.Now(), -5*time.Second); err != nil {
		t.Fatalf("retireSources: %v", err)
	}
	mustNotExist(t, path, "a negative grace is not a grace")
}

func TestRetireSourcesToleratesAlreadyDeletedSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gone.parquet")

	if err := retireSources([]Segment{{Path: path}}, time.Now(), time.Minute); err != nil {
		t.Fatalf("retireSources on a missing source: %v", err)
	}
	mustNotExist(t, layout.CompactedMarker(path), "nothing to hold, so nothing to mark")
}

func TestPurgeCompactedDirKeepsSegmentUntilDeadline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1786140844863329878-aaaaaaaa.parquet")
	writeSegmentFixture(t, path)
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	if err := retireSources([]Segment{{Path: path}}, now, 120*time.Second); err != nil {
		t.Fatal(err)
	}

	// One second before the deadline, and exactly on it: still held. The window
	// is a promise to readers, so it is inclusive of its last instant.
	for _, at := range []time.Time{now.Add(119 * time.Second), now.Add(120 * time.Second)} {
		n, err := purgeCompactedDir(dir, at)
		if err != nil {
			t.Fatalf("purgeCompactedDir at %s: %v", at, err)
		}
		if n != 0 {
			t.Fatalf("purged %d segments at %s, want 0 before the deadline passes", n, at)
		}
		mustExist(t, path, "grace has not expired")
	}

	n, err := purgeCompactedDir(dir, now.Add(121*time.Second))
	if err != nil {
		t.Fatalf("purgeCompactedDir: %v", err)
	}
	if n != 1 {
		t.Fatalf("purged %d segments, want 1 once the deadline passed", n)
	}
	mustNotExist(t, path, "grace expired")
	mustNotExist(t, layout.CompactedMarker(path), "marker goes with its segment")
}

func TestPurgeCompactedDirReapsOrphanMarker(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1786140844863329878-aaaaaaaa.parquet")
	writeSegmentFixture(t, path)
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	if err := retireSources([]Segment{{Path: path}}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	if _, err := purgeCompactedDir(dir, now); err != nil {
		t.Fatalf("purgeCompactedDir: %v", err)
	}
	mustNotExist(t, layout.CompactedMarker(path), "a marker without a segment holds nothing")
}

// An unreadable deadline must not strand bytes forever: reclaiming space is the
// safe direction to fail, since the segment's rows already live in its parent.
func TestPurgeCompactedDirTreatsCorruptMarkerAsExpired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "1786140844863329878-aaaaaaaa.parquet")
	writeSegmentFixture(t, path)
	if err := os.WriteFile(layout.CompactedMarker(path), []byte("whenever"), 0o600); err != nil {
		t.Fatal(err)
	}

	n, err := purgeCompactedDir(dir, time.Now())
	if err != nil {
		t.Fatalf("purgeCompactedDir: %v", err)
	}
	if n != 1 {
		t.Fatalf("purged %d segments, want the unreadable one gone", n)
	}
	mustNotExist(t, path, "corrupt marker purges")
	mustNotExist(t, layout.CompactedMarker(path), "corrupt marker purges")
}

func TestPurgeCompactedDirRemovesAbandonedMarkerWrite(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, "1786140844863329878-aaaaaaaa.parquet.compacted.tmp")
	writeSegmentFixture(t, tmp)

	if _, err := purgeCompactedDir(dir, time.Now()); err != nil {
		t.Fatalf("purgeCompactedDir: %v", err)
	}
	mustNotExist(t, tmp, "a half-written marker is garbage on the next pass")
}

func TestPurgeCompactedDirOnMissingDir(t *testing.T) {
	n, err := purgeCompactedDir(filepath.Join(t.TempDir(), "nope"), time.Now())
	if err != nil {
		t.Fatalf("purgeCompactedDir on a missing directory: %v", err)
	}
	if n != 0 {
		t.Fatalf("purged %d, want 0", n)
	}
}

func TestScanLogTierSkipsRetiredSegments(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	tierDir := layout.LogsTierDir(dataDir, graceTenant, artifact, 0)
	live := filepath.Join(tierDir, "1786140844863329878-aaaaaaaa.parquet")
	retired := filepath.Join(tierDir, "1786140844863329879-bbbbbbbb.parquet")
	writeSegmentFixture(t, live)
	writeSegmentFixture(t, retired)
	if err := retireSources([]Segment{{Path: retired}}, time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}

	segs, err := ScanLogTier(dataDir, graceTenant, artifact, 0)
	if err != nil {
		t.Fatalf("ScanLogTier: %v", err)
	}
	if len(segs) != 1 || segs[0].Path != live {
		t.Fatalf("ScanLogTier = %v, want only the live segment %s", segs, live)
	}
}

func TestScanLogLandingSkipsRetiredSegments(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, graceTenant, artifact)
	live := filepath.Join(landing, "1786140844863329878-aaaaaaaa.parquet")
	retired := filepath.Join(landing, "1786140844863329879-bbbbbbbb.parquet")
	writeSegmentFixture(t, live)
	writeSegmentFixture(t, retired)
	if err := retireSources([]Segment{{Path: retired}}, time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}

	segs, err := ScanLogLanding(dataDir, graceTenant, artifact)
	if err != nil {
		t.Fatalf("ScanLogLanding: %v", err)
	}
	if len(segs) != 1 || segs[0].Path != live {
		t.Fatalf("ScanLogLanding = %v, want only the live segment %s", segs, live)
	}
}

func TestScanTierSkipsRetiredSegments(t *testing.T) {
	dataDir := t.TempDir()
	tierDir := layout.TierDir(dataDir, graceTenant, 0)
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	live := filepath.Join(tierDir, "live.parquet")
	retired := filepath.Join(tierDir, "retired.parquet")
	testparquet.WriteSegmentWithTs(t, live, base, "up", 1)
	testparquet.WriteSegmentWithTs(t, retired, base, "up", 2)
	if err := retireSources([]Segment{{Path: retired}}, base, time.Hour); err != nil {
		t.Fatal(err)
	}

	segs, err := ScanTier(dataDir, graceTenant, 0, DuckDBCaps{})
	if err != nil {
		t.Fatalf("ScanTier: %v", err)
	}
	if len(segs) != 1 || segs[0].Path != live {
		t.Fatalf("ScanTier = %v, want only the live segment %s", segs, live)
	}
}

func TestPurgeCompactedSweepsLogTiersLandingAndMetricTiers(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	held := map[string]string{
		"landing": filepath.Join(layout.LogsLandingDir(dataDir, graceTenant, artifact), "1786140844863329878-aaaaaaaa.parquet"),
		"logtier": filepath.Join(layout.LogsTierDir(dataDir, graceTenant, artifact, 0), "1786140844863329879-bbbbbbbb.parquet"),
		"metrics": filepath.Join(layout.TierDir(dataDir, graceTenant, 2), "1786140844863329880-cccccccc.parquet"),
	}
	var sources []Segment
	for _, p := range held {
		writeSegmentFixture(t, p)
		sources = append(sources, Segment{Path: p})
	}
	if err := retireSources(sources, now, 120*time.Second); err != nil {
		t.Fatal(err)
	}

	n, err := PurgeCompacted(dataDir, graceTenant, 8, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("PurgeCompacted: %v", err)
	}
	if n != 0 {
		t.Fatalf("purged %d within the grace window, want 0", n)
	}
	for where, p := range held {
		mustExist(t, p, "held in "+where)
	}

	n, err = PurgeCompacted(dataDir, graceTenant, 8, now.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("PurgeCompacted: %v", err)
	}
	if n != len(held) {
		t.Fatalf("purged %d expired segments, want %d", n, len(held))
	}
	for where, p := range held {
		mustNotExist(t, p, "expired in "+where)
		mustNotExist(t, layout.CompactedMarker(p), "marker cleared in "+where)
	}
}

func TestPurgeCompactedOnEmptyTenant(t *testing.T) {
	n, err := PurgeCompacted(t.TempDir(), graceTenant, 8, time.Now())
	if err != nil {
		t.Fatalf("PurgeCompacted on an empty tenant: %v", err)
	}
	if n != 0 {
		t.Fatalf("purged %d, want 0", n)
	}
}

// The merge output is published immediately, so the sources must stay readable
// where a reader already found them without ever being merge inputs again.
func TestExecuteLogMergeHoldsSourcesForGrace(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, graceTenant, artifact)
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	var sources []Segment
	for i := 0; i < 2; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		path := filepath.Join(landing, layout.SegmentName(at))
		testparquet.WriteLogsRawFile(t, path, []testparquet.LogRow{{Message: "line", Format: "none"}})
		seg, err := StatLogSegment(path, logLandingTier)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{
		DataDir:      dataDir,
		Tenant:       graceTenant,
		RowGroupSize: 1000,
		DeleteGrace:  120 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{Artifact: artifact, Sources: sources, DestTier: 0}, base)
	if err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}
	mustExist(t, out.Path, "merge output is published right away")

	for _, s := range sources {
		mustExist(t, s.Path, "source held for the grace window")
		mustExist(t, layout.CompactedMarker(s.Path), "held source carries a deadline")
	}

	live, err := ScanLogLanding(dataDir, graceTenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 0 {
		t.Fatalf("ScanLogLanding = %v, want held sources excluded from the next merge", live)
	}
}

func TestExecuteLogMergeWithoutGraceDeletesSources(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, graceTenant, artifact)
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	var sources []Segment
	for i := 0; i < 2; i++ {
		path := filepath.Join(landing, layout.SegmentName(base.Add(time.Duration(i)*time.Minute)))
		testparquet.WriteLogsRawFile(t, path, []testparquet.LogRow{{Message: "line", Format: "none"}})
		seg, err := StatLogSegment(path, logLandingTier)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: graceTenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	if _, err := x.ExecuteLogMerge(artifact, LogMergeAction{Artifact: artifact, Sources: sources, DestTier: 0}, base); err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}
	for _, s := range sources {
		mustNotExist(t, s.Path, "no grace configured")
	}
}

func TestExecuteMergeHoldsSourcesForGrace(t *testing.T) {
	dataDir := t.TempDir()
	tierDir := layout.TierDir(dataDir, graceTenant, 0)
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	var sources []Segment
	for i := 0; i < 2; i++ {
		path := filepath.Join(tierDir, layout.SegmentName(base.Add(time.Duration(i)*time.Minute)))
		testparquet.WriteSegmentWithTs(t, path, base.Add(time.Duration(i)*time.Minute), "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{
		DataDir:      dataDir,
		Tenant:       graceTenant,
		RowGroupSize: 1000,
		DeleteGrace:  120 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	if _, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, base); err != nil {
		t.Fatalf("ExecuteMerge: %v", err)
	}
	for _, s := range sources {
		mustExist(t, s.Path, "metrics source held for the grace window")
	}
	live, err := ScanTier(dataDir, graceTenant, 0, DuckDBCaps{})
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 0 {
		t.Fatalf("ScanTier = %v, want held sources excluded from the next merge", live)
	}
}
