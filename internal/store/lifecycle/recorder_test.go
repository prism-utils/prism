package lifecycle

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const recorderTenant = "user-6f3a9c2b-apps"

type tickRecord struct {
	job string
	err error
}

type landingRecord struct {
	tenant   string
	artifact string
	files    int
}

type spyRecorder struct {
	mu          sync.Mutex
	ticks       []tickRecord
	segments    map[string]int
	landing     []landingRecord
	compactions map[string]float64
}

func newSpyRecorder() *spyRecorder {
	return &spyRecorder{segments: map[string]int{}, compactions: map[string]float64{}}
}

func (s *spyRecorder) ObserveTick(job string, _ time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ticks = append(s.ticks, tickRecord{job: job, err: err})
}

func (s *spyRecorder) ObserveTierSegments(tenant string, files int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segments[tenant] = files
}

func (s *spyRecorder) ObserveLogLandingFiles(tenant, artifact string, files int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.landing = append(s.landing, landingRecord{tenant: tenant, artifact: artifact, files: files})
}

func (s *spyRecorder) ObserveCompactionSeconds(tenant string, seconds float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compactions[tenant] += seconds
}

func (s *spyRecorder) jobs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.ticks))
	for _, t := range s.ticks {
		out = append(out, t.job)
	}
	return out
}

func (s *spyRecorder) lastLanding(artifact string) (landingRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.landing) - 1; i >= 0; i-- {
		if s.landing[i].artifact == artifact {
			return s.landing[i], true
		}
	}
	return landingRecord{}, false
}

func TestEveryTickReportsItsJobToTheRecorder(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	eng := engine.New(engine.Config{DataDir: dataDir}, clock)
	t.Cleanup(func() { _ = eng.Close() })

	rec := newSpyRecorder()
	runner := NewRunner(&Config{DataDir: dataDir, MaxTier: 8, RetentionDays: 15, Recorder: rec}, eng, clock)

	for _, tick := range []func() error{runner.TickHotSnapshot, runner.TickFlush, runner.TickMerge, runner.TickRetention} {
		if err := tick(); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}

	want := []string{JobHotSnapshot, JobFlush, JobMerge, JobRetention}
	got := rec.jobs()
	if len(got) != len(want) {
		t.Fatalf("observed jobs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("observed jobs = %v, want %v", got, want)
		}
	}
}

func TestTickRecorderCarriesTheTickError(t *testing.T) {
	rec := newSpyRecorder()
	// A data dir that is a regular file makes the tenant listing fail, which is
	// the error path an operator alerts on.
	dataDir := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(dataDir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now
	eng := engine.New(engine.Config{DataDir: dataDir}, now)
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{DataDir: dataDir, MaxTier: 8, Recorder: rec}, eng, now)

	if err := runner.TickMerge(); err == nil {
		t.Fatal("TickMerge on a non-directory data dir returned nil error")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.ticks) != 1 {
		t.Fatalf("tick observations = %d, want 1", len(rec.ticks))
	}
	if rec.ticks[0].err == nil {
		t.Fatal("recorder saw a nil error for a failed tick")
	}
}

func TestRetentionTickReportsLandingFileCountAfterDeletes(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, recorderTenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	const maxFiles = 3
	for i := 0; i < maxFiles+4; i++ {
		at := now.Add(-time.Duration(i) * time.Minute)
		testparquet.WriteLogsRawFile(t, filepath.Join(landing, layout.SegmentName(at)), []testparquet.LogRow{
			{Message: "hot", Format: "none"},
		})
	}

	clock := func() time.Time { return now }
	eng := engine.New(engine.Config{DataDir: dataDir}, clock)
	t.Cleanup(func() { _ = eng.Close() })

	rec := newSpyRecorder()
	runner := NewRunner(&Config{
		DataDir:       dataDir,
		RetentionDays: 15,
		MaxLogFiles:   maxFiles,
		MaxTier:       8,
		Recorder:      rec,
	}, eng, clock)

	if err := runner.TickRetention(); err != nil {
		t.Fatalf("TickRetention: %v", err)
	}

	got, ok := rec.lastLanding(artifact)
	if !ok {
		t.Fatal("retention tick reported no landing file count")
	}
	if got.tenant != recorderTenant {
		t.Fatalf("landing tenant = %q, want %q", got.tenant, recorderTenant)
	}
	if got.files > maxFiles {
		t.Fatalf("reported landing files = %d, want <= the cap %d", got.files, maxFiles)
	}
	onDisk, err := countDirParquet(landing)
	if err != nil {
		t.Fatal(err)
	}
	if got.files != onDisk {
		t.Fatalf("reported landing files = %d, on disk = %d", got.files, onDisk)
	}
}

func TestRunnerWithoutRecorderStillTicks(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Now
	eng := engine.New(engine.Config{DataDir: dataDir}, now)
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{DataDir: dataDir, MaxTier: 8}, eng, now)

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge without a recorder: %v", err)
	}
}
