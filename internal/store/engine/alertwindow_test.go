package engine

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLandAlertWindowEmptyIsNoop(t *testing.T) {
	dir, e := newLogEngine(t)
	n, err := e.LandAlertWindow("team-a", "alert-events", strings.NewReader(""))
	if err != nil || n != 0 {
		t.Fatalf("empty land = (%d, %v), want (0, nil)", n, err)
	}
	glob := filepath.Join(dir, "team-a", "alerts", "alert-events", "*.parquet")
	if m, _ := filepath.Glob(glob); len(m) != 0 {
		t.Fatalf("empty land wrote files: %v", m)
	}
}

func TestLandAlertWindowWritesFile(t *testing.T) {
	dir, e := newLogEngine(t)
	payload := []byte("PAR1-alert-events-window")
	n, err := e.LandAlertWindow("team-a", "alert-events", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("land: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("bytes = %d, want %d", n, len(payload))
	}
	glob := filepath.Join(dir, "team-a", "alerts", "alert-events", "*.parquet")
	m, _ := filepath.Glob(glob)
	if len(m) != 1 {
		t.Fatalf("want exactly 1 landed file under alerts/, got %v", m)
	}
	got, err := os.ReadFile(m[0])
	if err != nil {
		t.Fatalf("read landed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("landed content = %q, want %q", got, payload)
	}
	logsGlob := filepath.Join(dir, "team-a", "logs", "**", "*")
	if m, _ := filepath.Glob(logsGlob); len(m) != 0 {
		t.Fatalf("alert land must not write under logs/: %v", m)
	}
}

func TestLandAlertWindowDoesNotTouchHotCatalog(t *testing.T) {
	_, e := newLogEngine(t)
	if _, err := e.LandAlertWindow("team-a", "alert-events", bytes.NewReader([]byte("PAR1-x"))); err != nil {
		t.Fatalf("land: %v", err)
	}
	c, err := e.HotRowCount("team-a")
	if err != nil {
		t.Fatalf("HotRowCount: %v", err)
	}
	if c != 0 {
		t.Fatalf("hot rows = %d, want 0 (alerts must not insert into metrics hot)", c)
	}
}

func TestLandAlertWindowRejectsNonAlertArtifact(t *testing.T) {
	_, e := newLogEngine(t)
	for _, a := range []string{"metrics-raw", "logs-raw", "alert-events/../evil", "alerts", "alert_events"} {
		if _, err := e.LandAlertWindow("team-a", a, strings.NewReader("x")); err == nil {
			t.Errorf("artifact %q: want error, got nil", a)
		}
	}
}

func TestLandAlertWindowInvalidTenant(t *testing.T) {
	_, e := newLogEngine(t)
	if _, err := e.LandAlertWindow("BAD TENANT", "alert-events", strings.NewReader("x")); err == nil {
		t.Fatal("invalid tenant: want error, got nil")
	}
}

func TestLandAlertWindowClientAbort(t *testing.T) {
	_, e := newLogEngine(t)
	_, err := e.LandAlertWindow("team-a", "alert-events", errReader{err: io.ErrUnexpectedEOF})
	if !errors.Is(err, ErrClientAbort) {
		t.Fatalf("err = %v, want ErrClientAbort", err)
	}
}

func TestLandAlertWindowUsesClockForName(t *testing.T) {
	dir := t.TempDir()
	fixed := time.Unix(1_700_000_000, 0).UTC()
	e := New(Config{DataDir: dir, HotWindow: time.Hour}, func() time.Time { return fixed })
	t.Cleanup(func() { _ = e.Close() })
	if _, err := e.LandAlertWindow("team-a", "alert-events", bytes.NewReader([]byte("PAR1-named"))); err != nil {
		t.Fatalf("land: %v", err)
	}
	m, _ := filepath.Glob(filepath.Join(dir, "team-a", "alerts", "alert-events", "*.parquet"))
	if len(m) != 1 {
		t.Fatalf("landed = %v, want 1", m)
	}
	base := filepath.Base(m[0])
	if !strings.HasPrefix(base, "1700000000000000000-") || !strings.HasSuffix(base, ".parquet") {
		t.Fatalf("name = %q, want unix-nano prefix and .parquet suffix", base)
	}
}
