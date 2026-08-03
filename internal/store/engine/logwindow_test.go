package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newLogEngine(t *testing.T) (string, *Engine) {
	t.Helper()
	dir := t.TempDir()
	e := New(Config{DataDir: dir, HotWindow: time.Hour}, nil)
	t.Cleanup(func() { _ = e.Close() })
	return dir, e
}

func TestLandLogWindowEmptyIsNoop(t *testing.T) {
	dir, e := newLogEngine(t)
	n, err := e.LandLogWindow("team-a", "logs-summary", strings.NewReader(""))
	if err != nil || n != 0 {
		t.Fatalf("empty land = (%d, %v), want (0, nil)", n, err)
	}
	glob := filepath.Join(dir, "team-a", "logs", "logs-summary", "*.parquet")
	if m, _ := filepath.Glob(glob); len(m) != 0 {
		t.Fatalf("empty land wrote files: %v", m)
	}
}

func TestLandLogWindowWritesFile(t *testing.T) {
	dir, e := newLogEngine(t)
	payload := []byte("PAR1-nonempty-window-bytes")
	n, err := e.LandLogWindow("team-a", "logs-summary", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("land: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("bytes = %d, want %d", n, len(payload))
	}
	glob := filepath.Join(dir, "team-a", "logs", "logs-summary", "*.parquet")
	m, _ := filepath.Glob(glob)
	if len(m) != 1 {
		t.Fatalf("want exactly 1 landed file, got %v", m)
	}
	got, err := os.ReadFile(m[0])
	if err != nil {
		t.Fatalf("read landed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("landed content = %q, want %q", got, payload)
	}
}

func TestLandLogWindowRejectsNonLogArtifact(t *testing.T) {
	_, e := newLogEngine(t)
	for _, a := range []string{"metrics-raw", "logs-../evil", "logs bad"} {
		if _, err := e.LandLogWindow("team-a", a, strings.NewReader("x")); err == nil {
			t.Errorf("artifact %q: want error, got nil", a)
		}
	}
}

func TestLandLogWindowInvalidTenant(t *testing.T) {
	_, e := newLogEngine(t)
	if _, err := e.LandLogWindow("BAD TENANT", "logs-summary", strings.NewReader("x")); err == nil {
		t.Fatal("invalid tenant: want error, got nil")
	}
}
