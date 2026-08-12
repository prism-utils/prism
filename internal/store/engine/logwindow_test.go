package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/logmeta"
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
	for _, a := range []string{"metrics-raw", "logs-../evil", "logs bad", "logs-a/b", "logs-a.b"} {
		if _, err := e.LandLogWindow("team-a", a, strings.NewReader("x")); err == nil {
			t.Errorf("artifact %q: want error, got nil", a)
		}
	}
}

// Hyphenated log artifacts pass the shared artifact regex and must land (not
// 500) — the engine guard admits the same safe charset the ingest router uses.
func TestLandLogWindowAcceptsHyphenatedArtifact(t *testing.T) {
	dir, e := newLogEngine(t)
	if _, err := e.LandLogWindow("team-a", "logs-app-json", strings.NewReader("bytes")); err != nil {
		t.Fatalf("hyphenated log artifact: %v", err)
	}
	glob := filepath.Join(dir, "team-a", "logs", "logs-app-json", "*.parquet")
	if m, _ := filepath.Glob(glob); len(m) != 1 {
		t.Fatalf("want 1 landed file, got %v", m)
	}
}

func TestLandLogWindowInvalidTenant(t *testing.T) {
	_, e := newLogEngine(t)
	if _, err := e.LandLogWindow("BAD TENANT", "logs-summary", strings.NewReader("x")); err == nil {
		t.Fatal("invalid tenant: want error, got nil")
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestLandLogWindowClientAbort(t *testing.T) {
	_, e := newLogEngine(t)
	_, err := e.LandLogWindow("team-a", "logs-summary", errReader{err: io.ErrUnexpectedEOF})
	if !errors.Is(err, ErrClientAbort) {
		t.Fatalf("err = %v, want ErrClientAbort", err)
	}
}

// Concurrent lands for one tenant/artifact drive finishLogLand's generation
// bump + manifest sync + label-index carry from multiple goroutines at once.
// Without a per-tenant serialize around that sequence, SyncManifest's shared
// ".tmp" write races the same way the original Bump bug did (ENOENT on
// rename); this proves every land finishes without error and the generation
// stamp accounts for every one of them exactly once.
func TestLandLogWindowSerializesFinalizePerTenant(t *testing.T) {
	dir, e := newLogEngine(t)
	const tenant = "team-concurrent-land"
	const n = 24

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("PAR1-window-%02d", i))
			if _, err := e.LandLogWindow(tenant, "logs-summary", bytes.NewReader(payload)); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent LandLogWindow: %v", err)
	}

	gen, err := logmeta.Read(dir, tenant)
	if err != nil {
		t.Fatalf("Read generation: %v", err)
	}
	if gen != uint64(n) {
		t.Fatalf("generation = %d, want %d (each concurrent land must finalize exactly once)", gen, n)
	}

	glob := filepath.Join(dir, tenant, "logs", "logs-summary", "*.parquet")
	m, _ := filepath.Glob(glob)
	if len(m) != n {
		t.Fatalf("landed files = %d, want %d", len(m), n)
	}
}
