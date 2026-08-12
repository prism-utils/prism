package file

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/data"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// nxadm/tail starts a process-wide fsnotify tracker that outlives tests
	// (inotify on Linux, kqueue on macOS).
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/nxadm/tail/watch.(*InotifyTracker).run"),
		goleak.IgnoreAnyFunction("github.com/fsnotify/fsnotify.(*inotify).readEvents"),
		goleak.IgnoreAnyFunction("github.com/fsnotify/fsnotify.(*kqueue).readEvents"),
		goleak.IgnoreAnyFunction("github.com/fsnotify/fsnotify.(*kqueue).read"),
	)
}

func TestModeTailSeekInfoUsesSeekEnd(t *testing.T) {
	info := modeTailSeekInfo()
	if info == nil {
		t.Fatal("modeTailSeekInfo returned nil")
	}
	if info.Whence != io.SeekEnd {
		t.Fatalf("ModeTail Whence = %d, want io.SeekEnd (%d)", info.Whence, io.SeekEnd)
	}
	if info.Whence == io.SeekStart {
		t.Fatal("ModeTail must not use io.SeekStart")
	}
	if info.Offset != 0 {
		t.Fatalf("ModeTail Offset = %d, want 0 (EOF)", info.Offset)
	}
}

func TestModeTailSkipsExistingContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("old-line\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := &Input{
		cfg:     Config{Path: path, Mode: ModeTail, BatchSize: 1},
		batches: make(chan data.RawBatch, 8),
	}
	if err := in.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = in.Shutdown(context.Background()) }()

	// Existing content must not be emitted (SeekEnd). Give the tailer a moment
	// to open and settle before asserting emptiness and appending.
	select {
	case b := <-in.Batches():
		t.Fatalf("unexpected batch from pre-existing content: %q", b.Records)
	case <-time.After(200 * time.Millisecond):
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // test-owned path
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("new-line\n"); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case b := <-in.Batches():
		if len(b.Records) != 1 || string(b.Records[0]) != "new-line" {
			t.Fatalf("got %#v, want [new-line]", b.Records)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for appended line")
	}

	cancel()
	for range in.Batches() {
	}
}
