package logmeta

import (
	"path/filepath"
	"sync"
	"testing"
)

func TestBumpConcurrent(t *testing.T) {
	dir := t.TempDir()
	const tenant = "user-6f3a9c2b-apps"
	const goroutines = 64
	const bumpsEach = 100

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < bumpsEach; j++ {
				if err := Bump(dir, tenant); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("Bump: %v", err)
	}

	got, err := Read(dir, tenant)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := uint64(goroutines * bumpsEach)
	if got != want {
		t.Fatalf("generation = %d, want %d (monotonic under concurrency)", got, want)
	}
	// Shared fixed tmp must not linger after successful bumps.
	if _, err := filepath.Glob(filepath.Join(dir, tenant, "logs", ".meta_generation*.tmp*")); err != nil {
		t.Fatalf("glob: %v", err)
	}
}
