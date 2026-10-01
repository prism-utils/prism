package gc

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
)

const minGrace = 120 * time.Second

func applyGrace(d time.Duration) time.Duration {
	if d <= 0 {
		return minGrace
	}
	return d
}

func stale(mtime, now time.Time, grace time.Duration) bool {
	return mtime.Before(now.Add(-grace))
}

func removeScratch(path string) error {
	if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("gc: remove: %w", err)
	}
	return nil
}

// HotDir deletes allowlisted scratch in the tenant hot directory whose
// modification time is strictly older than grace. A missing or empty
// directory is a no-op. Grace of zero is treated as two minutes.
func HotDir(dataDir, tenant string, now time.Time, grace time.Duration) error {
	dir := filepath.Join(dataDir, tenant, "hot")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("gc: hot dir: %w", err)
	}
	grace = applyGrace(grace)
	for _, e := range entries {
		name := e.Name()
		if !layout.IsHotScratch(name) {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := e.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("gc: stat: %w", err)
		}
		if !stale(info.ModTime(), now, grace) {
			continue
		}
		if err := removeScratch(path); err != nil {
			return err
		}
	}
	return nil
}

// EngineSpill deletes the tenant engine spill path when no handle is open and
// the path is strictly older than grace. Live catalog files are left alone.
// A missing path is a no-op. Grace of zero is treated as two minutes.
func EngineSpill(dataDir, tenant string, now time.Time, grace time.Duration, open bool) error {
	if open {
		return nil
	}
	path := filepath.Join(dataDir, tenant, "engine.duckdb.tmp")
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("gc: engine spill: %w", err)
	}
	if !layout.IsEngineScratch(info.Name()) {
		return nil
	}
	if !stale(info.ModTime(), now, applyGrace(grace)) {
		return nil
	}
	return removeScratch(path)
}

// Tenant runs hot-directory and engine-spill reclaim for one tenant.
func Tenant(dataDir, tenant string, now time.Time, grace time.Duration, open bool) error {
	if err := HotDir(dataDir, tenant, now, grace); err != nil {
		return err
	}
	return EngineSpill(dataDir, tenant, now, grace, open)
}
