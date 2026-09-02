package promote

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/prism-utils/prism/internal/store/layout"
)

// CountTemps reports leftover promote temps for one tenant on both roots.
func CountTemps(dataDir, coldDir, tenant string, maxTier int) int {
	if maxTier <= 0 {
		maxTier = 8
	}
	n := countRoot(dataDir, tenant, maxTier)
	if layout.ColdEnabled(coldDir) {
		n += countRoot(coldDir, tenant, maxTier)
	}
	return n
}

func countRoot(root, tenant string, maxTier int) int {
	n := 0
	for tier := 0; tier <= maxTier; tier++ {
		n += countDir(layout.TierDir(root, tenant, tier))
	}
	logsRoot := filepath.Join(root, tenant, "logs")
	entries, err := os.ReadDir(logsRoot)
	if err != nil {
		return n
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		for tier := 0; tier <= maxTier; tier++ {
			n += countDir(layout.LogsTierDir(root, tenant, e.Name(), tier))
		}
	}
	return n
}

func countDir(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && layout.IsPromoteTemp(e.Name()) {
			n++
		}
	}
	return n
}

// GCTenant removes unfinished promote temps for one tenant on both roots.
func GCTenant(dataDir, coldDir, tenant string, maxTier int) error {
	if maxTier <= 0 {
		maxTier = 8
	}
	if err := gcRoot(dataDir, tenant, maxTier); err != nil {
		return err
	}
	if layout.ColdEnabled(coldDir) {
		if err := gcRoot(coldDir, tenant, maxTier); err != nil {
			return err
		}
	}
	return nil
}

func gcRoot(root, tenant string, maxTier int) error {
	for tier := 0; tier <= maxTier; tier++ {
		if err := gcDir(layout.TierDir(root, tenant, tier)); err != nil {
			return err
		}
	}
	logsRoot := filepath.Join(root, tenant, "logs")
	entries, err := os.ReadDir(logsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) < 5 {
			continue
		}
		for tier := 0; tier <= maxTier; tier++ {
			if err := gcDir(layout.LogsTierDir(root, tenant, e.Name(), tier)); err != nil {
				return err
			}
		}
	}
	return nil
}

func gcDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !layout.IsPromoteTemp(e.Name()) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("promote: gc temp: %w", err)
		}
	}
	return nil
}
