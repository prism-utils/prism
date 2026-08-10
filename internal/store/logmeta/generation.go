package logmeta

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const generationFile = ".meta_generation"

// bumpLocks serializes Bump per dataDir/tenant so concurrent lands cannot
// race on a shared tmp name (ENOENT on rename) or lose increments.
var bumpLocks sync.Map // key: filepath.Join(dataDir, tenant) → *sync.Mutex

func bumpLock(dataDir, tenant string) *sync.Mutex {
	key := filepath.Join(dataDir, tenant)
	v, _ := bumpLocks.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// Bump increments the logs metadata generation stamp for a tenant so catalog
// caches rescan after land, merge, or retention.
func Bump(dataDir, tenant string) error {
	mu := bumpLock(dataDir, tenant)
	mu.Lock()
	defer mu.Unlock()

	dir := filepath.Join(dataDir, tenant, "logs")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	path := filepath.Join(dir, generationFile)
	cur, _ := Read(dataDir, tenant)
	next := cur + 1

	tmp, err := os.CreateTemp(dir, generationFile+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.WriteString(strconv.FormatUint(next, 10)); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// Read returns the current generation stamp (0 when missing).
func Read(dataDir, tenant string) (uint64, error) {
	path := filepath.Join(dataDir, tenant, "logs", generationFile)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	v, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("logmeta: parse generation: %w", err)
	}
	return v, nil
}
