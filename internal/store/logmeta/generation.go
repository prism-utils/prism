package logmeta

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const generationFile = ".meta_generation"

// Bump increments the logs metadata generation stamp for a tenant so catalog
// caches rescan after land, merge, or retention.
func Bump(dataDir, tenant string) error {
	dir := filepath.Join(dataDir, tenant, "logs")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	path := filepath.Join(dir, generationFile)
	cur, _ := Read(dataDir, tenant)
	next := cur + 1
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(next, 10)), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
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
