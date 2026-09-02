package stats

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const meteringFile = ".metering.json"

type meteringFileData struct {
	CompactionCPUSeconds float64 `json:"compactionCpuSeconds"`
}

// TenantOnDiskBytes sums every regular file under the tenant root, including
// landing logs, temps, sidecars, and leftover metrics-raw.
func TenantOnDiskBytes(dataDir, tenant string) (int64, error) {
	return dirBytes(filepath.Join(dataDir, tenant))
}

// TenantOnDiskBytesRoots adds cold-root tenant bytes when coldDir is set.
func TenantOnDiskBytesRoots(dataDir, coldDir, tenant string) (int64, error) {
	hot, err := TenantOnDiskBytes(dataDir, tenant)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(coldDir) == "" {
		return hot, nil
	}
	cold, err := dirBytes(filepath.Join(coldDir, tenant))
	if err != nil {
		return 0, err
	}
	return hot + cold, nil
}

// CompactionCPUSeconds returns cumulative compaction CPU-seconds for a tenant.
func CompactionCPUSeconds(dataDir, tenant string) (float64, error) {
	data, err := readMetering(dataDir, tenant)
	if err != nil {
		return 0, err
	}
	return data.CompactionCPUSeconds, nil
}

// AddCompactionCPUSeconds increments the cumulative compaction CPU-seconds counter.
func AddCompactionCPUSeconds(dataDir, tenant string, seconds float64) error {
	if seconds <= 0 {
		return nil
	}
	data, err := readMetering(dataDir, tenant)
	if err != nil {
		return err
	}
	data.CompactionCPUSeconds += seconds
	return writeMetering(dataDir, tenant, data)
}

func readMetering(dataDir, tenant string) (meteringFileData, error) {
	path := filepath.Join(dataDir, tenant, meteringFile)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return meteringFileData{}, nil
		}
		return meteringFileData{}, err
	}
	var data meteringFileData
	if err := json.Unmarshal(b, &data); err != nil {
		return meteringFileData{}, fmt.Errorf("stats: parse %s: %w", path, err)
	}
	return data, nil
}

func writeMetering(dataDir, tenant string, data meteringFileData) error {
	if err := os.MkdirAll(filepath.Join(dataDir, tenant), 0o750); err != nil {
		return err
	}
	path := filepath.Join(dataDir, tenant, meteringFile)
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o640); err != nil { //nolint:gosec // G306: 0640 is intentional group-readable metering sidecar permission.
		return err
	}
	return os.Rename(tmp, path)
}

func dirBytes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return total, nil
}
