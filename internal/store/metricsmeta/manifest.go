// Package metricsmeta is the per-tenant catalog of metrics hot/tier segments.
// Query planners read min/max timestamps from this manifest so they can skip
// files whose range cannot overlap a query window without opening parquet.
package metricsmeta

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/prism-utils/prism/internal/store/layout"
)

const (
	manifestName   = "_manifest.json"
	generationFile = ".meta_generation"
	hotRelParquet  = "hot/current.parquet"
	hotRelDuckDB   = "hot/current.duckdb"
)

// ManifestFile describes one on-disk metrics segment relative to the tenant root.
type ManifestFile struct {
	Path    string `json:"path"`
	MinTsNs int64  `json:"min_ts_ns"`
	MaxTsNs int64  `json:"max_ts_ns"`
	Bytes   int64  `json:"bytes"`
}

// Manifest is the tenant-level open-set catalog written on flush/merge/retention.
type Manifest struct {
	Version uint64         `json:"version"`
	Files   []ManifestFile `json:"files"`
}

// ManifestPath returns the metrics catalog path for one tenant.
func ManifestPath(dataDir, tenant string) string {
	return filepath.Join(dataDir, tenant, "tiers", manifestName)
}

// WriteManifest atomically writes a metrics catalog.
func WriteManifest(dataDir, tenant string, m Manifest) error {
	path := ManifestPath(dataDir, tenant)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadManifest loads a catalog; a missing file yields an empty catalog.
func ReadManifest(dataDir, tenant string) (Manifest, error) {
	path := ManifestPath(dataDir, tenant)
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is dataDir/tenant/tiers/_manifest.json
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, nil
		}
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Manifest{}, fmt.Errorf("metricsmeta: parse manifest: %w", err)
	}
	return m, nil
}

var bumpLocks sync.Map

func bumpLock(dataDir, tenant string) *sync.Mutex {
	key := filepath.Join(dataDir, tenant)
	v, _ := bumpLocks.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// Bump increments the metrics catalog generation so planners rescan.
func Bump(dataDir, tenant string) error {
	mu := bumpLock(dataDir, tenant)
	mu.Lock()
	defer mu.Unlock()

	dir := filepath.Join(dataDir, tenant, "tiers")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	dest := filepath.Join(dir, generationFile)
	cur, _ := ReadGeneration(dataDir, tenant)
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
	return os.Rename(tmpPath, dest)
}

// ReadGeneration returns the current stamp (0 when missing).
func ReadGeneration(dataDir, tenant string) (uint64, error) {
	path := filepath.Join(dataDir, tenant, "tiers", generationFile)
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is dataDir/tenant/tiers/.meta_generation
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("metricsmeta: parse generation: %w", err)
	}
	return v, nil
}

func isSegmentName(name string) bool {
	switch filepath.Ext(name) {
	case ".parquet", ".duckdb":
		return true
	default:
		return false
	}
}

// RebuildManifest scans hot + live tiers and records min/max ts per file.
// Files whose bounds cannot be read are omitted (fail closed).
func RebuildManifest(ctx context.Context, dataDir, tenant string, version uint64) (Manifest, error) {
	return RebuildManifestRoots(ctx, dataDir, "", tenant, version)
}

// RebuildManifestRoots also lists compacted L1+ that already live on coldDir.
func RebuildManifestRoots(ctx context.Context, dataDir, coldDir, tenant string, version uint64) (Manifest, error) {
	tenantRoot := filepath.Join(dataDir, tenant)
	var files []ManifestFile
	seen := map[string]struct{}{}
	add := func(abs, rel string) error {
		if _, dup := seen[rel]; dup {
			return nil
		}
		fi, err := os.Stat(abs)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		minNs, maxNs, ok := FileBounds(ctx, abs)
		if !ok {
			return nil
		}
		seen[rel] = struct{}{}
		files = append(files, ManifestFile{
			Path:    filepath.ToSlash(rel),
			MinTsNs: minNs,
			MaxTsNs: maxNs,
			Bytes:   fi.Size(),
		})
		return nil
	}

	for _, rel := range []string{hotRelParquet, hotRelDuckDB} {
		if err := add(filepath.Join(tenantRoot, filepath.FromSlash(rel)), rel); err != nil {
			return Manifest{}, err
		}
	}

	scanTiers := func(root string, minTier int) error {
		for tier := minTier; tier <= 8; tier++ {
			dir := layout.TierDir(root, tenant, tier)
			entries, err := os.ReadDir(dir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return err
			}
			retired := layout.CompactedSet(entries)
			for _, e := range entries {
				if e.IsDir() || !isSegmentName(e.Name()) || e.Name()[0] == '.' {
					continue
				}
				if _, held := retired[e.Name()]; held {
					continue
				}
				rel := filepath.ToSlash(filepath.Join("tiers", fmt.Sprintf("L%d", tier), e.Name()))
				if err := add(filepath.Join(dir, e.Name()), rel); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := scanTiers(dataDir, 0); err != nil {
		return Manifest{}, err
	}
	if layout.ColdEnabled(coldDir) {
		if err := scanTiers(coldDir, 1); err != nil {
			return Manifest{}, err
		}
	}
	return Manifest{Version: version, Files: files}, nil
}

// SyncManifest rebuilds and writes the catalog at the current generation.
func SyncManifest(ctx context.Context, dataDir, tenant string) error {
	gen, err := ReadGeneration(dataDir, tenant)
	if err != nil {
		return err
	}
	m, err := RebuildManifest(ctx, dataDir, tenant, gen)
	if err != nil {
		return err
	}
	return WriteManifest(dataDir, tenant, m)
}

// SyncAfterChange bumps the generation then rewrites the catalog.
func SyncAfterChange(ctx context.Context, dataDir, tenant string) error {
	return SyncAfterChangeRoots(ctx, dataDir, "", tenant)
}

// SyncAfterChangeRoots rebuilds the catalog across hot and cold roots.
func SyncAfterChangeRoots(ctx context.Context, dataDir, coldDir, tenant string) error {
	if err := Bump(dataDir, tenant); err != nil {
		return err
	}
	gen, err := ReadGeneration(dataDir, tenant)
	if err != nil {
		return err
	}
	m, err := RebuildManifestRoots(ctx, dataDir, coldDir, tenant, gen)
	if err != nil {
		return err
	}
	return WriteManifest(dataDir, tenant, m)
}
